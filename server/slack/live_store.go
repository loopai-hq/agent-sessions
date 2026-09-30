package slack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// The live thread's reads. Same DB port, same ledger, different keys: where
// the digest says one thing per finished session, the live thread says many
// small things per running one, and every one of them is idempotent through
// the same claims table.

// threadPrefix is one thread's key namespace in the ledger. The legacy env
// flow keys by session alone; group fan-out keys by (session, group) so two
// destinations narrate the same session independently.
func threadPrefix(sessionID string) string { return "thread:" + sessionID }

func groupThreadPrefix(sessionID string, groupID int64) string {
	return "thread:" + sessionID + ":g" + strconv.FormatInt(groupID, 10)
}

// liveKey is the root message's deterministic id within a prefix.
func liveKey(prefix string) string { return prefix }

// liveTurnKey is one exchange's reply. Keyed by the seq of the turn's
// canonical prompt event, which is what the key was before the derive layer
// existed: a prompt event's seq within its stream, stable across
// re-delivery. Keeping the shape is what lets the slack_posts rows written
// under the old query stay valid, so a deploy does not re-post every thread's
// history; the anti-join in liveTurns also honours a key written under any
// other copy of the same prompt (the hook copy of a dual-origin turn).
func liveTurnKey(prefix string, seq int64) string {
	return prefix + ":turn:" + strconv.FormatInt(seq, 10)
}

// liveMilestoneKey is one milestone's reply, keyed by what it is about (a PR
// url, an artifact path) so a milestone is said once however many events
// mention it.
func liveMilestoneKey(prefix, about string) string {
	sum := sha256.Sum256([]byte(about))
	return prefix + ":m:" + hex.EncodeToString(sum[:8])
}

// liveCloseKey is the thread's one closing message.
func liveCloseKey(prefix string) string { return prefix + ":close" }

// liveCandidate is a session somebody asked to watch live, with the standing
// preference that decides whether the ask is honoured and where it lands.
type liveCandidate struct {
	SessionID string
	Email     string
	Repo      string
	Branch    string
	StartedAt time.Time
	UpdatedAt time.Time
	Ended     bool
	UserTurns int
	ToolCalls int
	Errors    int
	CostUSD   float64

	// Request is the session's own ask: "on", "dm", or a channel id.
	Request string
	// The owner's standing preference. Mode off means the request is refused —
	// an environment variable is not consent.
	Mode        Mode
	Channel     string
	SlackUserID string

	// RootTS and RootChannel are set when the thread already exists.
	RootTS      string
	RootChannel string
	// Closed reports the closing message has been posted.
	Closed bool
}

// liveEligible reports the sessions with a live ask whose owner's mirror is
// on, newest first, excluding those already closed.
//
// The freshness floor keeps a re-walked backfill from resurrecting month-old
// sessions as live threads: the walker re-delivers SessionStarted events with
// the request stamped, and without the floor every one of them would open a
// thread about work long finished.
func liveEligible(ctx context.Context, db DB, freshAfter time.Time, limit int) ([]liveCandidate, error) {
	rows, err := db.Query(ctx, `
		SELECT s.session_id, s.email, coalesce(s.repo, ''), coalesce(s.git_branch, ''),
		       s.started_at, s.updated_at, s.ended,
		       s.user_turns, s.tool_calls, s.errors, s.cost_usd::float8,
		       s.mirror_request,
		       p.mode, coalesce(p.channel, ''), coalesce(p.slack_user_id, ''),
		       coalesce(root.message_ts, ''), coalesce(root.channel, ''),
		       (closed.posted_at IS NOT NULL)
		  FROM sessions s
		  JOIN slack_prefs p ON p.email = s.email AND p.mode <> 'off'
		  LEFT JOIN slack_posts root   ON root.key   = 'thread:' || s.session_id
		  LEFT JOIN slack_posts closed ON closed.key = 'thread:' || s.session_id || ':close'
		 WHERE s.mirror_request <> ''
		   -- Only the spellings this path can act on: the flag's original
		   -- vocabulary. Group NAMES are resolved into activations by the
		   -- promotion step and must never fall through to be used as a
		   -- channel id.
		   AND (s.mirror_request IN ('on', 'dm') OR s.mirror_request ~ '^[CG][A-Z0-9]{6,20}$')
		   -- A session with group activations narrates through them; the
		   -- legacy env path stands down rather than double-threading.
		   AND NOT EXISTS (SELECT 1 FROM session_mirrors sm
		                    WHERE sm.session_id = s.session_id AND sm.detached_at IS NULL)
		   AND s.parent_session_id IS NULL
		   -- The store's default view, restated as in the digest query.
		   AND NOT (s.session_type IN ('empty', 'internal') AND s.head_state <> 'capture_loss')
		   AND s.started_at >= $1
		   AND (closed.posted_at IS NULL)
		 ORDER BY s.started_at DESC
		 LIMIT $2`, freshAfter, limit)
	if err != nil {
		return nil, fmt.Errorf("slack: select live sessions: %w", err)
	}
	defer rows.Close()

	var out []liveCandidate
	for rows.Next() {
		var c liveCandidate
		if err := rows.Scan(
			&c.SessionID, &c.Email, &c.Repo, &c.Branch,
			&c.StartedAt, &c.UpdatedAt, &c.Ended,
			&c.UserTurns, &c.ToolCalls, &c.Errors, &c.CostUSD,
			&c.Request, &c.Mode, &c.Channel, &c.SlackUserID,
			&c.RootTS, &c.RootChannel, &c.Closed,
		); err != nil {
			return nil, fmt.Errorf("slack: read a live session: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// liveTurn is one completed exchange worth a reply: what the person asked
// and what the agent finally said back. The thread is a conversation, so its
// unit is the exchange — a Devin thread reads this way, and the first
// shipped format (raw prompts plus a message per touched file) read as a
// dump and was thrown back within the hour.
type liveTurn struct {
	// Seq is the canonical prompt event's seq, the ledger key.
	Seq        int64
	OccurredAt time.Time
	// Kind is the prompt's kind (human or slash_command): the renderer spells
	// a command as the person spoke it.
	Kind string
	// Text is the prompt's own words, clipped by the renderer. The standing
	// digest never posts content; a live thread does, because the per-session
	// flag is explicit consent for exactly this session. The argument lives
	// in the package doc.
	Text string
	// Reply is the agent's final answer of the exchange, empty when the turn
	// ended without one.
	Reply string
}

// liveTurns reads a session's settled exchanges that have no reply in the
// ledger yet, from the derive layer's turns: the person's prompt is the
// turn's canonical prompt row, the reply is the row turns.final_event_id
// names, and a turn still in progress (the last one of a running session)
// waits until the fold closes it. The thread trails the conversation by one
// turn on purpose; legible beats instant.
//
// Only the kinds a person opens are exchanges. The harness's own records
// (notifications, caveats, summaries) attach to a turn as work under the
// fold, so no wrapper can ever be quoted as the person; the old query
// filtered two prefixes by hand and quoted the rest.
//
// The after horizon is the attachment time: attaching a ten-day session at
// noon must not replay ten days of history into the channel. The anti-join
// against the ledger replaces a cursor and checks every prompt copy the
// turn holds, so a thread that posted a turn under the hook copy's seq
// before the fold existed does not post it again under the transcript's.
func liveTurns(ctx context.Context, db DB, sessionID, prefix string, after time.Time, limit int) ([]liveTurn, error) {
	rows, err := db.Query(ctx, `
		SELECT p.seq, t.started_at, t.kind,
		       coalesce(p.body->>'text', ''),
		       coalesce(f.body->>'text', '')
		  FROM turns t
		  JOIN events p ON p.id = t.prompt_event_id
		  LEFT JOIN events f ON f.id = t.final_event_id
		 WHERE t.session_id = $1
		   AND t.thread = ''
		   AND t.kind IN ('human', 'slash_command')
		   AND t.outcome <> 'in_progress'
		   AND t.started_at >= $4
		   AND NOT EXISTS (
		         SELECT 1 FROM turn_events te
		           JOIN events c ON c.id = te.event_id
		           JOIN slack_posts o ON o.key = $3 || ':turn:' || c.seq
		          WHERE te.session_id = t.session_id AND te.thread = t.thread
		            AND te.turn_index = t.turn_index
		            AND c.type = 'user_prompt'
		            AND o.posted_at IS NOT NULL)
		 ORDER BY t.turn_index
		 LIMIT $2`, sessionID, limit, prefix, after)
	if err != nil {
		return nil, fmt.Errorf("slack: read live turns for %s: %w", sessionID, err)
	}
	defer rows.Close()

	var out []liveTurn
	for rows.Next() {
		var t liveTurn
		if err := rows.Scan(&t.Seq, &t.OccurredAt, &t.Kind, &t.Text, &t.Reply); err != nil {
			return nil, fmt.Errorf("slack: read a live turn: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// liveMilestone is one thing worth its own reply: a PR the session opened.
// File writes were once milestones too, and a real session buried its whole
// thread under "wrote <path>" sixty times — metadata is the dashboard's job;
// the thread carries the conversation and the deliverables.
type liveMilestone struct {
	Kind  string // "pr"
	About string // the url; also the dedup identity
}

// liveMilestones reads a session's unposted milestones from the derived
// tables the dashboard already maintains.
func liveMilestones(ctx context.Context, db DB, sessionID, prefix string, after time.Time, limit int) ([]liveMilestone, error) {
	rows, err := db.Query(ctx, `
		SELECT kind, about FROM (
			SELECT l.kind, l.url AS about, l.first_seen_at AS at
			  FROM links l
			 WHERE l.session_id = $1 AND l.kind = 'pr'
			   AND l.first_seen_at >= $4
		) m
		 WHERE NOT EXISTS (
		   SELECT 1 FROM slack_posts o
		    WHERE o.key = $3 || ':m:' || encode(substring(sha256(m.about::bytea) from 1 for 8), 'hex')
		      AND o.posted_at IS NOT NULL)
		 ORDER BY at
		 LIMIT $2`, sessionID, limit, prefix, after)
	if err != nil {
		return nil, fmt.Errorf("slack: read live milestones for %s: %w", sessionID, err)
	}
	defer rows.Close()

	var out []liveMilestone
	for rows.Next() {
		var m liveMilestone
		if err := rows.Scan(&m.Kind, &m.About); err != nil {
			return nil, fmt.Errorf("slack: read a live milestone: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
