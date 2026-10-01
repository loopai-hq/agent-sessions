package web

// The turns reader: one exchange at a time, as the runner folded it.
//
// Before this the page rebuilt turns from a window of events in the browser
// (conversation.js) and in a Go pass over the window, and each did it
// differently from the Slack mirror and the list counters. The fold lives in
// the runner now (server/store/derive); this file only arranges what it wrote
// for a page. Nothing here decides what a message is: the kind comes from the
// row, the role from turn_events, the outcome from the turn.

import (
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/normalize"
	"github.com/loopai-hq/loop-sessions/server/store/derive"
)

// TurnView is one turn prepared for the page.
type TurnView struct {
	Turn
	// Anchor is the element id: t<index> on the main thread, a<thread>-<index>
	// for a subagent's own turn.
	Anchor string
	// Head marks the fold's head turn: rows recorded before the first captured
	// prompt, which is what a session whose head was not imported opens with.
	Head bool

	// Prompt is the person's bubble (human or slash_command kinds only). It is
	// nil for a head turn and for a turn a wrapper opened.
	Prompt *Block
	// Command and Args render a slash command as a chip instead of a bubble.
	Command string
	Args    string
	// Context holds the harness's own records the fold attached to this turn
	// (caveats, command output, notifications, compaction summaries), each a
	// collapsed neutral row.
	Context []Block
	// Work is what the agent did, tool results paired onto their calls, in
	// time order.
	Work []Block
	// Final is the answer, when the outcome is answered (or an interruption
	// came after one).
	Final *Block
	// Agents are the subagent turns nested under this one.
	Agents []AgentView

	// Divider, when set, is drawn above the turn: the copied-prefix boundary
	// of a fork.
	Divider     string
	DividerLink string

	// Matched reports that a search term hit inside the turn's work, which
	// opens the collapsed sections.
	Matched bool
	// Elapsed is time since the session started, for the turn's own row.
	Elapsed time.Duration
}

// AgentView is a subagent's turn nested under the main turn that spawned it.
type AgentView struct {
	TurnView
	// Label names the agent: the descriptive middle of a transcript-derived
	// id, else a short form of the id.
	Label string
	// AgentType is what the harness called the agent when it started it, when
	// the start marker said.
	AgentType string
}

// HasWork reports whether the collapsed work section has anything to show.
func (v TurnView) HasWork() bool { return len(v.Work) > 0 || len(v.Agents) > 0 }

// WorkLabel is the collapsed section's summary: how long the agent was
// active and how long the turn sat idle, from the fold's own timings.
func (v TurnView) WorkLabel() string {
	if v.Active <= 0 && v.Idle <= 0 {
		return "Recorded work"
	}
	label := "Worked " + spanLabel(v.Active)
	if v.Idle > 0 {
		label += ", " + spanLabel(v.Idle) + " idle"
	}
	return label
}

// spanLabel renders a span for the work label, with seconds below a minute
// and no dash for zero.
func spanLabel(d time.Duration) string {
	if d < time.Second {
		return "under a second"
	}
	return durLabel(d)
}

// OutcomeLabel names the outcome the way the page says it.
func (v TurnView) OutcomeLabel() string {
	switch v.Outcome {
	case "answered":
		return "answered"
	case "interrupted":
		return "interrupted"
	case "no_answer_captured":
		return "answer not captured"
	case "no_work":
		return "no work"
	case "in_progress":
		return "in progress"
	}
	return v.Outcome
}

// TwoSources reports that both capture paths recorded this turn.
func (v TurnView) TwoSources() bool { return len(v.Origins) > 1 }

// AnswerAwaitsWalk separates the one reading of a missing answer that is not
// a defect from the one that is. Claude Code runs no Stop hook while
// background work keeps a turn open, so a long autonomous turn with
// subagents still running leaves the live path nothing to store and the
// transcript walk is what supplies the answer later. A reader told to
// upgrade a client in that case would be chasing a fault that is not there,
// so the page says so only for the exact shape: no answer captured,
// background work on the turn, and no transcript row on it yet.
func (v TurnView) AnswerAwaitsWalk() bool {
	return v.Outcome == "no_answer_captured" && v.heldOpen() && !v.hasOrigin(event.OriginTranscript)
}

// heldOpen reports the records that keep a turn open past the point the
// agent would otherwise have answered: the subagent threads the fold
// attached to it, the same threads nested onto it by the page, the start
// markers behind them, and a background task reporting back into it.
func (v TurnView) heldOpen() bool {
	if v.Subagents > 0 || len(v.Agents) > 0 {
		return true
	}
	for _, te := range v.Events {
		if te.Type == event.SubagentStart || normalize.Kind(te.Kind) == normalize.KindTaskNotification {
			return true
		}
	}
	return false
}

// hasOrigin reports whether any row of the turn came from the named capture
// path.
func (v TurnView) hasOrigin(o event.Origin) bool {
	for _, got := range v.Origins {
		if got == string(o) {
			return true
		}
	}
	return false
}

// buildTurnViews arranges a page of turns for the template: the main turns
// in order, each with its blocks, and every subagent turn nested under the
// main turn in progress when it started.
func buildTurnViews(page TurnPage, opts TranscriptOptions, parentID string) []TurnView {
	opts = opts.withDefaults()
	views := make([]TurnView, 0, len(page.Turns))
	for _, t := range page.Turns {
		views = append(views, buildTurnView(t, opts))
	}
	// Subagents attach by time: the last main turn started at or before the
	// agent's first row, which is the fold's own fallback rule
	// (derive.attachTarget) and the only one the page can reproduce.
	agentTypes := agentTypesOf(page)
	for _, a := range page.Agents {
		av := AgentView{TurnView: buildTurnView(a, opts)}
		av.Label = agentLabel(a, opts.Labels)
		av.AgentType = agentTypes[a.Thread]
		parent := -1
		for i := range views {
			if !views[i].StartedAt.After(a.StartedAt) {
				parent = i
			}
		}
		if parent < 0 && len(views) > 0 {
			parent = 0
		}
		if parent >= 0 {
			views[parent].Agents = append(views[parent].Agents, av)
			views[parent].Matched = views[parent].Matched || av.Matched
		}
	}
	// The fork boundary: the copied prefix is flagged inherited, and the
	// page says where the session's own work begins.
	for i := range views {
		switch {
		case i == 0 && views[i].Inherited:
			views[i].Divider = "Copied from"
			views[i].DividerLink = parentID
		case i > 0 && views[i-1].Inherited && !views[i].Inherited:
			views[i].Divider = "This session's own work begins"
		}
	}
	return views
}

// agentTypesOf reads what the harness called each subagent off the start
// markers on the page, keyed by canonical thread.
func agentTypesOf(page TurnPage) map[string]string {
	out := map[string]string{}
	for _, t := range page.Turns {
		for _, e := range t.Events {
			if e.Type != event.SubagentStart || e.AgentID == "" {
				continue
			}
			if line := firstLine(e.Text, 80); line != "" {
				out[derive.CanonicalThread(e.AgentID)] = line
			}
		}
	}
	return out
}

// agentLabel names a subagent's thread from the raw ids its rows carry.
func agentLabel(t Turn, labels map[string]string) string {
	var ids []string
	for _, e := range t.Events {
		if e.AgentID != "" && !containsType(ids, e.AgentID) {
			ids = append(ids, e.AgentID)
		}
	}
	if len(ids) == 0 {
		return shortID(t.Thread)
	}
	return threadLabel(ids, labels)
}

// buildTurnView arranges one turn's events into its slots.
func buildTurnView(t Turn, opts TranscriptOptions) TurnView {
	v := TurnView{Turn: t}
	if t.Thread == "" {
		v.Anchor = "t" + strconv.Itoa(t.Index)
	} else {
		v.Anchor = "a" + t.Thread + "-" + strconv.Itoa(t.Index)
	}
	v.Head = t.Kind == "" && t.PromptEventID == ""
	if !opts.Start.IsZero() && t.StartedAt.After(opts.Start) {
		v.Elapsed = t.StartedAt.Sub(opts.Start)
	}

	events := append([]TurnEvent(nil), t.Events...)
	sort.SliceStable(events, func(i, j int) bool {
		if !events[i].OccurredAt.Equal(events[j].OccurredAt) {
			return events[i].OccurredAt.Before(events[j].OccurredAt)
		}
		if events[i].AgentID != events[j].AgentID {
			return events[i].AgentID < events[j].AgentID
		}
		return events[i].Seq < events[j].Seq
	})

	work := &Group{}
	for _, te := range events {
		kind := normalize.Kind(te.Kind)
		b, ok := blockFor(te.Event, kind, opts)
		if !ok {
			continue
		}
		if !opts.Start.IsZero() && te.OccurredAt.After(opts.Start) {
			b.Elapsed = te.OccurredAt.Sub(opts.Start)
		}
		if opts.SessionID != "" && te.ID != "" {
			b.EventURL = "/sessions/" + url.PathEscape(opts.SessionID) + "/events/" + url.PathEscape(te.ID)
		}
		v.Matched = v.Matched || b.Matched()
		switch {
		case te.Role == "prompt" && te.ID == t.PromptEventID:
			if b.Kind == KindPrompt {
				prompt := b
				v.Prompt = &prompt
				if kind == normalize.KindSlashCommand {
					m := normalize.ClassifyUser(te.Text, nil, te.AgentID != "", false)
					v.Command, v.Args = m.Command, m.Args
					if v.Command == "" {
						v.Command = firstLine(te.Text, 80)
					}
				}
			} else {
				// A wrapper that opened a turn of its own kind (nothing to
				// fold it into): context, not a bubble.
				v.Context = append(v.Context, b)
			}
		case te.Role == "final" && te.ID == t.FinalEventID:
			final := b
			v.Final = &final
		case te.Type == event.UserPrompt:
			// Every other prompt row in the turn is the harness's: a caveat,
			// a notification, a summary, a copy that merged. A person's
			// second prompt folded into the same turn is shown as context too,
			// labelled as such, rather than as a second bubble the fold said
			// was one exchange.
			if b.Kind == KindPrompt {
				b.Kind = KindInternal
				b.Title = "Prompt (merged into this turn)"
			}
			v.Context = append(v.Context, b)
		default:
			if mergeToolResult(work, te.Event, b, opts) {
				continue
			}
			work.Blocks = append(work.Blocks, b)
		}
	}
	v.Work = work.Blocks
	return v
}

// kindTitle names a harness record for its collapsed row. The kind is the
// row's, decided at ingest; this is only how the page spells it.
func kindTitle(k normalize.Kind, source event.Source) string {
	switch k {
	case normalize.KindCaveat:
		return "Local command caveat"
	case normalize.KindCommandOutput:
		return "Local command output"
	case normalize.KindTaskNotification:
		return "Task notification"
	case normalize.KindSystemReminder:
		return "System reminder"
	case normalize.KindSystemNotification:
		return "System notification"
	case normalize.KindTeammateMessage:
		return "Teammate message"
	case normalize.KindInterrupted:
		return "Request interrupted"
	case normalize.KindIDEContext:
		return "IDE context"
	case normalize.KindCompactSummary:
		return "Context compacted"
	case normalize.KindEnvContext:
		if source == event.SourceCodex {
			return "Codex environment context"
		}
		return "Environment context"
	case normalize.KindHarnessInjected:
		return "Harness message"
	case normalize.KindSubagentTask:
		return "Task"
	}
	return strings.ReplaceAll(string(k), "_", " ")
}

// headBanner is what the page says above a session whose beginning is not
// on it.
type headBanner struct {
	// Kind is truncated_window, start_lost or capture_loss.
	Kind string
	// WindowStart is the first captured moment, for the truncation notice.
	WindowStart time.Time
	// Drops is the device's dropped-event count around the session, for the
	// capture-loss notice.
	Drops int64
	// Command is the CTA that imports the missing head.
	Command string
}

// headBannerFor decides the banner from the session's facet.
func headBannerFor(id string, f SessionFacet, firstTurn time.Time) *headBanner {
	switch f.HeadState {
	case "truncated_window", "start_lost":
		return &headBanner{Kind: f.HeadState, WindowStart: firstTurn, Command: "loop-sessions backfill --session " + id}
	case "capture_loss":
		return &headBanner{Kind: f.HeadState, Drops: f.CaptureLossDrops}
	}
	return nil
}

// parseTurnCursor reads the after parameter: a turn index, or nothing.
func parseTurnCursor(s string) *int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return nil
	}
	return &n
}
