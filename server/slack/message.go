package slack

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/loopai-hq/agent-sessions/internal/normalize"
)

// Summary is everything the mirror knows about a session, and it is the whole
// of what may be said about one in Slack.
//
// The fields that are not here are the point. sessions carries first_prompt and
// cwd, and a first prompt is the obvious thing to title a message with — it is
// what the dashboard's own list shows. It is also the person's own words, and
// once those words are in a channel they are outside every control this system
// has: the dashboard decides who may read a transcript, writes an audit row for
// every read of somebody else's, and can expire the body under a retention
// policy. A Slack message is subject to none of that. It is readable by whoever
// is in the channel now, whoever joins later, and every export of the workspace,
// with no record that anybody read it.
//
// So a mirror message is metadata: what was worked on, how much of it there was,
// and a link to the place where permission and audit still apply. Anybody who
// wants the content follows the link and is checked and recorded on the way in.
// Adding a field to this struct is the moment to re-read this paragraph.
type Summary struct {
	SessionID string
	Email     string
	Repo      string
	Branch    string

	StartedAt time.Time
	EndedAt   time.Time
	// Ended distinguishes a session we watched finish from one that simply
	// stopped producing events, exactly as internal/event does, and the message
	// says which. Reporting a crashed session as "finished" would misreport the
	// messy sessions this system exists to capture, in the one surface somebody
	// reads without opening the dashboard.
	Ended bool

	UserTurns int
	ToolCalls int
	Subagents int
	Errors    int
	CostUSD   float64
}

// render composes the message for one session.
//
// Three lines and a link, in Slack's mrkdwn. Not Block Kit: blocks would let
// this grow into a card with a transcript excerpt in it, and the format is the
// cheapest place to hold that line. A text message also degrades honestly — it
// is what a notification preview shows, what a search result shows, and what
// somebody sees on a watch.
func render(s Summary, publicURL string, mode Mode) string {
	var b strings.Builder

	// In a DM the audience is the person whose session it is, so naming them
	// reads as a form letter. In a channel the name is the first thing anybody
	// needs, because the whole point of the channel is that it carries several
	// people's work.
	if mode == ModeChannel {
		fmt.Fprintf(&b, "*%s* %s", escape(s.Email), verb(s.Ended))
	} else {
		fmt.Fprintf(&b, "Your session %s", verb(s.Ended))
	}
	if where := place(s); where != "" {
		b.WriteString(" " + where)
	}
	b.WriteString("\n")
	b.WriteString(stats(s))
	fmt.Fprintf(&b, "\n<%s/sessions/%s|Open in loop-sessions>", publicURL, s.SessionID)
	return b.String()
}

func verb(ended bool) string {
	if ended {
		return "finished"
	}
	// A session that stopped arriving is not a session that ended. The mirror
	// waits considerably longer before summarising one of these; see quietFor.
	return "went quiet"
}

// place names the work without quoting any of it. Repo and branch come from the
// agent's own git detection rather than from anything a person typed.
func place(s Summary) string {
	repo, branch := clip(s.Repo, 80), clip(s.Branch, 80)
	switch {
	case repo != "" && branch != "":
		return fmt.Sprintf("in `%s` on `%s`", escape(repo), escape(branch))
	case repo != "":
		return fmt.Sprintf("in `%s`", escape(repo))
	case branch != "":
		return fmt.Sprintf("on `%s`", escape(branch))
	}
	return ""
}

// stats is the counts line, with every zero left out.
//
// A line reading "0 subagents, 0 errors, $0.00" says nothing three times and
// makes the numbers that do matter harder to find. Errors are the exception
// worth stating loudly when present, because a session with errors is the one
// somebody might want to look at.
func stats(s Summary) string {
	parts := make([]string, 0, 6)
	parts = append(parts, plural(s.UserTurns, "turn", "turns"))
	if s.ToolCalls > 0 {
		parts = append(parts, plural(s.ToolCalls, "tool call", "tool calls"))
	}
	if s.Subagents > 0 {
		parts = append(parts, plural(s.Subagents, "subagent", "subagents"))
	}
	if s.Errors > 0 {
		parts = append(parts, plural(s.Errors, "error", "errors"))
	}
	if d := duration(s); d != "" {
		parts = append(parts, d)
	}
	// Cost is omitted at zero rather than printed as $0.00, because zero is what
	// an unpriced model produces as well as what a free session does, and the
	// two are indistinguishable in the number.
	if s.CostUSD > 0 {
		parts = append(parts, fmt.Sprintf("$%.2f", s.CostUSD))
	}
	return strings.Join(parts, " · ")
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// duration is how long the session ran, in the coarsest unit that is still
// true. A session is interesting at minute resolution; seconds of precision on
// an hour-long session is noise.
func duration(s Summary) string {
	if s.EndedAt.IsZero() || !s.EndedAt.After(s.StartedAt) {
		return ""
	}
	d := s.EndedAt.Sub(s.StartedAt)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// escape neutralises the three characters Slack reads as markup in a text
// field.
//
// Applied to every value that came from outside this file, which today is a
// branch name, a repository name and an address. None of the three is likely to
// carry one of these, and that is exactly why the escaping has to be structural
// rather than a judgement made per field: a branch called `a<b` would otherwise
// produce a message that silently renders as something else.
func escape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

// clip bounds a value that arrives from a laptop. A repository name is short in
// every real case; a bound is what keeps the one pathological case from
// becoming a message Slack rejects for length.
//
// Cut on a rune boundary rather than a byte one. A branch name carrying any
// non-ASCII character — an accented name, a CJK word, an emoji — would
// otherwise be sliced through the middle of a rune, and the message would carry
// invalid UTF-8 into a channel and render as a replacement character. The bound
// is on bytes because that is what Slack's own limit counts.
func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// renderCapNotice is the single message that explains a silence.
//
// The mirror stops posting for somebody who has hit the daily ceiling, and a
// feed that simply goes quiet is indistinguishable from a feed that broke. One
// line, once a day, is what makes the difference visible without becoming part
// of the noise it is reporting.
func renderCapNotice(publicURL string, ceiling int, mode Mode) string {
	who := "You have"
	if mode == ModeChannel {
		who = "This channel has"
	}
	return fmt.Sprintf(
		"%s reached the mirror's ceiling of %d session summaries in a day, so the rest of today's sessions will not be posted here. Everything is still captured: <%s/sessions|open the dashboard>.",
		who, ceiling, publicURL)
}

// ---------------------------------------------------------------- live thread

// liveRootText opens a thread the way the workspace already reads them: the
// Cosmo/Devin convention of a root that names the session and links back.
func liveRootText(c liveCandidate, publicURL string) string {
	link := publicURL + "/sessions/" + c.SessionID
	head := fmt.Sprintf(":thread: *%s* started a live session", escape(c.Email))
	if p := place(Summary{Repo: c.Repo, Branch: c.Branch}); p != "" {
		head += " " + p
	}
	return head + fmt.Sprintf(" — <%s|open in dashboard>", link)
}

// liveRootBlocks renders the root as Block Kit when the interactive endpoint
// is on: the same text plus a Stop button whose value names the thread.
func liveRootBlocks(text, sessionID string, groupID int64) string {
	b := []map[string]any{
		{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": text}},
		{"type": "actions", "elements": []map[string]any{{
			"type":      "button",
			"action_id": "stop_mirror",
			"text":      map[string]any{"type": "plain_text", "text": "Stop mirroring"},
			"value":     fmt.Sprintf("%s|%d", sessionID, groupID),
		}}},
	}
	j, _ := json.Marshal(b)
	return string(j)
}

// liveHeaderText is the root's live-edited body: the state a passerby needs
// without opening the thread.
func liveHeaderText(c liveCandidate, publicURL string, status string) string {
	link := publicURL + "/sessions/" + c.SessionID
	head := fmt.Sprintf("%s *%s* — live session", status, escape(c.Email))
	if p := place(Summary{Repo: c.Repo, Branch: c.Branch}); p != "" {
		head += " " + p
	}
	body := plural(c.UserTurns, "turn", "turns") + " · " + plural(c.ToolCalls, "tool call", "tool calls")
	if c.Errors > 0 {
		body += fmt.Sprintf(" · %d failed", c.Errors)
	}
	if c.CostUSD > 0 {
		body += fmt.Sprintf(" · $%.2f", c.CostUSD)
	}
	return head + "\n" + body + fmt.Sprintf("\n<%s|open in dashboard>", link)
}

// liveTurnText renders one exchange the way a conversation reads: who asked,
// what the agent answered. The prompt is quoted and clipped hard; the answer
// gets the room, because the answer is what a channel bystander wants. The
// standing digest never posts content; this thread does, because attaching
// is explicit consent for exactly this session — see the package doc.
func liveTurnText(t liveTurn, email string) string {
	who := email
	if i := strings.IndexByte(who, '@'); i > 0 {
		who = who[:i]
	}
	prompt := clip(strings.TrimSpace(strings.ReplaceAll(promptWords(t.Text), "\n", " ")), 280)
	if prompt == "" {
		prompt = "(empty prompt)"
	}
	msg := "*" + escape(who) + ":* " + escape(prompt)
	if reply := clip(strings.TrimSpace(t.Reply), 700); reply != "" {
		msg += "\n*agent:* " + escape(reply)
	}
	return msg
}

// promptWords is what a person typed, as the one normalizer reads it: a
// slash command as "/name args", a prompt with its attached harness blocks
// removed, anything else as it is. The turn's kind was decided at ingest;
// this only extracts the display text for it.
func promptWords(text string) string {
	m := normalize.ClassifyUser(text, nil, false, false)
	if m.Kind == normalize.KindSlashCommand {
		return strings.TrimSpace(m.Command + " " + m.Args)
	}
	if normalize.IsHumanKind(m.Kind) && m.Text != "" {
		return m.Text
	}
	return text
}

// liveMilestoneText names the thing worth its own reply.
func liveMilestoneText(m liveMilestone) string {
	switch m.Kind {
	case "pr":
		return ":rocket: opened <" + m.About + ">"
	default:
		return ":page_facing_up: wrote `" + escape(clip(m.About, 120)) + "`"
	}
}
