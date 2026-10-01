package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/normalize"
)

// BlockKind is what a transcript row represents. The set is small on purpose:
// a reader scanning a long session is looking for "who said what and what did
// it do", and a taxonomy with a row type per event type would put that
// distinction behind twelve visual treatments instead of four.
type BlockKind string

const (
	// KindPrompt is something a person typed.
	KindPrompt BlockKind = "prompt"
	// KindInternal is a user-role row the harness wrote rather than a person
	// typed: a hook's output, a system reminder, an automation's prompt.
	KindInternal BlockKind = "internal"
	// KindAssistant is a model turn.
	KindAssistant BlockKind = "assistant"
	// KindTool is a tool invocation, its result, or both once paired.
	KindTool BlockKind = "tool"
	// KindNotice is a lifecycle marker: session boundaries, compaction,
	// subagent boundaries, artifacts. Rendered small because it is orientation
	// rather than content.
	KindNotice BlockKind = "notice"
)

// Block is one rendered row of a transcript.
type Block struct {
	EventID string
	Seq     int64
	Kind    BlockKind
	At      time.Time

	// Elapsed is time since the session started. The absolute clock time is
	// available on hover; the elapsed figure is what makes a transcript
	// readable, because the question a reader has is "how long did this take",
	// not "what time was it in the author's timezone".
	Elapsed time.Duration
	// GapBefore is the idle time between the previous block and this one, set
	// only when it crosses the threshold worth drawing. Long gaps are where
	// sessions actually go wrong, and a transcript that renders a four minute
	// stall identically to a 200 ms one hides the most useful signal it has.
	GapBefore time.Duration

	Actor               string
	Model               string
	Title               string
	NotificationSummary string

	Text          []Segment
	TextTruncated bool

	Tool  *ToolView
	Diffs []FileDiff

	Usage *event.Usage

	// EventURL points at this event's own page, which is where a reader goes
	// when a block was shortened for display. Precomputed rather than assembled
	// in the template, because a template that builds URLs from ids is a
	// template that has to be trusted to escape them.
	EventURL string

	// Failed marks a tool call that errored, which is the other thing readers
	// scan for.
	Failed bool
	// Origin distinguishes a live hook record from one reconstructed off a
	// transcript file. Backfilled rows are not less true, but they are less
	// complete, and a reader wondering where the tool output went deserves to
	// see why.
	Origin string
	// ToolUseID pairs a result with its call across origins; adjacency is the
	// fallback for rows stored before the harness's id was a column.
	ToolUseID string
}

// Anchor is the element id a block renders with: the event id, which is
// unique across a session's threads where seq is not.
func (b Block) Anchor() string {
	if b.EventID != "" {
		return "ev-" + b.EventID
	}
	return "e" + strconv.FormatInt(b.Seq, 10)
}

// ToolView is a tool call prepared for display.
type ToolView struct {
	Name string
	// Summary is the one line that says what this call actually did, pulled
	// from whichever input field the tool uses for its subject. Without it a
	// session of forty Bash calls renders as forty rows that all say "Bash".
	Summary string

	Input          []Segment
	InputTruncated bool

	Output          []Segment
	OutputTruncated bool
	// OutputElided reports that the client itself shipped a truncated result,
	// which is a different fact from this page shortening it for display.
	OutputElided bool

	Error string
}

// Group is a run of blocks belonging to the same actor thread: the main
// conversation, or one subagent.
//
// Nesting is derived from the agent id on each event rather than from a
// start/end stack, because the page renders a window of a paginated session and
// the start marker for the subagent a reader is looking at may be several pages
// back. A depth that depends on events outside the window would render the same
// events differently depending on where the page boundary fell.
type Group struct {
	AgentID    string
	WorkflowID string
	Label      string
	Blocks     []Block
}

// Subagent reports whether this group is a subagent thread rather than the
// main conversation.
func (g Group) Subagent() bool { return g.AgentID != "" }

// GroupStats is a subagent group's header summary.
type GroupStats struct {
	Blocks int
	Tools  int
	Errors int
	Span   time.Duration
}

// Stats summarises a group for its header, so a reader can decide whether to
// open a subagent without opening it. It returns a struct rather than several
// values because templates can only bind a single one.
func (g Group) Stats() GroupStats {
	st := GroupStats{Blocks: len(g.Blocks)}
	for _, b := range g.Blocks {
		if b.Tool != nil {
			st.Tools++
		}
		if b.Failed {
			st.Errors++
		}
	}
	if len(g.Blocks) > 1 {
		st.Span = g.Blocks[len(g.Blocks)-1].At.Sub(g.Blocks[0].At)
	}
	return st
}

// TranscriptOptions bound how much of an unbounded thing gets rendered.
//
// Every limit here exists because transcript content is attacker-shaped even
// when nobody is attacking: a single tool result can be tens of megabytes of
// minified JSON, and the page that renders it in full is the page nobody can
// open. The limits are per block rather than per page so one enormous result
// cannot consume the budget for everything after it.
type TranscriptOptions struct {
	// Start anchors the elapsed column.
	Start time.Time
	// Terms are search words to mark, empty on a normal read.
	Terms []string
	// MaxText caps a prompt or assistant turn.
	MaxText           int
	FullAssistantText bool
	// MaxToolInput caps the rendered arguments.
	MaxToolInput int
	// MaxToolOutput caps the rendered result.
	MaxToolOutput int
	// GapThreshold is the idle time worth drawing.
	GapThreshold time.Duration
	// SessionID is used to address each event's own page.
	SessionID string
	// Labels maps agent ids to human names, supplied by the detail rollup
	// because the label lives on a subagent_start event that is usually not in
	// the window being rendered.
	Labels map[string]string
}

func (o TranscriptOptions) withDefaults() TranscriptOptions {
	if o.MaxText <= 0 {
		o.MaxText = 16000
	}
	if o.MaxToolInput <= 0 {
		o.MaxToolInput = 4000
	}
	if o.MaxToolOutput <= 0 {
		o.MaxToolOutput = 8000
	}
	if o.GapThreshold <= 0 {
		o.GapThreshold = 90 * time.Second
	}
	return o
}

// agentCoreRE extracts the 16-hex identity both capture paths embed: hook ids
// are a<16hex>, transcript ids end in the same 16 hex after a filename slug.
// The fold keys threads by it (derive.CanonicalThread); here it only strips
// the core off a transcript id to leave the descriptive slug for a label.
var agentCoreRE = regexp.MustCompile(`^a([0-9a-f]{16})$|([0-9a-f]{16})$`)

// threadLabel names the thread for its header: a supplied label for any of its
// spellings first, then the descriptive middle of a transcript-derived id
// ("agent-afub-adversarial-0043..." reads as "afub-adversarial"), then a short
// form of whatever is left.
func threadLabel(raw []string, labels map[string]string) string {
	for _, id := range raw {
		if l := labels[id]; l != "" {
			return l
		}
	}
	for _, id := range raw {
		if rest, ok := strings.CutPrefix(id, "agent-"); ok {
			if m := agentCoreRE.FindStringSubmatch(rest); m != nil {
				if slug := strings.TrimSuffix(rest, m[0]); slug != "" {
					return strings.TrimSuffix(slug, "-")
				}
			}
		}
	}
	if len(raw) > 0 {
		return shortID(raw[0])
	}
	return ""
}

// mergeToolResult folds a result or failure onto the call it answers, and
// reports whether it did. A result whose call is not in this window stays a
// block of its own so the output is never silently dropped.
func mergeToolResult(g *Group, e event.Event, b Block, _ TranscriptOptions) bool {
	if e.Type != event.ToolResult && e.Type != event.ToolFailed {
		return false
	}
	if len(g.Blocks) == 0 {
		return false
	}
	// By the harness's own id first: a result whose call carries the same
	// tool_use id is that call's whatever sits between them. Adjacency is
	// the fallback for rows stored before the id was a column.
	last := &g.Blocks[len(g.Blocks)-1]
	if e.ToolUseID != "" {
		last = nil
		for i := len(g.Blocks) - 1; i >= 0; i-- {
			c := &g.Blocks[i]
			if c.Tool != nil && c.ToolUseID == e.ToolUseID && len(c.Tool.Output) == 0 && c.Tool.Error == "" {
				last = c
				break
			}
		}
		if last == nil {
			last = &g.Blocks[len(g.Blocks)-1]
		}
	}
	if last.Tool == nil || last.Tool.Name != toolName(e) {
		return false
	}
	if len(last.Tool.Output) > 0 || last.Tool.Error != "" {
		return false
	}
	if b.Tool != nil {
		last.Tool.Output = b.Tool.Output
		last.Tool.OutputTruncated = b.Tool.OutputTruncated
		last.Tool.OutputElided = b.Tool.OutputElided
		last.Tool.Error = b.Tool.Error
	}
	last.Diffs = append(last.Diffs, b.Diffs...)
	if e.Type == event.ToolFailed {
		last.Failed = true
	}
	return true
}

// blockFor turns one event into a row. kind is messages.kind as stored at
// ingest: a user-role row is a person's bubble only when the normalizer said
// human or slash_command (or the row predates kinds, which reads as a
// person's rather than as noise); every other kind is the harness's own
// record and renders as a collapsed neutral row titled by its kind. Nothing
// is classified here.
func blockFor(e event.Event, kind normalize.Kind, opts TranscriptOptions) (Block, bool) {
	b := Block{
		EventID:   e.ID,
		Seq:       e.Seq,
		Kind:      KindNotice,
		At:        e.OccurredAt,
		Model:     e.Model,
		Usage:     e.Usage,
		Origin:    string(e.Origin),
		ToolUseID: e.ToolUseID,
	}

	switch e.Type {
	case event.UserPrompt:
		b.Kind = KindPrompt
		// The bubble prints its actor: a person's prompt is the user's, and
		// the task that drives a subagent, which opens the agent's own
		// thread page the way a prompt does, is labelled as the task it is.
		b.Actor = "User"
		if kind == normalize.KindSubagentTask {
			// The task that drives a subagent opens its turn the way a
			// person's prompt does, labelled as the task it is.
			b.Actor = "Task"
		} else if kind != "" && !normalize.IsHumanKind(kind) {
			b.Kind = KindInternal
			b.Actor = "Harness"
			b.Title = kindTitle(kind, e.Source)
			// The one line the collapsed row shows: the normalizer's display
			// text for the record (a notification's summary, a command's
			// name), which is extraction, not classification.
			m := normalize.ClassifyUser(e.Text, nil, e.AgentID != "", kind == normalize.KindSubagentTask)
			if kind == normalize.KindTaskNotification || kind == normalize.KindSystemNotification || kind == normalize.KindInterrupted {
				b.NotificationSummary = firstLine(m.Text, 180)
			}
		}
		b.Text, b.TextTruncated = textSegments(e.Text, opts.MaxText, opts.Terms)

	case event.AssistantTurn:
		b.Kind = KindAssistant
		b.Actor = "Assistant"
		limit := opts.MaxText
		if opts.FullAssistantText {
			limit = len(e.Text)
		}
		b.Text, b.TextTruncated = textSegments(e.Text, limit, opts.Terms)
		if len(b.Text) == 0 && e.Usage == nil {
			return b, false
		}

	case event.ToolCall, event.ToolResult, event.ToolFailed:
		b.Kind = KindTool
		b.Tool = toolView(e, opts)
		b.Failed = e.Type == event.ToolFailed || (e.Tool != nil && e.Tool.Error != "")
		b.Diffs = diffsFor(e)

	case event.FileChanged:
		b.Kind = KindTool
		b.Tool = toolView(e, opts)
		b.Diffs = diffsFor(e)
		if b.Tool != nil && b.Tool.Name == "" {
			b.Tool.Name = "File changed"
		}

	case event.SessionStarted:
		b.Title = "Session started"
		b.Text = Plain(strings.TrimSpace(e.Cwd + " " + e.GitBranch))
	case event.SessionEnded:
		b.Title = "Session ended"
	case event.SubagentStart:
		b.Title = "Subagent started"
		b.Text = Plain(firstLine(e.Text, 200))
	case event.SubagentEnd:
		b.Title = "Subagent finished"
	case event.Compaction:
		b.Title = "Context compacted"
		b.Text = Plain(firstLine(e.Text, 200))
	case event.Artifact:
		b.Title = "Artifact"
		b.Text, b.TextTruncated = textSegments(e.Text, opts.MaxText, opts.Terms)
	default:
		b.Title = string(e.Type)
		b.Text, b.TextTruncated = textSegments(e.Text, opts.MaxText, opts.Terms)
	}
	return b, true
}

func toolView(e event.Event, opts TranscriptOptions) *ToolView {
	tv := &ToolView{Name: toolName(e)}
	if e.Tool == nil {
		return tv
	}
	tv.Summary = toolSummary(e.Tool)
	tv.Error = firstLine(e.Tool.Error, 500)
	tv.OutputElided = e.Tool.Truncated

	if len(e.Tool.Input) > 0 {
		tv.Input, tv.InputTruncated = textSegments(prettyJSON(e.Tool.Input), opts.MaxToolInput, opts.Terms)
	}
	if e.Tool.Output != "" {
		tv.Output, tv.OutputTruncated = textSegments(e.Tool.Output, opts.MaxToolOutput, opts.Terms)
	}
	return tv
}

func toolName(e event.Event) string {
	if e.Tool != nil && e.Tool.Name != "" {
		return e.Tool.Name
	}
	return ""
}

func diffsFor(e event.Event) []FileDiff {
	if e.Tool == nil || e.Tool.Diff == nil {
		return nil
	}
	d := e.Tool.Diff
	return []FileDiff{BuildFileDiff(d.Path, d.Before, d.After, d.Created)}
}

// summaryKeys are the input fields tools use for their subject, in the order
// we prefer them. Reading a known key beats truncating raw JSON, and the list
// is ordered so a tool carrying both a command and a description shows the
// command, which is the thing that actually ran.
var summaryKeys = []string{
	"command", "file_path", "pattern", "path", "query", "url", "notebook_path",
	"prompt", "description", "content",
}

func toolSummary(t *event.Tool) string {
	if len(t.Input) == 0 {
		return ""
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(t.Input, &fields); err != nil {
		return firstLine(string(t.Input), 160)
	}
	for _, k := range summaryKeys {
		raw, ok := fields[k]
		if !ok {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			continue
		}
		if s = strings.TrimSpace(s); s != "" {
			return firstLine(s, 160)
		}
	}
	// Nothing recognised: name the arguments rather than dumping their values,
	// which keeps the row scannable and avoids putting a wall of content in a
	// line meant to be read at a glance.
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

// prettyJSON indents tool arguments when they are JSON and are small enough to
// be worth the reflow. Oversized payloads are passed through as-is because
// indenting a megabyte to display eight kilobytes of it is wasted work.
func prettyJSON(raw json.RawMessage) string {
	const indentLimit = 64 << 10
	if len(raw) > indentLimit {
		return string(raw)
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

// textSegments truncates on a rune boundary and reports whether it did. The
// boundary matters: slicing a UTF-8 sequence in half produces bytes that the
// escaper still escapes but that render as a replacement character, which looks
// like data corruption in an archive whose whole value is fidelity.
func textSegments(s string, limit int, terms []string) ([]Segment, bool) {
	if s == "" {
		return nil, false
	}
	truncated := false
	if limit > 0 && len(s) > limit {
		cut := limit
		for cut > 0 && !utf8Start(s[cut]) {
			cut--
		}
		s = s[:cut]
		truncated = true
	}
	return Highlight(s, terms), truncated
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

func firstLine(s string, limit int) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8Start(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

func shortID(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}

// AgentLabels builds the agent id to name map from a session's rollup, so the
// transcript can label a subagent group whose start event is not in the
// rendered window.
func AgentLabels(agents []Agent) map[string]string {
	if len(agents) == 0 {
		return nil
	}
	out := make(map[string]string, len(agents))
	for _, a := range agents {
		name := a.Name
		if name == "" {
			name = shortID(a.AgentID)
		}
		out[a.AgentID] = name
	}
	return out
}

// ElapsedLabel formats the transcript gutter. Sub-minute values keep their
// seconds because the interesting part of a fast session is the seconds, and
// anything past an hour drops them because at that scale they are noise.
func ElapsedLabel(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("+%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("+%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	case d >= 24*time.Hour:
		return fmt.Sprintf("+%dd%02dh%02dm", int(d.Hours())/24, int(d.Hours())%24, int(d.Minutes())%60)
	default:
		return fmt.Sprintf("+%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}
