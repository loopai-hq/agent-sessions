// Package normalize decides what a "user" record actually is.
//
// Both harnesses file a great deal under the user role that no person typed:
// slash-command envelopes, the caveat that precedes a local command, the
// output of that command, background task notifications, teammate messages,
// system reminders, IDE context, compaction summaries, environment dumps and
// the templates the harness runs against itself. Every consumer that ever
// tried to tell those apart grew its own list, and four lists disagreed about
// what a person said: the reader hid four envelopes, the Slack mirror two, the
// title picked the first user row whatever it held, and the turn counter took
// everything. This package is the one list. It is pure and imports nothing of
// the server so that ingest, the derive runner, the reader and the mirror all
// call the same function and cannot drift.
//
// The rules are text-pattern first and record-flag second, in that order on
// purpose. The flags a transcript record carries (isMeta, origin.kind,
// promptSource) are strong when present but sparse across harness versions,
// and a hook payload carries none of them, so a classifier that led with the
// flags would call the same prompt two different things depending on which
// capture path delivered it. The text is the same on both paths.
package normalize

import (
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// Kind is what a message is, as opposed to which role the harness filed it
// under. Only human and slash_command are things a person said.
type Kind string

const (
	// KindHuman is a prompt a person typed.
	KindHuman Kind = "human"
	// KindSlashCommand is a person invoking a command: the transcript's
	// <command-name> envelope, or the hook path's raw "/name args".
	KindSlashCommand Kind = "slash_command"
	// KindCaveat is the <local-command-caveat> record the harness writes
	// before a built-in command's output.
	KindCaveat Kind = "caveat"
	// KindCommandOutput is what a local command printed: <local-command-stdout>
	// and the "!" shell passthrough's <bash-input>/<bash-stdout> records.
	KindCommandOutput Kind = "command_output"
	// KindTaskNotification is a background task reporting back.
	KindTaskNotification Kind = "task_notification"
	// KindSystemReminder is a <system-reminder> block on its own.
	KindSystemReminder Kind = "system_reminder"
	// KindSystemNotification is a "[SYSTEM NOTIFICATION]" line.
	KindSystemNotification Kind = "system_notification"
	// KindTeammateMessage is a message from another Claude session.
	KindTeammateMessage Kind = "teammate_message"
	// KindInterrupted is the "[Request interrupted by user" marker.
	KindInterrupted Kind = "interrupted"
	// KindIDEContext is the editor telling the model which file is open.
	KindIDEContext Kind = "ide_context"
	// KindCompactSummary is the summary that replaces history at compaction.
	KindCompactSummary Kind = "compact_summary"
	// KindEnvContext is an environment or agent-history dump the harness
	// injects ahead of the real prompt (Codex's <environment_context> and
	// kin).
	KindEnvContext Kind = "env_context"
	// KindHarnessInjected is anything else the harness wrote under the user
	// role: AGENTS.md instructions, plugin lists, skill bodies, heartbeats,
	// isMeta records, the legacy subagent Warmup.
	KindHarnessInjected Kind = "harness_injected"
	// KindSubagentTask is the driving prompt of a subagent stream. The parent
	// agent wrote it, not a person.
	KindSubagentTask Kind = "subagent_task"

	// KindAssistantText is an assistant record with prose.
	KindAssistantText Kind = "assistant_text"
	// KindAssistantUsageOnly is an assistant record that carries token
	// accounting and no text; analytics reads it, a reader never renders it.
	KindAssistantUsageOnly Kind = "assistant_usage_only"
	// KindTool is a tool result in the search corpus.
	KindTool Kind = "tool"
)

// Message is one classified user record.
type Message struct {
	Kind Kind
	// Text is what a reader should show for the record: the typed text with
	// attached harness blocks removed for a human prompt, "/name args" for a
	// command, the summary for a task notification, the record's own text
	// otherwise.
	Text string
	// Command and Args are set for slash commands: the name with its leading
	// slash, and whatever followed it.
	Command string
	Args    string
	// Envelope is set on a slash command whose name came from the
	// transcript's <command-name> wrapper, the harness's own record of what
	// ran, rather than from the hook path's typed line, which is a shape
	// rule and can read prose as a command (SlashCommand says why the
	// difference is load-bearing).
	Envelope bool
	// Attached holds harness blocks that rode on a human record (trailing
	// <system-reminder> blocks), split off so Text is what was typed.
	Attached []string
	// Subkind refines a kind where one word is not enough: which output
	// envelope, which origin flag decided, whether an interruption was during
	// tool use.
	Subkind string
}

// ClassifyUser decides what a user-role record is.
//
// text is the record's text as the walker or the hook extracted it. raw is
// the transcript record line when there is one and nil otherwise; a hook
// payload must never be passed, because its keys mean different things, and
// one that is passed anyway is ignored rather than misread. agentStream says
// the record belongs to a subagent's stream, and firstInStream that it is the
// first user record of that stream, which is how the parent's task prompt is
// told from anything a person typed.
func ClassifyUser(text string, raw []byte, agentStream bool, firstInStream bool) Message {
	t := strings.TrimSpace(text)
	p := probeRaw(raw)

	// Compaction first: the harness re-files the summary under the user role,
	// and on a fork the flag is gone while the text stays, so the text wins
	// whatever the flags say.
	if p.IsCompactSummary || hasAnyPrefix(t, compactPrefixes) || t == compactBoundary {
		return Message{Kind: KindCompactSummary, Text: t}
	}
	if strings.HasPrefix(t, "[Request interrupted") {
		m := Message{Kind: KindInterrupted, Text: t}
		if strings.Contains(t, "for tool use") {
			m.Subkind = "tool_use"
		}
		return m
	}
	// Codex files an interruption as a user record opening with this
	// wrapper ("<turn_aborted> The user interrupted the previous turn on
	// purpose..."), stored several times per second; read as a person's
	// prompt it opened a turn and counted as one. Leading only: a person
	// quoting the tag mid-sentence is talking about it.
	if strings.HasPrefix(t, "<turn_aborted>") {
		return Message{Kind: KindInterrupted, Text: t, Subkind: "turn_aborted"}
	}
	if strings.HasPrefix(t, "<local-command-caveat>") {
		return Message{Kind: KindCaveat, Text: t}
	}
	switch {
	case strings.HasPrefix(t, "<local-command-stdout>"):
		return Message{Kind: KindCommandOutput, Text: t, Subkind: "local_command_stdout"}
	case strings.HasPrefix(t, "<bash-input>"):
		return Message{Kind: KindCommandOutput, Text: t, Subkind: "bash_input"}
	case strings.HasPrefix(t, "<bash-stdout>"), strings.HasPrefix(t, "<bash-stderr>"):
		return Message{Kind: KindCommandOutput, Text: t, Subkind: "bash_stdout"}
	}
	if body, ok := envelope(t, "task-notification"); ok && field(body, "task-id") != "" {
		m := Message{Kind: KindTaskNotification, Text: t, Subkind: field(body, "status")}
		if s := oneLine(field(body, "summary")); s != "" {
			m.Text = s
		}
		return m
	}
	if strings.HasPrefix(t, "[SYSTEM NOTIFICATION]") {
		return Message{Kind: KindSystemNotification, Text: strings.TrimSpace(strings.TrimPrefix(t, "[SYSTEM NOTIFICATION]"))}
	}
	if id, ok := teammateEnvelope(t); ok {
		return Message{Kind: KindTeammateMessage, Text: t, Subkind: id}
	}
	if hasAnyPrefix(t, idePrefixes) {
		return Message{Kind: KindIDEContext, Text: t}
	}
	if hasAnyPrefix(t, envPrefixes) || codexEnvironment(t) {
		return Message{Kind: KindEnvContext, Text: t}
	}
	if hasAnyPrefix(t, injectedPrefixes) || t == "Warmup" {
		return Message{Kind: KindHarnessInjected, Text: t}
	}

	// System reminders: on their own they are a reminder; riding on a typed
	// prompt they are attachments, and the prompt is what remains once they
	// are peeled off both ends. A reminder quoted in the middle of a sentence
	// is left alone, because that is a person talking about reminders.
	core, attached := stripReminders(t)
	if core == "" && len(attached) > 0 {
		return Message{Kind: KindSystemReminder, Text: t, Attached: attached}
	}
	// A block that opens and never closes is a reminder the harness cut off
	// at its capture cap. It stays a reminder so that the cut-off text can
	// never become a title. The harness always breaks the line after the
	// opening tag, so only that shape (or the bare tag) is a cut-off block; a
	// person who starts a sentence with the tag and keeps typing on the same
	// line is talking about reminders and stays human.
	if core == reminderOpen || strings.HasPrefix(core, reminderOpen+"\n") {
		return Message{Kind: KindSystemReminder, Text: t, Attached: attached, Subkind: "unterminated"}
	}

	// Record flags. Anything the harness marked as its own is never a person,
	// whatever the text looks like: a skill body carries the command envelope
	// of the command that loaded it, and only the flag tells them apart.
	// The origin flag names what the record is; isMeta only says it is not a
	// person, so a named origin is read first.
	switch {
	case p.Origin == "task-notification":
		return Message{Kind: KindTaskNotification, Text: t, Subkind: "origin"}
	case p.Origin == "peer":
		return Message{Kind: KindTeammateMessage, Text: t, Subkind: "origin"}
	case p.IsMeta:
		sub := "meta"
		if strings.Contains(t, "<skill-format>") {
			sub = "skill"
		}
		return Message{Kind: KindHarnessInjected, Text: t, Subkind: sub}
	case p.Origin == "auto-continuation", p.PromptSource == "system":
		return Message{Kind: KindHarnessInjected, Text: t, Subkind: firstNonEmpty(p.Origin, "system")}
	}

	if m, ok := slashCommand(core); ok {
		m.Attached = attached
		return m
	}
	if agentStream && firstInStream {
		return Message{Kind: KindSubagentTask, Text: core, Attached: attached}
	}
	return Message{Kind: KindHuman, Text: core, Attached: attached}
}

// ClassifyAssistant decides what an assistant record is. Whether a text turn is
// the final answer of its exchange is a property of the turn, decided by the
// turn folder, not of the record.
//
// The second argument (whether the record carried usage) is accepted so the
// call site states what it knows, and is not consulted: a record with neither
// text nor usage has no third kind to be, and lands as usage-only.
func ClassifyAssistant(text string, _ bool) Kind {
	if strings.TrimSpace(text) != "" {
		return KindAssistantText
	}
	return KindAssistantUsageOnly
}

// IsHumanKind reports whether a kind is something a person said.
func IsHumanKind(k Kind) bool { return k == KindHuman || k == KindSlashCommand }

// internalTemplates are the prompts the harness, or a helper riding on it,
// sends to itself: the title generator, the status-line labeler, the liveness
// ping, the memory plugin's observers, Conductor's system instruction and the
// plugin recommender. A session whose opening prompt is one of these is a
// helper run, not a person's work; the list is explicit and tested rather than
// inferred, because the default view hides what it names.
var internalTemplates = []struct{ name, prefix string }{
	{"title_generator", "You are generating a short"},
	{"statusline_labeler", "You label Claude Code work"},
	{"pong", "Reply with exactly: pong"},
	{"claude_mem", "You are a Claude-Mem"},
	{"claude_mem", "Hello memory agent"},
	{"system_instruction", "<system_instruction>"},
	{"recommended_plugins", "<recommended_plugins>"},
}

// InternalTemplatePrefixes returns the opening text of every internal
// template, for a caller that has to make the same decision inside a SQL
// statement. It is the list IsInternalTemplate reads, exported as data rather
// than re-typed, so the two cannot disagree.
func InternalTemplatePrefixes() []string {
	out := make([]string, 0, len(internalTemplates))
	for _, tpl := range internalTemplates {
		out = append(out, tpl.prefix)
	}
	return out
}

// TemplateCutset is the whitespace stripped from the front of an opening
// prompt before it is compared with the template list. It is exported as
// data for the same reason the prefixes are: the store applies the list
// inside a SQL statement, and a trimmer there that stripped a different set
// of characters than this one would make the same prompt a template to one
// side and a person to the other. It is a fixed cutset rather than
// strings.TrimSpace's Unicode class because Postgres has no such class, and
// the two sides have to agree by construction rather than by luck.
const TemplateCutset = " \t\r\n"

// IsInternalTemplate reports whether a session's opening prompt is one of the
// harness's own templates, and which.
func IsInternalTemplate(firstHumanText string) (pattern string, ok bool) {
	t := strings.TrimLeft(firstHumanText, TemplateCutset)
	for _, tpl := range internalTemplates {
		if strings.HasPrefix(t, tpl.prefix) {
			return tpl.name, true
		}
	}
	return "", false
}

// TitleMax bounds a title in runes. Long enough for a real sentence, short
// enough that a list row stays a list row. Exported as data, like the
// template prefixes, for the store statement that has to render a stored
// row's first line to the same bound.
const TitleMax = 200

// Title renders a message as a session title: its first line, at most
// TitleMax runes. A slash command renders as "/name args" whatever the
// envelope looked like on disk.
func Title(m Message) string {
	text := m.Text
	if m.Kind == KindSlashCommand {
		text = strings.TrimSpace(m.Command + " " + m.Args)
	}
	line := firstLine(text)
	if utf8.RuneCountInString(line) <= TitleMax {
		return line
	}
	n := 0
	for i := range line {
		if n == TitleMax {
			return line[:i]
		}
		n++
	}
	return line
}

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

// compactPrefixes open a compaction summary: the sentence Claude Code writes
// on the summary record. It is long enough that no person opens a prompt
// with it.
var compactPrefixes = []string{
	"This session is being continued from a previous conversation",
}

// compactBoundary is the boundary record's own content, kept so a walker
// that ever surfaces it as text lands here too. It is matched whole rather
// than as a prefix because two words are short enough to be prose: a person
// writing "Conversation compacted; can we pick up the auth refactor?" is a
// person.
const compactBoundary = "Conversation compacted"

var idePrefixes = []string{"<ide_opened_file>", "<ide_selection>"}

// envPrefixes are the context dumps the harnesses inject before a prompt:
// Codex's internal-context block, its agent-history preface and the in-app
// browser context. Codex's <environment_context> is recognised by its whole
// shape instead (codexEnvironment), because a person pasting one to ask about
// it is a shape this list would swallow.
var envPrefixes = []string{
	"<codex_internal_context",
	"<in-app-browser-context",
	"The following is the Codex agent history",
}

// codexEnvironment recognises Codex's environment dump: the whole text is one
// <environment_context> element that names a cwd and a shell, which is what
// the harness writes and what a person quoting the tag does not.
func codexEnvironment(text string) bool {
	body, ok := envelope(text, "environment_context")
	return ok && field(body, "cwd") != "" && field(body, "shell") != ""
}

// injectedPrefixes are instruction and plumbing blocks the harness writes under
// the user role: AGENTS.md (bare and wrapped), plugin recommendations, skill
// bodies, heartbeat pings, system instructions and permission preambles.
var injectedPrefixes = []string{
	"# AGENTS.md instructions",
	"<user_instructions>",
	"<recommended_plugins>",
	"<skill>",
	"<heartbeat>",
	"<system_instruction>",
	"<permissions",
}

// rawProbe is the handful of transcript-record fields the rules read. Decoded
// by name, never by substring: a record's tool output routinely quotes other
// records, and the quoted copy must not classify the quoting one.
type rawProbe struct {
	IsMeta           bool
	IsCompactSummary bool
	PromptSource     string
	Origin           string
}

func probeRaw(raw []byte) rawProbe {
	if len(raw) == 0 {
		return rawProbe{}
	}
	var r struct {
		IsMeta           bool   `json:"isMeta"`
		IsCompactSummary bool   `json:"isCompactSummary"`
		PromptSource     string `json:"promptSource"`
		Origin           struct {
			Kind string `json:"kind"`
		} `json:"origin"`
		// A hook payload names its event here; a transcript record never
		// does. Its presence means a caller passed the wrong document, and
		// the honest response is to read nothing from it.
		HookEventName string `json:"hook_event_name"`
	}
	if json.Unmarshal(raw, &r) != nil || r.HookEventName != "" {
		return rawProbe{}
	}
	return rawProbe{
		IsMeta:           r.IsMeta,
		IsCompactSummary: r.IsCompactSummary,
		PromptSource:     r.PromptSource,
		Origin:           r.Origin.Kind,
	}
}

// envelope reports the body of text when the whole of it is one <tag>...</tag>
// element. The whole-text rule is what keeps a person who pastes a notification
// and then asks about it classified as a person.
func envelope(text, tag string) (string, bool) {
	opening, closing := "<"+tag+">", "</"+tag+">"
	if !strings.HasPrefix(text, opening) || !strings.HasSuffix(text, closing) || strings.Count(text, closing) != 1 {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(text, opening), closing), true
}

// field reads the trimmed body of the first <name>...</name> inside text.
func field(text, name string) string {
	_, tail, ok := strings.Cut(text, "<"+name+">")
	if !ok {
		return ""
	}
	value, _, ok := strings.Cut(tail, "</"+name+">")
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

// teammatePreface is the line Claude Code puts in front of a peer message on
// the transcript path; the hook path delivers the element alone.
const teammatePreface = "Another Claude session sent a message:"

// teammateEnvelope recognises a peer message by its opening element, which
// must carry a teammate_id attribute, and returns that id.
func teammateEnvelope(text string) (string, bool) {
	t := strings.TrimSpace(strings.TrimPrefix(text, teammatePreface))
	if !strings.HasPrefix(t, "<teammate-message ") {
		return "", false
	}
	end := strings.IndexByte(t, '>')
	if end < 0 {
		return "", false
	}
	opening := t[:end]
	_, after, ok := strings.Cut(opening, "teammate_id=\"")
	if !ok {
		return "", false
	}
	id, _, _ := strings.Cut(after, "\"")
	return id, true
}

const reminderOpen, reminderClose = "<system-reminder>", "</system-reminder>"

// stripReminders peels complete <system-reminder> blocks off the start and end
// of a record and returns what is left. Blocks in the middle stay where they
// are, and so does a block that never closes.
func stripReminders(text string) (core string, attached []string) {
	const opening, closing = reminderOpen, reminderClose
	core = text
	for {
		core = strings.TrimSpace(core)
		switch {
		case strings.HasPrefix(core, opening):
			end := strings.Index(core, closing)
			if end < 0 {
				return core, attached
			}
			attached = append(attached, core[:end+len(closing)])
			core = core[end+len(closing):]
		case strings.HasSuffix(core, closing):
			start := strings.LastIndex(core, opening)
			if start < 0 {
				return core, attached
			}
			attached = append(attached, core[start:])
			core = core[:start]
		default:
			return core, attached
		}
	}
}

// slashCommand recognises the two shapes a command takes. On disk it is an
// envelope of <command-name>, <command-message> and <command-args> in either
// order; the human text is the args. On the hook path it is the raw line the
// person typed. Anything that merely starts with a slash and is not shaped
// like a command name, a path for instance, is prose.
//
// The hook-path rule is a shape, not a list: the hook carries no catalogue
// of installed commands, and the same line reaches the server from machines
// with different ones. So "/api endpoint returns 500" and "/Users is where
// homes live" are read as a command named /api and /Users, while
// "/usr/local/bin/x" and "/ divide" are prose. The trade is deliberate. A
// wrong call in either direction costs the provenance word alone (the turn
// is counted either way, both kinds are a person's, and the title reads the
// same, since a command renders as its own line), whereas a list would have
// to be shipped to every reader and kept in step with every machine.
func slashCommand(text string) (Message, bool) {
	if strings.HasPrefix(text, "<command-name>") || strings.HasPrefix(text, "<command-message>") {
		name := field(text, "command-name")
		if name == "" {
			// The envelope with nothing in it is the harness's, not a
			// person's: nobody types the tags. It stays out of the human
			// kinds so that it can never title a session.
			return Message{Kind: KindHarnessInjected, Text: text, Subkind: "empty_command"}, true
		}
		m := Message{
			Kind:     KindSlashCommand,
			Command:  name,
			Args:     oneLine(field(text, "command-args")),
			Subkind:  field(text, "command-message"),
			Envelope: true,
		}
		m.Text = strings.TrimSpace(m.Command + " " + m.Args)
		return m, true
	}
	if !strings.HasPrefix(text, "/") {
		return Message{}, false
	}
	i := 1
	for i < len(text) && isCommandByte(text[i]) {
		i++
	}
	if i == 1 || (i < len(text) && text[i] != ' ' && text[i] != '\t' && text[i] != '\n' && text[i] != '\r') {
		return Message{}, false
	}
	m := Message{Kind: KindSlashCommand, Command: text[:i], Args: strings.TrimSpace(text[i:])}
	m.Text = strings.TrimSpace(m.Command + " " + m.Args)
	return m, true
}

func isCommandByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '_', c == '-', c == ':', c == '.':
		return true
	}
	return false
}

func hasAnyPrefix(text string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(text, p) {
			return true
		}
	}
	return false
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	return strings.TrimSpace(line)
}

// oneLine collapses whitespace runs, which is how a multi-line summary or an
// args block that was indented on disk reads as one line.
func oneLine(text string) string { return strings.Join(strings.Fields(text), " ") }

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
