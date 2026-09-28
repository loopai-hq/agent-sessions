package derive

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
)

// The record shapes below are taken from research/evidence/scan*.out and
// hookprobe3.log: a user record with promptId, an assistant record with
// message.id, requestId and two tool_use blocks, a tool_result record, and
// the compaction boundary that carries logicalParentUuid.
const (
	rawUser = `{"parentUuid":"616a3c51-7e6e-41bc-9ebb-bc89a3842107","isSidechain":false,"promptId":"9bacf634-5c61-42e2-bb26-7072f8eaa574","type":"user","message":{"role":"user","content":"Make this artifact public"},"uuid":"d3c7608a-de5a-4258-95cd-cd8f0c6c0d32","timestamp":"2026-08-25T12:54:00.810Z","sessionId":"10db14c9-6373-4c36-a800-db092907faf6","version":"2.1.236","entrypoint":"cli"}`

	rawAssistant = `{"parentUuid":"d3c7608a-de5a-4258-95cd-cd8f0c6c0d32","isSidechain":false,"type":"assistant","uuid":"9f90859f-ba1b-4d9d-8292-5c182e2c831f","timestamp":"2026-08-25T12:54:05.000Z","sessionId":"10db14c9-6373-4c36-a800-db092907faf6","requestId":"req_011CTS6fcv","message":{"id":"msg_01Mk9x","type":"message","role":"assistant","model":"claude-opus-4-1","content":[{"type":"text","text":"Running it."},{"type":"tool_use","id":"toolu_012HH5HfZ73a2TttRtX5xLQU","name":"Bash","input":{"command":"echo probe-one"}},{"type":"tool_use","id":"toolu_01NnMRjoXabzKRRd4B5iVKjn","name":"Bash","input":{"command":"echo probe-two"}}],"usage":{"input_tokens":3,"output_tokens":40}}}`

	rawToolResult = `{"parentUuid":"9f90859f-ba1b-4d9d-8292-5c182e2c831f","type":"user","uuid":"2032b422-5710-418a-aca2-48e297c3ecda","timestamp":"2026-08-25T12:54:06.000Z","sessionId":"10db14c9-6373-4c36-a800-db092907faf6","message":{"role":"user","content":[{"tool_use_id":"toolu_012HH5HfZ73a2TttRtX5xLQU","type":"tool_result","content":"probe-one\n"},{"tool_use_id":"toolu_01NnMRjoXabzKRRd4B5iVKjn","type":"tool_result","content":[{"type":"text","text":"probe-two"}]}]}}`

	rawCompact = `{"parentUuid":null,"logicalParentUuid":"9f90859f-ba1b-4d9d-8292-5c182e2c831f","isSidechain":false,"type":"system","subtype":"compact_boundary","content":"Conversation compacted","uuid":"30d989dc-5764-4fd3-85ff-728150917230","timestamp":"2026-08-25T12:39:46.375Z","sessionId":"e8cb3609-1e27-4df4-ae59-f09bda35f1ca"}`

	rawHookPayload = `{"hook_event_name":"Stop","prompt_id":"72442f84-fe82-4691-a49a-8e194aa00a5d","session_id":"s","uuid":"never-read","last_assistant_message":"DONE"}`
)

func TestKeysOfReadsTranscriptRecordsByName(t *testing.T) {
	user := event.Event{Origin: event.OriginTranscript, Type: event.UserPrompt}
	k := KeysOf(user, []byte(rawUser))
	if k.PromptID != "9bacf634-5c61-42e2-bb26-7072f8eaa574" || k.RecordUUID != "d3c7608a-de5a-4258-95cd-cd8f0c6c0d32" {
		t.Errorf("user keys = %+v", k)
	}
	if k.MessageID != "" || k.ToolUseID != "" {
		t.Errorf("a user record has no message id or tool use id: %+v", k)
	}

	asst := event.Event{Origin: event.OriginTranscript, Type: event.AssistantTurn}
	k = KeysOf(asst, []byte(rawAssistant))
	if k.RecordUUID != "9f90859f-ba1b-4d9d-8292-5c182e2c831f" || k.RequestID != "req_011CTS6fcv" || k.MessageID != "msg_01Mk9x" {
		t.Errorf("assistant keys = %+v", k)
	}
	if k.ToolUseID != "" {
		t.Errorf("the turn row of a record with two tool blocks must not claim either: %q", k.ToolUseID)
	}

	compact := event.Event{Origin: event.OriginTranscript, Type: event.Compaction}
	k = KeysOf(compact, []byte(rawCompact))
	if k.ParentRecordUUID != "9f90859f-ba1b-4d9d-8292-5c182e2c831f" {
		t.Errorf("compaction marker = %+v", k)
	}
}

func TestKeysOfFindsTheToolBlockAnEventWasSplitFrom(t *testing.T) {
	call := event.Event{Origin: event.OriginTranscript, Type: event.ToolCall,
		Tool: &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command": "echo probe-two"}`)}}
	if k := KeysOf(call, []byte(rawAssistant)); k.ToolUseID != "toolu_01NnMRjoXabzKRRd4B5iVKjn" {
		t.Errorf("tool_call matched %q, want the second block by its input", k.ToolUseID)
	}
	// Same name and input twice is ambiguous and yields no id rather than a
	// guess.
	ambiguous := event.Event{Origin: event.OriginTranscript, Type: event.ToolCall,
		Tool: &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command":"x"}`)}}
	twice := `{"type":"assistant","uuid":"u","message":{"id":"m","content":[{"type":"tool_use","id":"a","name":"Bash","input":{"command":"x"}},{"type":"tool_use","id":"b","name":"Bash","input":{"command":"x"}}]}}`
	if k := KeysOf(ambiguous, []byte(twice)); k.ToolUseID != "" {
		t.Errorf("an ambiguous block match guessed %q", k.ToolUseID)
	}
	if _, ok := IdentityOf(ambiguous, KeysOf(ambiguous, []byte(twice))); ok {
		t.Error("a tool row with no tool use id must not carry a record identity: it would collide with its sibling")
	}

	result := event.Event{Origin: event.OriginTranscript, Type: event.ToolResult, Tool: &event.Tool{Output: "probe-two"}}
	if k := KeysOf(result, []byte(rawToolResult)); k.ToolUseID != "toolu_01NnMRjoXabzKRRd4B5iVKjn" {
		t.Errorf("tool_result matched %q, want the block whose flattened content is the output", k.ToolUseID)
	}
	one := event.Event{Origin: event.OriginTranscript, Type: event.ToolResult, Tool: &event.Tool{Output: "does not match"}}
	single := `{"type":"user","uuid":"u","message":{"content":[{"tool_use_id":"only","type":"tool_result","content":"whatever"}]}}`
	if k := KeysOf(one, []byte(single)); k.ToolUseID != "only" {
		t.Errorf("a record with one result block is that block: %q", k.ToolUseID)
	}
}

func TestKeysOfPrefersNamedFieldsAndIgnoresHookPayloads(t *testing.T) {
	named := event.Event{Origin: event.OriginTranscript, Type: event.UserPrompt, PromptID: "named", RecordUUID: "named-uuid"}
	if k := KeysOf(named, []byte(rawUser)); k.PromptID != "named" || k.RecordUUID != "named-uuid" {
		t.Errorf("named fields lost to raw: %+v", k)
	}
	hook := event.Event{Origin: event.OriginHook, Type: event.AssistantTurn, PromptID: "from-the-client"}
	k := KeysOf(hook, []byte(rawHookPayload))
	if k.PromptID != "from-the-client" || k.RecordUUID != "" {
		t.Errorf("a hook row read keys out of a hook payload: %+v", k)
	}
	if _, ok := IdentityOf(hook, k); ok {
		t.Error("hook rows keep id identity and never carry a record identity")
	}
	// A transcript row handed a hook payload by mistake reads nothing.
	wrong := event.Event{Origin: event.OriginTranscript, Type: event.AssistantTurn}
	if k := KeysOf(wrong, []byte(rawHookPayload)); k.RecordUUID != "" || k.PromptID != "" {
		t.Errorf("a hook payload was read as a transcript record: %+v", k)
	}
}

func TestIdentityOfNamesTranscriptRowsWithARecord(t *testing.T) {
	e := event.Event{Origin: event.OriginTranscript, Type: event.UserPrompt, SessionID: "s", AgentID: "agent-x"}
	id, ok := IdentityOf(e, KeysOf(e, []byte(rawUser)))
	if !ok {
		t.Fatal("a keyed transcript prompt has no identity")
	}
	want := Identity{SessionID: "s", AgentID: "agent-x", Record: "d3c7608a-de5a-4258-95cd-cd8f0c6c0d32", Type: event.UserPrompt}
	if id != want {
		t.Errorf("identity = %+v, want %+v", id, want)
	}
	if _, ok := IdentityOf(e, Keys{}); ok {
		t.Error("a transcript row with no record uuid has no identity")
	}
}

func TestKindOfMatchesTheIngestRule(t *testing.T) {
	human := event.Event{Type: event.UserPrompt, Origin: event.OriginTranscript, Text: "fix the bug", Raw: json.RawMessage(rawUser)}
	if m, ok := KindOf(human); !ok || m.Kind != normalize.KindHuman {
		t.Errorf("human = %+v %v", m, ok)
	}
	task := event.Event{Type: event.UserPrompt, Origin: event.OriginTranscript, AgentID: "agent-a", Seq: 1, Text: "look this up"}
	if m, _ := KindOf(task); m.Kind != normalize.KindSubagentTask {
		t.Errorf("subagent task = %s", m.Kind)
	}
	later := task
	later.Seq = 7
	if m, _ := KindOf(later); m.Kind != normalize.KindHuman {
		t.Errorf("a later prompt in an agent stream = %s", m.Kind)
	}
	usage := event.Event{Type: event.AssistantTurn, Usage: &event.Usage{InputTokens: 1}}
	if m, _ := KindOf(usage); m.Kind != normalize.KindAssistantUsageOnly {
		t.Errorf("usage-only = %s", m.Kind)
	}
	tool := event.Event{Type: event.ToolResult, Text: "out"}
	if m, _ := KindOf(tool); m.Kind != normalize.KindTool {
		t.Errorf("tool = %s", m.Kind)
	}
	if _, ok := KindOf(event.Event{Type: event.SessionStarted}); ok {
		t.Error("a lifecycle event has a kind")
	}
	// The hook path's raw is never consulted: the same text classifies the
	// same way with a hook payload attached.
	hooked := event.Event{Type: event.UserPrompt, Origin: event.OriginHook, Text: "fix the bug", Raw: json.RawMessage(rawHookPayload)}
	if m, _ := KindOf(hooked); m.Kind != normalize.KindHuman {
		t.Errorf("hook prompt = %s", m.Kind)
	}
}

// A key longer than KeyMaxBytes is dropped whichever way it arrived, named
// or in raw, and a key exactly at the cap is kept: the row lands by id with
// the key empty rather than the batch or the index build failing on it.
func TestKeysOfDropsValuesOverTheCap(t *testing.T) {
	over := strings.Repeat("x", KeyMaxBytes+1)
	at := strings.Repeat("y", KeyMaxBytes)

	named := event.Event{Origin: event.OriginHook, Type: event.UserPrompt,
		PromptID: over, RecordUUID: over, ParentRecordUUID: over, ToolUseID: over,
		Usage: &event.Usage{MessageID: over, RequestID: over}}
	if k := KeysOf(named, nil); k != (Keys{}) {
		t.Errorf("named keys over the cap = %+v, want every one dropped", k)
	}

	raw := `{"type":"assistant","uuid":"` + over + `","promptId":"` + over + `","logicalParentUuid":"` + over +
		`","requestId":"` + over + `","message":{"id":"` + over + `","role":"assistant","content":[{"type":"tool_use","id":"` + over + `","name":"Bash","input":{"command":"true"}}]}}`
	call := event.Event{Origin: event.OriginTranscript, Type: event.ToolCall,
		Tool: &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command":"true"}`)}}
	if k := KeysOf(call, []byte(raw)); k != (Keys{}) {
		t.Errorf("raw keys over the cap = %+v, want every one dropped", k)
	}
	if _, ok := IdentityOf(call, KeysOf(call, []byte(raw))); ok {
		t.Errorf("a row whose record uuid was dropped must not claim an identity")
	}

	kept := event.Event{Origin: event.OriginHook, Type: event.UserPrompt, PromptID: at, RecordUUID: at}
	if k := KeysOf(kept, nil); k.PromptID != at || k.RecordUUID != at {
		t.Errorf("keys exactly at the cap were dropped: %+v", k)
	}
}
