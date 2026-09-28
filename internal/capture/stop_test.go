package capture

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/scrub"
)

func TestStopVisibleResponse(t *testing.T) {
	cases := []struct {
		name, fields, want string
	}{
		{"current payload", `"last_assistant_message":"The answer"`, "The answer"},
		{"current wins", `"last_assistant_message":"Current","message":"Old"`, "Current"},
		{"empty is authoritative", `"last_assistant_message":"","message":"Old"`, ""},
		{"null does not revive legacy text", `"last_assistant_message":null,"message":"Old"`, ""},
		{"unexpected current shape", `"last_assistant_message":{},"message":"Old"`, ""},
		{"legacy string", `"message":"Legacy answer"`, "Legacy answer"},
		{"legacy content string", `"message":{"role":"assistant","content":"Legacy answer"}`, "Legacy answer"},
		{"legacy visible blocks only", `"message":{"role":"assistant","content":[{"type":"thinking","thinking":"private thought"},{"type":"text","text":"First"},{"type":"text","text":"Second"}]}`, "First\nSecond"},
		{"tool chatter", `"message":{"role":"assistant","content":[{"type":"text","text":"Running a tool"},{"type":"tool_use","name":"Bash","input":{"command":"private command"}}]}`, ""},
		{"not assistant", `"message":{"role":"user","content":"Not an answer"}`, ""},
		{"unknown role", `"message":{"content":"Not proven assistant"}`, ""},
		{"thinking only", `"message":{"role":"assistant","content":[{"type":"thinking","thinking":"private thought"}]}`, ""},
		{"scrub current", `"last_assistant_message":"SEKRIT"`, "[REDACTED:test]"},
		{"scrub legacy", `"message":{"role":"assistant","content":[{"type":"text","text":"SEKRIT"}]}`, "[REDACTED:test]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h HookEvent
			if err := json.Unmarshal([]byte(`{"hook_event_name":"Stop","session_id":"s1",`+tc.fields+`}`), &h); err != nil {
				t.Fatal(err)
			}
			r := &recorder{}
			if _, err := newCap(t, r).Handle(h); err != nil {
				t.Fatal(err)
			}
			e := r.events[0]
			if e.Text != tc.want {
				t.Fatalf("text = %q; want %q", e.Text, tc.want)
			}
			wantRaw, rawCounts := stubScrub(string(h.Message))
			if string(e.Raw) != wantRaw {
				t.Fatalf("raw = %s; want scrubbed original %s", e.Raw, wantRaw)
			}
			wantRedactions := rawCounts["test"]
			if strings.Contains(tc.want, "[REDACTED:test]") {
				wantRedactions++
			}
			if e.Redactions["test"] != wantRedactions {
				t.Fatalf("redaction count = %v; want %d", e.Redactions, wantRedactions)
			}
		})
	}
}

func TestStopResponseIsScrubbedByProductionRules(t *testing.T) {
	r := &recorder{}
	c, err := New(r, scrub.Func, func(string) int64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	text := "Example key: " + "ghp_" + strings.Repeat("a", 36)
	raw, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Handle(HookEvent{HookEventName: "Stop", SessionID: "s1", LastAssistantMessage: raw}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if strings.Contains(e.Text, "ghp_") || len(e.Redactions) == 0 || len(e.Raw) != 0 {
		t.Fatalf("visible answer was not scrubbed: %+v", e)
	}
}

func TestStopResponseDoesNotCarryAcrossHooks(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	for _, h := range []HookEvent{
		{HookEventName: "Stop", SessionID: "s1", LastAssistantMessage: json.RawMessage(`"First answer"`), Message: json.RawMessage(`{"role":"assistant","content":"Archived first answer"}`)},
		{HookEventName: "SubagentStop", SessionID: "s1", AgentID: "child", LastAssistantMessage: json.RawMessage(`"Child answer"`)},
		{HookEventName: "Stop", SessionID: "s1"},
		{HookEventName: "Stop", SessionID: "s2"},
	} {
		if _, err := c.Handle(h); err != nil {
			t.Fatal(err)
		}
	}
	// The subagent's own payload carried its own answer; nothing else did.
	if got := r.events[1]; got.Text != "Child answer" || len(got.Raw) != 0 {
		t.Fatalf("SubagentStop did not carry its own last_assistant_message: %+v", got)
	}
	for _, e := range r.events[2:] {
		if e.Text != "" || len(e.Raw) != 0 {
			t.Fatalf("response crossed hooks: %+v", e)
		}
	}
}

func TestStopDoesNotRecoverStaleTranscriptText(t *testing.T) {
	for _, suffix := range []string{
		"",
		`{"type":"user","sessionId":"s1","message":{"role":"user","content":"New prompt without an answer yet"}}`,
		`{"type":"assistant","sessionId":"other-session","message":{"role":"assistant","content":[{"type":"text","text":"Other session"}]}}`,
		`{"type":"assistant","sessionId":"s1","isSidechain":true,"agentId":"child","message":{"role":"assistant","content":[{"type":"text","text":"Subagent answer"}]}}`,
	} {
		r := &recorder{}
		path := transcriptWith(t, assistantRecord("previous", "claude-fable-5", 1, 2), suffix)
		if _, err := newCap(t, r).Handle(HookEvent{HookEventName: "Stop", SessionID: "s1", TranscriptPath: path}); err != nil {
			t.Fatal(err)
		}
		if r.events[0].Text != "" || len(r.events[0].Raw) != 0 {
			t.Fatalf("reused stale transcript text: %+v", r.events[0])
		}
	}
}

func TestStopCurrentResponseWithoutTranscript(t *testing.T) {
	for _, path := range []string{"", t.TempDir() + "/missing.jsonl"} {
		r := &recorder{}
		var h HookEvent
		if err := json.Unmarshal([]byte(`{"hook_event_name":"Stop","session_id":"s1","last_assistant_message":"Answer"}`), &h); err != nil {
			t.Fatal(err)
		}
		h.TranscriptPath = path
		if _, err := newCap(t, r).Handle(h); err != nil {
			t.Fatal(err)
		}
		if r.events[0].Text != "Answer" || r.events[0].Usage != nil {
			t.Fatalf("lost answer without transcript: %+v", r.events[0])
		}
	}
}
