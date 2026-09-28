// Every credential-shaped value in this file is synthetic. syntheticKey below
// is EXAMPLE filler behind an Anthropic key prefix, present so the server-side
// scrub pass has something to catch; it has never been a live credential.
// .gitleaks.toml and .github/secret_scanning.yml allowlist this file by path
// for that reason.

package ingest

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// syntheticKey is a fixture shaped like an Anthropic key and is not one. It
// exists to prove the server-side pass fires on something the agent's rule set
// might have been too old to recognise.
const syntheticKey = "sk-ant-api03-EXAMPLEEXAMPLEEXAMPLEEXAMPLE0000000000"

// TestServerCatchesWhatTheAgentMissed is the defence-in-depth pass. The agent
// scrubs before uploading, but its rules ship with the binary and the binary is
// months old on somebody's laptop, so this is what actually supports the claim
// that no live credential is retained centrally.
func TestServerCatchesWhatTheAgentMissed(t *testing.T) {
	st := newMemStore(testRates)
	log, logs := capturingLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	e := mkEvent("e1", 1, event.ToolResult)
	e.Text = "exported ANTHROPIC_API_KEY=" + syntheticKey + " into the shell"
	e.Tool = &event.Tool{
		Name:   "Bash",
		Input:  json.RawMessage(`{"command":"curl -H 'x-api-key: ` + syntheticKey + `' https://api.anthropic.com"}`),
		Output: "ok",
	}

	if w := postItems(t, h, []spool.Item{mkItem(t, e)}); w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}

	rec, ok := st.stored("e1")
	if !ok {
		t.Fatal("event not stored")
	}
	if strings.Contains(string(rec.Body), syntheticKey) {
		t.Fatalf("the key survived into the stored body: %s", rec.Body)
	}
	if strings.Contains(rec.Event.Text, syntheticKey) {
		t.Fatalf("the key survived into the event text: %s", rec.Event.Text)
	}
	if !strings.Contains(rec.Event.Text, "[REDACTED:anthropic_key]") {
		t.Fatalf("no redaction marker in %q", rec.Event.Text)
	}
	if rec.Event.Tool == nil || strings.Contains(string(rec.Event.Tool.Input), syntheticKey) {
		t.Fatalf("the key survived into the tool input: %+v", rec.Event.Tool)
	}

	// A catch here is the only signal that a laptop is running a rule set that
	// has fallen behind, so it is recorded rather than quietly fixed.
	if got := rec.Event.Redactions[ServerCaughtPrefix+"anthropic_key"]; got != 2 {
		t.Fatalf("server-caught tally %d, want 2 (text and tool input): %+v", got, rec.Event.Redactions)
	}
	if s := st.snapshot("sess-1"); s.Redactions[ServerCaughtPrefix+"anthropic_key"] != 2 {
		t.Fatalf("session tally %+v", s.Redactions)
	}
	if line := logs.String(); !strings.Contains(line, "agent missed") || !strings.Contains(line, "anthropic_key") {
		t.Fatalf("no operator-visible record of the catch: %s", line)
	}
	if strings.Contains(logs.String(), syntheticKey) {
		t.Fatal("the log line contains the credential")
	}
}

// TestServerCatchKeepsTheAgentsOwnTally checks the two sources stay separable.
// The agent's count says how much credential material somebody's work touches;
// the server's says how much of it the agent did not recognise, and only the
// second one is a reason to go and update an agent.
func TestServerCatchKeepsTheAgentsOwnTally(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	e := mkEvent("e1", 1, event.AssistantTurn)
	e.Text = "here is the key: " + syntheticKey
	e.Redactions = map[string]int{"github_token": 3}

	if w := postItems(t, h, []spool.Item{mkItem(t, e)}); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	rec, _ := st.stored("e1")
	if rec.Event.Redactions["github_token"] != 3 {
		t.Fatalf("the agent's own tally was lost: %+v", rec.Event.Redactions)
	}
	if rec.Event.Redactions[ServerCaughtPrefix+"anthropic_key"] != 1 {
		t.Fatalf("server tally missing: %+v", rec.Event.Redactions)
	}
}

// TestScrubReachesNestedRawRecords matters because Raw is the record of truth:
// the derived fields are a convenience layer, and a credential that only
// survives in the original record is still a credential we are storing.
func TestScrubReachesNestedRawRecords(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	e := mkEvent("e1", 1, event.AssistantTurn)
	e.Text = "clean"
	e.Raw = json.RawMessage(`{"message":{"content":[{"type":"text","text":"key ` + syntheticKey + `"}]}}`)

	if w := postItems(t, h, []spool.Item{mkItem(t, e)}); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	rec, _ := st.stored("e1")
	if strings.Contains(string(rec.Body), syntheticKey) {
		t.Fatalf("the key survived in the raw record: %s", rec.Body)
	}
	if !json.Valid(rec.Body) {
		t.Fatalf("scrubbing produced invalid JSON: %s", rec.Body)
	}
	if !strings.Contains(string(rec.Event.Raw), "[REDACTED:anthropic_key]") {
		t.Fatalf("raw record not redacted: %s", rec.Event.Raw)
	}
}

func TestScrubReplacesNULBeforePostgres(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	e := mkEvent("e1", 1, event.ToolResult)
	e.Tool = &event.Tool{Output: "binary\x00content"}
	e.Raw = json.RawMessage(`{"toolUseResult":{"file":{"content":"nested\u0000content"}}}`)

	if w := postItems(t, h, []spool.Item{mkItem(t, e)}); w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	rec, ok := st.stored("e1")
	if !ok {
		t.Fatal("event not stored")
	}
	if strings.ContainsRune(string(rec.Body), '\x00') || strings.Contains(string(rec.Body), `\u0000`) {
		t.Fatalf("NUL survived into the stored body: %q", rec.Body)
	}
	if rec.Event.Tool == nil || rec.Event.Tool.Output != "binary\uFFFDcontent" {
		t.Fatalf("tool output = %q, want replacement character", rec.Event.Tool.Output)
	}
	if !strings.Contains(string(rec.Event.Raw), "nested\uFFFDcontent") {
		t.Fatalf("nested raw content was not sanitized: %s", rec.Event.Raw)
	}
}

// TestScrubPreservesNumbersExactly guards the rewrite path. A document is only
// re-encoded when something was redacted, and re-encoding through float64 would
// silently change any integer past 2^53 that happened to share the record.
func TestScrubPreservesNumbersExactly(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	const bigSeq = int64(9007199254740993) // 2^53 + 1, not representable as float64
	e := mkEvent("e1", 1, event.AssistantTurn)
	e.Seq = bigSeq
	e.Text = "leaked " + syntheticKey
	e.Model = "claude-opus-5"
	e.Usage = &event.Usage{InputTokens: 9007199254740993, MessageID: "m", RequestID: "r"}

	if w := postItems(t, h, []spool.Item{mkItem(t, e)}); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	rec, _ := st.stored("e1")
	if rec.Event.Seq != bigSeq {
		t.Fatalf("seq %d, want %d", rec.Event.Seq, bigSeq)
	}
	if !strings.Contains(string(rec.Body), "9007199254740993") {
		t.Fatalf("the stored body lost the exact number: %s", rec.Body)
	}
	if rec.Event.Usage == nil || rec.Event.Usage.InputTokens != bigSeq {
		t.Fatalf("usage rewritten: %+v", rec.Event.Usage)
	}
}

// TestCleanPayloadIsStoredVerbatim keeps the common path honest: what is stored
// is what arrived, so a parser change later can be replayed against the real
// bytes.
func TestCleanPayloadIsStoredVerbatim(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	item := mkItem(t, mkEvent("e1", 1, event.UserPrompt))
	if w := postItems(t, h, []spool.Item{item}); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	rec, _ := st.stored("e1")
	if string(rec.Body) != string(item.Payload) {
		t.Fatalf("clean payload rewritten:\n got %s\nwant %s", rec.Body, item.Payload)
	}
	if len(rec.Event.Redactions) != 0 {
		t.Fatalf("clean payload produced a tally: %+v", rec.Event.Redactions)
	}
}

// TestScrubDoesNotRewriteKeys checks that only values are touched. A field name
// is chosen by the producer and rewriting one would change the shape of the
// record rather than its content.
func TestScrubDoesNotRewriteKeys(t *testing.T) {
	body := json.RawMessage(`{"` + syntheticKey + `":"` + syntheticKey + `"}`)
	ev := event.Event{}
	counts, err := rescan(&ev, &body)
	if err != nil {
		t.Fatalf("rescan: %v", err)
	}
	if counts["anthropic_key"] != 1 {
		t.Fatalf("counts %+v, want one value redacted", counts)
	}
	if !strings.Contains(string(body), `"`+syntheticKey+`":`) {
		t.Fatalf("the key name was rewritten: %s", body)
	}
}
