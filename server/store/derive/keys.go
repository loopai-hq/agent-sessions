// Package derive is the pure half of the derivation layer: the rules that
// turn stored events into keys, kinds and turns, with no database behind
// them.
//
// It imports the event and normalize packages and the standard library and
// nothing of the store, so that the same rule is applied at ingest, by the
// runner over history, and in a unit test over a slice, and cannot drift
// between the three. Everything here is a function of its arguments. The
// store owns the SQL that reads the projection in and writes the result
// out.
package derive

import (
	"bytes"
	"encoding/json"
	"reflect"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
)

// Keys are the cross-origin identities an event carries: the values that let
// a hook-captured row and the transcript walker's copy of the same moment be
// recognised as one thing, and that let a re-walk of a file name the rows it
// already stored.
type Keys struct {
	// PromptID is the harness's id for the human turn in progress, the same
	// value on both capture paths.
	PromptID string
	// RecordUUID is the transcript record's own uuid, the identity of a
	// transcript-origin row across walks.
	RecordUUID string
	// ParentRecordUUID is the compaction marker (logicalParentUuid): a record
	// inside the same file, never a session.
	ParentRecordUUID string
	// RequestID and MessageID identify the model call an assistant record
	// reports; MessageID alone is the usage ledger's key.
	RequestID string
	MessageID string
	// ToolUseID pairs a tool call with its result and with the transcript's
	// tool_use block.
	ToolUseID string
}

// rawRecord is the handful of transcript-record fields the keys are read
// from when the client did not stamp them as named fields. Decoded by name:
// a record's tool output routinely quotes other records, and a substring
// scan would read the quoted one.
type rawRecord struct {
	UUID              string `json:"uuid"`
	PromptID          string `json:"promptId"`
	LogicalParentUUID string `json:"logicalParentUuid"`
	RequestID         string `json:"requestId"`
	// A hook payload names its event here and a transcript record never
	// does; one that carries it is the wrong document and yields nothing.
	HookEventName string `json:"hook_event_name"`
	Message       *struct {
		ID      string          `json:"id"`
		Content json.RawMessage `json:"content"`
	} `json:"message"`
}

type rawBlock struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	Text      string          `json:"text"`
}

// KeyMaxBytes bounds every key value KeysOf returns. On every row the fleet
// has stored the keys are uuids (36 bytes), message ids (36), request ids
// (28) and tool_use ids (30); the bound is generous for all of them and far
// under the btree's 2,704-byte entry ceiling, which a value from a corrupt
// or hostile client would otherwise reach. With the identity index in place
// one such value fails the whole batch it arrived in, and stored before the
// index exists it fails the runner's index step on every attempt and parks
// the versioned pass for everyone. A longer value is dropped, not cut: a
// prefix of a uuid is not a uuid, and an empty key only costs the row the
// identity shortcut while the row still lands by id.
const KeyMaxBytes = 256

// KeysOf reads an event's keys: the named fields when the client stamped
// them, and the transcript record in raw otherwise. Named fields win because
// they are what a client that knows the field chose to send; raw is the
// fallback that keys every row an older client delivered and every row
// already stored.
//
// Only a transcript-origin event is read from raw. A hook payload carries
// none of these under these names, and its prompt_id reaches the named
// field from the client, so nothing is lost by refusing the parse; what is
// gained is that a hook row can never be keyed by a document that is not a
// transcript record.
func KeysOf(e event.Event, raw []byte) Keys {
	return keysOf(e, raw).bounded()
}

// bounded drops every value over KeyMaxBytes, named or read from raw alike,
// so the one rule holds at ingest (keysFor) and in the runner's event_keys
// step, which both come through KeysOf.
func (k Keys) bounded() Keys {
	for _, v := range []*string{&k.PromptID, &k.RecordUUID, &k.ParentRecordUUID, &k.RequestID, &k.MessageID, &k.ToolUseID} {
		if len(*v) > KeyMaxBytes {
			*v = ""
		}
	}
	return k
}

func keysOf(e event.Event, raw []byte) Keys {
	k := Keys{
		PromptID:         e.PromptID,
		RecordUUID:       e.RecordUUID,
		ParentRecordUUID: e.ParentRecordUUID,
		ToolUseID:        e.ToolUseID,
	}
	if e.Usage != nil {
		k.RequestID = e.Usage.RequestID
		k.MessageID = e.Usage.MessageID
	}
	if e.Origin != event.OriginTranscript || len(raw) == 0 {
		return k
	}
	var r rawRecord
	if json.Unmarshal(raw, &r) != nil || r.HookEventName != "" {
		return k
	}
	if k.PromptID == "" {
		k.PromptID = r.PromptID
	}
	if k.RecordUUID == "" {
		k.RecordUUID = r.UUID
	}
	if k.ParentRecordUUID == "" {
		k.ParentRecordUUID = r.LogicalParentUUID
	}
	if k.RequestID == "" {
		k.RequestID = r.RequestID
	}
	// A message id belongs to the assistant record that made the call. A user
	// record's message carries no id, and the tool rows split out of an
	// assistant record inherit its id through the same raw line, which is
	// right: they are parts of the same call.
	if k.MessageID == "" && r.Message != nil {
		k.MessageID = r.Message.ID
	}
	if k.ToolUseID == "" && r.Message != nil {
		k.ToolUseID = toolUseIDFor(e, r.Message.Content)
	}
	return k
}

// toolUseIDFor finds the block a tool event was split out of. One transcript
// record can hold several tool_use or tool_result blocks and the event keeps
// no index into them, so the block is matched by what the event does carry:
// the tool's name and input for a call, the flattened output for a result.
// When the match is ambiguous the id is left empty rather than guessed,
// because an empty id only costs this row the identity shortcut, while a
// wrong id would let a re-walk overwrite one call with another.
func toolUseIDFor(e event.Event, content json.RawMessage) string {
	if e.Tool == nil || len(content) == 0 {
		return ""
	}
	var blocks []rawBlock
	if json.Unmarshal(content, &blocks) != nil {
		return ""
	}
	var matches []string
	switch e.Type {
	case event.ToolCall:
		for _, b := range blocks {
			if b.Type != "tool_use" || b.Name != e.Tool.Name {
				continue
			}
			if !jsonEqual(b.Input, e.Tool.Input) {
				continue
			}
			matches = append(matches, b.ID)
		}
	case event.ToolResult, event.ToolFailed:
		for _, b := range blocks {
			if b.Type != "tool_result" {
				continue
			}
			if flatten(b.Content) != e.Tool.Output {
				continue
			}
			matches = append(matches, b.ToolUseID)
		}
	}
	if len(matches) == 1 && matches[0] != "" {
		return matches[0]
	}
	// Every block matched the same way and there is only one of them: the
	// event is that block whatever its content compares as.
	if len(matches) == 0 {
		var only []string
		for _, b := range blocks {
			switch {
			case e.Type == event.ToolCall && b.Type == "tool_use":
				only = append(only, b.ID)
			case e.Type != event.ToolCall && b.Type == "tool_result":
				only = append(only, b.ToolUseID)
			}
		}
		if len(only) == 1 {
			return only[0]
		}
	}
	return ""
}

// jsonEqual compares two JSON documents as values, so a stored copy whose
// keys Postgres reordered still equals the record it was split from.
func jsonEqual(a, b json.RawMessage) bool {
	a, b = bytes.TrimSpace(a), bytes.TrimSpace(b)
	if bytes.Equal(a, b) {
		return true
	}
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	var va, vb any
	if json.Unmarshal(a, &va) != nil || json.Unmarshal(b, &vb) != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}

// flatten renders a tool_result block's content the way the walker does: a
// string as itself, an array of text blocks joined by newlines. The two
// must agree or no result would ever match its block.
func flatten(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var bs []rawBlock
	if err := json.Unmarshal(raw, &bs); err == nil {
		var out []byte
		first := true
		for _, b := range bs {
			if b.Text == "" {
				continue
			}
			if !first {
				out = append(out, '\n')
			}
			first = false
			out = append(out, b.Text...)
		}
		return string(out)
	}
	return string(raw)
}

// Identity is the record identity of a transcript-origin event: what makes
// two walks of the same file name the same row even when walk order, and
// therefore the event id, changed between them.
type Identity struct {
	SessionID string
	AgentID   string
	Record    string
	Type      event.Type
	ToolUseID string
}

// IdentityOf reports the record identity of an event, and whether it has
// one. Only transcript rows with a record uuid qualify, and a tool row only
// when its block is known: a tool row with no tool_use id would collide with
// its siblings from the same record, and a collision here is a silent
// overwrite.
func IdentityOf(e event.Event, k Keys) (Identity, bool) {
	if e.Origin != event.OriginTranscript || k.RecordUUID == "" {
		return Identity{}, false
	}
	switch e.Type {
	case event.ToolCall, event.ToolResult, event.ToolFailed:
		if k.ToolUseID == "" {
			return Identity{}, false
		}
	}
	return Identity{
		SessionID: e.SessionID,
		AgentID:   e.AgentID,
		Record:    k.RecordUUID,
		Type:      e.Type,
		ToolUseID: k.ToolUseID,
	}, true
}

// KindOf names what a text-bearing event is, through the one normalizer, for
// the search corpus and the fold. It is the rule the store applies at ingest,
// held here so the runner classifies history with the same function.
//
// Only a transcript record's own line is handed over as raw. A subagent
// stream's driving prompt is its first user record: the walker numbers each
// stream from zero with the start marker at 0, so the task prompt is at seq
// 1; the hook path emits no user prompts inside a subagent at all.
func KindOf(e event.Event) (normalize.Message, bool) {
	switch e.Type {
	case event.UserPrompt:
		var raw []byte
		if e.Origin == event.OriginTranscript {
			raw = e.Raw
		}
		agentStream := e.AgentID != ""
		return normalize.ClassifyUser(e.Text, raw, agentStream, agentStream && e.Seq <= 1), true
	case event.AssistantTurn:
		return normalize.Message{
			Kind: normalize.ClassifyAssistant(e.Text, e.Usage != nil),
			Text: e.Text,
		}, true
	case event.ToolResult, event.ToolFailed:
		return normalize.Message{Kind: normalize.KindTool, Text: e.Text}, true
	}
	return normalize.Message{}, false
}
