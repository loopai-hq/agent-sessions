package capture

import (
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/backfill"
	"github.com/loopai-hq/agent-sessions/internal/event"
)

// StopTailWaits is how long the daemon waits for a turn's answer to reach the
// transcript before each re-read, in order, when a Stop copy reaches it
// without its record. The harness fires Stop and then flushes the turn's tail
// (the last tool result, its attachments, the answer records, the system
// record) about 100 ms later, measured on 2026-09-16 with a file poller and a
// second, slower hook; the hook's own read runs about 10 ms after its spawn
// and sees the file mid-flush. The wait cannot live in the hook: a Stop hook
// that waited was killed by the print-mode harness's exit before it could
// spool anything, and the copy was lost. The daemon survives the harness, so
// it waits, half a second in all, on the way out of the spool.
var StopTailWaits = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond, 200 * time.Millisecond}

// AnchorFor reads the transcript's tail once and reports the record a Stop
// copy for promptID may be anchored to: the last assistant record with
// something spent, when it carries no tool_use block (Stop fires when no tool
// call is pending, so such a record is a call from earlier in the turn) and,
// when the hook named a prompt, when the record answers that prompt.
//
// A user record for the same prompt BELOW the record makes that ambiguous:
// the record may be the origin session's last call in a fork's copied prefix
// (every copied user record carries the resuming prompt's id), the previous
// answer under a prompt that ends in Stop more than once, or the turn's own
// answer with a later user record under the turn's id after it (a teammate
// message, a blocking Stop hook's feedback, a cut-off continuation; these
// land 40 to 170 ms after the Stop hooks' own records, so the hook's read
// never sees them and a read seconds later always does). The hook's read
// keeps refusing that case (copy nil): under transcript lag its
// tail can be the previous answer, and a stamp it wrote is never re-read. A
// reader after the fact (the daemon) opens it with copy: the record's text
// must equal the answer the harness handed the hook, and the record must be
// no older than copy.At by ambiguousMaxAge, since the turn's own answer
// precedes its Stop by well under a second and any earlier answer under the
// prompt is older by a model call. The text is the best discriminator the
// copy offers, not proof: the same text twice under one prompt within the
// age window is the residual. A hook that named no prompt,
// from a harness that predates prompt_id, takes the tail on trust.
func AnchorFor(transcriptPath, promptID string, stop *StopCopy) (backfill.Tail, bool) {
	t, ok := backfill.LastAssistantRecord(transcriptPath)
	if !ok || t.ToolUse || t.UUID == "" {
		return backfill.Tail{}, false
	}
	if promptID == "" {
		return t, true
	}
	if t.PromptID != promptID {
		return backfill.Tail{}, false
	}
	if t.PromptAfter == promptID && !stop.matches(t) {
		return backfill.Tail{}, false
	}
	return t, true
}

// StopCopy is what a reader after the fact knows about the Stop copy it is
// anchoring: the answer text the harness handed the hook (scrubbed on the way
// in) and when the copy was made.
type StopCopy struct {
	Text string
	At   time.Time
}

// ambiguousMaxAge is how much older than the copy a record may be and still
// be taken as the copy's answer in the ambiguous case. The turn's own answer
// precedes its Stop by 10 to 695 ms on this machine (331 hook Stops). The
// previous answer under the same prompt is older by a model call: over 255
// such pairs with a user record between them, the closest was 1.67 s before
// the next answer, one under 2 s, 21 under 3 s, 66 under 5 s. Two seconds
// keeps every own answer with 1.3 s to spare and admits one observed
// previous answer, which the text must then also match (0 of 309 real
// consecutive answers were identical). The text is the primary guard; the
// age is what makes "the same text twice under one prompt" the only residual.
const ambiguousMaxAge = 2 * time.Second

// matches reports whether the record is this copy's own answer: the same
// text after trimming (a scrubbed answer, or one with no text, never
// matches, which is the safe direction) and a stamp within ambiguousMaxAge
// before the copy. A nil copy, the hook's own read, matches nothing.
func (c *StopCopy) matches(t backfill.Tail) bool {
	if c == nil {
		return false
	}
	record, want := strings.TrimSpace(t.Text), strings.TrimSpace(c.Text)
	if record == "" || record != want {
		return false
	}
	if t.Timestamp.IsZero() || c.At.IsZero() {
		return false
	}
	age := c.At.Sub(t.Timestamp)
	return age >= -time.Second && age <= ambiguousMaxAge
}

// Anchor stamps the record on the event: its usage (with the call's message
// id and request id), its model and its uuid.
func Anchor(e *event.Event, t backfill.Tail) {
	e.Usage, e.Model, e.RecordUUID = t.Usage, t.Model, t.UUID
}
