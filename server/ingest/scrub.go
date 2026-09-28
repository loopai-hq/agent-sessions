package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/scrub"
)

// ServerCaughtPrefix marks a redaction this server made rather than the agent.
//
// The two are counted separately, in the same map, because they answer different
// questions. The agent's own tally says how much credential material a person's
// work touches. This one says how much of it the agent failed to recognise, and
// a machine whose server-caught count is climbing is a machine running an agent
// whose pattern set has fallen behind the code being written on it. Merging the
// two would leave the second question unanswerable, and it is the one that
// requires action.
const ServerCaughtPrefix = "server:"

// rescan runs a delivered payload through the redaction rules a second time,
// rewriting the event and the bytes to be stored when it finds anything.
//
// The agent scrubs before it uploads. This pass exists because pattern matching
// on a laptop is best-effort by nature — an agent runs whatever rule set it was
// installed with, and the rules change — and the promise being made is that no
// live credential is retained centrally. A promise that depends on fifty
// independently-versioned copies of a regex list is not a promise.
//
// It works on decoded string values rather than on the raw document, which is
// the difference between a scrub that is safe and one that occasionally produces
// unparseable JSON. Several rules match across newlines, and a key block whose
// BEGIN and END markers straddle two fields would otherwise be replaced along
// with the structure between them. Scanning each string separately cannot do
// that, at the cost of missing a credential split across two fields, which is
// the trade to make: the client already scrubbed each field, and a corrupted
// record is unrecoverable while a missed straddling match is caught by the
// stored-body scan an operator can run later.
//
// A clean payload is left byte-for-byte as delivered. Re-encoding an untouched
// document would gain nothing and lose the property that what is stored is what
// arrived.
func rescan(ev *event.Event, body *json.RawMessage) (map[string]int, error) {
	var doc any
	dec := json.NewDecoder(bytes.NewReader(*body))
	// Numbers stay in their original notation. Decoding them to float64 and
	// re-encoding would rewrite token counts and sequence numbers into whatever
	// Go's float formatting produces, which for a large sequence number is a
	// different number.
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("ingest: re-read payload: %w", err)
	}

	counts := map[string]int{}
	rewritten := false
	doc = scrubValue(doc, counts, &rewritten)
	if len(counts) == 0 && !rewritten {
		return nil, nil
	}

	clean, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("ingest: re-encode scrubbed payload: %w", err)
	}
	// The event is re-derived from the scrubbed document rather than patched
	// field by field. The fields that carry text are spread across the event, the
	// tool call, the diff and the raw record, and a list of them here would be one
	// more place to forget a field the day somebody adds one.
	var scrubbed event.Event
	if err := json.Unmarshal(clean, &scrubbed); err != nil {
		return nil, fmt.Errorf("ingest: re-read scrubbed payload: %w", err)
	}

	scrubbed.Redactions = mergeCounts(scrubbed.Redactions, counts, ServerCaughtPrefix)
	*ev = scrubbed
	*body = clean
	return counts, nil
}

// scrubValue walks a decoded JSON document, redacting every string in it.
//
// Object keys are left alone. A credential can appear as a value in any
// harness's record shape, but a key is a field name chosen by the producer, and
// rewriting keys would change the shape of the record rather than its content.
func scrubValue(v any, counts map[string]int, rewritten *bool) any {
	switch t := v.(type) {
	case string:
		clean, hits := scrub.Func(t)
		for k, n := range hits {
			counts[k] += n
		}
		if strings.ContainsRune(clean, '\x00') {
			clean = strings.ReplaceAll(clean, "\x00", "\uFFFD")
			*rewritten = true
		}
		return clean
	case []any:
		for i, e := range t {
			t[i] = scrubValue(e, counts, rewritten)
		}
		return t
	case map[string]any:
		for k, e := range t {
			t[k] = scrubValue(e, counts, rewritten)
		}
		return t
	default:
		return v
	}
}

// rescanReport applies the same pass to a health report.
//
// A report is mostly counters, but LastError holds whatever the delivery path
// last failed with and a condition's detail holds whatever produced it, and a
// failing request URL is one of the places a credential turns up. The report is
// stored as JSON and read by the fleet page, so it is subject to the same rule
// as everything else that lands here.
func rescanReport(r *health.Report) map[string]int {
	counts := map[string]int{}
	r.LastError = scrubInto(r.LastError, counts)
	for i := range r.Conditions {
		r.Conditions[i].Detail = scrubInto(r.Conditions[i].Detail, counts)
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
}

func scrubInto(s string, counts map[string]int) string {
	clean, hits := scrub.Func(s)
	for k, n := range hits {
		counts[k] += n
	}
	return clean
}

// mergeCounts adds prefixed counters to a tally, allocating only if there is
// something to add. The tally travels with the event into the session's
// redactions column, where the prefix is what keeps the two sources apart.
func mergeCounts(into map[string]int, add map[string]int, prefix string) map[string]int {
	if len(add) == 0 {
		return into
	}
	if into == nil {
		into = make(map[string]int, len(add))
	}
	for k, n := range add {
		into[prefix+k] += n
	}
	return into
}
