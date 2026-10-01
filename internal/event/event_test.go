package event

import (
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// ---------------------------------------------------------------------------
// DeterministicID — the idempotency key that makes at-least-once safe
// ---------------------------------------------------------------------------

// The id must not drift when the process restarts, when the binary is rebuilt,
// or when a backfill re-reads a transcript months later. Recomputing the digest
// here from the spec rather than from the implementation is the point: a golden
// value derived independently catches a change to the hashed layout, which is
// the one change that turns every spooled retry into a duplicate row.
func TestSameEventIdentityAlwaysYieldsSameID(t *testing.T) {
	cases := []struct {
		name    string
		session string
		seq     int64
		typ     Type
		disc    string
		want    string
	}{
		{
			name:    "session start from a live hook",
			session: "019a4f7c-3d2e-7b91-9f0a-1c2d3e4f5a6b",
			seq:     0,
			typ:     SessionStarted,
			disc:    "SessionStart",
			want:    "f47826195358141cfb4c75400439e129",
		},
		{
			name:    "tool call from a live hook",
			session: "019a4f7c-3d2e-7b91-9f0a-1c2d3e4f5a6b",
			seq:     7,
			typ:     ToolCall,
			disc:    "PreToolUse",
			want:    "4d8a8bdf3fac46047d7f5c16c4cb2f86",
		},
		{
			// The backfill walker packs agent, workflow and record uuid into the
			// discriminator, so the compound shape has to be pinned too.
			name:    "assistant turn replayed from a transcript",
			session: "019a4f7c-3d2e-7b91-9f0a-1c2d3e4f5a6b",
			seq:     12,
			typ:     AssistantTurn,
			disc:    "agent-w1|wf_abc|msg_01|0",
			want:    "80ad231a9fe19efc302b61d1d4b5ee07",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeterministicID(tc.session, tc.seq, tc.typ, tc.disc)
			if got != tc.want {
				t.Fatalf("id = %q, want %q; the key layout changed and every "+
					"already-delivered event will now re-land as a new row", got, tc.want)
			}
			// Repeat calls model the crash-and-re-read path, which must land on
			// the same key rather than a fresh one.
			for i := range 3 {
				if again := DeterministicID(tc.session, tc.seq, tc.typ, tc.disc); again != got {
					t.Fatalf("call %d returned %q, want %q", i+2, again, got)
				}
			}
		})
	}
}

// A server upsert keys on this string, so its width and alphabet are part of
// the storage contract, not an implementation detail.
func TestIDIsFixedWidthLowercaseHex(t *testing.T) {
	id := DeterministicID("s", 1, ToolCall, "d")
	if len(id) != 32 {
		t.Fatalf("id length = %d, want 32 (128 bits of sha256)", len(id))
	}
	if strings.Trim(id, "0123456789abcdef") != "" {
		t.Fatalf("id %q contains non-lowercase-hex characters", id)
	}
}

// Every component is part of the event's identity. If one of them stopped
// reaching the digest, two distinct events would share a key and the second
// would overwrite the first with no error raised anywhere.
func TestEveryIdentityComponentChangesTheID(t *testing.T) {
	const (
		baseSession = "sess-a"
		baseSeq     = int64(3)
		baseDisc    = "PreToolUse"
	)
	baseType := ToolCall
	base := DeterministicID(baseSession, baseSeq, baseType, baseDisc)

	cases := []struct {
		name    string
		session string
		seq     int64
		typ     Type
		disc    string
	}{
		{"different session", "sess-b", baseSeq, baseType, baseDisc},
		{"different seq", baseSession, 4, baseType, baseDisc},
		{"different type", baseSession, baseSeq, ToolResult, baseDisc},
		{"different discriminator", baseSession, baseSeq, baseType, "PostToolUse"},
		// A transcript re-read can legitimately produce seq 0 where a hook
		// produced seq 3; the ids must still part ways.
		{"zero seq", baseSession, 0, baseType, baseDisc},
		{"empty discriminator", baseSession, baseSeq, baseType, ""},
		{"empty session", "", baseSeq, baseType, baseDisc},
	}

	seen := map[string]string{base: "base"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeterministicID(tc.session, tc.seq, tc.typ, tc.disc)
			if got == base {
				t.Fatalf("%s produced the base id %q; this component does not "+
					"reach the digest", tc.name, base)
			}
			if prev, dup := seen[got]; dup {
				t.Fatalf("%s collides with %s on id %q", tc.name, prev, got)
			}
			seen[got] = tc.name
		})
	}
}

// The classic failure is concatenating components without a delimiter, so that
// ("ab", "c") and ("a", "bc") hash alike. Each pair below shifts one character
// across a component boundary while leaving the naive concatenation identical;
// under an unseparated implementation every pair collides and one of the two
// events is silently swallowed by dedup.
func TestComponentBoundariesDoNotCollide(t *testing.T) {
	type id struct {
		session string
		seq     int64
		typ     Type
		disc    string
	}
	cases := []struct {
		name string
		a, b id
	}{
		{
			name: "character shifts between session and seq",
			a:    id{"s1", 23, ToolCall, "x"},
			b:    id{"s", 123, ToolCall, "x"},
		},
		{
			name: "digit shifts between seq and type",
			a:    id{"s", 11, Type("tool_call"), "x"},
			b:    id{"s", 1, Type("1tool_call"), "x"},
		},
		{
			name: "character shifts between type and discriminator",
			a:    id{"s", 1, Type("tool_calls"), "x"},
			b:    id{"s", 1, Type("tool_call"), "sx"},
		},
		{
			// Emptiness must not be a free pass either: an absent component
			// still has to occupy its own slot in the digest.
			name: "component emptied and its content moved right",
			a:    id{"sess", 1, Type(""), "abc"},
			b:    id{"sess", 1, Type("abc"), ""},
		},
		{
			// An empty session id still has to occupy its slot, or the seq
			// digits slide left into it.
			name: "empty session and the seq's leading digit",
			a:    id{"", 12, ToolCall, "x"},
			b:    id{"1", 2, ToolCall, "x"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := DeterministicID(tc.a.session, tc.a.seq, tc.a.typ, tc.a.disc)
			b := DeterministicID(tc.b.session, tc.b.seq, tc.b.typ, tc.b.disc)
			if a == b {
				t.Fatalf("%+v and %+v both hash to %q; components are not "+
					"separated and one of these events will be lost", tc.a, tc.b, a)
			}
		})
	}
}

// The id must depend on its arguments and nothing else. A package-level hasher
// or counter introduced as an "optimization" would make ids depend on call
// order and on concurrency, which -race plus a shared expectation catches here.
func TestIDDependsOnArgumentsNotProcessState(t *testing.T) {
	want := DeterministicID("sess", 5, UserPrompt, "d")

	// Interleaving unrelated calls must not perturb the result.
	for i := range 20 {
		DeterministicID("other", int64(i), ToolCall, "noise")
		if got := DeterministicID("sess", 5, UserPrompt, "d"); got != want {
			t.Fatalf("after %d interleaved calls id = %q, want %q", i+1, got, want)
		}
	}

	var wg sync.WaitGroup
	errs := make([]string, 32)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if got := DeterministicID("sess", 5, UserPrompt, "d"); got != want {
				errs[i] = got
			}
		}(i)
	}
	wg.Wait()
	for i, got := range errs {
		if got != "" {
			t.Fatalf("goroutine %d computed %q, want %q", i, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Validate — the last gate before an event leaves the laptop
// ---------------------------------------------------------------------------

func validEvent() Event {
	return Event{
		ID:         DeterministicID("sess", 1, UserPrompt, "UserPromptSubmit"),
		Source:     SourceClaudeCode,
		Origin:     OriginHook,
		Type:       UserPrompt,
		SessionID:  "sess",
		Seq:        1,
		OccurredAt: time.Date(2026, 8, 1, 9, 30, 0, 0, time.UTC),
		Text:       "ship it",
	}
}

// Anything that slips past here becomes a malformed row on the server, long
// after the machine that could explain it has moved on.
func TestValidateRejectsEveryUnshippableEvent(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Event)
		// want is a fragment of the rejection reason. Asserting on the text
		// keeps the message useful to whoever reads it in a log.
		want string
	}{
		{
			name: "no session id",
			mut:  func(e *Event) { e.SessionID = "" },
			want: "session_id is required",
		},
		{
			name: "no type",
			mut:  func(e *Event) { e.Type = "" },
			want: "type is required",
		},
		{
			name: "no source",
			mut:  func(e *Event) { e.Source = "" },
			want: "source is required",
		},
		{
			name: "no origin",
			mut:  func(e *Event) { e.Origin = "" },
			want: "origin is required",
		},
		{
			// Ingest time is not event time. An unset clock here would let the
			// server stamp arrival and quietly relabel history.
			name: "no occurred_at",
			mut:  func(e *Event) { e.OccurredAt = time.Time{} },
			want: "occurred_at is required",
		},
		{
			// Without an id the server cannot dedup, so every retry of this
			// event lands as a fresh row and inflates the numbers.
			name: "no id",
			mut:  func(e *Event) { e.ID = "" },
			want: "id is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent()
			tc.mut(&e)
			err := e.Validate()
			if err == nil {
				t.Fatalf("accepted an event with %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateAcceptsAFullyPopulatedEvent(t *testing.T) {
	e := validEvent()
	if err := e.Validate(); err != nil {
		t.Fatalf("rejected a well-formed event: %v", err)
	}
}

// Reporting one problem at a time turns a single malformed producer into a
// sequence of round trips, so every reason has to come back at once.
func TestValidateReportsAllProblemsAtOnce(t *testing.T) {
	var e Event
	err := e.Validate()
	if err == nil {
		t.Fatal("accepted a zero event")
	}
	for _, want := range []string{
		"session_id is required",
		"type is required",
		"source is required",
		"origin is required",
		"occurred_at is required",
		"id is required",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q omits %q", err, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Session.Apply — the rollup behind every count the dashboard shows
// ---------------------------------------------------------------------------

func at(minute int) time.Time {
	return time.Date(2026, 8, 1, 9, minute, 0, 0, time.UTC)
}

func ev(typ Type, minute int) Event {
	return Event{
		Source:     SourceClaudeCode,
		Origin:     OriginHook,
		Type:       typ,
		SessionID:  "sess",
		Seq:        int64(minute),
		OccurredAt: at(minute),
	}
}

// counters is the subset of the rollup the session list renders. Comparing it
// wholesale means a type that starts moving the wrong counter fails here rather
// than showing up as a plausible-looking wrong number in the product.
type counters struct {
	UserTurns   int
	ToolCalls   int
	Subagents   int
	Compactions int
	Errors      int
	Ended       bool
}

func countersOf(s Session) counters {
	return counters{s.UserTurns, s.ToolCalls, s.Subagents, s.Compactions, s.Errors, s.Ended}
}

func TestEachEventTypeMovesExactlyItsOwnCounter(t *testing.T) {
	cases := []struct {
		typ  Type
		want counters
	}{
		{UserPrompt, counters{UserTurns: 1}},
		{ToolCall, counters{ToolCalls: 1}},
		{ToolFailed, counters{Errors: 1}},
		{SubagentStart, counters{Subagents: 1}},
		{Compaction, counters{Compactions: 1}},
		{SessionEnded, counters{Ended: true}},
		// The remainder are recorded but must not inflate any tally: a turn
		// count that also counts tool results is not a turn count.
		{SessionStarted, counters{}},
		{AssistantTurn, counters{}},
		{ToolResult, counters{}},
		{FileChanged, counters{}},
		{SubagentEnd, counters{}},
		{Artifact, counters{}},
	}

	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			var s Session
			s.Apply(ev(tc.typ, 1))
			if got := countersOf(s); got != tc.want {
				t.Fatalf("counters = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// session builds the fixture the ordering tests permute: a realistic mix that
// touches every counter, both usage and redaction accumulation, and more than
// one harness version.
func sessionEvents() []Event {
	start := ev(SessionStarted, 1)
	start.Cwd = "/repo"
	start.GitBranch = "main"
	start.HarnessVersion = "2.1.0"

	prompt := ev(UserPrompt, 2)
	prompt.Text = "first"
	prompt.HarnessVersion = "2.1.0"

	turn := ev(AssistantTurn, 3)
	turn.HarnessVersion = "2.2.0"
	turn.Usage = &Usage{
		InputTokens:         10,
		OutputTokens:        20,
		CacheReadTokens:     30,
		CacheCreationTokens: 40,
		Ephemeral5m:         50,
		Ephemeral1h:         60,
	}
	turn.Redactions = map[string]int{"anthropic_key": 1}

	call := ev(ToolCall, 4)
	call.Redactions = map[string]int{"anthropic_key": 2, "aws_secret": 1}

	failed := ev(ToolFailed, 5)
	sub := ev(SubagentStart, 6)
	sub.ParentSessionID = "parent"
	compact := ev(Compaction, 7)

	second := ev(UserPrompt, 8)
	second.Text = "second"

	end := ev(SessionEnded, 9)

	return []Event{start, prompt, turn, call, failed, sub, compact, second, end}
}

// Events reach the server out of order after a retry, so folding them in a
// different order must not change what the rollup says happened. FirstPrompt
// and the harness-version ORDER are the two documented exceptions, covered
// separately below; everything else is compared wholesale so a newly added
// order-sensitive field fails here instead of shipping.
func TestApplyIsOrderIndependent(t *testing.T) {
	events := sessionEvents()

	normalize := func(s Session) Session {
		// Arrival order decides which prompt lands first; that is the caller's
		// job to get right by sorting on Seq, and is pinned by its own test.
		s.FirstPrompt = ""
		// The SET of versions is what matters. Append order is arrival order.
		sort.Strings(s.HarnessVers)
		return s
	}

	fold := func(order []int) Session {
		var s Session
		for _, i := range order {
			s.Apply(events[i])
		}
		return normalize(s)
	}

	inOrder := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	want := fold(inOrder)

	orders := map[string][]int{
		"reversed":            {8, 7, 6, 5, 4, 3, 2, 1, 0},
		"end marker first":    {8, 0, 1, 2, 3, 4, 5, 6, 7},
		"late batch replayed": {5, 6, 7, 8, 0, 1, 2, 3, 4},
		"interleaved":         {4, 0, 7, 2, 8, 1, 5, 3, 6},
	}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			if got := fold(order); !reflect.DeepEqual(got, want) {
				t.Fatalf("rollup differs when events arrive %s:\n got %+v\nwant %+v", name, got, want)
			}
		})
	}
}

// Apply is a fold, not a set union: it assumes the id-level dedup upstream
// already removed duplicates. Pinning that here so a caller that folds a
// re-delivered event cannot mistake double-counting for a bug in the counters.
func TestApplyFoldsRepeatsWithoutDeduplicating(t *testing.T) {
	e := ev(ToolCall, 1)
	e.HarnessVersion = "2.1.0"
	e.Usage = &Usage{InputTokens: 10}
	e.Redactions = map[string]int{"anthropic_key": 1}

	var s Session
	s.Apply(e)
	s.Apply(e)

	if s.ToolCalls != 2 {
		t.Fatalf("ToolCalls = %d, want 2; Apply must not silently dedup, dedup "+
			"belongs to the id-keyed upsert", s.ToolCalls)
	}
	if s.TotalUsage.InputTokens != 20 {
		t.Fatalf("InputTokens = %d, want 20", s.TotalUsage.InputTokens)
	}
	if s.RedactionSum["anthropic_key"] != 2 {
		t.Fatalf("redactions = %v, want anthropic_key 2", s.RedactionSum)
	}
	// The version list is the one field that is a set, because a repeated
	// version is not new information about the machine.
	if len(s.HarnessVers) != 1 {
		t.Fatalf("HarnessVers = %v, want one entry", s.HarnessVers)
	}
}

// A single machine runs several harness versions at once, so a session-level
// version is a lie unless it records all of them.
func TestHarnessVersionsAreCollectedAsASet(t *testing.T) {
	var s Session
	for _, v := range []string{"2.1.0", "2.2.0", "2.1.0", "", "2.2.0", "2.3.0"} {
		e := ev(AssistantTurn, 1)
		e.HarnessVersion = v
		s.Apply(e)
	}
	want := []string{"2.1.0", "2.2.0", "2.3.0"}
	if !reflect.DeepEqual(s.HarnessVers, want) {
		t.Fatalf("HarnessVers = %v, want %v", s.HarnessVers, want)
	}
}

// The window shown against a session must cover every event in it, whatever
// order they were folded in, or a retried tail truncates the session's span.
func TestTimeBoundsSpanTheEarliestAndLatestEvent(t *testing.T) {
	var s Session
	for _, m := range []int{5, 2, 9, 7} {
		s.Apply(ev(AssistantTurn, m))
	}
	if !s.StartedAt.Equal(at(2)) {
		t.Errorf("StartedAt = %s, want %s", s.StartedAt, at(2))
	}
	if !s.EndedAt.Equal(at(9)) {
		t.Errorf("EndedAt = %s, want %s", s.EndedAt, at(9))
	}
}

// A session that simply stopped producing events is exactly the messy session
// this system exists to capture, and must not be dressed up as a clean finish.
func TestEndedRequiresAnExplicitEndMarker(t *testing.T) {
	var s Session
	s.Apply(ev(UserPrompt, 1))
	s.Apply(ev(ToolCall, 2))
	if s.Ended {
		t.Fatal("Ended is true without a session_ended event; a crash would be " +
			"reported as a completed session")
	}
	s.Apply(ev(SessionEnded, 3))
	if !s.Ended {
		t.Fatal("Ended is false after a session_ended event")
	}
}

// Cache traffic is the overwhelming majority of real spend, so a total that
// only added input and output would misreport cost by roughly an order of
// magnitude. Every field has to be carried.
func TestUsageTotalsIncludeAllCacheTiers(t *testing.T) {
	var s Session
	for i := range 2 {
		e := ev(AssistantTurn, i+1)
		e.Usage = &Usage{
			InputTokens:         1,
			OutputTokens:        2,
			CacheReadTokens:     4,
			CacheCreationTokens: 8,
			Ephemeral5m:         16,
			Ephemeral1h:         32,
		}
		s.Apply(e)
	}
	// An event with no usage at all is normal and must not disturb the totals.
	s.Apply(ev(ToolCall, 3))

	want := Usage{
		InputTokens:         2,
		OutputTokens:        4,
		CacheReadTokens:     8,
		CacheCreationTokens: 16,
		Ephemeral5m:         32,
		Ephemeral1h:         64,
	}
	if s.TotalUsage != want {
		t.Fatalf("TotalUsage = %+v, want %+v", s.TotalUsage, want)
	}
}

// A machine whose scrubber fires constantly is a machine to look at, which
// only works if the counts survive the rollup per kind rather than collapsing
// into one number.
func TestRedactionCountsAccumulatePerKind(t *testing.T) {
	var s Session
	first := ev(UserPrompt, 1)
	first.Redactions = map[string]int{"anthropic_key": 2}
	second := ev(ToolCall, 2)
	second.Redactions = map[string]int{"anthropic_key": 1, "aws_secret": 3}

	s.Apply(first)
	s.Apply(second)

	want := map[string]int{"anthropic_key": 3, "aws_secret": 3}
	if !reflect.DeepEqual(s.RedactionSum, want) {
		t.Fatalf("RedactionSum = %v, want %v", s.RedactionSum, want)
	}
}

// A clean session should not carry an empty map into storage, since the field
// is omitempty and a present-but-empty map is a different row than an absent one.
func TestRedactionMapStaysNilWhenNothingWasScrubbed(t *testing.T) {
	var s Session
	s.Apply(ev(UserPrompt, 1))
	if s.RedactionSum != nil {
		t.Fatalf("RedactionSum = %v, want nil for a session with no redactions", s.RedactionSum)
	}
}

// FirstPrompt follows fold order, not event time. That makes sorting by Seq
// before folding part of the caller's contract; this pins the behaviour so the
// requirement cannot be quietly forgotten.
func TestFirstPromptFollowsFoldOrderNotEventTime(t *testing.T) {
	early := ev(UserPrompt, 1)
	early.Text = "earlier in the session"
	late := ev(UserPrompt, 9)
	late.Text = "later in the session"

	var inOrder Session
	inOrder.Apply(early)
	inOrder.Apply(late)
	if inOrder.FirstPrompt != "earlier in the session" {
		t.Fatalf("FirstPrompt = %q, want the first folded prompt", inOrder.FirstPrompt)
	}

	var outOfOrder Session
	outOfOrder.Apply(late)
	outOfOrder.Apply(early)
	if outOfOrder.FirstPrompt != "later in the session" {
		t.Fatalf("FirstPrompt = %q; Apply is documented to fold in Seq order and "+
			"this pins what happens when a caller does not", outOfOrder.FirstPrompt)
	}
}

// The stored prefix is rendered directly in the session list, so cutting a
// multi-byte rune in half would put replacement characters in the product.
func TestFirstPromptIsTruncatedAtARuneBoundary(t *testing.T) {
	// Three-byte runes do not divide evenly into the 500-byte cap, which is
	// what forces the boundary walk-back.
	long := strings.Repeat("世", 400)

	var s Session
	e := ev(UserPrompt, 1)
	e.Text = long
	s.Apply(e)

	if !utf8.ValidString(s.FirstPrompt) {
		t.Fatalf("stored prompt is not valid UTF-8: %q", s.FirstPrompt)
	}
	if !strings.HasPrefix(long, s.FirstPrompt) {
		t.Fatal("stored prompt is not a prefix of the original")
	}
	if len(s.FirstPrompt) > 500 {
		t.Fatalf("stored prompt is %d bytes, want at most 500", len(s.FirstPrompt))
	}
	// At most one rune may be dropped to reach a boundary; anything more means
	// the walk-back overshot and the prefix is losing real text.
	if len(s.FirstPrompt) < 500-utf8.UTFMax {
		t.Fatalf("stored prompt is only %d bytes, want close to the 500-byte cap",
			len(s.FirstPrompt))
	}
}

func TestShortPromptIsStoredVerbatim(t *testing.T) {
	var s Session
	e := ev(UserPrompt, 1)
	e.Text = "ship it"
	s.Apply(e)
	if s.FirstPrompt != "ship it" {
		t.Fatalf("FirstPrompt = %q, want it stored unchanged", s.FirstPrompt)
	}
}

// A continuation spread across several transcript files is one logical task.
// Losing the link counts it as several, which corrupts any rework or
// turns-to-completion metric built on top.
func TestParentLinkSurvivesEventsThatDoNotCarryIt(t *testing.T) {
	var s Session
	s.Apply(ev(UserPrompt, 1))
	linked := ev(AssistantTurn, 2)
	linked.ParentSessionID = "parent-sess"
	s.Apply(linked)
	s.Apply(ev(ToolCall, 3))

	if s.ParentSessionID != "parent-sess" {
		t.Fatalf("ParentSessionID = %q, want it retained once seen", s.ParentSessionID)
	}
}

// Hook events carry cwd and branch; many transcript-derived events do not. The
// rollup has to keep what it learned rather than being blanked by the next
// event that happens to omit them, because cwd is how a session maps to a repo
// and therefore how repo-scoped capture policy is applied.
func TestContextFieldsAreNotBlankedByEventsThatOmitThem(t *testing.T) {
	var s Session
	located := ev(SessionStarted, 1)
	located.Cwd = "/repo"
	located.GitBranch = "feature"
	s.Apply(located)
	s.Apply(ev(ToolCall, 2))

	if s.Cwd != "/repo" {
		t.Errorf("Cwd = %q, want it retained", s.Cwd)
	}
	if s.GitBranch != "feature" {
		t.Errorf("GitBranch = %q, want it retained", s.GitBranch)
	}
}

func TestIdentityIsTakenFromTheFirstEventFolded(t *testing.T) {
	var s Session
	s.Apply(ev(ToolCall, 1))
	if s.SessionID != "sess" {
		t.Errorf("SessionID = %q, want %q", s.SessionID, "sess")
	}
	if s.Source != SourceClaudeCode {
		t.Errorf("Source = %q, want %q", s.Source, SourceClaudeCode)
	}
}
