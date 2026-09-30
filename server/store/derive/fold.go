package derive

import (
	"sort"
	"strconv"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
)

// Row is the column projection of one stored event: everything the fold
// needs and nothing that lives in the body. The fold never reads a body,
// which is what keeps a re-fold of a session an index-bound read rather than
// a detoast of megabytes, and what lets the runner fold history at the same
// cost as a live session.
type Row struct {
	ID         string
	Seq        int64
	Type       event.Type
	Origin     event.Origin
	AgentID    string
	OccurredAt time.Time
	PromptID   string
	MessageID  string
	ToolUseID  string
	// Kind is messages.kind for prompt and assistant rows, "" for rows that
	// have no message or were stored before the column existed.
	Kind normalize.Kind
	// HasText and HasUsage say what an assistant or prompt row carries;
	// TextHash is the first eight hex digits of sha256 over the message text,
	// "" when there is none. Text itself never enters the fold.
	HasText  bool
	HasUsage bool
	TextHash string
}

// Options are the facts about the session the projection does not carry.
type Options struct {
	// SessionEnded says the session's end marker was seen, which is what
	// tells an exchange with no answer yet from one that never got one.
	SessionEnded bool
}

// Role is what an event is inside its turn.
type Role string

const (
	// RolePrompt is the canonical copy of the prompt that opened the turn.
	RolePrompt Role = "prompt"
	// RoleFinal is the canonical copy of the answer that closed it.
	RoleFinal Role = "final"
	// RoleWork is everything else the turn holds: tools, files, partial
	// answers, notifications, the copies a reader renders once.
	RoleWork Role = "work"
	// RoleSuperseded is a copy the fold elected out: a hook prompt or answer
	// whose transcript twin is canonical, or a transcript tool row whose
	// hook twin carries the fuller output. Still the turn's, still stored,
	// rendered by nobody.
	RoleSuperseded Role = "superseded"
)

// Outcome is what became of an exchange.
type Outcome string

const (
	OutcomeAnswered         Outcome = "answered"
	OutcomeInterrupted      Outcome = "interrupted"
	OutcomeNoAnswerCaptured Outcome = "no_answer_captured"
	OutcomeNoWork           Outcome = "no_work"
	OutcomeInProgress       Outcome = "in_progress"
	// HeadKey names the turn that holds a thread's rows from before its first
	// prompt: a session whose head was not imported still has work to show.
	HeadKey = "head"
)

// TurnEvent is one event's membership in a turn.
type TurnEvent struct {
	ID   string
	Role Role
}

// Turn is one logical exchange: a prompt, the work it caused and the answer
// it got, whichever capture paths recorded them.
type Turn struct {
	Thread string
	Index  int
	Key    string
	Kind   normalize.Kind

	PromptEventID string
	FinalEventID  string
	Outcome       Outcome

	StartedAt       time.Time
	FirstActivityAt time.Time
	LastActivityAt  time.Time
	AnsweredAt      time.Time

	WallMS            int64
	ActiveMS          int64
	IdleMS            int64
	WaitingForHumanMS int64

	Origins      []string
	Merged       int
	Prompts      int
	ToolCalls    int
	Errors       int
	Subagents    int
	FilesChanged int

	Events []TurnEvent
}

// Result is what a fold produced.
type Result struct {
	Turns []Turn
	// Superseded maps a hook row's id to the transcript row that stands in
	// for it. These are the only rows that ever carry events.superseded_by.
	Superseded map[string]string
}

const (
	// pairWindow is how far apart two copies of one prompt may be stamped
	// and still be the same prompt. The hook stamps the moment the person
	// pressed enter; the transcript record is written when the harness
	// files it. Measured on production (research/r6 F3): a five-second
	// window matched exactly as many copies as exact text did, and none
	// more at thirty.
	pairWindow = 5 * time.Second
	// idleGap is the pause between two events of one turn beyond which the
	// time is idle rather than active: a person reading, a laptop asleep,
	// not the agent working.
	idleGap = 5 * time.Minute
)

// FoldTurns folds a session's rows into turns.
//
// The rows are one session's, of any thread and any origin, in any order;
// the fold sorts them. The algorithm per thread, in this order and not
// another: pair prompt copies across origins first (by prompt id when both
// carry one, else by identical text within pairWindow), then name each
// prompt group (pid:<prompt_id> when either copy has one, ts:<second>:<hash8>
// otherwise, with an ordinal when a name would repeat), then attach every
// other row by prompt id first and by order second. Pairing before naming is
// what makes an old hook prompt with no id and its keyed transcript twin one
// turn rather than two; naming after pairing is what makes the key the
// group's rather than a row's.
func FoldTurns(rows []Row, opts Options) Result {
	res := Result{Superseded: map[string]string{}}
	byThread := map[string][]*Row{}
	for i := range rows {
		r := &rows[i]
		key := CanonicalThread(r.AgentID)
		byThread[key] = append(byThread[key], r)
	}
	threads := make([]string, 0, len(byThread))
	for k := range byThread {
		threads = append(threads, k)
	}
	sort.Slice(threads, func(i, j int) bool {
		// Main thread first, then agent threads by the time they started.
		if threads[i] == "" || threads[j] == "" {
			return threads[i] == ""
		}
		a, b := earliest(byThread[threads[i]]), earliest(byThread[threads[j]])
		if !a.Equal(b) {
			return a.Before(b)
		}
		return threads[i] < threads[j]
	})

	var main []*turn
	var mainIndex map[string]*turn
	folded := make([][]*turn, 0, len(threads))
	for _, thread := range threads {
		trs, pidIndex := foldThread(thread, byThread[thread], opts, res.Superseded)
		folded = append(folded, trs)
		if thread == "" {
			main, mainIndex = trs, pidIndex
		} else if len(byThread[thread]) > 0 {
			// A subagent belongs to the main-thread turn that spawned it:
			// the one its first row's prompt id names, else the one in
			// progress when it started.
			first := byThread[thread][0]
			if parent := attachTarget(main, mainIndex, first.PromptID, first.OccurredAt); parent != nil {
				parent.Subagents++
			}
		}
	}
	// Copied out only once every thread has been seen: the subagent counts
	// above land on the main thread's turns after the main thread folded.
	for _, trs := range folded {
		for _, t := range trs {
			res.Turns = append(res.Turns, t.Turn)
		}
	}
	return res
}

func earliest(rows []*Row) time.Time {
	var t time.Time
	for _, r := range rows {
		if t.IsZero() || r.OccurredAt.Before(t) {
			t = r.OccurredAt
		}
	}
	return t
}

// group is one prompt and its copies: the transcript row first when there is
// one, then the hook row.
type group struct {
	rows      []*Row
	canonical *Row
	pid       string
	pids      []string
	kind      normalize.Kind
	at        time.Time
	seq       int64
	hash      string
	// dupOf names the group this one is a walk duplicate of: the same prompt
	// stored again under the same prompt id, or, with no prompt id, from
	// the same origin with identical text inside the same second. A
	// duplicate's rows are the turn's and are elected out behind dupOf's
	// canonical row.
	dupOf *group
	// owner is the turn that adopted the group.
	owner *turn
}

// turn is a Turn under construction.
type turn struct {
	Turn
	groups []*group
	work   []*Row
	// closed is set once an answer with text has attached, which is what
	// "the last closed turn" means when a notification looks for a home.
	closed bool
	// opened is set once a prompt group that opens a turn has been adopted;
	// a second opener under the same prompt id is a copy of the first, and a
	// copy is merged rather than counted.
	opened bool
	// interruptedAt is the time of the last interruption marker in the turn.
	interruptedAt time.Time
}

func (t *turn) attachedAt(at time.Time) {
	if t.LastActivityAt.IsZero() || at.After(t.LastActivityAt) {
		t.LastActivityAt = at
	}
}

// item is one unit of the time-ordered sweep: a prompt group or a row that
// is not a prompt.
type item struct {
	at  time.Time
	seq int64
	id  string
	g   *group
	r   *Row
}

func foldThread(thread string, rows []*Row, opts Options, superseded map[string]string) ([]*turn, map[string]*turn) {
	sort.SliceStable(rows, func(i, j int) bool { return rowLess(rows[i], rows[j]) })

	var prompts []*Row
	var others []*Row
	threadEnded := opts.SessionEnded
	for _, r := range rows {
		switch r.Type {
		case event.UserPrompt:
			prompts = append(prompts, r)
		case event.SessionStarted, event.SessionEnded:
			// Lifecycle, not an exchange.
		case event.SubagentEnd:
			threadEnded = true
			others = append(others, r)
		default:
			others = append(others, r)
		}
	}
	groups := pairPrompts(prompts)

	items := make([]item, 0, len(groups)+len(others))
	for _, g := range groups {
		items = append(items, item{at: g.at, seq: g.seq, id: g.canonical.ID, g: g})
	}
	for _, r := range others {
		items = append(items, item{at: r.OccurredAt, seq: r.Seq, id: r.ID, r: r})
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if !a.at.Equal(b.at) {
			return a.at.Before(b.at)
		}
		// A prompt group sorts ahead of a row stamped at the same instant:
		// the answer cannot precede its question.
		if (a.g != nil) != (b.g != nil) {
			return a.g != nil
		}
		if a.seq != b.seq {
			return a.seq < b.seq
		}
		return a.id < b.id
	})

	var turns []*turn
	pidIndex := map[string]*turn{}
	keys := map[string]int{}
	var head *turn

	// pending holds bookkeeping ahead of any turn on the thread: a
	// subagent's start marker, a compaction marker. Such a row belongs to
	// the exchange it precedes (the harness spawns an agent and then writes
	// its task), so it joins the first turn the thread opens, or a head turn
	// of its own when the thread never opens one. Dropped, it was absent
	// from turn_events and from the content_events recount while ingest had
	// counted it (review-2 finding 28).
	var pending []*Row
	place := func(t *turn) {
		for _, r := range pending {
			t.work = append(t.work, r)
			t.attachedAt(r.OccurredAt)
		}
		pending = nil
	}
	// dupIndex names the first group of a run of walk duplicates by the
	// origin, second and text of its canonical row, for the groups that
	// carry no prompt id; see dupKey.
	dupIndex := map[string]*group{}

	open := func(g *group, kind normalize.Kind) *turn {
		t := &turn{Turn: Turn{Thread: thread, Kind: kind, StartedAt: g.at}}
		// The ordinal is counted on the base key, not on the suffixed one:
		// the count for "k" must keep growing through "k#2", "k#3", or the
		// third group is named "#2" again and turns' UNIQUE (session_id,
		// thread, turn_key) refuses the whole session. Production holds the
		// shape: Codex prompts stored three to six times in one second with
		// no prompt id.
		base := groupKey(g)
		n := keys[base]
		keys[base]++
		t.Key = base
		if n > 0 {
			t.Key += "#" + strconv.Itoa(n+1)
		}
		turns = append(turns, t)
		place(t)
		return t
	}
	// adopt folds a group into a turn. dupOf names the group this one is a
	// copy of, or nil: a copy is merged, never counted, and its rows are
	// elected out in finish. One logical prompt counts once, whichever path
	// or how many times it was captured, so user_turns (the sum of prompts)
	// stays at the number of prompts a person typed.
	adopt := func(t *turn, g *group, dupOf *group) {
		t.groups = append(t.groups, g)
		g.owner = t
		g.dupOf = dupOf
		switch {
		case dupOf != nil:
			// Counted per row in finish.
		case !opensTurn(g.kind):
			// A notification, a caveat, a summary folded into the turn is the
			// harness's own record and counts as one of the turn's prompts.
			t.Prompts++
		case !t.opened:
			t.opened = true
			t.Prompts++
		default:
			// A second opener under the turn's own prompt id is a copy the
			// walker stored twice (a file walked under two emission rules
			// before record identity existed).
			g.dupOf = t.groups[0]
		}
		for _, pid := range g.pids {
			if _, taken := pidIndex[pid]; !taken {
				pidIndex[pid] = t
			}
		}
		if k := dupKey(g); k != "" {
			if _, seen := dupIndex[k]; !seen {
				dupIndex[k] = g
			}
		}
		t.attachedAt(g.at)
		if g.kind == normalize.KindInterrupted {
			t.interruptedAt = g.at
		}
	}

	for _, it := range items {
		if it.g != nil {
			g := it.g
			// Walk duplicates: the Codex walker stores one prompt three to
			// six times, from one origin, inside one second, with no prompt
			// id to name it by (production, review-1's prod-tskey.out). Such
			// a group merges into the turn that adopted the first copy and is
			// counted once (lead decision after review-2, finding 29); a
			// person really typing the same text twice inside one second is
			// the accepted cost.
			if k := dupKey(g); k != "" {
				if first, ok := dupIndex[k]; ok {
					adopt(first.owner, g, first)
					continue
				}
			}
			if opensTurn(g.kind) {
				// The same prompt id names the same prompt: a copy delivered
				// under a second event id merges rather than repeats.
				if g.pid != "" {
					if t, ok := pidIndex[g.pid]; ok && t.Key == "pid:"+g.pid {
						adopt(t, g, nil)
						continue
					}
				}
				adopt(open(g, g.kind), g, nil)
				continue
			}
			// A notification, a caveat, a summary: the harness's own record
			// folds into the turn its prompt id names, else the last closed
			// turn, else whatever is in progress. With nothing to fold into
			// it opens a turn of its own kind, so nothing is dropped. An
			// interruption is the exception to "last closed": it stops the
			// exchange in progress, so without a prompt id it belongs to the
			// last turn, whose answer it is the absence of, and never to an
			// earlier turn that already has one.
			t := byPid(pidIndex, g.pids)
			if t == nil && g.kind != normalize.KindInterrupted {
				t = lastClosed(turns)
			}
			if t == nil && len(turns) > 0 {
				t = turns[len(turns)-1]
			}
			if t == nil {
				t = open(g, g.kind)
			}
			adopt(t, g, nil)
			continue
		}

		r := it.r
		t := byPid(pidIndex, []string{r.PromptID})
		if t == nil && len(turns) > 0 {
			t = turns[len(turns)-1]
		}
		if t == nil {
			if isMarker(r.Type) {
				pending = append(pending, r)
				continue
			}
			t = openHead(&head, &turns, keys, thread, r.OccurredAt)
			place(t)
		}
		t.work = append(t.work, r)
		t.attachedAt(r.OccurredAt)
		if r.Type == event.AssistantTurn && r.HasText {
			t.closed = true
		}
	}
	if len(pending) > 0 {
		// Markers and nothing else: a subagent captured by hooks alone has
		// its start and end and no task; they are still its work to show.
		place(openHead(&head, &turns, keys, thread, pending[0].OccurredAt))
	}

	for i, t := range turns {
		finish(t, superseded)
		t.Index = i
		last := i == len(turns)-1
		t.Outcome = outcome(t, last, threadEnded)
		if i+1 < len(turns) {
			if wait := turns[i+1].StartedAt.Sub(t.LastActivityAt); wait > 0 {
				t.WaitingForHumanMS = wait.Milliseconds()
			}
		}
	}
	return turns, pidIndex
}

// openHead makes the thread's head turn, the one that holds rows from
// before its first prompt, ahead of every other turn.
func openHead(head **turn, turns *[]*turn, keys map[string]int, thread string, at time.Time) *turn {
	if *head == nil {
		*head = &turn{Turn: Turn{Thread: thread, Key: HeadKey, StartedAt: at}}
		keys[HeadKey]++
		*turns = append([]*turn{*head}, *turns...)
	}
	return *head
}

// isMarker reports the bookkeeping rows that have no exchange of their own.
func isMarker(t event.Type) bool {
	return t == event.SubagentStart || t == event.SubagentEnd || t == event.Compaction
}

// dupKey names a group for the walk-duplicate rule: its canonical row's
// origin, second and text hash, for groups with text and no prompt id.
// Groups with a prompt id are named by it, and groups without text have
// nothing to compare, so both get "" and take ordinals instead.
func dupKey(g *group) string {
	if g.pid != "" || g.hash == "" {
		return ""
	}
	return string(g.canonical.Origin) + "|" + strconv.FormatInt(g.canonical.OccurredAt.Unix(), 10) + "|" + g.hash
}

func rowLess(a, b *Row) bool {
	if !a.OccurredAt.Equal(b.OccurredAt) {
		return a.OccurredAt.Before(b.OccurredAt)
	}
	if a.Seq != b.Seq {
		return a.Seq < b.Seq
	}
	return a.ID < b.ID
}

// opensTurn reports whether a prompt of this kind starts an exchange: a
// person's prompt, a command, the task that drives a subagent, or a row
// stored before kinds existed, which is read as a person's rather than
// silently folded away.
func opensTurn(k normalize.Kind) bool {
	return normalize.IsHumanKind(k) || k == "" || k == normalize.KindSubagentTask
}

func byPid(index map[string]*turn, pids []string) *turn {
	for _, pid := range pids {
		if pid == "" {
			continue
		}
		if t, ok := index[pid]; ok {
			return t
		}
	}
	return nil
}

func lastClosed(turns []*turn) *turn {
	for i := len(turns) - 1; i >= 0; i-- {
		if turns[i].closed {
			return turns[i]
		}
	}
	return nil
}

// attachTarget finds the main-thread turn a subagent belongs to.
func attachTarget(turns []*turn, index map[string]*turn, pid string, at time.Time) *turn {
	if t := byPid(index, []string{pid}); t != nil {
		return t
	}
	var found *turn
	for _, t := range turns {
		if !t.StartedAt.After(at) {
			found = t
		}
	}
	return found
}

func groupKey(g *group) string {
	if g.pid != "" {
		return "pid:" + g.pid
	}
	hash := g.hash
	if hash == "" {
		hash = "notext"
	}
	return "ts:" + strconv.FormatInt(g.at.Unix(), 10) + ":" + hash
}

// pairPrompts pairs prompt copies across origins and returns the groups in
// time order. Pass one pairs by prompt id; pass two pairs what is left by
// identical text inside the window, nearest first. Both passes require the
// two rows to be the same kind of thing, because one prompt id is shared by
// a person's prompt and the notifications the harness injected under it.
func pairPrompts(prompts []*Row) []*group {
	var hooks, transcripts []*Row
	for _, p := range prompts {
		if p.Origin == event.OriginHook {
			hooks = append(hooks, p)
		} else {
			transcripts = append(transcripts, p)
		}
	}
	twin := map[*Row]*Row{}
	taken := map[*Row]bool{}
	pair := func(h *Row, accept func(t *Row) bool) {
		var best *Row
		var bestGap time.Duration
		for _, t := range transcripts {
			if taken[t] || !accept(t) {
				continue
			}
			gap := absDuration(t.OccurredAt.Sub(h.OccurredAt))
			if best == nil || gap < bestGap {
				best, bestGap = t, gap
			}
		}
		if best != nil {
			twin[best] = h
			taken[best] = true
			taken[h] = true
		}
	}
	for _, h := range hooks {
		if h.PromptID == "" {
			continue
		}
		pair(h, func(t *Row) bool {
			return t.PromptID == h.PromptID && kindsAgree(h.Kind, t.Kind)
		})
	}
	for _, h := range hooks {
		if taken[h] || h.TextHash == "" {
			continue
		}
		pair(h, func(t *Row) bool {
			if t.TextHash != h.TextHash || !kindsAgree(h.Kind, t.Kind) {
				return false
			}
			if absDuration(t.OccurredAt.Sub(h.OccurredAt)) > pairWindow {
				return false
			}
			// Two people's prompts that happen to read the same are told
			// apart by the harness's ids when both carry one.
			if t.PromptID != "" && h.PromptID != "" && t.PromptID != h.PromptID &&
				normalize.IsHumanKind(t.Kind) && normalize.IsHumanKind(h.Kind) {
				return false
			}
			return true
		})
	}

	groups := make([]*group, 0, len(transcripts)+len(hooks))
	for _, t := range transcripts {
		g := &group{rows: []*Row{t}, canonical: t}
		if h, ok := twin[t]; ok {
			g.rows = append(g.rows, h)
		}
		groups = append(groups, g)
	}
	for _, h := range hooks {
		if taken[h] {
			continue
		}
		groups = append(groups, &group{rows: []*Row{h}, canonical: h})
	}
	for _, g := range groups {
		for _, r := range g.rows {
			if r.PromptID != "" {
				g.pids = append(g.pids, r.PromptID)
				if g.pid == "" {
					g.pid = r.PromptID
				}
			}
			if g.kind == "" {
				g.kind = r.Kind
			}
			if g.hash == "" {
				g.hash = r.TextHash
			}
			if g.at.IsZero() || r.OccurredAt.Before(g.at) {
				g.at, g.seq = r.OccurredAt, r.Seq
			}
		}
	}
	sort.SliceStable(groups, func(i, j int) bool {
		a, b := groups[i], groups[j]
		if !a.at.Equal(b.at) {
			return a.at.Before(b.at)
		}
		if a.seq != b.seq {
			return a.seq < b.seq
		}
		return a.canonical.ID < b.canonical.ID
	})
	return groups
}

func kindsAgree(a, b normalize.Kind) bool {
	if a == "" || b == "" {
		return true
	}
	if normalize.IsHumanKind(a) && normalize.IsHumanKind(b) {
		return true
	}
	return a == b
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// finish elects the canonical copies inside one turn and fills its counts
// and timings.
func finish(t *turn, superseded map[string]string) {
	roles := map[*Row]Role{}
	origins := map[string]bool{}
	var all []*Row

	// Prompts. The transcript copy is canonical for text; a hook copy that
	// paired with one is superseded by it. A walk duplicate's rows are all
	// copies: elected out behind the first copy's canonical row, counted as
	// merged, and marked when they are hook rows.
	for i, g := range t.groups {
		for _, r := range g.rows {
			all = append(all, r)
			origins[string(r.Origin)] = true
		}
		if g.dupOf != nil {
			for _, r := range g.rows {
				roles[r] = RoleSuperseded
				t.Merged++
				if r.Origin == event.OriginHook {
					superseded[r.ID] = g.dupOf.canonical.ID
				}
			}
			continue
		}
		role := RoleWork
		if i == 0 && opensTurn(g.kind) {
			role = RolePrompt
			t.PromptEventID = g.canonical.ID
		}
		roles[g.canonical] = role
		for _, r := range g.rows {
			if r != g.canonical {
				roles[r] = RoleSuperseded
				if r.Origin == event.OriginHook {
					superseded[r.ID] = g.canonical.ID
					t.Merged++
				}
			}
		}
	}

	var assistants, calls, results []*Row
	for _, r := range t.work {
		all = append(all, r)
		origins[string(r.Origin)] = true
		roles[r] = RoleWork
		switch r.Type {
		case event.AssistantTurn:
			assistants = append(assistants, r)
		case event.ToolCall:
			calls = append(calls, r)
		case event.ToolResult, event.ToolFailed:
			results = append(results, r)
		case event.FileChanged:
			t.FilesChanged++
		}
	}

	// Answers. A hook copy pairs with its transcript twin by message id,
	// else by identical text inside the window; a hook Stop that captured
	// neither text nor usage proves only that a Stop fired and stands aside
	// for the nearest transcript answer when there is one.
	for _, h := range assistants {
		if h.Origin != event.OriginHook {
			continue
		}
		var tw *Row
		switch {
		case h.MessageID != "":
			tw = nearest(assistants, h, func(c *Row) bool { return c.MessageID == h.MessageID }, 0)
		case h.HasText:
			tw = nearest(assistants, h, func(c *Row) bool { return c.HasText && c.TextHash == h.TextHash }, pairWindow)
		case !h.HasUsage:
			tw = nearest(assistants, h, func(*Row) bool { return true }, 0)
		}
		if tw != nil {
			roles[h] = RoleSuperseded
			superseded[h.ID] = tw.ID
			t.Merged++
		}
	}
	var final *Row
	for _, a := range assistants {
		if !a.HasText || roles[a] == RoleSuperseded {
			continue
		}
		if final == nil || !rowLess(a, final) {
			final = a
		}
	}
	if final != nil {
		roles[final] = RoleFinal
		t.FinalEventID = final.ID
		t.AnsweredAt = final.OccurredAt
	}

	// Tools. The hook copy carries the fuller output and is canonical; its
	// transcript twin is elected out in the mapping only, never marked on
	// the row. Counts are by origin election: each origin counted alone,
	// the larger kept, so a copy can never be counted twice.
	t.ToolCalls = electTools(calls, roles, event.ToolCall)
	t.Errors = electTools(results, roles, event.ToolFailed)

	// Timings over every row the turn holds from its prompt on: a marker
	// placed ahead of the prompt is the turn's but not its activity.
	sort.SliceStable(all, func(i, j int) bool { return rowLess(all[i], all[j]) })
	var prev time.Time
	for _, r := range all {
		if r.OccurredAt.Before(t.StartedAt) {
			continue
		}
		if roles[r] != RolePrompt && roles[r] != RoleSuperseded && r.Type != event.UserPrompt && t.FirstActivityAt.IsZero() {
			t.FirstActivityAt = r.OccurredAt
		}
		if !prev.IsZero() {
			if gap := r.OccurredAt.Sub(prev); gap <= idleGap {
				t.ActiveMS += gap.Milliseconds()
			}
		}
		prev = r.OccurredAt
	}
	if t.LastActivityAt.IsZero() {
		t.LastActivityAt = t.StartedAt
	}
	t.WallMS = t.LastActivityAt.Sub(t.StartedAt).Milliseconds()
	if t.WallMS < 0 {
		t.WallMS = 0
	}
	t.IdleMS = t.WallMS - t.ActiveMS
	if t.IdleMS < 0 {
		t.IdleMS = 0
	}

	for _, r := range all {
		t.Events = append(t.Events, TurnEvent{ID: r.ID, Role: roles[r]})
	}
	for o := range origins {
		t.Origins = append(t.Origins, o)
	}
	sort.Strings(t.Origins)
}

// nearest finds the transcript row that accepts h, closest in time, within
// window when window is set.
func nearest(rows []*Row, h *Row, accept func(*Row) bool, window time.Duration) *Row {
	var best *Row
	var bestGap time.Duration
	for _, c := range rows {
		if c == h || c.Origin != event.OriginTranscript || !accept(c) {
			continue
		}
		gap := absDuration(c.OccurredAt.Sub(h.OccurredAt))
		if window > 0 && gap > window {
			continue
		}
		if best == nil || gap < bestGap {
			best, bestGap = c, gap
		}
	}
	return best
}

// electTools pairs the tool rows of one shape across origins and reports
// the elected count. Paired by tool_use id where both carry one, then by
// order for what is left; a transcript twin of a hook row is elected out of
// the mapping. The count is the larger origin's, with the transcript's on a
// tie.
func electTools(rows []*Row, roles map[*Row]Role, countType event.Type) int {
	var hooks, transcripts []*Row
	hookCount, transcriptCount := 0, 0
	for _, r := range rows {
		if r.Origin == event.OriginHook {
			hooks = append(hooks, r)
			if r.Type == countType {
				hookCount++
			}
		} else {
			transcripts = append(transcripts, r)
			if r.Type == countType {
				transcriptCount++
			}
		}
	}
	taken := map[*Row]bool{}
	for _, h := range hooks {
		if h.ToolUseID == "" {
			continue
		}
		for _, tr := range transcripts {
			if !taken[tr] && tr.ToolUseID == h.ToolUseID {
				taken[tr], taken[h] = true, true
				roles[tr] = RoleSuperseded
				break
			}
		}
	}
	// By order: the k-th unpaired hook row is the k-th unpaired transcript
	// row, which is the walker's and the hook's shared notion of sequence.
	ti := 0
	for _, h := range hooks {
		if taken[h] {
			continue
		}
		for ti < len(transcripts) && taken[transcripts[ti]] {
			ti++
		}
		if ti >= len(transcripts) {
			break
		}
		taken[transcripts[ti]], taken[h] = true, true
		roles[transcripts[ti]] = RoleSuperseded
		ti++
	}
	if hookCount > transcriptCount {
		return hookCount
	}
	return transcriptCount
}

func outcome(t *turn, last, ended bool) Outcome {
	hasFinal := t.FinalEventID != ""
	if !t.interruptedAt.IsZero() && (!hasFinal || t.interruptedAt.After(t.AnsweredAt)) {
		return OutcomeInterrupted
	}
	if hasFinal {
		return OutcomeAnswered
	}
	if last && !ended {
		return OutcomeInProgress
	}
	if len(t.work) == 0 {
		return OutcomeNoWork
	}
	return OutcomeNoAnswerCaptured
}
