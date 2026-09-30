package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Analytics reads: aggregates over the sessions rollup.
//
// Everything here reads the sessions table alone. The rollup is maintained at
// ingest precisely so that questions like "tokens per day" are a GROUP BY over
// one table with an index on started_at, rather than a scan of seventy thousand
// event bodies. If a number this page needs is not on the rollup, the fix is to
// put it on the rollup, not to reach into events at read time.
//
// Visibility follows the reading model everywhere else: an admin aggregates
// everyone, a member aggregates only themselves. The scoping happens here, in
// the store, so a handler that forgets to scope cannot leak the fleet's numbers
// to a member — the same reasoning as canRead, applied to aggregates.

// DayUsage is one day's totals.
type DayUsage struct {
	Day         time.Time `json:"day"`
	Email       string    `json:"email,omitempty"`
	SessionType string    `json:"session_type,omitempty"`
	Sessions    int64     `json:"sessions"`
	People      int64     `json:"people"`
	TokensIn    int64     `json:"tokens_in"`
	TokensOut   int64     `json:"tokens_out"`
	CacheRead   int64     `json:"cache_read"`
	CacheWrite  int64     `json:"cache_write"`
	CostUSD     float64   `json:"cost_usd"`
	ToolCalls   int64     `json:"tool_calls"`
	Errors      int64     `json:"errors"`
}

// PersonUsage is one person's totals over a range.
type PersonUsage struct {
	Email       string    `json:"email"`
	DisplayName string    `json:"display_name,omitempty"`
	Sessions    int64     `json:"sessions"`
	TokensIn    int64     `json:"tokens_in"`
	TokensOut   int64     `json:"tokens_out"`
	CacheRead   int64     `json:"cache_read"`
	CostUSD     float64   `json:"cost_usd"`
	ToolCalls   int64     `json:"tool_calls"`
	Errors      int64     `json:"errors"`
	LastActive  time.Time `json:"last_active"`
}

// scopeEmails returns the emails a query may aggregate over: the requested set
// for an admin, and exactly the viewer for anyone else. Returning it from one
// place keeps the privacy rule out of each query's WHERE clause, where three
// hand-written copies would eventually disagree.
func scopeEmails(v Viewer, requested []string) []string {
	if v.IsAdmin() {
		return requested
	}
	return []string{v.Email}
}

// bucketUnits are the units DailyUsage may truncate to. A whitelist because the
// unit is interpolated into date_trunc's argument position via parameter, but
// validating it here turns a typo into a named error instead of a Postgres one.
var bucketUnits = map[string]bool{"hour": true, "day": true}

// DailyUsage returns per-day totals over [from, to), bucketed in tz.
//
// The zone matters and is the caller's, not the database's: a day boundary drawn
// in UTC puts every evening session in Bengaluru on tomorrow's bar, and the
// person reading the chart knows which day they worked.
func (s *Store) DailyUsage(ctx context.Context, v Viewer, from, to time.Time, tz, unit string, emails, types []string) ([]DayUsage, error) {
	emails = scopeEmails(v, emails)
	if err := validZone(tz); err != nil {
		return nil, err
	}
	if !bucketUnits[unit] {
		unit = "day"
	}
	q := `
		SELECT date_trunc($4, started_at AT TIME ZONE $3) AS day,
		       count(*), count(DISTINCT email),
		       coalesce(sum(tokens_input),0), coalesce(sum(tokens_output),0),
		       coalesce(sum(tokens_cache_read),0), coalesce(sum(tokens_cache_write),0),
		       coalesce(sum(cost_usd),0), coalesce(sum(tool_calls),0), coalesce(sum(errors),0)
		FROM sessions
		WHERE started_at >= $1 AND started_at < $2
		  AND ` + sessionTypePredicate("sessions", "$5")
	args := []any{from, to, tz, unit, validSessionTypes(types)}
	if len(emails) > 0 {
		q += ` AND email = ANY($6)`
		args = append(args, emails)
	}
	q += ` GROUP BY 1 ORDER BY 1`

	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: daily usage: %w", err)
	}
	defer rows.Close()

	out := []DayUsage{}
	for rows.Next() {
		var d DayUsage
		if err := rows.Scan(&d.Day, &d.Sessions, &d.People, &d.TokensIn, &d.TokensOut,
			&d.CacheRead, &d.CacheWrite, &d.CostUSD, &d.ToolCalls, &d.Errors); err != nil {
			return nil, fmt.Errorf("store: scan daily usage: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// DailyUsageByType returns per-bucket totals split by session type, for the
// stacked charts. It takes no type filter on purpose: a stack's whole point is
// showing every part of the whole, and a filtered stack is just a shorter bar
// with a missing explanation. The store default does not apply here either:
// the empty and internal classes are two of the parts, and a chart that
// shows how much of a day was harness noise is the one place they belong.
func (s *Store) DailyUsageByType(ctx context.Context, v Viewer, from, to time.Time, tz, unit string, emails []string) ([]DayUsage, error) {
	emails = scopeEmails(v, emails)
	if err := validZone(tz); err != nil {
		return nil, err
	}
	if !bucketUnits[unit] {
		unit = "day"
	}
	q := `
		SELECT date_trunc($4, started_at AT TIME ZONE $3) AS day, session_type,
		       count(*), coalesce(sum(cost_usd),0)
		FROM sessions
		WHERE started_at >= $1 AND started_at < $2`
	args := []any{from, to, tz, unit}
	if len(emails) > 0 {
		q += ` AND email = ANY($5)`
		args = append(args, emails)
	}
	q += ` GROUP BY 1, 2 ORDER BY 1, 2`

	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: daily usage by type: %w", err)
	}
	defer rows.Close()

	out := []DayUsage{}
	for rows.Next() {
		var d DayUsage
		if err := rows.Scan(&d.Day, &d.SessionType, &d.Sessions, &d.CostUSD); err != nil {
			return nil, fmt.Errorf("store: scan daily usage by type: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// comparableLimit bounds a per-person timeseries. Eight is where a line chart
// stops being readable and starts being modern art; past it the answer is the
// table, not more lines.
const comparableLimit = 8

// DailyUsageByPerson returns per-day, per-person rows for a small set of people.
func (s *Store) DailyUsageByPerson(ctx context.Context, v Viewer, from, to time.Time, tz, unit string, emails, types []string) ([]DayUsage, error) {
	emails = scopeEmails(v, emails)
	if len(emails) == 0 {
		return []DayUsage{}, nil
	}
	if len(emails) > comparableLimit {
		emails = emails[:comparableLimit]
	}
	if err := validZone(tz); err != nil {
		return nil, err
	}
	if !bucketUnits[unit] {
		unit = "day"
	}
	rows, err := s.db.Query(ctx, `
		SELECT date_trunc($5, started_at AT TIME ZONE $3) AS day, email,
		       count(*), coalesce(sum(tokens_input),0), coalesce(sum(tokens_output),0),
		       coalesce(sum(tokens_cache_read),0), coalesce(sum(tokens_cache_write),0),
		       coalesce(sum(cost_usd),0), coalesce(sum(tool_calls),0), coalesce(sum(errors),0)
		FROM sessions
		WHERE started_at >= $1 AND started_at < $2 AND email = ANY($4)
		  AND `+sessionTypePredicate("sessions", "$6")+`
		GROUP BY 1, 2 ORDER BY 1, 2`, from, to, tz, emails, unit, validSessionTypes(types))
	if err != nil {
		return nil, fmt.Errorf("store: daily usage by person: %w", err)
	}
	defer rows.Close()

	out := []DayUsage{}
	for rows.Next() {
		var d DayUsage
		if err := rows.Scan(&d.Day, &d.Email, &d.Sessions, &d.TokensIn, &d.TokensOut,
			&d.CacheRead, &d.CacheWrite, &d.CostUSD, &d.ToolCalls, &d.Errors); err != nil {
			return nil, fmt.Errorf("store: scan daily usage by person: %w", err)
		}
		d.People = 1
		out = append(out, d)
	}
	return out, rows.Err()
}

// PersonUsage sort keys, whitelisted because the sort key is interpolated into
// ORDER BY and anything not on this list would be a SQL injection wearing a
// query parameter's clothes.
// The tokens entry repeats the aggregate expressions rather than adding the
// aliases, because Postgres accepts an output name in ORDER BY only bare — an
// expression over aliases like "tokens_in + tokens_out" is a column reference
// error, not a sort.
var personSorts = map[string]string{
	"sessions": "sessions DESC",
	"tokens":   "coalesce(sum(s.tokens_input),0) + coalesce(sum(s.tokens_output),0) DESC",
	"cost":     "cost_usd DESC",
	"recent":   "last_active DESC",
}

// PersonUsage returns per-person totals over [from, to), ordered by sortKey,
// filtered to emails or display names starting with prefix, at most top rows.
func (s *Store) PersonUsage(ctx context.Context, v Viewer, from, to time.Time, sortKey, prefix string, top int, types []string) ([]PersonUsage, error) {
	order, ok := personSorts[sortKey]
	if !ok {
		order = personSorts["tokens"]
	}
	if top <= 0 || top > 200 {
		top = 25
	}
	emails := scopeEmails(v, nil)

	q := `
		SELECT s.email, coalesce(p.display_name, ''),
		       count(*) AS sessions,
		       coalesce(sum(s.tokens_input),0)  AS tokens_in,
		       coalesce(sum(s.tokens_output),0) AS tokens_out,
		       coalesce(sum(s.tokens_cache_read),0),
		       coalesce(sum(s.cost_usd),0) AS cost_usd,
		       coalesce(sum(s.tool_calls),0), coalesce(sum(s.errors),0),
		       max(s.started_at) AS last_active
		FROM sessions s LEFT JOIN principals p ON p.email = s.email
		WHERE s.started_at >= $1 AND s.started_at < $2
		  AND ` + sessionTypePredicate("s", "$3")
	args := []any{from, to, validSessionTypes(types)}
	n := 3
	if len(emails) > 0 {
		n++
		q += fmt.Sprintf(` AND s.email = ANY($%d)`, n)
		args = append(args, emails)
	}
	if prefix = strings.TrimSpace(prefix); prefix != "" {
		n++
		// Prefix on the email or on the display name, case-insensitively: the
		// filter box autocompletes both, so both have to match. The input is
		// escaped because it is a literal from a search box, not a pattern — an
		// unescaped % here would turn "show me people starting with %" into
		// "show me everyone", which on a member-scoped page is merely wrong and
		// on this one is wrong at fleet width.
		q += fmt.Sprintf(` AND (s.email ILIKE $%d || '%%' OR coalesce(p.display_name,'') ILIKE $%d || '%%')`, n, n)
		args = append(args, escapeLike(prefix))
	}
	n++
	q += ` GROUP BY 1, 2 ORDER BY ` + order + fmt.Sprintf(` LIMIT $%d`, n)
	args = append(args, top)

	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: person usage: %w", err)
	}
	defer rows.Close()

	out := []PersonUsage{}
	for rows.Next() {
		var p PersonUsage
		if err := rows.Scan(&p.Email, &p.DisplayName, &p.Sessions, &p.TokensIn, &p.TokensOut,
			&p.CacheRead, &p.CostUSD, &p.ToolCalls, &p.Errors, &p.LastActive); err != nil {
			return nil, fmt.Errorf("store: scan person usage: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SessionCounts reports how many sessions each person has, ever.
//
// It exists for the roster page, whose Sessions column shipped rendering a
// hard-coded zero with a comment admitting as much. The count is total rather
// than windowed because the roster's question is "does this person's capture
// work at all", and a person on holiday for two weeks still has their history.
// The store default applies: a machine that spawns fifty lifecycle-only
// sessions a day would otherwise read as the roster's busiest person.
func (s *Store) SessionCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.Query(ctx, `SELECT email, count(*) FROM sessions WHERE `+sessionTypeDefault("sessions")+` GROUP BY email`)
	if err != nil {
		return nil, fmt.Errorf("store: session counts: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var email string
		var n int64
		if err := rows.Scan(&email, &n); err != nil {
			return nil, fmt.Errorf("store: scan session count: %w", err)
		}
		out[email] = n
	}
	return out, rows.Err()
}

// FilterOptions returns the values worth offering in the dashboard's filter
// dropdowns: the people whose sessions the viewer may see, and the repositories
// among them.
//
// Scoped like every other read. A member's repo dropdown lists their own
// repositories, not the org's, because the org's is a directory of who works on
// what — which is exactly the kind of thing the visibility model exists to not
// leak through a side channel.
func (s *Store) FilterOptions(ctx context.Context, v Viewer) (people, repos []string, err error) {
	emails := scopeEmails(v, nil)

	// Both lists are capped: an autocomplete with three hundred entries is a
	// scroll nobody finishes, and the box still accepts anything typed — the
	// list is a convenience, not a constraint.
	pq := `SELECT DISTINCT email FROM sessions`
	rq := `SELECT DISTINCT repo FROM sessions WHERE repo IS NOT NULL AND repo <> ''`
	var pargs, rargs []any
	if len(emails) > 0 {
		pq += ` WHERE email = ANY($1)`
		rq += ` AND email = ANY($1)`
		pargs = append(pargs, emails)
		rargs = append(rargs, emails)
	}
	pq += ` ORDER BY email LIMIT 60`
	rq += ` ORDER BY repo LIMIT 60`

	rows, err := s.db.Query(ctx, pq, pargs...)
	if err != nil {
		return nil, nil, fmt.Errorf("store: filter people: %w", err)
	}
	for rows.Next() {
		var e string
		if err := rows.Scan(&e); err != nil {
			rows.Close()
			return nil, nil, err
		}
		people = append(people, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	rows, err = s.db.Query(ctx, rq, rargs...)
	if err != nil {
		return nil, nil, fmt.Errorf("store: filter repos: %w", err)
	}
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			rows.Close()
			return nil, nil, err
		}
		repos = append(repos, r)
	}
	rows.Close()
	return people, repos, rows.Err()
}

// VersionFirstSeen reports when each agent version was first seen in a health
// report. It is what turns a bare hash on the fleet page into an age: "runs
// 580e794, first reported 5 Aug" is a statement someone can act on, where the
// hash alone is a question they have to bring to an engineer.
func (s *Store) VersionFirstSeen(ctx context.Context) (map[string]time.Time, error) {
	rows, err := s.db.Query(ctx, `
		SELECT report->>'agent_version', min(received_at)
		FROM health_reports
		WHERE coalesce(report->>'agent_version', '') <> ''
		GROUP BY 1`)
	if err != nil {
		return nil, fmt.Errorf("store: version first seen: %w", err)
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var v string
		var at time.Time
		if err := rows.Scan(&v, &at); err != nil {
			return nil, fmt.Errorf("store: scan version first seen: %w", err)
		}
		out[v] = at
	}
	return out, rows.Err()
}

// escapeLike makes a user-typed string literal inside an ILIKE pattern.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

func escapeLike(s string) string { return likeEscaper.Replace(s) }

// validZone refuses a timezone string Postgres would reject mid-query.
//
// The zone reaches AT TIME ZONE as a parameter, so injection is not the risk —
// a typo'd zone failing every analytics query with an opaque Postgres error is,
// and this turns it into a message that names the input.
func validZone(tz string) error {
	if tz == "" {
		return fmt.Errorf("store: empty timezone for day bucketing")
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return fmt.Errorf("store: unusable timezone %q: %w", tz, err)
	}
	return nil
}
