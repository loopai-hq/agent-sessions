package slack

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// The facade the dashboard renders through. The web layer owns the page and
// the chrome; this package owns the tables and the rules. Every method takes
// the acting email explicitly because the web layer resolved the viewer
// already, and resolving twice is how two answers happen.

// SettingsView is everything the notifications settings page shows.
type SettingsView struct {
	Groups       []Group
	DefaultGroup int64
	LiveDisabled bool
	// DestLabels decorates raw destination ids with what a person would call
	// them ("#eng-pod", "@morgan"), best-effort from the caches: a miss just
	// shows the id, never an error.
	DestLabels map[string]string
	// DigestMode and DigestChannel are the standing digest preference,
	// shown beside the live settings because people will look for them in
	// one place.
	DigestMode    string
	DigestChannel string
}

// Settings assembles the page for one person.
func (m *Mirror) Settings(ctx context.Context, email string) (SettingsView, error) {
	var v SettingsView
	gs, err := listGroups(ctx, m.db, email)
	if err != nil {
		return v, err
	}
	v.Groups = gs
	p, err := readPrefs(ctx, m.db, email)
	if err != nil {
		return v, err
	}
	v.DigestMode, v.DigestChannel = string(p.Mode), p.Channel
	dg, err := defaultGroupFor(ctx, m.db, email)
	if err != nil {
		return v, err
	}
	v.DefaultGroup = dg
	if err := m.db.QueryRow(ctx,
		`SELECT coalesce(live_disabled, FALSE) FROM slack_prefs WHERE email = $1`,
		email).Scan(&v.LiveDisabled); err != nil && !noRowsErr(err) {
		return v, err
	}
	v.DestLabels = m.destLabels(ctx, v.Groups)
	return v, nil
}

// destLabels names the ids the groups point at, from whatever the caches
// already hold. Errors are swallowed on purpose: this is decoration, and a
// Slack hiccup must not take the settings page down with it.
func (m *Mirror) destLabels(ctx context.Context, gs []Group) map[string]string {
	out := make(map[string]string, len(gs))
	needCh, needU := false, false
	for _, g := range gs {
		switch {
		case strings.HasPrefix(g.Destination, "U"):
			needU = true
		case g.Destination != "dm":
			needCh = true
		}
	}
	if needCh {
		if chs, err := m.Channels(ctx, "", 0); err == nil {
			byID := make(map[string]string, len(chs))
			for _, c := range chs {
				byID[c.ID] = "#" + c.Name
			}
			for _, g := range gs {
				if l, ok := byID[g.Destination]; ok {
					out[g.Destination] = l
				}
			}
		}
	}
	if needU {
		if us, err := m.Users(ctx); err == nil {
			byID := make(map[string]string, len(us))
			for _, u := range us {
				byID[u.ID] = "@" + u.Handle
			}
			for _, g := range gs {
				if l, ok := byID[g.Destination]; ok {
					out[g.Destination] = l
				}
			}
		}
	}
	return out
}

// CreateGroup makes a group owned by email.
func (m *Mirror) CreateGroup(ctx context.Context, email, name, visibility, destination string) (Group, error) {
	return saveGroup(ctx, m.db, Group{
		Name: name, OwnerEmail: email, Visibility: visibility, Destination: destination,
	}, m.now())
}

// UpdateGroup edits a group email owns; ErrGroupNotFound covers absent and
// not-yours alike.
func (m *Mirror) UpdateGroup(ctx context.Context, email string, g Group) (Group, error) {
	g.OwnerEmail = email
	return saveGroup(ctx, m.db, g, m.now())
}

// SetLiveKill flips the per-person master switch and nothing else. The
// default-group column stays whatever it was: the dashboard stopped offering
// it, and a toggle that silently rewrote an unrelated preference would be the
// kind of surprise this page exists to avoid.
func (m *Mirror) SetLiveKill(ctx context.Context, email string, disabled bool) error {
	_, err := m.db.Exec(ctx, `
		INSERT INTO slack_prefs (email, mode, mirror_from, updated_at, live_disabled)
		VALUES ($1, 'off', $2, $2, $3)
		ON CONFLICT (email) DO UPDATE
		   SET live_disabled = $3, updated_at = $2`,
		email, m.now(), disabled)
	return err
}

// DeleteGroup removes a group email owns. The schema cascades: active
// attachments vanish with it and any default_group references null out;
// existing threads simply stop growing, which is detach semantics applied
// group-wide. Absent and not-yours are one answer, as everywhere.
func (m *Mirror) DeleteGroup(ctx context.Context, email string, id int64) error {
	return deleteGroup(ctx, m.db, email, id)
}

// SessionMirror is one active attachment shown on the session page.
type SessionMirror struct {
	GroupID   int64
	GroupName string
	Dest      string
}

// SessionMirrors lists a session's active attachments.
func (m *Mirror) SessionMirrors(ctx context.Context, sessionID string) ([]SessionMirror, error) {
	rows, err := m.db.Query(ctx, `
		SELECT g.id, g.name, g.destination
		  FROM session_mirrors sm JOIN slack_groups g ON g.id = sm.group_id
		 WHERE sm.session_id = $1 AND sm.detached_at IS NULL
		 ORDER BY g.name`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionMirror
	for rows.Next() {
		var s SessionMirror
		if err := rows.Scan(&s.GroupID, &s.GroupName, &s.Dest); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Attach and Detach are the facade forms of the API handlers' core, with the
// same ownership rule: the session must be email's own.
func (m *Mirror) Attach(ctx context.Context, email, sessionID, groupRef string) error {
	if ok, err := m.ownsSessionCtx(ctx, sessionID, email); err != nil || !ok {
		if err != nil {
			return err
		}
		return ErrGroupNotFound
	}
	g, err := resolveGroup(ctx, m.db, email, groupRef)
	if err != nil {
		return err
	}
	return attachSession(ctx, m.db, sessionID, g.ID, email, m.now())
}

// Detach removes one of the caller's own sessions from a group. A session the
// caller does not own reads as no such group, so the call reveals nothing.
func (m *Mirror) Detach(ctx context.Context, email, sessionID, groupRef string) error {
	if ok, err := m.ownsSessionCtx(ctx, sessionID, email); err != nil || !ok {
		if err != nil {
			return err
		}
		return ErrGroupNotFound
	}
	g, err := resolveGroup(ctx, m.db, email, groupRef)
	if err != nil {
		return err
	}
	return detachSession(ctx, m.db, sessionID, g.ID, m.now())
}

func (m *Mirror) ownsSessionCtx(ctx context.Context, sessionID, email string) (bool, error) {
	var one int
	err := m.db.QueryRow(ctx,
		`SELECT 1 FROM sessions WHERE session_id = $1 AND email = $2`, sessionID, email).Scan(&one)
	if err == nil {
		return true, nil
	}
	if noRowsErr(err) {
		return false, nil
	}
	return false, err
}

func noRowsErr(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// The directory caches behind the destination picker. One sweep of
// conversations.list (and one of users.list) serves every page render for ten
// minutes: the datalists want the whole directory so the browser can prune it
// natively as the person types, and a workspace's channel and people sets
// change on the timescale of days.
type channelCache struct {
	mu       sync.Mutex
	channels []Channel
	users    []User
	chanAt   time.Time
	userAt   time.Time
}

var chanCacheTTL = 10 * time.Minute

// Channels returns workspace channels whose names start with prefix,
// case-insensitively. limit <= 0 means all of them, which is what the
// datalist wants; the browser is the one doing the pruning.
func (m *Mirror) Channels(ctx context.Context, prefix string, limit int) ([]Channel, error) {
	lister, ok := m.slack.(interface {
		ListChannels(context.Context) ([]Channel, error)
	})
	if !ok {
		return nil, nil
	}
	m.chanCache.mu.Lock()
	stale := m.now().Sub(m.chanCache.chanAt) > chanCacheTTL || m.chanCache.channels == nil
	m.chanCache.mu.Unlock()
	if stale {
		chs, err := lister.ListChannels(ctx)
		if err != nil {
			return nil, err
		}
		sort.Slice(chs, func(i, j int) bool { return chs[i].Name < chs[j].Name })
		m.chanCache.mu.Lock()
		m.chanCache.channels, m.chanCache.chanAt = chs, m.now()
		m.chanCache.mu.Unlock()
	}
	p := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(prefix, "#")))
	m.chanCache.mu.Lock()
	defer m.chanCache.mu.Unlock()
	var out []Channel
	for _, ch := range m.chanCache.channels {
		if p == "" || strings.HasPrefix(strings.ToLower(ch.Name), p) {
			out = append(out, ch)
			if limit > 0 && len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// Users returns the workspace's people, alphabetically by handle, cached the
// same way channels are.
func (m *Mirror) Users(ctx context.Context) ([]User, error) {
	lister, ok := m.slack.(interface {
		ListUsers(context.Context) ([]User, error)
	})
	if !ok {
		return nil, nil
	}
	m.chanCache.mu.Lock()
	stale := m.now().Sub(m.chanCache.userAt) > chanCacheTTL || m.chanCache.users == nil
	m.chanCache.mu.Unlock()
	if stale {
		us, err := lister.ListUsers(ctx)
		if err != nil {
			return nil, err
		}
		sort.Slice(us, func(i, j int) bool { return us[i].Handle < us[j].Handle })
		m.chanCache.mu.Lock()
		m.chanCache.users, m.chanCache.userAt = us, m.now()
		m.chanCache.mu.Unlock()
	}
	m.chanCache.mu.Lock()
	defer m.chanCache.mu.Unlock()
	return append([]User(nil), m.chanCache.users...), nil
}

// ErrNoSuchDestination is a picker submission that names nothing the
// workspace has; the page repeats the spelling back so the person can see
// what went wrong.
var ErrNoSuchDestination = errors.New("slack: no such destination")

// ResolveDestination turns what a person picked or typed into the id the
// group stores. kind is the mode radio: "dm" is the attacher's own DM;
// "channel" accepts "#name", "name" or a raw C…/G… id (the id spelling is the
// only door for private channels the bot cannot list); "person" accepts
// "@handle", "handle" or a raw U… id.
func (m *Mirror) ResolveDestination(ctx context.Context, kind, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	switch kind {
	case "dm", "":
		return "dm", nil
	case "channel":
		name := strings.TrimPrefix(raw, "#")
		// Only the raw uppercase spelling reads as an id: channel names are
		// lowercase by Slack's own rules, so "circleci" is a name even though
		// its uppercase form is id-shaped.
		if destinationRE.MatchString(name) && !strings.HasPrefix(name, "U") {
			return name, nil
		}
		chs, err := m.Channels(ctx, "", 0)
		if err != nil {
			return "", err
		}
		for _, ch := range chs {
			if strings.EqualFold(ch.Name, name) {
				return ch.ID, nil
			}
		}
		return "", fmt.Errorf("%w: no channel named %q", ErrNoSuchDestination, raw)
	case "person":
		handle := strings.TrimPrefix(raw, "@")
		if strings.HasPrefix(handle, "U") && destinationRE.MatchString(handle) {
			return handle, nil
		}
		// The datalist submits "@handle — Real Name"; typing just the handle
		// works too. Everything after the separator is display-only.
		if i := strings.Index(handle, " "); i > 0 {
			handle = handle[:i]
		}
		handle = strings.TrimSuffix(handle, "—")
		us, err := m.Users(ctx)
		if err != nil {
			return "", err
		}
		for _, u := range us {
			if strings.EqualFold(u.Handle, handle) {
				return u.ID, nil
			}
		}
		return "", fmt.Errorf("%w: nobody with the handle %q", ErrNoSuchDestination, raw)
	}
	return "", fmt.Errorf("%w: unknown destination kind %q", ErrNoSuchDestination, kind)
}
