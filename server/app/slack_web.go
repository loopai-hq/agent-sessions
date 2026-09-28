package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/loopai-hq/agent-sessions/server/slack"
	"github.com/loopai-hq/agent-sessions/server/web"
)

// slackWeb adapts the mirror's facade to the dashboard's port, translating
// types and nothing else: the rules live in server/slack, the chrome in
// server/web, and this file is the whole of what connects them.
type slackWeb struct{ m *slack.Mirror }

// NewSlackWeb wraps a mirror for the dashboard; nil in, nil out, so a
// deployment without a mirror hides the whole settings surface.
func NewSlackWeb(m *slack.Mirror) web.SlackSettings {
	if m == nil {
		return nil
	}
	return slackWeb{m: m}
}

func webSlackGroup(g slack.Group) web.SlackGroup {
	return web.SlackGroup{
		ID: g.ID, Name: g.Name, OwnerEmail: g.OwnerEmail,
		Visibility: g.Visibility, Destination: g.Destination, Disabled: g.Disabled,
	}
}

func (s slackWeb) Settings(ctx context.Context, email string) (web.SlackSettingsView, error) {
	v, err := s.m.Settings(ctx, email)
	if err != nil {
		return web.SlackSettingsView{}, err
	}
	out := web.SlackSettingsView{
		DefaultGroup: v.DefaultGroup, LiveDisabled: v.LiveDisabled,
		DigestMode: v.DigestMode, DigestChannel: v.DigestChannel,
		DestLabels: v.DestLabels,
	}
	for _, g := range v.Groups {
		out.Groups = append(out.Groups, webSlackGroup(g))
	}
	return out, nil
}

func (s slackWeb) Users(ctx context.Context) ([]web.SlackUser, error) {
	us, err := s.m.Users(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]web.SlackUser, 0, len(us))
	for _, u := range us {
		out = append(out, web.SlackUser{ID: u.ID, Handle: u.Handle, RealName: u.RealName})
	}
	return out, nil
}

func (s slackWeb) ResolveDestination(ctx context.Context, kind, raw string) (string, error) {
	dest, err := s.m.ResolveDestination(ctx, kind, raw)
	if errors.Is(err, slack.ErrNoSuchDestination) {
		// The flash shows this text to a person: keep the sentence, drop the
		// wrapped sentinel prefixes ("slack: no such destination: …").
		msg := strings.TrimPrefix(err.Error(), slack.ErrNoSuchDestination.Error()+": ")
		return "", fmt.Errorf("%w: %s", web.ErrNoSuchDestination, msg)
	}
	return dest, err
}

func (s slackWeb) DeleteGroup(ctx context.Context, email string, id int64) error {
	return s.m.DeleteGroup(ctx, email, id)
}

func (s slackWeb) SetLiveKill(ctx context.Context, email string, disabled bool) error {
	return s.m.SetLiveKill(ctx, email, disabled)
}

func (s slackWeb) Channels(ctx context.Context, prefix string, limit int) ([]web.SlackChannel, error) {
	chs, err := s.m.Channels(ctx, prefix, limit)
	if err != nil {
		return nil, err
	}
	out := make([]web.SlackChannel, 0, len(chs))
	for _, c := range chs {
		out = append(out, web.SlackChannel{ID: c.ID, Name: c.Name})
	}
	return out, nil
}

func (s slackWeb) CreateGroup(ctx context.Context, email, name, visibility, destination string) error {
	_, err := s.m.CreateGroup(ctx, email, name, visibility, destination)
	if err != nil && strings.Contains(err.Error(), "duplicate key") {
		return web.ErrSlackConflict
	}
	return err
}

func (s slackWeb) UpdateGroup(ctx context.Context, email string, g web.SlackGroup) error {
	_, err := s.m.UpdateGroup(ctx, email, slack.Group{
		ID: g.ID, Name: g.Name, Visibility: g.Visibility,
		Destination: g.Destination, Disabled: g.Disabled,
	})
	if errors.Is(err, slack.ErrGroupNotFound) {
		return web.ErrSlackConflict
	}
	return err
}

func (s slackWeb) SessionMirrors(ctx context.Context, sessionID string) ([]web.SlackSessionMirror, error) {
	ms, err := s.m.SessionMirrors(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]web.SlackSessionMirror, 0, len(ms))
	for _, m := range ms {
		out = append(out, web.SlackSessionMirror{GroupID: m.GroupID, GroupName: m.GroupName, Dest: m.Dest})
	}
	return out, nil
}

func (s slackWeb) Attach(ctx context.Context, email, sessionID, groupRef string) error {
	return s.m.Attach(ctx, email, sessionID, groupRef)
}

func (s slackWeb) Detach(ctx context.Context, email, sessionID, groupRef string) error {
	return s.m.Detach(ctx, email, sessionID, groupRef)
}
