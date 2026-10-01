package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/slack"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// The Slack mirror's half of the composition root: the identity its preference
// route runs on, the construction the routes call, and the loop that makes the
// poster a thing that actually posts.
//
// All three exist because a mirror is not one component. It is a preference
// somebody sets and a sweeper that acts on it, and either half alone is a
// feature that appears to work: a route with no loop stores an intention nobody
// serves, and a loop with no route runs forever over a table no one can write.
// They are wired here, together, in the one file that fails at boot rather than
// at the first pass.

// NewSlackViewer adapts the dashboard session cookie to the mirror's viewer
// port.
//
// It reports the caller's own address and nothing else. The mirror's route
// takes its subject from this and never from a request body, so the port
// deliberately has no way to express "some other person": the narrowness is the
// authorization argument, not an accident of it.
//
// The port has no error channel, so a roster that cannot be read presents as a
// signed-out caller and answers 401. That is the safe direction — the
// alternative is writing a preference for somebody this server could not
// confirm — but it is invisible from outside, so the failure is logged here
// rather than swallowed. Same shape and same reasoning as NewWebViewer.
func NewSlackViewer(c *auth.Cookies, s *store.Store) func(*http.Request) (string, bool) {
	v := slackViewer{cookies: c, roster: s}
	return v.viewer
}

type slackViewer struct {
	cookies *auth.Cookies
	roster  authPrincipals
	// log is nil in production and set by the tests that assert on what was
	// recorded; slog.Default() is where the rest of the process writes.
	log *slog.Logger
}

func (v slackViewer) viewer(r *http.Request) (string, bool) {
	p, err := resolvePrincipal(r, v.cookies, v.roster)
	if err != nil {
		if !errors.Is(err, errNoSession) {
			v.logger().Error("resolve slack preference caller", "err", err, "path", r.URL.Path)
		}
		return "", false
	}
	return p.Email, true
}

func (v slackViewer) logger() *slog.Logger {
	if v.log != nil {
		return v.log
	}
	return slog.Default()
}

// newSlackMirror builds the mirror, or reports that this deployment has not
// been given one.
//
// A nil mirror and a nil error is "not configured", and it is the same shape
// the download routes use for an absent release bucket. Two things can withhold
// it and neither is a misconfiguration: a deployment with no bot token has not
// been connected to a workspace, and a caller assembling the server without a
// raw connection source — every test that predates this feature — has not asked
// for one.
//
// What must not happen is half of it. If this returns nil, the preference route
// is not mounted either, so there is no way to record an intention that nothing
// will act on. A person who cannot switch the mirror on has a feature that is
// absent; a person who can switch on a mirror that never runs has one that is
// broken, and only the second generates a bug report nobody can reproduce.
func newSlackMirror(cfg Config, log *slog.Logger, db slack.DB, viewer, deviceEmail func(*http.Request) (string, bool)) (*slack.Mirror, error) {
	if cfg.SlackBotToken == "" || db == nil {
		return nil, nil
	}
	client, err := slack.NewClient(slack.ClientOptions{Token: cfg.SlackBotToken})
	if err != nil {
		return nil, err
	}
	return slack.New(slack.Options{
		DB:            db,
		Slack:         client,
		PublicURL:     cfg.PublicURL,
		Viewer:        viewer,
		DeviceEmail:   deviceEmail,
		SigningSecret: cfg.SlackSigningSecret,
		Logger:        log.With("component", "slack"),
	})
}

// deviceEmailResolver answers "whose machine is this" from the same device
// credential ingest trusts, so the agent CLI can manage its owner's mirror
// with the token it already holds.
func deviceEmailResolver(st *store.Store) func(*http.Request) (string, bool) {
	return func(r *http.Request) (string, bool) {
		tok := auth.BearerToken(r)
		if tok == "" || st == nil {
			return "", false
		}
		id, err := st.AuthenticateDevice(r.Context(), auth.HashToken(tok))
		if err != nil {
			return "", false
		}
		return id.Email, true
	}
}

// slackDisabledReason names which of the two conditions withheld the mirror, so
// the boot line says something an operator can act on.
//
// The token is reported as present or absent and never printed. It is a
// workspace credential, and a boot log is the least private place in this
// system.
func slackDisabledReason(cfg Config, db slack.DB) string {
	switch {
	case cfg.SlackBotToken == "" && db == nil:
		return envSlackBotToken + " is unset and this server was assembled without a connection source"
	case cfg.SlackBotToken == "":
		return envSlackBotToken + " is unset"
	default:
		return "this server was assembled without a connection source"
	}
}

// startMirror runs the poster for as long as ctx lives, and reports a channel
// that closes once it has stopped.
//
// This is the caller slack.Mirror.Run has to have, and it is the reason this
// feature is not the seventh component in this repository that was written,
// tested and never invoked. Everything under server/slack passes its own tests
// whether or not this function exists; the only symptom of its absence is that
// a person switches their mirror on and no message ever arrives.
//
// In-process on the serving instance, for the reasons startRetention gives at
// length: cloudrun.yaml holds minScale at 1 with CPU always allocated, so a
// background timer actually fires, and a separate job would need its own image,
// its own Cloud SQL binding and its own deploy to drift from this one.
//
// Two instances both sweeping is harmless by construction rather than by
// election. Every message is claimed through a primary key before it is sent,
// so the loser of the race posts nothing; see slack_posts in migration 0006.
func (a *App) startMirror(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	// No mirror is the ordinary case for a deployment with no bot token and for
	// every test that assembles the server without a connection source. Neither
	// should start a goroutine, and neither is an error.
	if a.mirror == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		a.mirror.Run(ctx)
	}()
	return done
}
