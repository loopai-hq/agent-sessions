// Command loop-sessions-server serves the loop-sessions dashboard, its read
// API, the admin surface and the endpoint the agents on people's laptops upload
// to, all on one listener.
//
// It is deliberately thin. Everything it does is read the environment, build
// the server, serve it and shut it down; the assembly, the route table and the
// drain policy live in server/app, where they can be tested. A binary that
// carries logic of its own is logic that only runs in production.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loopai-hq/agent-sessions/server/app"
	"github.com/loopai-hq/agent-sessions/server/export"
)

// version is stamped at build time by examples/deploy-gcp/Dockerfile
// (-ldflags "-X main.version=..."). It is logged once at startup, which is what
// makes a log line attributable to a revision when two are running side by side
// during a rollout.
var version = "dev"

// buildDate is stamped alongside the version (RFC3339, UTC). It is what the
// fleet page prints beside "Latest build": a hash tells an operator which
// build, the timestamp tells them how old the question "has everyone
// upgraded" actually is.
var buildDate = ""

func main() {
	if err := run(); err != nil {
		// The error has already been logged with its context by run. Exit
		// non-zero so the platform records the start as failed rather than as a
		// container that chose to stop.
		os.Exit(1)
	}
}

func run() error {
	// The one subcommand. "export" is the hourly Cloud Run job
	// (examples/deploy-gcp/analytics/job.yaml), built from this same image so a
	// deploy ships both; it reads its own environment, runs once and exits.
	// Everything else it needs lives in server/export, for the reason the
	// server's own assembly lives in server/app.
	if len(os.Args) > 1 && os.Args[1] == "export" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return export.Main(ctx, os.Getenv, os.Stdout, version)
	}

	cfg, cfgErr := app.Load(os.Getenv)
	// slog.LevelInfo is the zero value, so a Config that failed to load still
	// produces a logger at the default level and the failure below is a
	// structured line rather than an unparsed one.
	//
	// The Cloud variant, not NewLogger: this is the one process whose stdout
	// Cloud Logging reads, and its severity filters see nothing unless the keys
	// are the ones it promotes. The build is stamped on every line here, before
	// SetDefault, so the components that log through the default logger carry
	// it too.
	log := app.NewCloudLogger(cfg.LogLevel, os.Stdout, version)
	// The default logger is what components that take no logger write to,
	// including the dashboard's viewer adapter, so setting it here is what keeps
	// their output in the same JSON stream as everything else.
	slog.SetDefault(log)

	if cfgErr != nil {
		log.Error("cannot start", "err", cfgErr)
		return cfgErr
	}

	// SIGTERM is what Cloud Run sends before it removes an instance; Interrupt
	// is what a developer sends. Both mean the same thing here: stop accepting,
	// finish what is in flight, exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The config is logged through its own LogValue, which reports each secret
	// as present or absent rather than by value. time.Local is logged with it
	// because every timestamp the dashboard renders is formatted in that zone,
	// so a deployment that lost TZ produces pages that are wrong by hours with
	// nothing else to say so. The version is not repeated here: the logger
	// already stamps it on this line and on every other.
	log.Info("starting", "config", cfg, "timezone", time.Local.String())

	cfg.Version = version
	if t, err := time.Parse(time.RFC3339, buildDate); err == nil {
		cfg.BuildDate = t
	}
	a, err := app.New(ctx, cfg, log)
	if err != nil {
		log.Error("cannot start", "err", err)
		return err
	}
	// After Serve returns, never before: closing the pool while the drain is
	// still finishing requests would fail exactly the requests the drain exists
	// to protect.
	defer a.Close()

	if err := a.Serve(ctx); err != nil {
		log.Error("stopped", "err", err)
		return err
	}
	log.Info("stopped")
	return nil
}
