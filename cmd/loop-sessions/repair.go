package main

// The repair walk: the sessions the server says it holds incomplete, walked
// one file at a time.
//
// Two things the corpus is missing can only come from transcripts still on
// laptops: the final answers of the turns the old Stop handler captured
// without them, and the tokens of Codex sessions stored before the extraction
// read token_count. A full re-walk cannot fix either (the first are hook rows,
// the second are upgrades usage was never credited from) and would rewrite
// 4.8 million rows to try. So the server names the sessions, per device, and
// the daemon walks exactly those, paced against the outbox like an import,
// once a day. A server that has no such endpoint yet, or no list, is an
// ordinary day: the request is skipped in silence.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/backfill"
	"github.com/loopai-hq/loop-sessions/internal/config"
	"github.com/loopai-hq/loop-sessions/internal/discovery"
	"github.com/loopai-hq/loop-sessions/internal/scrub"
)

const (
	// repairPath is the server's list of sessions this device should re-walk.
	// Duplicated here for the same reason eventsPath is: the halves deploy
	// separately.
	repairPath = "/v1/repair"
	// repairEvery is how often a machine asks. The list changes when the
	// server's derive runner runs, which is nightly.
	repairEvery = 24 * time.Hour
	// repairPace is the pause between files, on top of the outbox pacing the
	// importer applies per 250 events. A repair list can name hundreds of
	// sessions on one laptop; walking them back to back would contend with
	// the session in progress for the disk and the uplink.
	repairPace = 250 * time.Millisecond
	// repairTimeout bounds the list request; the walks that follow are bounded
	// by the importer's own settle timeouts.
	repairTimeout = 30 * time.Second
)

// repairSession is one entry in the server's list.
type repairSession struct {
	SessionID      string `json:"session_id"`
	TranscriptPath string `json:"transcript_path"`
	Reason         string `json:"reason"`
	// Source is "claude_code" or "codex"; inferred from the path when absent.
	Source string `json:"source,omitempty"`
}

// repairList is the wire shape: an object with a sessions array. A bare array
// is accepted too, so the server can start simple.
type repairList struct {
	Sessions []repairSession `json:"sessions"`
}

// runRepairIfDue asks the server once a day and walks what it names. force
// skips the daily gate (the operator CTA).
func runRepairIfDue(ctx context.Context, p config.Paths, cfg config.Config, force bool) {
	now := time.Now()
	if !force && !stampDue(p, stampRepairLast, repairEvery, now) {
		return
	}
	if cfg.IsPaused(now) {
		return
	}
	list, answered, err := fetchRepairList(ctx, p, cfg)
	if answered {
		// Stamped once the server has answered at all, before the walk
		// rather than after it, so a walk that fails halfway is retried
		// tomorrow rather than every session start today. A request that
		// never reached the server (an offline laptop, a DNS or TLS error)
		// is not stamped: the day has not been spent, and the next daemon
		// asks again.
		if err := writeStamp(p, stampRepairLast, now); err != nil {
			logf("repair: could not record the check: %v", err)
		}
	}
	if err != nil {
		// Skipped silently by design: a 404 is a server that predates the
		// route and a 5xx is the server's problem; neither is this machine's.
		return
	}
	if len(list) == 0 {
		return
	}
	logf("repair: the server named %d session(s) to re-walk", len(list))
	n, err := walkRepairList(ctx, p, cfg, list)
	if err != nil {
		logf("repair: stopped after %d session(s): %v", n, err)
		return
	}
	logf("repair: re-walked %d session(s)", n)
}

// fetchRepairList reads the device's list. Any status but 200, or a body
// that is not a list, is an error the caller ignores. answered reports
// whether a response of any status arrived, which is what separates a day
// the server has spoken for from one a bad connection interrupted.
func fetchRepairList(ctx context.Context, p config.Paths, cfg config.Config) (list []repairSession, answered bool, err error) {
	token, err := loadDeviceToken(p)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, false, errors.New("repair: no endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, repairTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(cfg.Endpoint, "/")+repairPath, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "loop-sessions/"+Version)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, true, errors.New("repair: " + res.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return nil, true, err
	}
	var parsed repairList
	if err := json.Unmarshal(raw, &parsed); err != nil || parsed.Sessions == nil {
		var bare []repairSession
		if err := json.Unmarshal(raw, &bare); err != nil {
			return nil, true, errors.New("repair: the list did not parse")
		}
		parsed.Sessions = bare
	}
	return parsed.Sessions, true, nil
}

// walkRepairList imports each named session whole, through the same importer
// a backfill uses, so the outbox stays bounded and delivery is composed in
// one place.
func walkRepairList(ctx context.Context, p config.Paths, cfg config.Config, list []repairSession) (int, error) {
	im := &importer{paths: p, cfg: cfg, out: io.Discard, now: time.Now, sessions: map[string]struct{}{}}
	if err := im.open(); err != nil {
		return 0, err
	}
	defer im.close()

	roots := repairRoots(p, cfg)
	codexRoots := codexRepairRoots(p, cfg)
	var done int
	for i, s := range list {
		if err := ctx.Err(); err != nil {
			return done, err
		}
		if s.SessionID == "" {
			continue
		}
		if s.TranscriptPath == "" {
			// A Codex entry carries no path: the server can rebuild a Claude
			// transcript's path from the session's cwd and id, and a Codex
			// rollout's name says nothing about which session it holds, so
			// the file is found here by reading the session_meta line the
			// walker itself decides from. Without this the entry was skipped
			// and a Codex session's answers were never repaired.
			if !isCodexEntry(s) {
				continue
			}
			found, err := findCodexRollout(codexRoots, s.SessionID)
			if err != nil {
				logf("repair: could not search for session %s: %v", s.SessionID, err)
				continue
			}
			if found == "" {
				// Deleted, compressed, or on another of this person's
				// machines. Said once per session rather than silently, so a
				// list that never shrinks has a reason on the machine.
				logf("repair: no Codex rollout for session %s under %s", s.SessionID, strings.Join(codexRoots, ", "))
				continue
			}
			s.TranscriptPath = found
		}
		if !underAny(s.TranscriptPath, roots) {
			// The list names a path this machine does not keep transcripts
			// under. The server is device-authenticated and the Session
			// filter keeps emission to matching records, but the walk root
			// is the file's parent and the walker reads the head of every
			// .jsonl beneath it, so the roots this machine discovered are
			// the only places it will look.
			logf("repair: refused %s for session %s: outside this machine's session roots", s.TranscriptPath, s.SessionID)
			continue
		}
		if fi, err := os.Stat(s.TranscriptPath); err != nil || fi.IsDir() {
			// Gone, or never on this machine: the server's list is per
			// device, but transcripts are deleted after thirty days.
			continue
		}
		codex := s.Source == "codex" || strings.Contains(filepath.ToSlash(s.TranscriptPath), "/.codex/")
		if ok, _ := cfg.ShouldCapture(cwdOf(s.TranscriptPath, codex)); !ok {
			continue
		}
		im.session = s.SessionID
		walk := backfill.Walk
		if codex {
			walk = backfill.WalkCodex
		}
		_, err := walk(backfill.Options{
			Root: filepath.Dir(s.TranscriptPath), Session: s.SessionID,
			Scrub: scrub.Func, OnSession: im.onFile,
		}, im.emit(ctx))
		if err != nil {
			return done, err
		}
		done++
		if i < len(list)-1 {
			if err := sleepFor(ctx, repairPace); err != nil {
				return done, err
			}
		}
	}
	if err := im.settle(ctx, 0); err != nil {
		return done, err
	}
	return done, nil
}

// isCodexEntry reports whether a repair entry names a Codex session. The
// source is the server's word; the path is the fallback for a server that
// predates the field, and an entry with neither is not one this can place.
func isCodexEntry(s repairSession) bool {
	return s.Source == "codex" || strings.Contains(filepath.ToSlash(s.TranscriptPath), "/.codex/")
}

// findCodexRollout looks for the session in each Codex root in turn and
// returns the first rollout that holds it, or "" when none does.
func findCodexRollout(roots []string, sessionID string) (string, error) {
	for _, root := range roots {
		if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
			continue
		}
		path, err := backfill.FindCodexRollout(root, sessionID)
		if err != nil {
			return "", err
		}
		if path != "" {
			return path, nil
		}
	}
	return "", nil
}

// codexRepairRoots is the Codex half of repairRoots: only the places this
// machine keeps Codex rollouts, because searching a Claude projects
// directory for one reads every transcript on the laptop to learn nothing.
// Discovery's settled root and its candidates come first, for the reason
// repairRoots gives, and the config's root for the tool covers a machine
// whose summary is gone or predates it.
func codexRepairRoots(p config.Paths, cfg config.Config) []string {
	var roots []string
	if sum, err := readDiscovery(p.DiscoveryFile()); err == nil {
		for _, f := range sum.Findings {
			if f.Tool != discovery.Codex {
				continue
			}
			if f.Root != "" {
				roots = append(roots, f.Root)
			}
			for _, c := range f.Candidates {
				roots = append(roots, c.Path)
			}
		}
	}
	if r := cfg.Roots[string(discovery.Codex)]; r != "" {
		roots = append(roots, r)
	}
	return dedupeStrings(roots)
}

// dedupeStrings keeps the first of each value, so a root that discovery
// settled on and also probed is walked once.
func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := in[:0]
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// repairRoots is where this machine keeps transcripts: every location the
// last `discover` run probed, plus the roots install recorded in the config.
// The discovery summary lists each candidate path whether or not it existed
// at the time, so a default layout a tool adopts after discovery (Codex
// installed a month later) is still this machine's; the config's roots cover
// a machine whose summary is gone or predates them. Both are read rather than
// probed again, for the reason historySources gives: a second answer to
// "where do transcripts live" would drift from discovery's.
func repairRoots(p config.Paths, cfg config.Config) []string {
	var roots []string
	if sum, err := readDiscovery(p.DiscoveryFile()); err == nil {
		for _, f := range sum.Findings {
			for _, c := range f.Candidates {
				roots = append(roots, c.Path)
			}
		}
	}
	for _, r := range cfg.Roots {
		roots = append(roots, r)
	}
	return roots
}

// underAny reports whether path lies inside one of the roots, by path
// arithmetic on the cleaned absolute forms: a relative path or one that
// climbs out with ".." is not under anything.
func underAny(path string, roots []string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	for _, root := range roots {
		if root == "" {
			continue
		}
		r, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(r, abs)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		return true
	}
	return false
}

// cwdOf is the best guess at a transcript's project directory for the
// exclusion policy: the head of the file names it (session_meta for a Codex
// rollout), and a file with no head is not excluded by a path it does not
// have.
func cwdOf(transcript string, codex bool) string {
	if codex {
		return backfill.CodexCwd(transcript)
	}
	if h, ok := backfill.ReadHead(transcript); ok && h.SessionID != "" {
		return h.Cwd
	}
	return ""
}
