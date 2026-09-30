package main

// The mirror subcommand: the terminal's handle on live Slack threads.
//
//	loop-sessions mirror list             the groups you can post to
//	loop-sessions mirror use <group>      mirror every new session to it
//	loop-sessions mirror use <g> --once   ...just the next session
//	loop-sessions mirror off              stop mirroring new sessions
//	loop-sessions mirror attach <group>   attach the current session, now
//	loop-sessions mirror status           what this machine will do
//
// `use` writes the machine's standing answer into the agent config, which the
// SessionStart hook consults when LOOP_SESSIONS_SLACK says nothing; --once
// self-consumes at the next session start, which is the cure for the stale
// env var this replaces. `attach` acts server-side immediately, against the
// most recently active session on this machine, using the device credential
// delivery already holds — the server lets a device do exactly what its owner
// could do in the browser.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
)

func runMirror(args []string) error {
	if len(args) == 0 {
		return mirrorStatus()
	}
	switch args[0] {
	case "list":
		return mirrorList()
	case "use":
		// Flags and the group name in either order: the flag package stops at
		// the first positional, and "use e2e --once" is the order people type.
		once := false
		var pos []string
		for _, a := range args[1:] {
			if a == "--once" || a == "-once" {
				once = true
				continue
			}
			pos = append(pos, a)
		}
		if len(pos) != 1 {
			return fmt.Errorf("usage: loop-sessions mirror use <group|on|dm|C…> [--once]")
		}
		return mirrorUse(pos[0], once)
	case "off":
		return mirrorUse("", false)
	case "attach":
		session := ""
		var pos []string
		rest := args[1:]
		for i := 0; i < len(rest); i++ {
			if rest[i] == "--session" || rest[i] == "-session" {
				if i+1 < len(rest) {
					session = rest[i+1]
					i++
				}
				continue
			}
			pos = append(pos, rest[i])
		}
		if len(pos) != 1 {
			return fmt.Errorf("usage: loop-sessions mirror attach <group> [--session <id>]")
		}
		return mirrorAttach(pos[0], session)
	case "status":
		return mirrorStatus()
	default:
		return fmt.Errorf("unknown mirror command %q; try list, use, off, attach, status", args[0])
	}
}

func mirrorUse(ref string, once bool) error {
	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		return err
	}
	cfg.MirrorDefault = strings.TrimSpace(ref)
	cfg.MirrorOnce = once && cfg.MirrorDefault != ""
	if err := config.Save(p, cfg); err != nil {
		return err
	}
	switch {
	case cfg.MirrorDefault == "":
		fmt.Println("mirroring off for new sessions (running threads finish on their own)")
	case once:
		fmt.Printf("the NEXT session will mirror to %q, then this resets\n", cfg.MirrorDefault)
	default:
		fmt.Printf("new sessions will mirror to %q (loop-sessions mirror off to stop)\n", cfg.MirrorDefault)
	}
	return nil
}

func mirrorStatus() error {
	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		return err
	}
	if env := os.Getenv("LOOP_SESSIONS_SLACK"); env != "" {
		fmt.Printf("env override active this shell: LOOP_SESSIONS_SLACK=%s\n", env)
	}
	switch {
	case cfg.MirrorDefault == "":
		fmt.Println("standing default: off")
	case cfg.MirrorOnce:
		fmt.Printf("standing default: %q for the NEXT session only\n", cfg.MirrorDefault)
	default:
		fmt.Printf("standing default: %q\n", cfg.MirrorDefault)
	}
	return nil
}

// mirrorAPI performs one authenticated call against the server.
func mirrorAPI(method, path string, body any) ([]byte, error) {
	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		return nil, err
	}
	token, err := os.ReadFile(filepath.Join(p.Root(), "device.token"))
	if err != nil {
		return nil, fmt.Errorf("no device credential; run loop-sessions install first: %w", err)
	}
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, strings.TrimRight(cfg.Endpoint, "/")+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("server said %s: %s", resp.Status, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func mirrorList() error {
	out, err := mirrorAPI("GET", "/v1/slack/groups", nil)
	if err != nil {
		return err
	}
	var groups []struct {
		ID          int64  `json:"id"`
		Name        string `json:"name"`
		OwnerEmail  string `json:"owner_email"`
		Visibility  string `json:"visibility"`
		Destination string `json:"destination"`
		Disabled    bool   `json:"disabled"`
	}
	if err := json.Unmarshal(out, &groups); err != nil {
		return fmt.Errorf("unreadable answer from the server: %w", err)
	}
	if len(groups) == 0 {
		fmt.Println("no groups yet; create one on the dashboard under Settings")
		return nil
	}
	for _, g := range groups {
		state := ""
		if g.Disabled {
			state = "  (disabled)"
		}
		fmt.Printf("%-24s %-8s %-12s owner %s%s\n", g.Name, g.Visibility, g.Destination, g.OwnerEmail, state)
	}
	return nil
}

// currentSessionID finds the session most recently active on this machine, by
// the per-session sequence files the hook writes on every event.
func currentSessionID() (string, error) {
	p := config.Paths{}
	entries, err := os.ReadDir(p.SeqDir())
	if err != nil {
		return "", fmt.Errorf("no sessions recorded on this machine yet: %w", err)
	}
	type cand struct {
		id string
		at time.Time
	}
	var cands []cand
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		cands = append(cands, cand{id: strings.TrimSuffix(e.Name(), ".seq"), at: info.ModTime()})
	}
	if len(cands) == 0 {
		return "", fmt.Errorf("no sessions recorded on this machine yet")
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].at.After(cands[j].at) })
	return cands[0].id, nil
}

func mirrorAttach(group, session string) error {
	if session == "" {
		var err error
		session, err = currentSessionID()
		if err != nil {
			return err
		}
	}
	out, err := mirrorAPI("POST", "/v1/sessions/"+session+"/mirror/"+group, nil)
	if err != nil {
		return err
	}
	fmt.Printf("attached: %s\n", strings.TrimSpace(string(out)))
	return nil
}
