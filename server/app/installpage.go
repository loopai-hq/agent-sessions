package app

import (
	"html/template"
	"net/http"
	"strings"
)

// InstallPagePath is the install page: how somebody with a signed-in dashboard
// and no agent gets from one to the other.
//
// It exists because the dashboard's empty state is otherwise indistinguishable
// from a broken one. A person signs in, sees no sessions, and has no way to
// tell whether capture is failing or has simply never been set up. Everything
// here answers the second case in one screen.
//
// The page carries no script, so it keeps the dashboard's strict policy rather
// than the relaxed one the sign-in page needs. Nothing on it is derived from a
// request: the only interpolated value is this deployment's own public URL.
const InstallPagePath = "/install"

// Platforms offered as direct downloads, in the order they are shown.
//
// macOS first and Apple silicon first within it, because that is what the
// people being onboarded are actually holding. The Linux rows exist for the
// handful of remote boxes people also run agents on, and cost nothing: the
// release build already produces them.
var installPlatforms = []struct{ Label, Asset string }{
	{"macOS, Apple silicon", "loop-sessions_darwin_arm64"},
	{"macOS, Intel", "loop-sessions_darwin_amd64"},
	{"Linux, x86-64", "loop-sessions_linux_amd64"},
	{"Linux, arm64", "loop-sessions_linux_arm64"},
}

type installPageData struct {
	BaseURL   string
	OneLiner  string
	Platforms []struct{ Label, Asset string }
}

// handleInstall renders the page. It is deliberately reachable without a
// session: somebody setting up a second machine has a dashboard open on the
// first, and making them sign in twice to read an instruction helps nobody.
// Nothing on the page is private; the same text is in the repository.
func (d *Downloads) handleInstall(w http.ResponseWriter, _ *http.Request) {
	base := strings.TrimSuffix(d.publicURL, "/")
	data := installPageData{
		BaseURL:   base,
		OneLiner:  "curl -fsSL " + base + InstallScriptPath + " | sh",
		Platforms: installPlatforms,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; style-src 'unsafe-inline'; form-action 'none'; "+
			"frame-ancestors 'none'; base-uri 'none'; script-src 'none'")
	if err := installTmpl.Execute(w, data); err != nil {
		d.log.Error("render the install page", "err", err)
	}
}

// Parsed once at package init. A template that does not parse is then a failed
// build rather than a 500 on the one page somebody visits when they are already
// confused about why they have no data.
var installTmpl = template.Must(template.New("install").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Install the agent — loop sessions</title>
<style>
 body{font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      margin:0;background:#fafafa;color:#1a1a1a}
 @media (prefers-color-scheme:dark){body{background:#141414;color:#eaeaea}}
 .topbar{display:flex;align-items:center;gap:22px;padding:10px 20px;
         border-bottom:1px solid rgba(128,128,128,.25)}
 .brand{font-weight:700;text-decoration:none;color:inherit;letter-spacing:-.01em}
 .brand span{opacity:.4;font-weight:400}
 .topbar nav{display:flex;gap:16px;font-size:.9rem}
 .topbar nav a{text-decoration:none;color:inherit;opacity:.75}
 .topbar nav a:hover{opacity:1;text-decoration:underline}
 .topbar nav a.on{opacity:1;font-weight:600}
 main{max-width:44rem;margin:0 auto;padding:3rem 1.5rem 5rem}
 h1{font-size:1.5rem;margin:0 0 .25rem;font-weight:600;letter-spacing:-.01em}
 h2{font-size:1.0625rem;margin:2.5rem 0 .5rem;font-weight:600}
 .lead{margin:0 0 2rem;opacity:.75}
 pre{background:#f0f0f0;border:1px solid rgba(128,128,128,.25);border-radius:.5rem;
     padding:1rem;overflow-x:auto;font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}
 @media (prefers-color-scheme:dark){pre{background:#1c1c1c}}
 ol{padding-left:1.25rem}
 li{margin:.4rem 0}
 table{border-collapse:collapse;width:100%;margin:.5rem 0}
 td{padding:.45rem .5rem;border-bottom:1px solid rgba(128,128,128,.2)}
 td:last-child{text-align:right}
 a{color:inherit}
 .note{opacity:.7;font-size:.9375rem}
 code{font:13.5px ui-monospace,SFMono-Regular,Menlo,monospace;
      background:rgba(128,128,128,.15);padding:.1rem .3rem;border-radius:.25rem}
</style>
</head>
<body>
{{/* The same topbar shape the dashboard has, because this page is reached from
     it and previously offered no way back but the browser's own button. The
     links go to authenticated pages; an unauthenticated visitor lands on the
     sign-in redirect, which is the correct place for them anyway. */}}
<header class="topbar">
  <a class="brand" href="{{.BaseURL}}/sessions">loop<span>/</span>sessions</a>
  <nav aria-label="Sections">
    <a href="{{.BaseURL}}/sessions">Sessions</a>
    <a href="{{.BaseURL}}/analytics">Analytics</a>
    <a class="on" href="{{.BaseURL}}/install" aria-current="page">Install</a>
  </nav>
</header>
<main>
  <h1>Install the agent</h1>
  <p class="lead">Until this runs on a machine, that machine's sessions are not
  captured and your dashboard stays empty.</p>

  <h2>One line, in a terminal</h2>
  <pre>{{.OneLiner}}</pre>
  <p class="note">Downloads the agent to <code>~/.local/bin</code>, verifies its
  checksum, and stops. No sudo, no cloud login.</p>

  <h2>Then finish the setup</h2>
  <pre>loop-sessions install</pre>
  <ol>
    <li>It finds the AI coding sessions already on the machine and tells you what it found.</li>
    <li>It opens a browser so you can sign in with your work Google account.</li>
    <li>It registers capture hooks in <code>~/.claude/settings.json</code>, preserving any hooks you already have.</li>
  </ol>
  <p class="note">The two steps are separate on purpose. Piping the installer to
  <code>sh</code> leaves this program reading the rest of the script instead of
  your keyboard, so an installer that tried to ask you anything would either
  hang or answer its own questions.</p>

  <h2>Prefer to download it yourself</h2>
  <table>
    {{range .Platforms}}<tr>
      <td>{{.Label}}</td>
      <td><a href="{{$.BaseURL}}/dl/latest/{{.Asset}}">{{.Asset}}</a></td>
    </tr>{{end}}
  </table>
  <p class="note">Checksums:
  <a href="{{.BaseURL}}/dl/latest/SHA256SUMS">SHA256SUMS</a>.
  After downloading, <code>chmod +x</code> the file, move it onto your
  <code>PATH</code>, and run <code>loop-sessions install</code>.</p>

  <h2>Checking on it later</h2>
  <pre>loop-sessions status
loop-sessions doctor</pre>
  <p class="note"><code>status</code> says what is being captured and whether
  uploads are landing. <code>doctor</code> diagnoses a machine that has stopped
  reporting and says what to do about it. To stop capturing, run
  <code>loop-sessions uninstall</code>.</p>

  <h2>It keeps itself up to date</h2>
  <p class="note">When a session starts, the agent compares itself against the
  published build and replaces itself if they differ, so a fix reaches your
  machine without you doing anything. The download is checksum-verified and run
  once before it is installed; if either check fails the existing agent is kept.
  A paused agent is never replaced. To turn it off, set
  <code>LOOP_SESSIONS_NO_UPGRADE=1</code> or add
  <code>"disable_auto_upgrade": true</code> to
  <code>~/.loop/sessions/config.json</code>.</p>
</main>
</body>
</html>
`))
