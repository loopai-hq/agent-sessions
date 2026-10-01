// Regenerates the README's dashboard screenshots from the demo server.
//
// The demo server (TestServeDemo in demo_test.go) renders the whole dashboard
// against the in-memory fake and its fixtures: placeholder people on
// example.com, placeholder repositories under acme/, no database and no
// Firebase. That is the only thing this script ever photographs. A real
// deployment shows real names, repositories and prompts, and CONTRIBUTING.md
// forbids any of it in the tree; the demo fixtures were written so a
// screenshot cannot leak anything.
//
// Run from the repository root:
//
//   make screenshots
//   # equivalently
//   node server/web/screenshots.cjs
//
// It starts the demo server on a free loopback port (`go test ./server/web
// -run TestServeDemo -timeout 0`, LOOP_WEB_DEMO set to that port), waits for
// it to answer, captures the four pages below with Playwright's Chromium at a
// 1280x800 viewport in the light colour scheme, writes them to docs/images/,
// checks each file is under 500 KB and that the text on every page names only
// fixture identities, then stops the server and waits for it to be gone. Exit
// status 0 means every file was written and checked; the FAIL line go test
// prints for the interrupted TestServeDemo on the way out is how it reports
// being stopped, not a failure.
//
// Playwright resolves from node_modules, NODE_PATH, or the global module root
// (`npm root -g`); the browser is Playwright's own cached Chromium unless
// CHROMIUM_PATH names an executable (see conversation.browser.cjs for the
// macOS path). The versions are whatever the machine has: these are
// illustrations, not a regression suite, so a pixel of drift between
// Chromium releases is fine.
//
// Refresh policy: regenerate the screenshots in the same change that alters a
// template, app.css or the demo fixtures, and commit the PNGs with it. They
// are not diffed in CI; a reviewer looks at the images in the pull request.
// Do not replace them with captures of a real deployment.
"use strict";

const { spawn, execFileSync } = require("node:child_process");
const { createServer } = require("node:net");
const { mkdirSync, statSync } = require("node:fs");
const { resolve } = require("node:path");
const http = require("node:http");

const repoRoot = resolve(__dirname, "..", "..");
const outDir = resolve(repoRoot, "docs", "images");
const viewport = { width: 1280, height: 800 };
const maxBytes = 500 * 1024;
const readyTimeoutMs = 180_000; // the first run compiles the test binary

// The four pages the README shows, in the order it shows them. The session
// page is s-01, the fixture with a subagent, an edit with a diff and a failed
// tool call, so the transcript has something to show; its transcript ships
// collapsed (the reader opens what it needs), so `prepare` unfolds every
// disclosure except the usage panel at the top, which would otherwise push
// the conversation below the fold.
const pages = [
  { file: "sessions.png", path: "/sessions" },
  {
    file: "session.png",
    path: "/sessions/s-01",
    prepare: page => page.evaluate(() => {
      for (const d of document.querySelectorAll("details:not(.session-details)")) d.open = true;
    }),
  },
  { file: "fleet.png", path: "/admin/fleet" },
  { file: "analytics.png", path: "/analytics" },
];

// Identities that may appear on a page: the demo fixtures use example.com
// addresses, acme/ repositories and a home directory under /home/alex. A mail
// address on any other domain, a cloud hostname or a macOS home directory is
// a fixture that drifted towards something real, and the run fails on it.
const allowedEmail = /^[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+\.)*example\.(com|org|net)$/;
const emailShape = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[a-z]{2,}/g;
const hostShape = /[a-z0-9-]+\.[a-z0-9-]+\.run\.app|\/Users\/[a-z]|AIza[0-9A-Za-z_-]{30,}/g;

function loadPlaywright() {
  try {
    return require("playwright");
  } catch (err) {
    if (err.code !== "MODULE_NOT_FOUND") throw err;
  }
  const globalRoot = execFileSync("npm", ["root", "-g"], { encoding: "utf8" }).trim();
  try {
    return require(resolve(globalRoot, "playwright"));
  } catch (err) {
    if (err.code !== "MODULE_NOT_FOUND") throw err;
    throw new Error("playwright is not installed: npm i -g playwright, or set NODE_PATH to a node_modules that has it");
  }
}

function freePort() {
  return new Promise((done, fail) => {
    const probe = createServer();
    probe.once("error", fail);
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address();
      probe.close(() => done(port));
    });
  });
}

function get(url) {
  return new Promise((done, fail) => {
    const req = http.get(url, res => {
      res.resume();
      res.on("end", () => done(res.statusCode));
    });
    req.on("error", fail);
    req.setTimeout(5000, () => req.destroy(new Error("timeout")));
  });
}

async function waitForReady(base, child) {
  const deadline = Date.now() + readyTimeoutMs;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`demo server exited with status ${child.exitCode} before it was ready`);
    try {
      if ((await get(`${base}/sessions`)) === 200) return;
    } catch {
      // not listening yet
    }
    await new Promise(r => setTimeout(r, 500));
  }
  throw new Error(`demo server did not answer on ${base} within ${readyTimeoutMs / 1000}s`);
}

// go test runs the compiled test binary as a child of its own, so the whole
// process group is signalled, not just the go process.
function startDemo(port) {
  const child = spawn("go", ["test", "./server/web", "-run", "TestServeDemo", "-timeout", "0", "-count=1"], {
    cwd: repoRoot,
    env: { ...process.env, LOOP_WEB_DEMO: `127.0.0.1:${port}` },
    stdio: ["ignore", "inherit", "inherit"],
    detached: true,
  });
  child.unref();
  return child;
}

// Stops the demo and waits for it to be gone. The group gets SIGINT rather
// than SIGTERM: go test handles an interrupt by waiting for its test binary
// and then exiting, whereas SIGTERM ends go outright and orphans web.test as
// a zombie for init to reap. The go process is then waited for here, so node
// does not exit with an unreaped child of its own. A group still there after
// ten seconds is killed outright.
function stopDemo(child) {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return new Promise(done => {
    child.ref();
    const hammer = setTimeout(() => {
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {
        // already gone
      }
    }, 10_000);
    child.once("exit", () => {
      clearTimeout(hammer);
      done();
    });
    try {
      process.kill(-child.pid, "SIGINT");
    } catch {
      child.kill("SIGINT");
    }
  });
}

function checkIdentities(file, text) {
  const bad = [];
  for (const m of text.match(emailShape) || []) if (!allowedEmail.test(m)) bad.push(m);
  for (const m of text.match(hostShape) || []) bad.push(m);
  if (bad.length) throw new Error(`${file}: page text names non-fixture identities: ${[...new Set(bad)].join(", ")}`);
}

async function main() {
  const { chromium } = loadPlaywright();
  mkdirSync(outDir, { recursive: true });

  const port = await freePort();
  const base = `http://127.0.0.1:${port}`;
  const demo = startDemo(port);
  let browser;
  try {
    await waitForReady(base, demo);
    browser = await chromium.launch({ headless: true, executablePath: process.env.CHROMIUM_PATH || undefined, args: ["--no-sandbox"] });
    const context = await browser.newContext({ viewport, deviceScaleFactor: 1, colorScheme: "light", reducedMotion: "reduce" });
    const page = await context.newPage();
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));

    for (const { file, path, prepare } of pages) {
      const out = resolve(outDir, file);
      const res = await page.goto(base + path, { waitUntil: "networkidle" });
      if (!res || res.status() !== 200) throw new Error(`${path}: HTTP ${res ? res.status() : "no response"}`);
      if (prepare) await prepare(page);
      await page.waitForTimeout(250); // fonts and the last layout pass
      checkIdentities(file, await page.evaluate(() => document.body.innerText));
      await page.screenshot({ path: out, type: "png", fullPage: false, animations: "disabled" });
      const { size } = statSync(out);
      if (size > maxBytes) throw new Error(`${file} is ${size} bytes, over the ${maxBytes} byte budget`);
      console.log(`wrote docs/images/${file} (${path}, ${viewport.width}x${viewport.height}, ${size} bytes)`);
    }
    if (errors.length) throw new Error(`page errors: ${errors.join("; ")}`);
  } finally {
    if (browser) await browser.close();
    await stopDemo(demo);
  }
}

main().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
