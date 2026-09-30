# Design note: Firebase authentication

> **Historical design note.** Written while the server was being assembled and kept as it was; moved from `server/` to `docs/design/` on 2026-09-30. It is not maintained and may no longer match the code. The current description of the system is [docs/ARCHITECTURE.md](../ARCHITECTURE.md); the source is the truth where the two differ.

Supersedes the Google OAuth design in `CONTRACT-assembly.md` §W7 and every
OAuth-client environment variable in §W6.

## Why this changed

The OAuth design required the server to own two OAuth clients and their
secrets. Firebase Auth with the Google provider needs none: the server verifies
Firebase ID tokens against Google's published certificates, and any Firebase
project with Google sign-in enabled works. Adopting it deletes an entire
dependency rather than working around one.

## What this deletes

Gone entirely: `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`,
`GOOGLE_OAUTH_DESKTOP_CLIENT_ID`, `GOOGLE_OAUTH_DESKTOP_CLIENT_SECRET`, their
four Secret Manager secrets, the authorization-code exchange, PKCE, the OAuth
state cookie, and `isPlaceholderCredential`. The server no longer holds an OAuth
secret of any kind.

## Token facts that change the verifier

A Firebase ID token is **not** a Google OAuth ID token, and three differences
are load-bearing:

1. **No `hd` claim.** The Workspace-membership proof the old design leaned on
   does not exist here. Domain enforcement is on the `email` claim alone, which
   makes the roster check the primary control rather than a secondary one.
2. **Signing keys are x509 certificates**, not a JWKS document, served from
   `https://www.googleapis.com/service_accounts/v1/metadata/x509/securetoken@system.gserviceaccount.com`
   as a JSON object of `{kid: "-----BEGIN CERTIFICATE-----..."}`. The existing
   `auth.HTTPKeySource` parses a JWKS and will not read this. Honour the
   `Cache-Control: max-age` on that response.
3. **`firebase.sign_in_provider` must be `google.com`.** Firebase Auth on the
   project may have other providers enabled, and without this check anybody who
   can obtain a Firebase account bearing a company address by any other method
   authenticates as that person. Verify it explicitly and reject otherwise.

Also verify, as the old verifier did: signature, `exp`, `iat`, `aud` equals the
project id, `iss` equals `https://securetoken.google.com/<project id>`, `sub`
non-empty, and `email_verified` true.

## File ownership

| File | Owner | Change |
|---|---|---|
| `server/auth/firebase.go` | F1 | New: Firebase ID token verifier + x509 key source |
| `server/auth/google.go` | F1 | Delete, once nothing imports it |
| `server/app/signin.go` | F2 | Replace the OAuth dance with a session exchange |
| `server/web/templates/signin.html` + static JS | F2 | The one page that carries script |
| `server/app/enroll.go` | F3 | Accept a Firebase ID token, drop the code exchange |
| `internal/enroll/enroll.go` | F3 | Client half: loopback listener, open the CLI page |
| `server/app/config.go`, `examples/deploy-gcp/cloudrun.yaml` | F4 | Config surface |
| `examples/deploy-gcp/README.md` | F4 | Docs |

## F2: web sign-in, and the CSP question

`/auth/signin` renders a page that loads the Firebase Auth SDK, calls
`signInWithPopup` with `GoogleAuthProvider`, takes `getIdToken()`, and POSTs it
to `/auth/session`. The server verifies it, applies the domain and roster
checks, mints its own session cookie, and redirects to `next`.

**This page, and only this page, may run script.** The dashboard's
`script-src 'none'` exists because it renders untrusted transcript content, so
an escaping mistake there becomes code execution. The sign-in page renders no
user-controlled content at all: no transcript, no session title, no prompt text.
Scope the relaxed policy to this one route and leave every other response
exactly as it is. Do not weaken the global policy.

The page's own policy needs `script-src 'self' https://www.gstatic.com`,
`connect-src https://identitytoolkit.googleapis.com https://securetoken.googleapis.com`,
and `frame-src https://<project>.firebaseapp.com`. The SDK comes from
`gstatic.com` because it is Google's own origin; do not add a third-party CDN.

`next` keeps the existing `localPath` validation. An absolute URL accepted
there is an open redirect that phishes a colleague from the deployment's own domain.

`/auth/session` is a POST carrying a CSRF token minted into the sign-in page.

## F3: CLI enrollment without an OAuth client

The agent cannot use a Firebase web popup, and there is no longer a Desktop
OAuth client. The flow becomes:

1. The CLI binds `127.0.0.1:0` and learns its port.
2. It opens `<endpoint>/auth/cli?port=<port>&state=<random>` in a browser.
3. That page performs the same Firebase sign-in, then POSTs the ID token and
   the state to `http://127.0.0.1:<port>/callback`.
4. The CLI checks the state matches, then POSTs the ID token to
   `/v1/enroll/complete` with its hostname, os, arch and agent version.
5. The server verifies the token exactly as for the web, then enrols the device
   and returns the device token, which is stored once and never recoverable.

The page MUST refuse any port outside 1024-65535 and MUST post only to
`127.0.0.1`. A page that posts a live ID token to a host taken from its own
query string is an exfiltration primitive; the loopback literal is what stops
it. Do not accept `localhost`, which can resolve elsewhere.

Keep the existing per-email rate limit on `/v1/enroll/complete`. It still mints
long-lived credentials and is still the highest-value unauthenticated surface.

## F4: configuration

| Variable | Required | Meaning |
|---|---|---|
| `FIREBASE_PROJECT_ID` | yes | The Firebase project id; both the audience and the issuer suffix |
| `FIREBASE_API_KEY` | yes | The web API key. **Public by design**, like every Firebase web config; it is not a secret and must not be stored as one or logged as redacted, which would imply it is. |
| `FIREBASE_AUTH_DOMAIN` | no | Defaults to `<project>.firebaseapp.com` |

`ALLOWED_DOMAINS`, `PUBLIC_URL`, `SESSION_KEY` and every database variable are
unchanged. Delete the four OAuth variables and their secrets.

## Definition of done

- `go build ./...`, `go vet ./...`, `gofmt -l` clean
- `go test ./... -race` green
- No reference to `GOOGLE_OAUTH_` anywhere, including docs
- A test proving a token with `sign_in_provider != "google.com"` is refused
- A test proving `/auth/cli` refuses a non-loopback or out-of-range port
- A test proving every authentication failure is still indistinguishable to the
  caller
- The sign-in page's relaxed CSP applies to that route only, proven by a test
  asserting `script-src 'none'` still holds on a transcript page
