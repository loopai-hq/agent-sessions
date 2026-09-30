package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Release distribution: the installer script and the agent binaries.
//
// These are the only unauthenticated routes on this service that return
// something other than a sign-in page, and they have to be. A person following
// the one-liner on a new laptop has no cookie, no device token and no gcloud;
// requiring any of those is the thing the terminal one-liner exists to avoid.
//
// Nothing here is secret. The agent binary talks to endpoints that authenticate
// every request, so possessing it grants nothing, and install.sh verifies a
// SHA256 from a manifest fetched over the same TLS connection.
//
// The artifacts live in a PRIVATE bucket and are proxied through this service
// rather than served from a public one. A world-readable bucket is a standing
// surface that outlives whatever put it there and that nobody revisits; a route
// here can be reasoned about, logged, and removed by a deploy.
const (
	// InstallScriptPath is what the published one-liner curls.
	InstallScriptPath = "/install.sh"
	// DownloadPrefix serves the manifest and the binaries beneath a channel.
	DownloadPrefix = "/dl/"
)

// Assets that may be requested, as an exact allowlist rather than a pattern.
//
// The name is interpolated into a Cloud Storage object path, so the question is
// not "does this look reasonable" but "can this name reach an object I did not
// mean to publish". An allowlist answers that with the set itself and cannot be
// argued with; a regexp is a claim about every string that has to stay true as
// the regexp is edited. The set is four binaries and three files, so the cost
// of being exact is one line per release artifact.
var downloadableAssets = map[string]string{
	"SHA256SUMS": "text/plain; charset=utf-8",
	"install.sh": "text/x-shellscript; charset=utf-8",
	// The release manifest the release step writes beside the binaries:
	// version, commit, build date, capture schema and the asset list. It is
	// what the fleet evaluator compares every machine against and what the
	// agent reads to decide whether the published build differs from its own,
	// so it has to be reachable through the same proxied route as the binaries
	// rather than through a bucket URL the fleet has no credential for.
	"latest.json":                "application/json; charset=utf-8",
	"loop-sessions_darwin_arm64": "application/octet-stream",
	"loop-sessions_darwin_amd64": "application/octet-stream",
	"loop-sessions_linux_amd64":  "application/octet-stream",
	"loop-sessions_linux_arm64":  "application/octet-stream",
}

// Channels that may be requested. Same reasoning as the asset allowlist.
//
// Two, and only two. "latest" is what every installed agent follows and what
// install.sh fetches by default. "canary" is the build CI publishes on every
// merge to main, which one machine runs ahead of the fleet so a capture bug is
// found on one laptop rather than forty; "latest" is promoted from it by hand.
// A channel that is not on this list answers 404 exactly as an unknown asset
// does, so a typo in an agent's configured channel cannot reach an object
// under a name nobody published.
var downloadableChannels = map[string]bool{"latest": true, "canary": true}

const (
	// A binary is about 7MB and comes from a bucket in the same region, so a
	// download that has not finished in this long is not going to.
	downloadTimeout = 2 * time.Minute
	// The metadata server is on the local link. A slow answer from it means
	// something is wrong with the instance, not with the network.
	metadataTimeout = 5 * time.Second
	// Tokens live an hour; refreshing early avoids racing the expiry on a
	// download that starts just before it.
	tokenRefreshMargin = 5 * time.Minute

	metadataTokenURL = "http://metadata.google.internal/computeMetadata/v1/" +
		"instance/service-accounts/default/token"
)

// DownloadOptions configure a Downloads.
type DownloadOptions struct {
	// Bucket holds the release artifacts. Required.
	Bucket string
	// PublicURL is the absolute base this service is reached at, printed into
	// the install page's one-liner. A page that tells somebody to curl a
	// relative path is a page that tells them nothing.
	PublicURL string
	// TokenSource returns an OAuth token for reading the bucket, and its
	// expiry. Zero takes the instance metadata server, which is how Cloud Run
	// supplies the service account's own credentials without a key file.
	TokenSource func() (string, time.Time, error)
	// HTTPClient talks to Cloud Storage. Zero takes a client with a timeout.
	HTTPClient *http.Client
	Logger     *slog.Logger
}

// Downloads serves release artifacts out of a private bucket.
type Downloads struct {
	bucket    string
	publicURL string
	token     func() (string, time.Time, error)
	http      *http.Client
	log       *slog.Logger

	mu       sync.Mutex
	cached   string
	cachedAt time.Time
}

// NewDownloads builds a Downloads.
func NewDownloads(o DownloadOptions) (*Downloads, error) {
	if strings.TrimSpace(o.Bucket) == "" {
		return nil, errors.New("app: downloads need a bucket")
	}
	d := &Downloads{
		bucket:    o.Bucket,
		publicURL: o.PublicURL,
		token:     o.TokenSource,
		http:      o.HTTPClient,
		log:       o.Logger,
	}
	if d.token == nil {
		d.token = metadataToken
	}
	if d.http == nil {
		d.http = &http.Client{Timeout: downloadTimeout}
	}
	if d.log == nil {
		d.log = slog.Default()
	}
	return d, nil
}

// Register adds the download routes to a mux owned by the caller.
func (d *Downloads) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET "+InstallScriptPath, d.handleInstallScript)
	mux.HandleFunc("GET "+DownloadPrefix+"{channel}/{asset}", d.handleAsset)
	mux.HandleFunc("GET "+InstallPagePath, d.handleInstall)
}

// handleInstallScript serves the installer from the default channel, so the
// published one-liner is a URL on this service rather than one that names a
// bucket. Where the artifacts live is then an implementation detail this
// service can change without reprinting the instruction everybody has saved.
func (d *Downloads) handleInstallScript(w http.ResponseWriter, r *http.Request) {
	d.serve(w, r, "latest", "install.sh")
}

func (d *Downloads) handleAsset(w http.ResponseWriter, r *http.Request) {
	d.serve(w, r, r.PathValue("channel"), r.PathValue("asset"))
}

func (d *Downloads) serve(w http.ResponseWriter, r *http.Request, channel, asset string) {
	contentType, ok := downloadableAssets[asset]
	if !ok || !downloadableChannels[channel] {
		// One answer for an unknown channel and an unknown asset. Telling them
		// apart would enumerate which channels exist, and there is nothing a
		// caller does differently on learning it.
		http.NotFound(w, r)
		return
	}

	tok, err := d.accessToken()
	if err != nil {
		d.log.Error("cannot obtain a token to read the release bucket", "err", err)
		http.Error(w, "downloads are temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	// The object name is escaped as a single path segment: the slash between
	// channel and asset must arrive at Cloud Storage as %2F or the API reads it
	// as a path of its own.
	object := url.PathEscape(channel + "/" + asset)
	api := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media",
		url.PathEscape(d.bucket), object)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, api, nil)
	if err != nil {
		http.Error(w, "downloads are temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := d.http.Do(req)
	if err != nil {
		d.log.Error("release bucket read failed", "asset", asset, "err", err)
		http.Error(w, "downloads are temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// A 404 from the bucket is a release that was never uploaded, which is
		// our problem rather than the caller's, so it is logged loudly and
		// still answered as a plain not-found.
		d.log.Error("release artifact is missing from the bucket",
			"channel", channel, "asset", asset, "status", resp.StatusCode)
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", contentType)
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	// Never cached. A channel called "latest" that a proxy holds for a day is a
	// fleet installing yesterday's agent and no way to tell from here.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, err := io.Copy(w, resp.Body); err != nil {
		// The response is already streaming, so the status is sent and there is
		// nothing to tell the caller. It is worth a log line because a download
		// that dies halfway leaves install.sh reporting a checksum mismatch,
		// which reads like a tampered binary rather than a dropped connection.
		d.log.Warn("release download interrupted", "asset", asset, "err", err)
	}
}

// accessToken returns a cached token, refreshing before it expires.
func (d *Downloads) accessToken() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cached != "" && time.Now().Add(tokenRefreshMargin).Before(d.cachedAt) {
		return d.cached, nil
	}
	tok, exp, err := d.token()
	if err != nil {
		return "", err
	}
	d.cached, d.cachedAt = tok, exp
	return tok, nil
}

// metadataToken reads the instance service account's token from the metadata
// server, which is how Cloud Run hands a workload its own identity without a
// key file ever existing.
func metadataToken() (string, time.Time, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, metadataTokenURL, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := (&http.Client{Timeout: metadataTimeout}).Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("app: reach the metadata server: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("app: metadata server answered %d", resp.StatusCode)
	}
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	// The body is a few hundred bytes; the cap is here so a wrong URL cannot be
	// read into memory unbounded.
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&body); err != nil {
		return "", time.Time{}, fmt.Errorf("app: read the metadata token: %w", err)
	}
	if body.AccessToken == "" {
		return "", time.Time{}, errors.New("app: the metadata server returned no access token")
	}
	return body.AccessToken, time.Now().Add(time.Duration(body.ExpiresIn) * time.Second), nil
}
