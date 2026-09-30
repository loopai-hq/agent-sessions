// Package links finds and classifies the URLs a session mentioned.
//
// The point is not to collect every string beginning with http. It is that a
// session's real outputs are mostly things that live somewhere else — the PR it
// opened, the issue it closed, the runbook it followed — and today those exist
// only as text buried in a transcript nobody re-reads. Classifying them is what
// turns "this session mentioned a URL" into "this session produced PR #4".
//
// Extraction is deliberately conservative. A false positive here becomes a dead
// hyperlink on a session page, which teaches people the panel is unreliable and
// is worse than omitting a borderline match.
package links

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Kind classifies what a URL points at, in the vocabulary somebody scanning a
// session would use.
type Kind string

const (
	// KindPR is a pull request.
	KindPR Kind = "pr"
	// KindIssue is an issue.
	KindIssue Kind = "issue"
	// KindCommit is a commit.
	KindCommit Kind = "commit"
	// KindRepo is a repository.
	KindRepo Kind = "repo"
	// KindDoc is a document.
	KindDoc Kind = "doc"
	// KindSlack is a Slack message or channel.
	KindSlack Kind = "slack"
	// KindOther is a URL none of the rules recognised.
	KindOther Kind = "other"
)

// Link is one classified URL.
type Link struct {
	URL  string
	Kind Kind
	Host string
	// Ref is the short human name for the link: "owner/repo#4" for a pull
	// request, "owner/repo@abc1234" for a commit, "owner/repo" for a repository.
	// Empty when there is no shorter honest way to say it than the URL.
	Ref string
}

// urlPattern matches an absolute http(s) URL.
//
// The character class stops at whitespace and at the handful of characters that
// are far more often prose than URL — quotes, angle brackets, backticks. What it
// deliberately still allows through is trailing punctuation, because "see
// https://x/y." is indistinguishable from a URL ending in a dot at this stage;
// trim handles that afterwards, where the balance of brackets can be considered.
var urlPattern = regexp.MustCompile(`https?://[^\s<>"'` + "`" + `\\|^{}]+`)

// trailing is punctuation that ends a sentence rather than a URL.
const trailing = ".,;:!?"

// MaxURL bounds what is considered a link at all.
//
// Two reasons, and the second is the one that would page somebody. Nothing a
// person clicks is this long, so past it the string is a data URI, a signed
// blob, or base64 that happened to start with http. And links are stored under a
// unique index on (session_id, url), where a btree entry over roughly 2,700
// bytes is rejected outright — so an unbounded URL would not be an ugly row, it
// would be an ingest error that fails the whole batch containing it.
const MaxURL = 2048

// Extract returns the distinct links in text, in the order they first appear.
//
// Order matters: it is the order a reader would meet them, so a session panel
// listing them reads as a narrative rather than as an arbitrary set.
func Extract(text string) []Link {
	if !strings.Contains(text, "http") {
		return nil
	}
	seen := make(map[string]bool)
	var out []Link
	for _, raw := range urlPattern.FindAllString(text, -1) {
		clean := trim(raw)
		if clean == "" || len(clean) > MaxURL || seen[clean] {
			continue
		}
		l, ok := classify(clean)
		if !ok {
			continue
		}
		seen[clean] = true
		out = append(out, l)
	}
	return out
}

// trim removes the punctuation that belongs to the sentence rather than to the
// URL.
//
// The bracket handling is the part worth spelling out: markdown writes
// [text](https://x/y), and a URL that swallows the closing paren 404s. But
// Wikipedia-style URLs legitimately contain balanced parens, so the rule is to
// drop a trailing ')' only when the URL does not open one itself.
func trim(s string) string {
	for {
		if s == "" {
			return ""
		}
		last := s[len(s)-1]
		switch {
		case strings.IndexByte(trailing, last) >= 0:
			s = s[:len(s)-1]
		case last == ')' && strings.Count(s, "(") < strings.Count(s, ")"):
			s = s[:len(s)-1]
		case last == ']' && strings.Count(s, "[") < strings.Count(s, "]"):
			s = s[:len(s)-1]
		default:
			return s
		}
	}
}

// docHosts are hosts whose links are reference material rather than code.
// Matched as suffixes so a subdomain counts.
var docHosts = []string{
	"notion.so", "docs.google.com", "drive.google.com",
	"atlassian.net", "linear.app", "figma.com", "getoutline.com",
}

func classify(raw string) (Link, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return Link{}, false
	}
	host := strings.ToLower(u.Host)
	l := Link{URL: raw, Host: host, Kind: KindOther}

	switch {
	case host == "github.com" || strings.HasSuffix(host, ".github.com"):
		if k, ref, ok := classifyGitHub(u.Path); ok {
			l.Kind, l.Ref = k, ref
		}
	case strings.HasSuffix(host, ".slack.com"):
		l.Kind = KindSlack
	default:
		for _, d := range docHosts {
			if host == d || strings.HasSuffix(host, "."+d) {
				l.Kind = KindDoc
				break
			}
		}
	}
	return l, true
}

// classifyGitHub reads the shape GitHub URLs have had for a decade:
// /owner/repo/<what>/<which>.
func classifyGitHub(path string) (Kind, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return KindOther, "", false
	}
	repo := parts[0] + "/" + parts[1]
	if len(parts) == 2 {
		return KindRepo, repo, true
	}
	switch parts[2] {
	case "pull", "pulls":
		if n, ok := number(parts, 3); ok {
			return KindPR, repo + "#" + n, true
		}
	case "issues":
		if n, ok := number(parts, 3); ok {
			return KindIssue, repo + "#" + n, true
		}
	case "commit", "commits":
		if len(parts) > 3 && parts[3] != "" {
			sha := parts[3]
			if len(sha) > 7 {
				sha = sha[:7]
			}
			return KindCommit, repo + "@" + sha, true
		}
	}
	return KindRepo, repo, true
}

func number(parts []string, i int) (string, bool) {
	if len(parts) <= i {
		return "", false
	}
	if _, err := strconv.Atoi(parts[i]); err != nil {
		return "", false
	}
	return parts[i], true
}
