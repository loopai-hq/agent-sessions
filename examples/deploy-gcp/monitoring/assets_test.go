// Package monitoring holds no Go code; this test guards the monitoring
// assets beside it, which are applied by apply.sh and read by nothing else
// in the repository. Without it a policy that names a metric nobody defined,
// or a runbook anchor nobody wrote, is discovered by the alert that never
// fires.
package monitoring

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/loopai-hq/loop-sessions/server/fleet"
)

// policy is the part of the Monitoring v3 AlertPolicy shape these assertions
// read. Unknown fields are left to the API.
type policy struct {
	DisplayName string            `json:"displayName"`
	Combiner    string            `json:"combiner"`
	Enabled     *bool             `json:"enabled"`
	Severity    string            `json:"severity"`
	UserLabels  map[string]string `json:"userLabels"`
	Conditions  []struct {
		DisplayName string `json:"displayName"`
		Threshold   *struct {
			Filter       string        `json:"filter"`
			Duration     string        `json:"duration"`
			Aggregations []aggregation `json:"aggregations"`
		} `json:"conditionThreshold"`
		Absent *struct {
			Filter       string        `json:"filter"`
			Aggregations []aggregation `json:"aggregations"`
		} `json:"conditionAbsent"`
		// A PromQL condition is the only way to alert on the delta of a
		// GAUGE metric (10b); its query names metrics in Prometheus form,
		// so the user-metric check below reads it like a filter.
		Prometheus *struct {
			Query string `json:"query"`
		} `json:"conditionPrometheusQueryLanguage"`
	} `json:"conditions"`
	AlertStrategy struct {
		AutoClose string `json:"autoClose"`
	} `json:"alertStrategy"`
	Documentation struct {
		MimeType string `json:"mimeType"`
		Content  string `json:"content"`
		Links    []struct {
			DisplayName string `json:"displayName"`
			URL         string `json:"url"`
		} `json:"links"`
	} `json:"documentation"`
	NotificationChannels []string `json:"notificationChannels"`
}

type aggregation struct {
	PerSeriesAligner   string `json:"perSeriesAligner"`
	CrossSeriesReducer string `json:"crossSeriesReducer"`
}

var (
	userMetric  = regexp.MustCompile(`logging\.googleapis\.com/user/loop_sessions/([a-z0-9_]+)`)
	placeholder = regexp.MustCompile(`__([A-Z_]+)__`)
	anchorRef   = regexp.MustCompile(`README\.md#([a-z0-9-]+)`)
)

// The metrics contract A names, every one of which must have a definition
// and count lines (a DELTA INT64 counter).
var requiredMetrics = []string{
	"ingest_batches", "ingest_rejected_items", "ingest_undecided", "ingest_5xx",
	"health_posts", "readyz_failed", "server_catch", "slack_live_pass_failed",
	// The skill-usage route counters (design 7.2): accepted and refused
	// posts, the soft-revoke replies, 5xx from the request log, and the
	// derived copies that displaced an API row.
	"skill_accepted", "skill_rejected", "skill_soft_revoked", "skill_5xx", "skill_preempted",
	// The reconciler runs whose row count the server did not see (design
	// 5.4, T12), policy 17's fourth condition.
	"skill_reconciler_mismatch",
}

// The counters contract E adds: one line per machine over a rule, so a
// count of lines is the count of machines.
var requiredFleetCounters = []string{"fleet_drops", "fleet_empty_start", "derive_step_failed", "derive_session_failed",
	// The skill-usage counters (design 7.2): a store failure behind 17c,
	// and one line per ingest batch that derived a skill row.
	"skill_store_error", "skill_derived"}

// The gauge-like figures contract E adds. A counter metric counts lines, not
// field values, so a number the evaluator writes once per tick has to go
// through a DISTRIBUTION with a valueExtractor; ALIGN_PERCENTILE_99 over one
// alignment period reads the single sample back as the value.
var requiredFleetDistributions = []string{
	"fleet_capture_blocked", "fleet_quarantine_devices", "fleet_parked_devices", "fleet_silent",
	"fleet_empty_rate", "fleet_missing_answer_rate", "fleet_behind", "fleet_unmanaged",
	"derive_stalled_minutes", "derive_rows_per_sec",
	// The skill-usage gauges (design 7.2): the silence of each expected
	// series, and the re-derive queue's depth and age, all from the "skill
	// platform summary" line.
	"skill_silence_minutes", "skill_rederive_queue", "skill_rederive_oldest_minutes",
	// The days until each live source token expires, from the "skill
	// token summary" line, read at REDUCE_MIN by 17d.
	"skill_token_expiry_days",
}

// The r3 alert numbers contract A names, as filename prefixes.
var requiredPolicies = []string{
	"01-", "01b-", "03-", "06b-", "10-", "10b-", "11-", "11b-", "12-", "12b-", "12c-", "13-", "13b-",
	"17c-",
	"17-", "17d-",
}

// The r3 alert numbers contract E adds (the fleet tier, the parked and
// derive policies), as filename prefixes.
var requiredFleetPolicies = []string{
	"02-", "04-", "04b-", "04c-", "05-", "06a-", "07-", "08-", "09-", "09b-", "15-", "16-", "16b-",
	"18-",
	"17b-", "17e-", "18b-",
}

// disabledPolicyNote is the sentence a policy that ships disabled must open
// its documentation with: a disabled alert is a decision, and the reader of
// the console has to be able to see that it was one.
const disabledPolicyNote = "This policy ships disabled"

func loadPolicies(t *testing.T) map[string]policy {
	t.Helper()
	files, err := filepath.Glob("policies/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no policy files: %v", err)
	}
	out := map[string]policy{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		var p policy
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatalf("%s is not valid JSON: %v", f, err)
		}
		out[f] = p
	}
	return out
}

func metricYAML(t *testing.T, name string) (string, bool) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("metrics", name+".yaml"))
	if err != nil {
		return "", false
	}
	return string(raw), true
}

// yamlField reads a top-level "key: value" line from a metric file. The
// files are flat enough that a YAML parser would be a dependency for nothing.
func yamlField(doc, key string) string {
	for _, line := range strings.Split(doc, "\n") {
		if v, ok := strings.CutPrefix(line, key+":"); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// TestEveryPolicyIsShapedForApply is the per-policy contract: labelled so
// apply.sh can find it again, documented so the page that fires carries its
// runbook, and pointed at a channel so somebody hears it.
func TestEveryPolicyIsShapedForApply(t *testing.T) {
	for f, p := range loadPolicies(t) {
		if !strings.HasPrefix(p.DisplayName, "loop-sessions: ") {
			t.Errorf("%s: displayName %q does not start with \"loop-sessions: \"; the console lists seventy policies and the prefix is how ours are found", f, p.DisplayName)
		}
		if p.UserLabels["app"] != "loop-sessions" {
			t.Errorf("%s: userLabels.app = %q, want loop-sessions; apply.sh lists by it", f, p.UserLabels["app"])
		}
		switch p.UserLabels["tier"] {
		case "infra", "fleet", "data":
		default:
			t.Errorf("%s: userLabels.tier = %q, want infra, fleet or data", f, p.UserLabels["tier"])
		}
		switch p.Severity {
		case "CRITICAL", "ERROR", "WARNING":
		default:
			t.Errorf("%s: severity = %q", f, p.Severity)
		}
		if p.Combiner == "" {
			t.Errorf("%s: no combiner", f)
		}
		switch {
		case p.Enabled == nil:
			t.Errorf("%s: no enabled field", f)
		case !*p.Enabled && !strings.Contains(p.Documentation.Content, disabledPolicyNote):
			// A policy nobody meant to disable is an alert that never fires;
			// one that ships disabled on purpose (the missing-answer rate
			// before the rollout day) says so, and says when to enable it.
			t.Errorf("%s: not enabled, and the documentation does not open with %q to say why and when to enable it", f, disabledPolicyNote)
		case !*p.Enabled && !strings.Contains(p.Documentation.Content, "enable"):
			t.Errorf("%s: disabled without saying how to enable it", f)
		}
		if p.UserLabels["tier"] == "fleet" {
			// The fleet tier goes to the fleet channel: an alert about a
			// person's laptop in the infra channel is an alert nobody there can act on.
			found := false
			for _, ch := range p.NotificationChannels {
				if strings.Contains(ch, "__SLACK_FLEET__") {
					found = true
				}
			}
			if !found {
				t.Errorf("%s: a fleet-tier policy that does not notify __SLACK_FLEET__", f)
			}
		}
		if len(p.Conditions) == 0 {
			t.Errorf("%s: no conditions", f)
		}
		for i, c := range p.Conditions {
			if c.DisplayName == "" {
				t.Errorf("%s: condition %d has no displayName", f, i)
			}
			// The API refuses a threshold duration over 24h30m (observed
			// on a 172800s rule: "Durations longer than 24h30m are not supported"),
			// and apply.sh stops at the first refused policy, leaving the rest
			// unapplied. Longer rules are written as a shorter duration on a
			// metric that already carries the age.
			if c.Threshold != nil && c.Threshold.Duration != "" {
				secs, err := strconv.Atoi(strings.TrimSuffix(c.Threshold.Duration, "s"))
				if err != nil || secs < 0 || secs > 24*3600+30*60 || secs%60 != 0 {
					t.Errorf("%s: condition %d duration %q must be a whole number of minutes up to 24h30m", f, i, c.Threshold.Duration)
				}
			}
			if c.Threshold == nil && c.Absent == nil && c.Prometheus == nil {
				t.Errorf("%s: condition %d is neither a threshold, an absence nor a PromQL rule", f, i)
			}
		}
		if p.AlertStrategy.AutoClose == "" {
			t.Errorf("%s: no alertStrategy.autoClose; an incident that resolved itself stays open forever", f)
		}
		if p.Documentation.MimeType != "text/markdown" {
			t.Errorf("%s: documentation.mimeType = %q, want text/markdown", f, p.Documentation.MimeType)
		}
		if strings.TrimSpace(p.Documentation.Content) == "" {
			t.Errorf("%s: no documentation.content; the alert would arrive with nothing to do", f)
		}
		if len(p.Documentation.Links) == 0 || len(p.Documentation.Links) > 3 {
			t.Errorf("%s: %d documentation links, want 1 to 3", f, len(p.Documentation.Links))
		}
		if len(p.NotificationChannels) == 0 {
			t.Errorf("%s: no notificationChannels", f)
		}
		for _, ch := range p.NotificationChannels {
			if !strings.HasPrefix(ch, "projects/__PROJECT__/notificationChannels/") {
				t.Errorf("%s: channel %q is not a rendered projects/__PROJECT__/notificationChannels/ name", f, ch)
			}
		}
	}
}

// TestPlaceholdersAreOnlyTheOnesApplyRenders: a placeholder apply.sh does
// not know is one it will refuse the file for, which is the right outcome at
// apply time and a wasted round trip at review time.
func TestPlaceholdersAreOnlyTheOnesApplyRenders(t *testing.T) {
	known := map[string]bool{"PROJECT": true, "PUBLIC_URL": true, "RELEASE_BUCKET": true, "ANALYTICS_BUCKET": true, "SLACK_INFRA": true, "SLACK_FLEET": true, "EMAIL_OWNER": true, "UPTIME_CHECK_ID": true}
	apply, err := os.ReadFile("apply.sh")
	if err != nil {
		t.Fatalf("read apply.sh: %v", err)
	}
	files, _ := filepath.Glob("policies/*.json")
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, m := range placeholder.FindAllStringSubmatch(string(raw), -1) {
			if !known[m[1]] {
				t.Errorf("%s uses placeholder %s, which apply.sh does not render", f, m[0])
			}
			if !strings.Contains(string(apply), m[0]) {
				t.Errorf("%s uses placeholder %s, which does not appear in apply.sh", f, m[0])
			}
		}
	}
}

// TestEveryMetricAPolicyReferencesIsDefined: the filter in a policy names a
// log-based metric by type; the yaml beside it is what creates that metric,
// and a policy on a metric with no yaml is created fine and never evaluates.
func TestEveryMetricAPolicyReferencesIsDefined(t *testing.T) {
	for f, p := range loadPolicies(t) {
		for _, c := range p.Conditions {
			var filter string
			switch {
			case c.Threshold != nil:
				filter = c.Threshold.Filter
			case c.Absent != nil:
				filter = c.Absent.Filter
			case c.Prometheus != nil:
				filter = c.Prometheus.Query
			}
			for _, m := range userMetric.FindAllStringSubmatch(filter, -1) {
				doc, ok := metricYAML(t, m[1])
				if !ok {
					t.Errorf("%s references metric %s, and metrics/%s.yaml does not exist", f, m[0], m[1])
					continue
				}
				if got := yamlField(doc, "name"); got != "loop_sessions/"+m[1] {
					t.Errorf("metrics/%s.yaml declares name %q, want loop_sessions/%s", m[1], got, m[1])
				}
			}
		}
	}
}

// alignersByValueType is the Monitoring API's Aligner table
// (https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alertPolicies#Aligner)
// restricted to DELTA metrics, which every log-based metric is. The API
// refuses the others at apply time with "The aligner cannot be applied to
// metrics with kind DELTA and value type DISTRIBUTION" (seen on
// ALIGN_COUNT over export_lag_seconds), and apply.sh stops at the first
// refused policy, so the ones after it are never created.
var alignersByValueType = map[string]map[string]bool{
	"INT64":        set("ALIGN_NONE", "ALIGN_DELTA", "ALIGN_RATE", "ALIGN_MIN", "ALIGN_MAX", "ALIGN_MEAN", "ALIGN_COUNT", "ALIGN_SUM", "ALIGN_STDDEV", "ALIGN_PERCENT_CHANGE"),
	"DISTRIBUTION": set("ALIGN_NONE", "ALIGN_DELTA", "ALIGN_SUM", "ALIGN_PERCENTILE_99", "ALIGN_PERCENTILE_95", "ALIGN_PERCENTILE_50", "ALIGN_PERCENTILE_05"),
}

func set(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// metricValueType reads the descriptor's valueType from a metric file: the
// first "valueType:" line after "metricDescriptor:" (label valueTypes come
// later, under labels). Empty when the file has no descriptor.
func metricValueType(doc string) string {
	inDescriptor := false
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(line, "metricDescriptor:") {
			inDescriptor = true
			continue
		}
		if !inDescriptor {
			continue
		}
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "valueType:"); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// TestEveryAlignerFitsItsMetric: a condition's per-series aligner must be one
// the API accepts for the value type of the log-based metric it reads, and a
// threshold on a distribution must align by percentile, since the comparison
// needs a number. The other conditions on a distribution (absence) may also
// delta or sum it.
func TestEveryAlignerFitsItsMetric(t *testing.T) {
	for f, p := range loadPolicies(t) {
		for i, c := range p.Conditions {
			var filter string
			var aggs []aggregation
			threshold := false
			switch {
			case c.Threshold != nil:
				filter, aggs, threshold = c.Threshold.Filter, c.Threshold.Aggregations, true
			case c.Absent != nil:
				filter, aggs = c.Absent.Filter, c.Absent.Aggregations
			default:
				continue
			}
			m := userMetric.FindStringSubmatch(filter)
			if m == nil {
				continue
			}
			doc, ok := metricYAML(t, m[1])
			if !ok {
				continue // TestEveryMetricAPolicyReferencesIsDefined reports it
			}
			vt := metricValueType(doc)
			allowed, known := alignersByValueType[vt]
			if !known {
				t.Errorf("metrics/%s.yaml: valueType %q is not one this test knows (INT64 or DISTRIBUTION)", m[1], vt)
				continue
			}
			for _, a := range aggs {
				if !allowed[a.PerSeriesAligner] {
					t.Errorf("%s: condition %d aligns %s (%s) with %s, which the API refuses for that value type", f, i, m[1], vt, a.PerSeriesAligner)
				}
				if threshold && vt == "DISTRIBUTION" && !strings.HasPrefix(a.PerSeriesAligner, "ALIGN_PERCENTILE_") {
					t.Errorf("%s: condition %d compares a threshold against %s (a distribution) aligned by %s; a threshold needs a percentile", f, i, m[1], a.PerSeriesAligner)
				}
			}
		}
	}
}

// TestRequiredMetricsAreDefinedAndWellFormed pins the set contract A names
// and the shape apply.sh feeds to `gcloud logging metrics create`.
func TestRequiredMetricsAreDefinedAndWellFormed(t *testing.T) {
	for _, name := range requiredMetrics {
		doc, ok := metricYAML(t, name)
		if !ok {
			t.Errorf("metrics/%s.yaml is missing", name)
			continue
		}
		if got := yamlField(doc, "name"); got != "loop_sessions/"+name {
			t.Errorf("metrics/%s.yaml: name = %q, want loop_sessions/%s", name, got, name)
		}
		if yamlField(doc, "description") == "" {
			t.Errorf("metrics/%s.yaml: no description", name)
		}
		if !strings.Contains(doc, `resource.labels.service_name="loop-sessions"`) {
			t.Errorf("metrics/%s.yaml: the filter is not scoped to the loop-sessions service", name)
		}
		if !strings.Contains(doc, "metricKind: DELTA") || !strings.Contains(doc, "valueType: INT64") {
			t.Errorf("metrics/%s.yaml: not a DELTA INT64 counter", name)
		}
	}
	for _, name := range requiredFleetCounters {
		doc, ok := metricYAML(t, name)
		if !ok {
			t.Errorf("metrics/%s.yaml is missing", name)
			continue
		}
		if !strings.Contains(doc, `resource.labels.service_name="loop-sessions"`) || !strings.Contains(doc, `jsonPayload.message=`) {
			t.Errorf("metrics/%s.yaml: the filter is not scoped to one of the service's log lines", name)
		}
		if !strings.Contains(doc, "metricKind: DELTA") || !strings.Contains(doc, "valueType: INT64") {
			t.Errorf("metrics/%s.yaml: not a DELTA INT64 counter", name)
		}
	}
	for _, name := range requiredFleetDistributions {
		doc, ok := metricYAML(t, name)
		if !ok {
			t.Errorf("metrics/%s.yaml is missing", name)
			continue
		}
		if !strings.Contains(doc, `resource.labels.service_name="loop-sessions"`) || !strings.Contains(doc, `jsonPayload.message=`) {
			t.Errorf("metrics/%s.yaml: the filter is not scoped to one of the service's log lines", name)
		}
		if !strings.Contains(doc, "valueType: DISTRIBUTION") {
			t.Errorf("metrics/%s.yaml: a gauge-like figure that is not a DISTRIBUTION; a counter would count lines, not the value", name)
		}
		if v := yamlField(doc, "valueExtractor"); !strings.HasPrefix(v, "EXTRACT(jsonPayload.") {
			t.Errorf("metrics/%s.yaml: valueExtractor %q does not read a jsonPayload field", name, v)
		}
		if !strings.Contains(doc, "bucketOptions:") {
			t.Errorf("metrics/%s.yaml: a DISTRIBUTION with no bucketOptions", name)
		}
	}
	files, _ := filepath.Glob("metrics/*.yaml")
	for _, f := range files {
		raw, _ := os.ReadFile(f)
		want := "loop_sessions/" + strings.TrimSuffix(filepath.Base(f), ".yaml")
		if got := yamlField(string(raw), "name"); got != want {
			t.Errorf("%s declares name %q, want %q (apply.sh reads the name from the file, so the two must agree)", f, got, want)
		}
	}
}

// TestRequiredPoliciesExist pins the r3 alert numbers contract A names.
func TestRequiredPoliciesExist(t *testing.T) {
	files, _ := filepath.Glob("policies/*.json")
	for _, prefix := range append(append([]string{}, requiredPolicies...), requiredFleetPolicies...) {
		found := false
		for _, f := range files {
			if strings.HasPrefix(filepath.Base(f), prefix) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no policy file for alert %s", strings.TrimSuffix(prefix, "-"))
		}
	}
}

// slug renders a markdown heading the way GitHub anchors it: lower case,
// punctuation dropped, spaces to hyphens.
func slug(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(heading)) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('-')
		}
	}
	return b.String()
}

// readmeAnchors collects the anchors the deploy README's headings produce.
func readmeAnchors(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "README.md"))
	if err != nil {
		t.Fatalf("read ../README.md: %v", err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "#") {
			out[slug(strings.TrimLeft(line, "# "))] = true
		}
	}
	return out
}

// TestEveryRunbookAnchorAPolicyNamesExistsInTheREADME: the documentation on
// a firing alert links to a README anchor, and an anchor that does not exist
// lands the operator at the top of a long page during an incident.
func TestEveryRunbookAnchorAPolicyNamesExistsInTheREADME(t *testing.T) {
	anchors := readmeAnchors(t)
	required := []string{
		"runbook-ingest-5xx", "runbook-undecided", "runbook-cloudsql",
		"runbook-migrations", "runbook-apply-monitoring",
		// The fleet runbooks contract E adds; the CTA rows on /admin/fleet
		// link the same anchors (server/web/fleet_page.go, runbookBase).
		"runbook-capture-blocked", "runbook-quarantine", "runbook-parked", "runbook-drops",
		"runbook-backlog", "runbook-enrol", "runbook-hooks", "runbook-silent", "runbook-upgrade",
		"runbook-empty-sessions", "runbook-missing-answers", "runbook-derive",
		// The skill-usage runbook (design 7.4): 17c and 18 link it.
		"runbook-skill-invocations",
		// The source token runbook: 17, 17b, 17d, 17e and 18b link it.
		"runbook-source-tokens",
	}
	for _, a := range required {
		if !anchors[a] {
			t.Errorf("../README.md has no heading that anchors as #%s", a)
		}
	}
	for f, p := range loadPolicies(t) {
		refs := anchorRef.FindAllStringSubmatch(p.Documentation.Content, -1)
		for _, l := range p.Documentation.Links {
			refs = append(refs, anchorRef.FindAllStringSubmatch(l.URL, -1)...)
		}
		if len(refs) == 0 {
			t.Errorf("%s: documentation names no README runbook anchor", f)
		}
		for _, m := range refs {
			if !anchors[m[1]] {
				t.Errorf("%s: documentation links to README.md#%s, which no heading produces", f, m[1])
			}
		}
	}
}

// TestChannelsAndScriptAreInPlace: the pieces apply.sh needs beside it.
func TestChannelsAndScriptAreInPlace(t *testing.T) {
	env, err := os.ReadFile("channels.env")
	if err != nil {
		t.Fatalf("read channels.env: %v", err)
	}
	if !strings.Contains(string(env), "\nSLACK_INFRA=") {
		t.Error("channels.env has no SLACK_INFRA line")
	}
	if !strings.Contains(string(env), "\nEMAIL_OWNER=") {
		t.Error("channels.env has no EMAIL_OWNER line")
	}
	// The fleet channel is a placeholder until the console step; the line
	// has to exist so apply.sh's refusal names it rather than an unset
	// variable.
	if !strings.Contains(string(env), "\nSLACK_FLEET=") {
		t.Error("channels.env has no SLACK_FLEET line")
	}
	if script, _ := os.ReadFile("apply.sh"); !strings.Contains(string(script), "__SLACK_FLEET__") ||
		!strings.Contains(string(script), "for var in SLACK_INFRA SLACK_FLEET EMAIL_OWNER") {
		t.Error("apply.sh does not render __SLACK_FLEET__ or does not refuse an empty SLACK_FLEET")
	}

	info, err := os.Stat("apply.sh")
	if err != nil {
		t.Fatalf("stat apply.sh: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Error("apply.sh is not executable")
	}
	apply, _ := os.ReadFile("apply.sh")
	for _, want := range []string{"#!/usr/bin/env bash", "--dry-run", "logging metrics describe", "logging metrics update", "logging metrics create",
		"uptime list-configs", "monitoring policies list", "monitoring policies update", "monitoring policies create",
		"metricDescriptors", "__[A-Z_]+__"} {
		if !strings.Contains(string(apply), want) {
			t.Errorf("apply.sh does not contain %q", want)
		}
	}
	// The descriptor wait GETs .../metricDescriptors/<metric type> with the
	// type as it is. The API answers 400 "Invalid metric name" to the type
	// with its slashes escaped as %2F, and the script once sent exactly
	// that, so every real apply died in the wait after the metrics and the
	// uptime check had already been applied. Comment lines are skipped: the
	// script is allowed to say why it does not escape.
	for i, line := range strings.Split(string(apply), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if strings.Contains(line, "%2F") {
			t.Errorf("apply.sh line %d escapes the metric type in the descriptor URL; the Monitoring API rejects that form (see the descriptors phase)", i+1)
		}
	}

	raw, err := os.ReadFile("uptime/readyz.json")
	if err != nil {
		t.Fatalf("read uptime/readyz.json: %v", err)
	}
	var u struct {
		DisplayName    string   `json:"displayName"`
		Host           string   `json:"host"`
		Path           string   `json:"path"`
		Regions        []string `json:"regions"`
		MatcherType    string   `json:"matcherType"`
		MatcherContent string   `json:"matcherContent"`
	}
	if err := json.Unmarshal(raw, &u); err != nil {
		t.Fatalf("uptime/readyz.json: %v", err)
	}
	if u.Path != "/readyz" {
		t.Errorf("uptime path = %q, want /readyz; /healthz is swallowed by Google's front end on a run.app URL", u.Path)
	}
	if u.MatcherContent != "ready" {
		t.Errorf("uptime matcher = %q, want the body /readyz writes when the database answered", u.MatcherContent)
	}
	// apply.sh passes matcherType straight to `gcloud monitoring uptime
	// create --matcher-type`, and gcloud enum flags do not prefix-match: the
	// file once said "contains", and the first real apply would have died in
	// its uptime phase after the metrics were already applied. These are the
	// six choices `gcloud monitoring uptime create --help` lists.
	switch u.MatcherType {
	case "contains-string", "not-contains-string", "matches-regex", "not-matches-regex", "matches-json-path", "not-matches-json-path":
	default:
		t.Errorf("uptime matcherType = %q, which is not a --matcher-type value gcloud accepts (contains-string, not-contains-string, matches-regex, not-matches-regex, matches-json-path, not-matches-json-path)", u.MatcherType)
	}
	if len(u.Regions) < 3 {
		t.Errorf("uptime check uses %d regions; Monitoring requires at least three", len(u.Regions))
	}
	if u.DisplayName == "" || u.Host == "" {
		t.Error("uptime check has no displayName or host")
	}
}

// TestThePolicyAndThePageAgreeOnTheMissingAnswerThreshold: the fleet page's
// CTA rule and alert policy 08 both mean "act on the missing answers", and a
// version of this repository where they disagreed about when would give an
// operator two different answers from two surfaces. The page reads
// fleet.MissingAnswerAlertRate; this pins the policy file to the same number,
// so changing one without the other fails here rather than in production.
func TestThePolicyAndThePageAgreeOnTheMissingAnswerThreshold(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("policies", "08-fleet-missing-answer.json"))
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		DisplayName string `json:"displayName"`
		Conditions  []struct {
			ConditionThreshold struct {
				ThresholdValue float64 `json:"thresholdValue"`
			} `json:"conditionThreshold"`
		} `json:"conditions"`
	}
	if err := json.Unmarshal(raw, &policy); err != nil {
		t.Fatalf("08-fleet-missing-answer.json: %v", err)
	}
	if len(policy.Conditions) != 1 {
		t.Fatalf("the policy has %d conditions, want one", len(policy.Conditions))
	}
	if got := policy.Conditions[0].ConditionThreshold.ThresholdValue; got != fleet.MissingAnswerAlertRate {
		t.Errorf("policy 08 fires at %v, the fleet page's CTA at %v; they must be the same number",
			got, fleet.MissingAnswerAlertRate)
	}
	// The title carries the number too, and a title that disagrees with the
	// threshold is what an operator reads in the incident.
	pct := fmt.Sprintf("%.0f%%", 100*fleet.MissingAnswerAlertRate)
	if !strings.Contains(policy.DisplayName, pct) {
		t.Errorf("the policy is titled %q, which does not name its own threshold of %s", policy.DisplayName, pct)
	}
}

// TestEveryPolicyCarriesItsFileNameAsTheAssetLabel: apply.sh finds the live
// policy to update by userLabels.asset, not by display name. A display name
// is prose somebody is meant to improve, and keying on it means the first
// such improvement orphans the live policy and creates a second one beside
// it. That happened once: policy 08's title moved from "more than
// 5%" to "more than 10%", the apply created a second policy, and the stale
// one alerted at the old threshold until it was deleted by hand. This keeps
// every file's key present and equal to its name, so a new policy cannot
// ship without one.
func TestEveryPolicyCarriesItsFileNameAsTheAssetLabel(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("policies", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no policy files: %v", err)
	}
	seen := map[string]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var policy struct {
			DisplayName string            `json:"displayName"`
			UserLabels  map[string]string `json:"userLabels"`
		}
		if err := json.Unmarshal(raw, &policy); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		want := strings.TrimSuffix(filepath.Base(f), ".json")
		got := policy.UserLabels["asset"]
		if got != want {
			t.Errorf("%s carries userLabels.asset=%q, want %q (apply.sh matches the live policy on it)", filepath.Base(f), got, want)
		}
		if policy.UserLabels["app"] != "loop-sessions" {
			t.Errorf("%s is not labelled app=loop-sessions, so the apply would not find it at all", filepath.Base(f))
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("%s and %s share the asset label %q; the apply would refuse both", prev, filepath.Base(f), got)
		}
		seen[got] = filepath.Base(f)
	}
}
