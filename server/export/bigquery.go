package export

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// BigQuery starts load jobs through the REST API and polls them.
//
// Two endpoints: jobs.insert with a load configuration, and jobs.get until
// the state is DONE. The job id is chosen by the run (LoadJob.ID), so an
// insert that is retried after a lost response is answered 409 and treated
// as the job it already started, never as a second load into the same
// partition.
type BigQuery struct {
	Project  string
	Dataset  string
	Location string
	// SourceBucket is the bucket the loads read from: the one the GCS
	// client beside this loader writes to.
	SourceBucket string
	Token        TokenSource
	// HTTP is the client; nil takes one with a request timeout, since every
	// call here is a small JSON exchange.
	HTTP *http.Client
	// Endpoint is the API base, for the tests; empty is the real one.
	Endpoint string
	// Poll is the interval between jobs.get calls; zero takes two seconds.
	Poll time.Duration
	// Backoff is the wait before the one retry of a call the API answered
	// 429 or 5xx; zero takes two seconds.
	Backoff time.Duration
}

const (
	bigqueryEndpoint = "https://bigquery.googleapis.com"
	bigqueryTimeout  = 60 * time.Second
	bigqueryPoll     = 2 * time.Second
	bigqueryBackoff  = 2 * time.Second
	// bigqueryAttempts is how many times one call is made: the first, and
	// one retry after a 429 or a 5xx. Without it a transient answer while
	// polling a load that is in fact succeeding marks the load failed, fails
	// the run (safe: no watermark moves) and fires the load-failed alert for
	// a load that did not fail (review-1 M4). One retry, not a loop: the
	// run has its own budget, and a second 5xx is worth a line in the log.
	// A retried insert is safe because the job id is the run's (409 is
	// "already started").
	bigqueryAttempts = 2
)

func (b *BigQuery) base() string {
	if b.Endpoint != "" {
		return b.Endpoint
	}
	return bigqueryEndpoint
}

func (b *BigQuery) client() *http.Client {
	if b.HTTP != nil {
		return b.HTTP
	}
	return &http.Client{Timeout: bigqueryTimeout}
}

// loadRequest is the part of the Jobs resource a load needs. Field names
// are the API's.
type loadRequest struct {
	JobReference struct {
		ProjectID string `json:"projectId"`
		JobID     string `json:"jobId"`
		Location  string `json:"location"`
	} `json:"jobReference"`
	Configuration struct {
		Load struct {
			SourceURIs       []string `json:"sourceUris"`
			SourceFormat     string   `json:"sourceFormat"`
			WriteDisposition string   `json:"writeDisposition"`
			DestinationTable struct {
				ProjectID string `json:"projectId"`
				DatasetID string `json:"datasetId"`
				TableID   string `json:"tableId"`
			} `json:"destinationTable"`
			// A row with a field the table lacks fails the load rather than
			// being loaded without it. That is the schema drift an operator
			// wants to hear about (the DDL gains the column, provision.sh
			// applies it) instead of a column that is silently null in
			// BigQuery for months.
			IgnoreUnknownValues bool `json:"ignoreUnknownValues"`
			MaxBadRecords       int  `json:"maxBadRecords"`
		} `json:"load"`
	} `json:"configuration"`
}

// Start inserts the load job for one partition: the object into the
// table's day partition, WRITE_TRUNCATE, so the partition holds exactly
// what the file holds.
func (b *BigQuery) Start(ctx context.Context, job LoadJob) error {
	var req loadRequest
	req.JobReference.ProjectID = b.Project
	req.JobReference.JobID = job.ID
	req.JobReference.Location = b.Location
	load := &req.Configuration.Load
	load.SourceURIs = []string{job.SourceURI(b.bucket())}
	load.SourceFormat = "NEWLINE_DELIMITED_JSON"
	load.WriteDisposition = "WRITE_TRUNCATE"
	load.DestinationTable.ProjectID = b.Project
	load.DestinationTable.DatasetID = b.Dataset
	load.DestinationTable.TableID = fmt.Sprintf("%s$%s", job.Table, job.Day.Decorator())
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	api := fmt.Sprintf("%s/bigquery/v2/projects/%s/jobs", b.base(), url.PathEscape(b.Project))
	resp, err := b.do(ctx, http.MethodPost, api, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		// The job exists: this run inserted it and lost the answer. Wait
		// polls it like any other.
		return nil
	default:
		return fmt.Errorf("export: insert load job %s: %s", job.ID, apiError(resp))
	}
}

// Wait polls the job until it is done and returns its error result, if any.
func (b *BigQuery) Wait(ctx context.Context, job LoadJob) error {
	api := fmt.Sprintf("%s/bigquery/v2/projects/%s/jobs/%s?location=%s",
		b.base(), url.PathEscape(b.Project), url.PathEscape(job.ID), url.QueryEscape(b.Location))
	poll := b.Poll
	if poll <= 0 {
		poll = bigqueryPoll
	}
	for {
		resp, err := b.do(ctx, http.MethodGet, api, nil)
		if err != nil {
			return err
		}
		var status struct {
			Status struct {
				State       string `json:"state"`
				ErrorResult *struct {
					Reason   string `json:"reason"`
					Message  string `json:"message"`
					Location string `json:"location"`
				} `json:"errorResult"`
			} `json:"status"`
		}
		if resp.StatusCode != http.StatusOK {
			msg := apiError(resp)
			resp.Body.Close()
			return fmt.Errorf("export: read load job %s: %s", job.ID, msg)
		}
		err = json.NewDecoder(resp.Body).Decode(&status)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("export: read load job %s: %w", job.ID, err)
		}
		if status.Status.State == "DONE" {
			if e := status.Status.ErrorResult; e != nil {
				return fmt.Errorf("load job %s failed: %s: %s (%s)", job.ID, e.Reason, e.Message, e.Location)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

// do makes one API call, retried once after a 429 or a 5xx (see
// bigqueryAttempts). The body is a byte slice so the retry can send it
// again; every call here is a small JSON document.
func (b *BigQuery) do(ctx context.Context, method, api string, body []byte) (*http.Response, error) {
	backoff := b.Backoff
	if backoff <= 0 {
		backoff = bigqueryBackoff
	}
	for attempt := 1; ; attempt++ {
		resp, err := b.once(ctx, method, api, body)
		if err != nil {
			return nil, err
		}
		if !retryable(resp.StatusCode) || attempt >= bigqueryAttempts {
			return resp, nil
		}
		// Drain the answer so the connection is reused, then wait.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
}

// retryable is the set of answers that mean "not now" rather than "no":
// rate limiting and the server's own errors.
func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func (b *BigQuery) once(ctx context.Context, method, api string, body []byte) (*http.Response, error) {
	tok, err := b.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("export: BigQuery token: %w", err)
	}
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, api, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := b.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("export: BigQuery %s: %w", method, err)
	}
	return resp, nil
}

// bucket is where the loads read from. The loader is built beside the GCS
// client from the same configuration; it is stored here so a LoadJob can
// name its source without carrying the bucket itself.
func (b *BigQuery) bucket() string { return b.SourceBucket }

// SourceURI is the gs:// URI of the job's object in bucket.
func (j LoadJob) SourceURI(bucket string) string {
	return fmt.Sprintf("gs://%s/%s", bucket, j.Object)
}
