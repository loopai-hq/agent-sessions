package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/agent-sessions/server/app"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// Config is what the job reads from its environment. The database names are
// the server's own (examples/deploy-gcp/cloudrun.yaml), so job.yaml and
// cloudrun.yaml can carry the same block and a value that reaches one
// reaches the other; the export names are the job's alone.
type Config struct {
	DatabaseHost     string
	DatabasePort     int
	DatabaseName     string
	DatabaseUser     string
	DatabasePassword string

	// Project owns the bucket, the dataset and the load jobs.
	Project string
	// Bucket receives the files. Location is the dataset's region; the
	// bucket must be in the same one for a load to read it, which is why
	// both are provisioned together (examples/deploy-gcp/analytics/provision.sh).
	Bucket string
	// Dataset is the raw dataset the loads go into
	// (loop_sessions_raw). Analysts query the governed views in
	// loop_sessions, which the job never writes; the two are separate so
	// nothing a WRITE_TRUNCATE load does can touch the access rule
	// (examples/deploy-gcp/analytics/bigquery/views.sql).
	Dataset  string
	Location string

	MaxSessions    int
	MaxSessionDays int
	MaxEventDays   int

	// AccessToken, when set, is used instead of the metadata server: a
	// local run passes `gcloud auth print-access-token`. Never set in
	// job.yaml.
	AccessToken string

	LogLevel slog.Level
}

// Environment variable names.
const (
	envDatabaseHost     = "DATABASE_HOST"
	envDatabasePort     = "DATABASE_PORT"
	envDatabaseName     = "DATABASE_NAME"
	envDatabaseUser     = "DATABASE_USER"
	envDatabasePassword = "DATABASE_PASSWORD"
	envProject          = "EXPORT_PROJECT"
	envBucket           = "EXPORT_BUCKET"
	envDataset          = "EXPORT_DATASET"
	envLocation         = "EXPORT_LOCATION"
	envMaxSessions      = "EXPORT_MAX_SESSIONS"
	envMaxSessionDays   = "EXPORT_MAX_SESSION_DAYS"
	envMaxEventDays     = "EXPORT_MAX_EVENT_DAYS"
	envAccessToken      = "EXPORT_ACCESS_TOKEN"
	envLogLevel         = "LOG_LEVEL"
)

// Load reads the configuration. Every missing required name is reported in
// one error, so an operator fixes the job's environment once.
func Load(getenv func(string) string) (Config, error) {
	get := func(name string) string { return strings.TrimSpace(getenv(name)) }
	c := Config{
		DatabasePort:   5432,
		DatabaseName:   "loop_sessions",
		Dataset:        "loop_sessions_raw",
		Location:       "asia-south1",
		MaxSessions:    DefaultMaxSessions,
		MaxSessionDays: DefaultMaxSessionDays,
		MaxEventDays:   DefaultMaxEventDays,
	}
	var missing []string
	require := func(name string) string {
		v := get(name)
		if v == "" {
			missing = append(missing, name)
		}
		return v
	}
	c.DatabaseHost = require(envDatabaseHost)
	c.DatabaseUser = require(envDatabaseUser)
	c.DatabasePassword = require(envDatabasePassword)
	c.Project = require(envProject)
	c.Bucket = require(envBucket)
	if v := get(envDatabaseName); v != "" {
		c.DatabaseName = v
	}
	if v := get(envDataset); v != "" {
		c.Dataset = v
	}
	if v := get(envLocation); v != "" {
		c.Location = v
	}
	c.AccessToken = get(envAccessToken)

	var errs []error
	if len(missing) > 0 {
		errs = append(errs, fmt.Errorf("missing: %s", strings.Join(missing, ", ")))
	}
	if v := get(envDatabasePort); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 65535 {
			errs = append(errs, fmt.Errorf("%s must be a port, got %q", envDatabasePort, v))
		} else {
			c.DatabasePort = n
		}
	}
	for _, f := range []struct {
		name string
		dst  *int
	}{{envMaxSessions, &c.MaxSessions}, {envMaxSessionDays, &c.MaxSessionDays}, {envMaxEventDays, &c.MaxEventDays}} {
		if v := get(f.name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				errs = append(errs, fmt.Errorf("%s must be a positive integer, got %q", f.name, v))
			} else {
				*f.dst = n
			}
		}
	}
	if v := get(envLogLevel); v != "" {
		var lvl slog.Level
		if err := lvl.UnmarshalText([]byte(strings.ToUpper(v))); err != nil {
			errs = append(errs, fmt.Errorf("%s must be one of debug, info, warn, error, got %q", envLogLevel, v))
		} else {
			c.LogLevel = lvl
		}
	}
	if len(errs) > 0 {
		return c, fmt.Errorf("export: configuration: %w", errors.Join(errs...))
	}
	return c, nil
}

// LogValue reports the configuration with the password as present or
// absent, the way the server's Config does.
func (c Config) LogValue() slog.Value {
	secret := "unset"
	if c.DatabasePassword != "" {
		secret = "set"
	}
	token := "metadata"
	if c.AccessToken != "" {
		token = "static"
	}
	return slog.GroupValue(
		slog.String("database_host", c.DatabaseHost),
		slog.Int("database_port", c.DatabasePort),
		slog.String("database_name", c.DatabaseName),
		slog.String("database_user", c.DatabaseUser),
		slog.String("database_password", secret),
		slog.String("project", c.Project),
		slog.String("bucket", c.Bucket),
		slog.String("dataset", c.Dataset),
		slog.String("location", c.Location),
		slog.Int("max_sessions", c.MaxSessions),
		slog.Int("max_session_days", c.MaxSessionDays),
		slog.Int("max_event_days", c.MaxEventDays),
		slog.String("token", token),
		slog.String("log_level", c.LogLevel.String()),
	)
}

// The job's pool, and why it is not the server's.
const (
	// Two connections: one for the partition COPY in progress and one for a
	// bundle beside it. The instance's budget (server/app/config.go,
	// poolMaxConns) reserves exactly these two for this job; a job that
	// inherited the service's 36 would, at full fan-out, leave the instance
	// one connection for everything else (ops review F5).
	poolMaxConns = 2
	// Nothing warm: the job runs for minutes and exits.
	poolMinConns = 0
	// The pool's own statement ceiling, the same as the server's, so a
	// stray statement here is bounded the same way; each COPY lifts it to
	// the export ceiling with SET LOCAL inside its own transaction.
	poolStatementTimeout = 30 * time.Second
	poolConnectTimeout   = 10 * time.Second
)

// PoolConfig builds the job's pool from the discrete parts, the password
// set on the parsed config rather than folded into the string, for the
// reason the server does the same: the string is what the driver would put
// in an error.
func (c Config) PoolConfig() (*pgxpool.Config, error) {
	kv := fmt.Sprintf("host=%s port=%d dbname=%s user=%s",
		quoteConnValue(c.DatabaseHost), c.DatabasePort,
		quoteConnValue(c.DatabaseName), quoteConnValue(c.DatabaseUser))
	pc, err := pgxpool.ParseConfig(kv)
	if err != nil {
		return nil, fmt.Errorf("export: database connection parameters: %w", err)
	}
	pc.ConnConfig.Password = c.DatabasePassword
	pc.ConnConfig.ConnectTimeout = poolConnectTimeout
	pc.MaxConns = poolMaxConns
	pc.MinConns = poolMinConns
	pc.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%dms'", poolStatementTimeout.Milliseconds()))
		if err != nil {
			return fmt.Errorf("export: set statement_timeout on a new connection: %w", err)
		}
		return nil
	}
	return pc, nil
}

// quoteConnValue renders one keyword/value parameter the way libpq
// specifies: single quoted, backslashes and quotes escaped.
func quoteConnValue(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(v) + "'"
}

// Main is the export subcommand: read the environment, open the pool, run
// once, log the run line, exit. It is what server/cmd/loop-sessions-server
// dispatches to for `export`.
//
// It does not run migrations. The server is the one migrator (it applies
// them at boot, under its lock), and a job that ran them too would be a
// second process racing for the same lock at deploy time. A job that starts
// before the server revision carrying 0021 has booted fails on the missing
// table, says so, and runs again next hour.
func Main(ctx context.Context, getenv func(string) string, stdout io.Writer, version string) error {
	cfg, cfgErr := Load(getenv)
	log := app.NewCloudLogger(cfg.LogLevel, stdout, version).With("job", "export")
	slog.SetDefault(log)
	if cfgErr != nil {
		log.Error("cannot start", "err", cfgErr)
		return cfgErr
	}
	log.Info("export starting", "config", cfg)

	pc, err := cfg.PoolConfig()
	if err != nil {
		log.Error("cannot start", "err", err)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, RunBudget)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		log.Error("cannot start", "err", err)
		return err
	}
	defer pool.Close()

	var token TokenSource
	if cfg.AccessToken != "" {
		token = StaticToken(cfg.AccessToken)
	} else {
		token = MetadataToken()
	}
	objs := &GCS{Bucket: cfg.Bucket, Token: token}
	loader := &BigQuery{Project: cfg.Project, Dataset: cfg.Dataset, Location: cfg.Location, SourceBucket: cfg.Bucket, Token: token}
	src := NewStoreSource(store.New(pool, nil))

	res, runErr := Run(ctx, src, objs, loader, Options{MaxSessions: cfg.MaxSessions, MaxSessionDays: cfg.MaxSessionDays, MaxEventDays: cfg.MaxEventDays}, log)
	LogRun(log, res, runErr)
	return runErr
}

// LogRun writes the "export run" line the metrics read: partitions, rows,
// bytes, seconds, lag_seconds and status, with the error beside them when
// there is one. INFO on success, ERROR otherwise.
func LogRun(log *slog.Logger, res Result, err error) {
	attrs := []any{
		"partitions", res.Partitions,
		"bundles", res.Bundles,
		"rows", res.Rows,
		"bytes", res.Bytes,
		"seconds", res.Seconds,
		"lag_seconds", res.LagSeconds,
		"status", res.Status,
		"capped", res.Capped,
		"bootstrap", res.Bootstrap,
		"failed_loads", res.FailedLoads,
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
		log.Error("export run", attrs...)
		return
	}
	log.Info("export run", attrs...)
}
