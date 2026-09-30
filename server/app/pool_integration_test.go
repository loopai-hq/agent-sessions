//go:build integration

package app

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestIntegrationEveryPooledConnectionCarriesTheStatementTimeout is the pool
// half of the timeout contract, measured against a real Postgres rather than
// asserted from the config: the hook runs on connect, so the only way to know
// it took is to borrow a connection and ask.
//
// Same gate as the store's integration tests:
//
//	LOOP_SESSIONS_TEST_DSN=postgres:///loop_sessions_test go test -tags integration ./server/app/...
func TestIntegrationEveryPooledConnectionCarriesTheStatementTimeout(t *testing.T) {
	dsn := os.Getenv("LOOP_SESSIONS_TEST_DSN")
	if dsn == "" {
		t.Skip("LOOP_SESSIONS_TEST_DSN is unset")
	}
	ctx := context.Background()

	// The pool is built the way main.go builds it, through Config.poolConfig,
	// with the DSN's parts carried over one by one because the DSN is what
	// the developer has and discrete parts are what the server reads. A test
	// that installed the hook itself would stay green after poolConfig
	// stopped installing it, and that is the one regression this test is for.
	parsed, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg := Config{
		DatabaseHost:     parsed.ConnConfig.Host,
		DatabasePort:     int(parsed.ConnConfig.Port),
		DatabaseName:     parsed.ConnConfig.Database,
		DatabaseUser:     parsed.ConnConfig.User,
		DatabasePassword: parsed.ConnConfig.Password,
	}
	pc, err := cfg.poolConfig()
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	// Several connections, so the assertion is not about one lucky one, and
	// few, so the test does not hold thirty-six of them open on a shared
	// instance.
	pc.MinConns = 3
	pc.MaxConns = 3
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	want := poolStatementTimeout.String()
	for range 5 {
		var got string
		if err := pool.QueryRow(ctx, `SHOW statement_timeout`).Scan(&got); err != nil {
			t.Fatalf("SHOW statement_timeout: %v", err)
		}
		if got != want {
			t.Fatalf("statement_timeout on a pooled connection = %q, want %q", got, want)
		}
	}

	// And it is enforced, not merely reported: a statement that outlives it is
	// cancelled with Postgres' query_canceled code (57014). Checked through a
	// transaction with a much shorter override so the test does not wait
	// thirty seconds, which also exercises the SET LOCAL path callers use.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '200ms'`); err != nil {
		t.Fatalf("SET LOCAL: %v", err)
	}
	start := time.Now()
	_, err = tx.Exec(ctx, `SELECT pg_sleep(5)`)
	if err == nil {
		t.Fatal("a five second statement ran to completion under a 200ms statement_timeout")
	}
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) || coded.SQLState() != "57014" {
		t.Fatalf("statement failed with %v, want query_canceled (57014)", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("the statement took %s to be cancelled; the timeout is not what cut it off", elapsed)
	}
}
