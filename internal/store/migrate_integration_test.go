//go:build integration

package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DISPATCH_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("run make integration: DISPATCH_TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	root, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		root.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := root.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		root.Close()
		if err != nil {
			t.Error(err)
		}
	})
	return pool
}

func TestConcurrentMigrationsAndDrift(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); errors <- Migrate(ctx, pool) }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil || count != 11 {
		t.Fatalf("applied %d migrations: %v", count, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE schema_migrations SET checksum=repeat('0',64)"); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err == nil {
		t.Fatal("modified migration accepted")
	}
}

func TestMigrationRollbackAndIncrementalUpgrade(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	files := fstest.MapFS{"0001_first.up.sql": {Data: []byte("CREATE TABLE first_table(id integer PRIMARY KEY)")}}
	if err := migrateFiles(ctx, pool, files); err != nil {
		t.Fatal(err)
	}
	files["0002_second.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE second_table(id integer); SELECT missing_function()")}
	if err := migrateFiles(ctx, pool, files); err == nil {
		t.Fatal("broken migration accepted")
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('second_table') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Fatal("failed migration leaked DDL", err)
	}
	files["0002_second.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE second_table(id integer)")}
	if err := migrateFiles(ctx, pool, files); err != nil {
		t.Fatal(err)
	}
	delete(files, "0002_second.up.sql")
	if err := migrateFiles(ctx, pool, files); err == nil {
		t.Fatal("older binary accepted newer database")
	}
}
