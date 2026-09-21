package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"time"

	"dispatch.local/dispatch/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return migrateFiles(ctx, pool, migrations.Files)
}

func migrateFiles(ctx context.Context, pool *pgxpool.Pool, files fs.FS) error {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return err
	}
	var names []string
	sqlByName := map[string][]byte{}
	validName := regexp.MustCompile(`^[0-9]{4}_[a-z0-9_]+\.up\.sql$`)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		if !validName.MatchString(entry.Name()) {
			return fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		b, err := fs.ReadFile(files, entry.Name())
		if err != nil {
			return err
		}
		names = append(names, entry.Name())
		sqlByName[entry.Name()] = b
	}
	if len(names) == 0 {
		return fmt.Errorf("no migrations embedded")
	}
	for i, name := range names {
		if name[:4] != fmt.Sprintf("%04d", i+1) {
			return fmt.Errorf("migration sequence must start at 0001 without gaps")
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'"); err != nil {
		return err
	}
	// Serialize startup migrations before even creating the history table. This
	// separate lock never nests with scheduler ownership locks or external RPCs.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(1146310733)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT name,checksum FROM schema_migrations ORDER BY name")
	if err != nil {
		return err
	}
	applied := map[string]string{}
	for rows.Next() {
		var name, hash string
		if err := rows.Scan(&name, &hash); err != nil {
			rows.Close()
			return err
		}
		applied[name] = hash
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for name, hash := range applied {
		b, ok := sqlByName[name]
		if !ok {
			return fmt.Errorf("database contains unknown migration %s; binary may be too old", name)
		}
		if fmt.Sprintf("%x", sha256.Sum256(b)) != hash {
			return fmt.Errorf("migration checksum mismatch: %s", name)
		}
	}
	for i, name := range names {
		if _, ok := applied[name]; ok {
			if i > 0 {
				if _, previous := applied[names[i-1]]; !previous {
					return fmt.Errorf("database migration history has a gap")
				}
			}
			continue
		}
		if _, err = tx.Exec(ctx, string(sqlByName[name])); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(sqlByName[name]))
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(name,checksum) VALUES ($1,$2)", name, hash); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
