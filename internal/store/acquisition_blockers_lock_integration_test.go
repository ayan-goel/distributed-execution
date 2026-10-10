//go:build integration

package store

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAcquisitionDiagnosticLocksDoNotPrelockWinnerOutOfOrder(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	ids := []string{queueAcquisitionJob(t, pool, nil).ID, queueAcquisitionJob(t, pool, nil).ID}
	slices.Sort(ids)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(holder)
	if _, err := holder.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", ids[0]); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	app := "diagnostic_lock_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	done := make(chan error, 1)
	go func() {
		tx, err := beginTransition(ctx, waiting)
		if err == nil {
			defer rollback(tx)
			err = lockAcquisitionJobs(ctx, tx, acquisitionSelection{jobID: ids[1], blockers: []acquisitionBlocker{{JobID: ids[0]}}})
			if err == nil {
				err = tx.Commit(ctx)
			}
		}
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT j.id::text FROM jobs j WHERE j.id=ANY%')", app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("diagnostic lock query never waited on the first job")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A renewal-style transaction must still be able to lock the later UUID.
	// Locking the winner first would create a cycle with sorted renewal batches.
	probe, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(probe)
	if _, err := probe.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE NOWAIT", ids[1]); err != nil {
		t.Fatal("winner was locked ahead of an earlier diagnostic job", err)
	}
	rollback(probe)
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal("sorted diagnostic locks did not complete", err)
	}
}
