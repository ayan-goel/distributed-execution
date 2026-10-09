//go:build integration

package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sweepReadBarrier struct{ before, resume chan struct{} }

func (b *sweepReadBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "SELECT j.id::text,j.sweep_index") {
		// Pause at the query boundary without changing database locks or results.
		// A real cancellation can commit after counts but before child retrieval.
		close(b.before)
		select {
		case <-b.resume:
		case <-ctx.Done():
		}
	}
	return ctx
}

func (*sweepReadBarrier) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestSweepProgressSnapshotSurvivesConcurrentCancellation(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	job, _ := admittedExample(t)
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: spec.Metadata{Name: "snapshot-grid", Project: "research"},
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{"SEED": {"1", "2"}}, MaxConcurrent: 1}}
	created, err := SubmitSweepResolved(ctx, pool, uuid.NewString(), strings.Repeat("f", 64), sweep, nil)
	if err != nil {
		t.Fatal(err)
	}
	barrier := &sweepReadBarrier{make(chan struct{}), make(chan struct{})}
	config := pool.Config()
	config.ConnConfig.Tracer = barrier
	waiting, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	type outcome struct {
		page SweepPage
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		page, err := GetSweep(ctx, waiting, created.ProjectID, created.ID, -1, 2)
		done <- outcome{page, err}
	}()
	select {
	case <-barrier.before:
	case <-ctx.Done():
		t.Fatal("inspection never reached child query", ctx.Err())
	}
	if _, err := RequestCancellation(ctx, pool, created.ProjectID, created.ChildIDs[0]); err != nil {
		t.Fatal(err)
	}
	close(barrier.resume)
	result := <-done
	if result.err != nil || result.page.Sweep.Progress.Queued != 2 || len(result.page.Children) != 2 || result.page.Children[0].State != "QUEUED" {
		t.Fatal("one page combined different snapshots", result)
	}
	fresh, err := GetSweep(ctx, pool, created.ProjectID, created.ID, -1, 2)
	if err != nil || fresh.Sweep.Progress.Cancelled != 1 || fresh.Children[0].State != "CANCELLED" {
		t.Fatal("fresh inspection missed committed cancellation", fresh, err)
	}
}
