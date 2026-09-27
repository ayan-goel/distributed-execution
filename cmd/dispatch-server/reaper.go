package main

import (
	"context"
	"log/slog"
	"time"

	"dispatch.local/dispatch/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

const leaseReaperInterval = 2 * time.Second

func reapExpiredUntilCaughtUp(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	total := 0
	for {
		count, err := store.ReapExpiredAttempts(ctx, pool, store.MaxReapBatch)
		total += count
		if err != nil || count < store.MaxReapBatch {
			return total, err
		}
	}
}

func runLeaseReaper(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) {
	ticker := time.NewTicker(leaseReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := reapExpiredUntilCaughtUp(ctx, pool)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Retrying preserves the database as the source of truth through a
				// transient outage; surfacing the failure keeps a stuck reaper visible.
				logger.Error("lease_reaper_failed", "error", err)
			} else if count > 0 {
				logger.Info("lease_reaped", "count", count)
			}
		}
	}
}
