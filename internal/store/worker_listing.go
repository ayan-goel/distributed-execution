package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Admitted worker labels fit in one page even after JSON escaping. Bounding the
// encoded page also bounds responses when many workers have large label sets.
const MaxWorkerPageBytes = 1 << 20

type WorkerCapacity struct {
	CPUMillis  int64 `json:"cpuMillis"`
	MemoryMiB  int64 `json:"memoryMiB"`
	ScratchMiB int64 `json:"scratchMiB"`
	Slots      int64 `json:"slots"`
}

type WorkerSummary struct {
	ID                     string            `json:"id"`
	Name                   string            `json:"name"`
	State                  string            `json:"state"`
	Labels                 map[string]string `json:"labels"`
	DrainRequested         bool              `json:"drainRequested"`
	RuntimeHealthy         bool              `json:"runtimeHealthy"`
	ReconciliationComplete bool              `json:"reconciliationComplete"`
	DiskPressure           bool              `json:"diskPressure"`
	LastHeartbeatAt        *time.Time        `json:"lastHeartbeatAt"`
	Capacity               WorkerCapacity    `json:"capacity"`
	Reserved               WorkerCapacity    `json:"reserved"`
	Available              WorkerCapacity    `json:"available"`
}

type WorkerListPage struct {
	AsOf    time.Time       `json:"asOf"`
	Workers []WorkerSummary `json:"workers"`
	HasMore bool            `json:"hasMore"`
	NextID  string          `json:"-"`
}

func ListWorkers(ctx context.Context, pool *pgxpool.Pool, projectID, after string, limit int) (WorkerListPage, error) {
	page := WorkerListPage{Workers: []WorkerSummary{}}
	if !canonicalUUID(projectID) || after != "" && !canonicalUUID(after) || limit < 1 || limit > 100 {
		return page, ErrInvalid
	}
	var position any
	if after != "" {
		position = after
	}
	// Repeatable read keeps the timestamp, health, and reservation evidence in
	// one snapshot without taking scheduler locks or changing lifecycle state.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return page, err
	}
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&page.AsOf); err != nil {
		return page, err
	}
	page.AsOf = page.AsOf.UTC()
	// Membership scopes the host list, while reservations cover the whole host.
	// Quarantined capacity remains occupied until cleanup proves it reusable;
	// filtering totals by project would misrepresent headroom on shared workers.
	rows, err := tx.Query(ctx, `SELECT w.id::text,w.name,w.state,w.labels,
		w.drain_requested,w.runtime_healthy,w.reconciliation_complete,w.disk_pressure,w.last_heartbeat_at,
		w.cpu_millis,w.memory_mib,w.scratch_mib,w.slots,
		COALESCE(r.cpu,0),COALESCE(r.memory,0),COALESCE(r.scratch,0),r.slots
		FROM workers w JOIN worker_projects wp ON wp.worker_id=w.id
		LEFT JOIN LATERAL (SELECT sum(cpu_millis) cpu,sum(memory_mib) memory,
			sum(scratch_mib) scratch,count(*) slots FROM reservations
			WHERE worker_id=w.id AND state IN ('active','quarantined')) r ON true
		WHERE wp.project_id=$1 AND ($2::uuid IS NULL OR w.id>$2::uuid)
		ORDER BY w.id LIMIT $3`, projectID, position, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	used := 1024 // Reserve the bounded public envelope and continuation cursor.
	for rows.Next() {
		var worker WorkerSummary
		var labels []byte
		if err = rows.Scan(&worker.ID, &worker.Name, &worker.State, &labels,
			&worker.DrainRequested, &worker.RuntimeHealthy, &worker.ReconciliationComplete, &worker.DiskPressure, &worker.LastHeartbeatAt,
			&worker.Capacity.CPUMillis, &worker.Capacity.MemoryMiB, &worker.Capacity.ScratchMiB, &worker.Capacity.Slots,
			&worker.Reserved.CPUMillis, &worker.Reserved.MemoryMiB, &worker.Reserved.ScratchMiB, &worker.Reserved.Slots); err != nil {
			return WorkerListPage{}, err
		}
		if err = json.Unmarshal(labels, &worker.Labels); err != nil {
			return WorkerListPage{}, err
		}
		if worker.LastHeartbeatAt != nil {
			utc := worker.LastHeartbeatAt.UTC()
			worker.LastHeartbeatAt = &utc
		}
		// Reduced advertised capacity can be below older reservations. Keep that
		// evidence intact and clamp only free headroom to prevent negative values.
		worker.Available = WorkerCapacity{
			CPUMillis:  max(0, worker.Capacity.CPUMillis-worker.Reserved.CPUMillis),
			MemoryMiB:  max(0, worker.Capacity.MemoryMiB-worker.Reserved.MemoryMiB),
			ScratchMiB: max(0, worker.Capacity.ScratchMiB-worker.Reserved.ScratchMiB),
			Slots:      max(0, worker.Capacity.Slots-worker.Reserved.Slots),
		}
		encoded, err := json.Marshal(worker)
		if err != nil {
			return WorkerListPage{}, err
		}
		if len(page.Workers) == limit || used+len(encoded)+1 > MaxWorkerPageBytes {
			if len(page.Workers) == 0 {
				return WorkerListPage{}, errors.New("worker summary exceeds page bound")
			}
			page.HasMore = true
			page.NextID = page.Workers[len(page.Workers)-1].ID
			break
		}
		page.Workers = append(page.Workers, worker)
		used += len(encoded) + 1
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return WorkerListPage{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return WorkerListPage{}, err
	}
	return page, nil
}
