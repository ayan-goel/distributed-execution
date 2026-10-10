package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// Bound writes and reduce polling churn while unsampled jobs gain coverage.
// These limits do not bound queue-scan cost; recording requires the cluster lock.
const queueBlockerSampleLimit = 16
const queueBlockerResampleAfter = 30 * time.Second

type acquisitionBlocker struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

type acquisitionSelection struct {
	jobID, reason string
	blockers      []acquisitionBlocker
	observedAt    time.Time
}

func selectAcquisitionCandidate(ctx context.Context, tx pgx.Tx, workerID string, w acquisitionWorker, cursor *string) (selection acquisitionSelection, err error) {
	var body []byte
	// Winner eligibility and project rotation are unchanged. Disabled/backoff jobs
	// enter only the diagnostic pool; observing them cannot make them assignable.
	err = tx.QueryRow(ctx, `WITH queue_clock AS MATERIALIZED (SELECT clock_timestamp() AS now), usage AS (
        SELECT j.project_id,sum(r.cpu_millis) cpu,sum(r.memory_mib) memory,count(*) slots
        FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id
        WHERE a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING') GROUP BY j.project_id
    ), sweep_usage AS (
        SELECT j.sweep_id,count(*) slots FROM attempts a JOIN jobs j ON j.id=a.job_id
        WHERE j.sweep_id IS NOT NULL AND a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING') GROUP BY j.sweep_id
    ), candidates AS MATERIALIZED (
        SELECT j.id,j.project_id,j.created_at,`+effectivePrioritySQL+` AS effective_priority,
        p.enabled AND j.next_eligible_at<=queue_clock.now AS visible,CASE
        WHEN NOT p.enabled THEN 'PROJECT_DISABLED'
        WHEN j.next_eligible_at>queue_clock.now THEN 'RETRY_BACKOFF'
        WHEN NOT ($6::jsonb @> COALESCE(j.spec->'spec'->'placement'->'labels','{}'::jsonb)) THEN 'PLACEMENT_MISMATCH'
        WHEN j.cpu_millis>$2 OR j.memory_mib>$3 OR j.scratch_mib>$4 OR $5<1 THEN 'NO_RESOURCE_FIT'
        WHEN j.cpu_millis>p.cpu_quota-COALESCE(u.cpu,0) OR j.memory_mib>p.memory_quota_mib-COALESCE(u.memory,0) OR COALESCE(u.slots,0)>=p.concurrency_quota THEN 'PROJECT_QUOTA'
        WHEN s.id IS NOT NULL AND COALESCE(su.slots,0)>=s.max_concurrent THEN 'SWEEP_CONCURRENCY'
        ELSE '' END reason
        FROM jobs j JOIN projects p ON p.id=j.project_id JOIN worker_projects wp ON wp.project_id=p.id AND wp.worker_id=$1
        LEFT JOIN usage u ON u.project_id=p.id LEFT JOIN sweeps s ON s.id=j.sweep_id
        LEFT JOIN sweep_usage su ON su.sweep_id=j.sweep_id CROSS JOIN queue_clock
        WHERE j.state IN ('QUEUED','RETRY_WAIT') AND NOT j.cancel_requested
    ), selected AS (
        SELECT id,reason FROM candidates WHERE visible
        ORDER BY (reason='') DESC,($7::uuid IS NULL OR project_id>$7::uuid) DESC,
            project_id,effective_priority DESC,created_at,id LIMIT 1
    ), sampled AS (
        SELECT c.id,c.reason FROM candidates c CROSS JOIN queue_clock
        LEFT JOIN LATERAL (SELECT observed_at FROM job_queue_blockers WHERE job_id=c.id ORDER BY id DESC LIMIT 1) previous ON true
        WHERE c.reason<>'' AND (previous.observed_at IS NULL OR previous.observed_at<=queue_clock.now-$8::bigint*interval '1 second')
        ORDER BY previous.observed_at NULLS FIRST,c.created_at,c.id LIMIT $9
    ) SELECT COALESCE(selected.id::text,''),COALESCE(selected.reason,'QUEUE_EMPTY'),queue_clock.now,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('jobId',id,'reason',reason) ORDER BY id) FROM sampled),'[]'::jsonb)
        FROM queue_clock LEFT JOIN selected ON true`, workerID, w.free.CPUMillis, w.free.MemoryMiB, w.free.ScratchMiB, w.slots, w.labels, cursor,
		int64(queueBlockerResampleAfter/time.Second), queueBlockerSampleLimit).Scan(&selection.jobID, &selection.reason, &selection.observedAt, &body)
	if err == nil {
		err = json.Unmarshal(body, &selection.blockers)
	}
	return
}

func lockAcquisitionJobs(ctx context.Context, tx pgx.Tx, selection acquisitionSelection) error {
	ids := make([]string, 0, len(selection.blockers)+1)
	if selection.jobID != "" {
		ids = append(ids, selection.jobID)
	}
	for _, blocker := range selection.blockers {
		ids = append(ids, blocker.JobID)
	}
	// Renewal batches lock sorted jobs without the cluster lock. Lock the full
	// deduplicated selection before worker/accounting rows to avoid a lock cycle.
	rows, err := tx.Query(ctx, "SELECT j.id::text FROM jobs j WHERE j.id=ANY($1::uuid[]) ORDER BY j.id FOR UPDATE OF j", ids)
	if err != nil {
		return err
	}
	var id string
	_, err = pgx.ForEachRow(rows, []any{&id}, func() error { return nil })
	return err
}
