-- INVARIANT: at most 16 placement observations may survive per job. A fixed-slot
-- ring bounds diagnostic growth even when workers keep polling a blocked queue.
CREATE TABLE job_queue_blockers (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    slot smallint NOT NULL CHECK (slot BETWEEN 0 AND 15),
    id bigserial NOT NULL UNIQUE CHECK (id > 0),
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    request_id uuid NOT NULL,
    attempt_counter bigint NOT NULL CHECK (attempt_counter >= 0),
    reason text NOT NULL CHECK (reason IN ('PLACEMENT_MISMATCH','NO_RESOURCE_FIT',
        'PROJECT_QUOTA','SWEEP_CONCURRENCY','PROJECT_DISABLED','RETRY_BACKOFF')),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(observed_at)),
    PRIMARY KEY(job_id,slot),
    UNIQUE(job_id,worker_id,session_id,request_id),
    FOREIGN KEY(worker_id,session_id) REFERENCES worker_sessions(worker_id,id)
);
CREATE INDEX queue_blockers_history ON job_queue_blockers(job_id,id DESC);
