-- An operator authorizes one specific replacement for one specific incarnation.
-- A generic takeover flag could let a stale process consume the approval.
CREATE TABLE worker_takeovers (
    worker_id uuid NOT NULL,
    from_session_id uuid NOT NULL,
    to_session_id uuid NOT NULL,
    approved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    used_at timestamptz,
    PRIMARY KEY(worker_id,from_session_id),
    UNIQUE(worker_id,to_session_id),
    FOREIGN KEY(worker_id,from_session_id) REFERENCES worker_sessions(worker_id,id),
    CHECK (from_session_id<>to_session_id)
);
