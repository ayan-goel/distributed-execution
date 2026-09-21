-- A retry must not mint fresh authority. Keep the original batch grant separate
-- from acquisition replay records, which identify at most one assignment.
CREATE TABLE worker_lease_requests (
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    results jsonb NOT NULL CHECK (jsonb_typeof(results)='array' AND jsonb_array_length(results)<=64 AND octet_length(results::text)<=65536),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(worker_id,session_id,request_id),
    FOREIGN KEY(worker_id,session_id) REFERENCES worker_sessions(worker_id,id)
);
