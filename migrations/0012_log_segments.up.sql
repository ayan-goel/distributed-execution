-- A segment catalogs one verified immutable LOG object and a contiguous
-- per-stream sequence interval. Gaps inside that interval remain explicit.
CREATE TABLE log_segments (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    artifact_id uuid NOT NULL,
    upload_id uuid NOT NULL,
    stream text NOT NULL CHECK (stream IN ('STDOUT','STDERR')),
    logical_name text NOT NULL CHECK (logical_name = lower(stream)),
    kind text NOT NULL DEFAULT 'LOG' CHECK (kind = 'LOG'),
    first_sequence bigint NOT NULL CHECK (first_sequence >= 1),
    last_sequence bigint NOT NULL CHECK (last_sequence >= first_sequence),
    gaps jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(gaps) = 'array'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(worker_id,session_id,request_id),
    UNIQUE(artifact_id),
    UNIQUE(attempt_id,stream,first_sequence),
    FOREIGN KEY(artifact_id,upload_id) REFERENCES artifacts(id,upload_id),
    FOREIGN KEY(upload_id,attempt_id,logical_name,kind)
        REFERENCES artifact_uploads(upload_id,attempt_id,logical_name,kind)
);
CREATE INDEX log_segment_cursor ON log_segments(attempt_id,stream,last_sequence);
-- INVARIANT: a registered range must never be rebound to another object.
CREATE TRIGGER protect_log_segment BEFORE UPDATE ON log_segments
FOR EACH ROW EXECUTE FUNCTION protect_artifact_record();
