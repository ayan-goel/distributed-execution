ALTER TABLE jobs ADD CONSTRAINT jobs_project_identity UNIQUE(project_id,id);
ALTER TABLE attempts ADD CONSTRAINT attempts_upload_authority UNIQUE(job_id,id,worker_id,session_id,generation);

CREATE TABLE artifact_uploads (
    upload_id uuid PRIMARY KEY,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    request_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    kind text NOT NULL CHECK (kind IN ('OUTPUT','LOG','MANIFEST')),
    logical_name text NOT NULL CHECK (logical_name ~ '^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$'),
    size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 0 AND 67108864),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    part_count integer NOT NULL CHECK (part_count=1),
    -- Only server-owned identifiers form writable object paths. Logical names
    -- cannot redirect a worker's capability to a shared or another attempt's key.
    object_key text GENERATED ALWAYS AS (
        'projects/' || project_id::text || '/jobs/' || job_id::text ||
        '/attempts/' || attempt_id::text || '/uploads/' || upload_id::text
    ) STORED UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(worker_id,session_id,request_id),
    FOREIGN KEY(project_id,job_id) REFERENCES jobs(project_id,id),
    FOREIGN KEY(job_id,attempt_id,worker_id,session_id,generation)
        REFERENCES attempts(job_id,id,worker_id,session_id,generation)
);
CREATE INDEX attempt_upload_inventory ON artifact_uploads(attempt_id);

-- INVARIANT: a replay identity cannot be rebound to different bytes or ownership.
-- Verified versions will reference this immutable declaration in a separate record.
CREATE FUNCTION protect_upload_declaration() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD IS DISTINCT FROM NEW THEN
        RAISE EXCEPTION 'upload declarations are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- Compare only after PostgreSQL recomputes the generated key; BEFORE sees an
-- incomplete NEW row and incorrectly rejects even unchanged declarations.
CREATE TRIGGER protect_upload_declaration AFTER UPDATE ON artifact_uploads
FOR EACH ROW EXECUTE FUNCTION protect_upload_declaration();
