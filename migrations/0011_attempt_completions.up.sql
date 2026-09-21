ALTER TABLE attempts ADD CONSTRAINT attempt_terminal_identity UNIQUE(id,state);
ALTER TABLE artifacts ADD CONSTRAINT artifact_upload_reference UNIQUE(id,upload_id);
ALTER TABLE artifact_uploads ADD CONSTRAINT upload_result_identity UNIQUE(upload_id,attempt_id,logical_name,kind);

CREATE TABLE attempt_completions (
    attempt_id uuid PRIMARY KEY,
    job_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    generation bigint NOT NULL,
    completion_id uuid NOT NULL,
    payload_digest text NOT NULL CHECK (payload_digest ~ '^[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('SUCCEEDED','FAILED','CANCELLED')),
    manifest_json bytea NOT NULL CHECK (octet_length(manifest_json) BETWEEN 1 AND 2097152),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(worker_id,session_id,completion_id),
    UNIQUE(job_id,attempt_id),
    FOREIGN KEY(job_id,attempt_id,worker_id,session_id,generation) REFERENCES attempts(job_id,id,worker_id,session_id,generation),
    FOREIGN KEY(attempt_id,state) REFERENCES attempts(id,state) DEFERRABLE INITIALLY DEFERRED,
    CHECK (jsonb_typeof(convert_from(manifest_json,'UTF8')::jsonb)='object')
);

CREATE TABLE completion_artifacts (
    attempt_id uuid NOT NULL REFERENCES attempt_completions(attempt_id),
    artifact_id uuid NOT NULL,
    upload_id uuid NOT NULL,
    logical_name text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('OUTPUT','LOG')),
    PRIMARY KEY(attempt_id,artifact_id),
    FOREIGN KEY(artifact_id,upload_id) REFERENCES artifacts(id,upload_id),
    FOREIGN KEY(upload_id,attempt_id,logical_name,kind) REFERENCES artifact_uploads(upload_id,attempt_id,logical_name,kind)
);
CREATE UNIQUE INDEX completed_output_names ON completion_artifacts(attempt_id,logical_name) WHERE kind='OUTPUT';

CREATE TRIGGER protect_completion BEFORE UPDATE ON attempt_completions
FOR EACH ROW EXECUTE FUNCTION protect_artifact_record();
CREATE TRIGGER protect_completion_artifact BEFORE UPDATE ON completion_artifacts
FOR EACH ROW EXECUTE FUNCTION protect_artifact_record();

ALTER TABLE jobs ADD CONSTRAINT accepted_completion FOREIGN KEY(id,accepted_attempt_id)
REFERENCES attempt_completions(job_id,attempt_id) DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION check_accepted_manifest() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE j jobs%ROWTYPE;
BEGIN
    SELECT * INTO j FROM jobs WHERE id=NEW.id;
    -- INVARIANT: the canonical result is exactly the successful completion's
    -- immutable manifest, never independently supplied or mutable job metadata.
    IF j.state='SUCCEEDED' AND NOT EXISTS (
        SELECT 1 FROM attempt_completions c WHERE c.job_id=j.id AND c.attempt_id=j.accepted_attempt_id
        AND c.state='SUCCEEDED' AND convert_from(c.manifest_json,'UTF8')::jsonb=j.accepted_manifest
    ) THEN
        RAISE EXCEPTION 'accepted manifest must match successful completion' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER accepted_manifest_identity AFTER INSERT OR UPDATE ON jobs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_accepted_manifest();
