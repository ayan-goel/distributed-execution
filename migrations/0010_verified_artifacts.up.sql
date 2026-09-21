ALTER TABLE artifact_uploads ADD CONSTRAINT artifact_upload_worker_identity UNIQUE(upload_id,worker_id,session_id);

CREATE TABLE artifact_finalizations (
    id uuid PRIMARY KEY,
    upload_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    object_version text NOT NULL CHECK (octet_length(object_version) BETWEEN 1 AND 1024 AND object_version <> 'null' AND object_version COLLATE "C" ~ '^[!-~]+$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(worker_id,session_id,request_id),
    UNIQUE(id,upload_id,object_version),
    FOREIGN KEY(upload_id,worker_id,session_id) REFERENCES artifact_uploads(upload_id,worker_id,session_id)
);
CREATE INDEX upload_finalization_inventory ON artifact_finalizations(upload_id);

-- Verification selects one immutable version per upload. Completion will refer
-- to this artifact; inserting it alone does not accept a job or release capacity.
CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    upload_id uuid NOT NULL UNIQUE REFERENCES artifact_uploads(upload_id),
    finalization_id uuid NOT NULL,
    object_version text NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(finalization_id,upload_id,object_version) REFERENCES artifact_finalizations(id,upload_id,object_version)
);

CREATE FUNCTION protect_artifact_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD IS DISTINCT FROM NEW THEN
        RAISE EXCEPTION 'artifact verification records are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- INVARIANT: neither request replay nor later uploads may rebind verified bytes.
CREATE TRIGGER protect_finalization BEFORE UPDATE ON artifact_finalizations
FOR EACH ROW EXECUTE FUNCTION protect_artifact_record();
CREATE TRIGGER protect_artifact BEFORE UPDATE ON artifacts
FOR EACH ROW EXECUTE FUNCTION protect_artifact_record();
