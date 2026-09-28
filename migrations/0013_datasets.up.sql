CREATE TABLE dataset_uploads (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    request_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    name text NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'),
    size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 1 AND 1073741824),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    -- INVARIANT: only server-generated IDs determine the writable path.
    -- A client-supplied dataset name must never redirect an upload grant.
    object_key text GENERATED ALWAYS AS (
        'projects/' || project_id::text || '/datasets/uploads/' || id::text
    ) STORED UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(project_id,request_id),
    UNIQUE(project_id,id,name)
);

CREATE TABLE datasets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id uuid NOT NULL,
    name text NOT NULL,
    upload_id uuid NOT NULL UNIQUE,
    object_version text NOT NULL CHECK (
        octet_length(object_version) BETWEEN 1 AND 1024 AND
        object_version <> 'null' AND object_version COLLATE "C" ~ '^[!-~]+$'
    ),
    manifest jsonb NOT NULL CHECK (
        jsonb_typeof(manifest)='object' AND octet_length(manifest::text)<=2097152
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(project_id,name),
    FOREIGN KEY(project_id,upload_id,name) REFERENCES dataset_uploads(project_id,id,name)
);

CREATE FUNCTION protect_dataset_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD IS DISTINCT FROM NEW THEN
        RAISE EXCEPTION 'dataset records are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- INVARIANT: neither a replay nor a later PUT may rebind a registered name
-- to different bytes. The exact version is the authority for future staging.
CREATE TRIGGER protect_dataset_upload AFTER UPDATE ON dataset_uploads
FOR EACH ROW EXECUTE FUNCTION protect_dataset_record();
CREATE TRIGGER protect_dataset AFTER UPDATE ON datasets
FOR EACH ROW EXECUTE FUNCTION protect_dataset_record();
