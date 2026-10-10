CREATE TABLE artifact_multipart_completions (
    upload_id uuid PRIMARY KEY REFERENCES artifact_multipart_uploads(upload_id),
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    parts jsonb NOT NULL CHECK (jsonb_typeof(parts)='array' AND octet_length(parts::text)<=2097152),
    object_version text CHECK (octet_length(object_version) BETWEEN 1 AND 1024 AND object_version<>'null' AND object_version COLLATE "C" ~ '^[!-~]+$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    stored_at timestamptz,
    CHECK ((object_version IS NULL)=(stored_at IS NULL)),
    UNIQUE(worker_id,session_id,request_id),
    FOREIGN KEY(upload_id,worker_id,session_id) REFERENCES artifact_uploads(upload_id,worker_id,session_id)
);

CREATE FUNCTION protect_multipart_completion() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        -- Completion intent must bind the complete ordered part set before any
        -- external completion call; an uninitialized upload cannot be completed.
        IF NEW.object_version IS NOT NULL OR NEW.stored_at IS NOT NULL OR NOT EXISTS (
            SELECT 1 FROM artifact_uploads u JOIN artifact_multipart_uploads m USING(upload_id)
            WHERE u.upload_id=NEW.upload_id AND m.backend_upload_id IS NOT NULL
              AND u.part_count=jsonb_array_length(NEW.parts)
        ) OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(NEW.parts) WITH ORDINALITY AS p(part,number)
            WHERE jsonb_typeof(part) IS DISTINCT FROM 'object'
               OR jsonb_typeof(part->'number') IS DISTINCT FROM 'number'
               OR part->>'number' IS DISTINCT FROM number::text
               OR jsonb_typeof(part->'etag') IS DISTINCT FROM 'string'
               OR (octet_length(part->>'etag') BETWEEN 1 AND 1024 AND (part->>'etag') COLLATE "C" ~ '^[!-~]+$') IS NOT TRUE
               OR jsonb_typeof(part->'sha256') IS DISTINCT FROM 'string'
               OR ((part->>'sha256') ~ '^[0-9a-f]{64}$') IS NOT TRUE
        ) THEN
            RAISE EXCEPTION 'invalid multipart completion intent' USING ERRCODE='23514';
        END IF;
    ELSIF ROW(NEW.upload_id,NEW.worker_id,NEW.session_id,NEW.request_id,NEW.request_hash,NEW.parts,NEW.created_at)
          IS DISTINCT FROM ROW(OLD.upload_id,OLD.worker_id,OLD.session_id,OLD.request_id,OLD.request_hash,OLD.parts,OLD.created_at)
          OR (OLD.object_version IS NOT NULL AND ROW(NEW.object_version,NEW.stored_at) IS DISTINCT FROM ROW(OLD.object_version,OLD.stored_at)) THEN
        -- INVARIANT: retries cannot replace part evidence or the stored version.
        RAISE EXCEPTION 'multipart completion identity is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_multipart_completion BEFORE INSERT OR UPDATE ON artifact_multipart_completions
FOR EACH ROW EXECUTE FUNCTION protect_multipart_completion();

CREATE FUNCTION require_multipart_stored_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- INVARIANT: even matching bytes cannot replace the exact version bound by
    -- multipart completion. Single-part publication retains its original path.
    IF EXISTS (SELECT 1 FROM artifact_uploads WHERE upload_id=NEW.upload_id AND part_count>1)
       AND NOT EXISTS (SELECT 1 FROM artifact_multipart_completions WHERE upload_id=NEW.upload_id AND object_version=NEW.object_version) THEN
        RAISE EXCEPTION 'multipart version is not stored' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER require_multipart_stored_version BEFORE INSERT ON artifact_finalizations
FOR EACH ROW EXECUTE FUNCTION require_multipart_stored_version();
