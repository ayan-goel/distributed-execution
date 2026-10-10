CREATE TABLE artifact_upload_parts (
    upload_id uuid NOT NULL REFERENCES artifact_multipart_uploads(upload_id),
    part_number integer NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(upload_id,part_number)
);

-- Preserve already prepared/completed evidence from the previous schema. New
-- capability requests will bind the same hashes, never invent replacement bytes.
INSERT INTO artifact_upload_parts(upload_id,part_number,sha256,created_at)
SELECT c.upload_id,(p.part->>'number')::integer,p.part->>'sha256',c.created_at
FROM artifact_multipart_completions c CROSS JOIN LATERAL jsonb_array_elements(c.parts) AS p(part);

CREATE FUNCTION protect_multipart_part() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NOT EXISTS (
            SELECT 1 FROM artifact_uploads u JOIN artifact_multipart_uploads m USING(upload_id)
            WHERE u.upload_id=NEW.upload_id AND m.backend_upload_id IS NOT NULL
              AND NEW.part_number BETWEEN 1 AND u.part_count
        ) THEN
            RAISE EXCEPTION 'part requires a bound multipart upload' USING ERRCODE='23514';
        END IF;
    ELSIF OLD IS DISTINCT FROM NEW THEN
        -- INVARIANT: repeated part grants cannot authorize different content.
        RAISE EXCEPTION 'multipart part evidence is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_multipart_part BEFORE INSERT OR UPDATE ON artifact_upload_parts
FOR EACH ROW EXECUTE FUNCTION protect_multipart_part();

CREATE FUNCTION require_multipart_part_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Completion can reference only checksums previously bound by part grants.
    -- Storage still validates ETags/checksums; full-object SHA-256 remains separate.
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(NEW.parts) AS p(part)
        WHERE NOT EXISTS (
            SELECT 1 FROM artifact_upload_parts b WHERE b.upload_id=NEW.upload_id
              AND b.part_number=(p.part->>'number')::integer AND b.sha256=p.part->>'sha256'
        )
    ) THEN
        RAISE EXCEPTION 'completion part evidence is not bound' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER require_multipart_part_evidence BEFORE INSERT ON artifact_multipart_completions
FOR EACH ROW EXECUTE FUNCTION require_multipart_part_evidence();
