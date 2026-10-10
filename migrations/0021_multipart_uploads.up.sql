ALTER TABLE artifact_uploads DROP CONSTRAINT artifact_uploads_size_bytes_check;
ALTER TABLE artifact_uploads DROP CONSTRAINT artifact_uploads_part_count_check;
ALTER TABLE artifact_uploads ADD COLUMN part_size_bytes bigint NOT NULL DEFAULT 0;
-- Single-part replay retains its original limits. Multipart output declarations
-- bind a bounded part plan; no storage I/O or result acceptance occurs here.
ALTER TABLE artifact_uploads ADD CONSTRAINT artifact_uploads_transfer_shape CHECK (
    (part_count=1 AND part_size_bytes=0 AND size_bytes BETWEEN 0 AND 67108864) OR
    (kind='OUTPUT' AND part_count BETWEEN 2 AND 10000 AND
     part_size_bytes BETWEEN 5242880 AND 67108864 AND
     size_bytes BETWEEN 1 AND 8589934592 AND
     part_count=1+(size_bytes-1)/NULLIF(part_size_bytes,0))
);

CREATE TABLE artifact_multipart_uploads (
    upload_id uuid PRIMARY KEY REFERENCES artifact_uploads(upload_id),
    initialization_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid()
        CHECK (initialization_id<>'00000000-0000-0000-0000-000000000000'),
    backend_upload_id text CHECK (octet_length(backend_upload_id) BETWEEN 1 AND 1024 AND backend_upload_id ~ '^[!-~]+$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION protect_multipart_upload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NOT EXISTS (SELECT 1 FROM artifact_uploads WHERE upload_id=NEW.upload_id AND part_count>1) THEN
            RAISE EXCEPTION 'multipart identity requires a multipart declaration' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.upload_id IS DISTINCT FROM OLD.upload_id OR
          NEW.initialization_id IS DISTINCT FROM OLD.initialization_id OR
          NEW.created_at IS DISTINCT FROM OLD.created_at OR
          (OLD.backend_upload_id IS NOT NULL AND NEW.backend_upload_id IS DISTINCT FROM OLD.backend_upload_id) THEN
        -- INVARIANT: retry cannot rebind a declaration to another storage upload.
        RAISE EXCEPTION 'multipart upload identity is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_multipart_upload BEFORE INSERT OR UPDATE ON artifact_multipart_uploads
FOR EACH ROW EXECUTE FUNCTION protect_multipart_upload();
