-- Active part capabilities can outlive a process. Retain their content bindings
-- rather than permitting a downgraded server to issue different bytes on retry.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifact_upload_parts) THEN
        RAISE EXCEPTION 'multipart part declarations prevent downgrade' USING ERRCODE='23514';
    END IF;
END $$;
DROP TRIGGER require_multipart_part_evidence ON artifact_multipart_completions;
DROP FUNCTION require_multipart_part_evidence();
DROP TABLE artifact_upload_parts;
DROP FUNCTION protect_multipart_part();
