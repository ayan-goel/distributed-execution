-- Refuse a downgrade that would discard multipart identities or declarations.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifact_uploads WHERE part_count<>1) THEN
        RAISE EXCEPTION 'multipart uploads prevent downgrade' USING ERRCODE='23514';
    END IF;
END $$;
DROP TABLE artifact_multipart_uploads;
DROP FUNCTION protect_multipart_upload();
ALTER TABLE artifact_uploads DROP CONSTRAINT artifact_uploads_transfer_shape;
ALTER TABLE artifact_uploads DROP COLUMN part_size_bytes;
ALTER TABLE artifact_uploads ADD CONSTRAINT artifact_uploads_size_bytes_check CHECK (size_bytes BETWEEN 0 AND 67108864);
ALTER TABLE artifact_uploads ADD CONSTRAINT artifact_uploads_part_count_check CHECK (part_count=1);
