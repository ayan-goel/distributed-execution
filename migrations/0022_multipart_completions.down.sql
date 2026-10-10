-- Prepared intent is needed to retry ambiguous completion safely, even before
-- a version is saved. Downgrade must not silently discard either state.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifact_multipart_completions) THEN
        RAISE EXCEPTION 'multipart completion intents prevent downgrade' USING ERRCODE='23514';
    END IF;
END $$;
DROP TRIGGER require_multipart_stored_version ON artifact_finalizations;
DROP FUNCTION require_multipart_stored_version();
DROP TABLE artifact_multipart_completions;
DROP FUNCTION protect_multipart_completion();
