DROP TABLE artifacts;
DROP TABLE artifact_finalizations;
DROP FUNCTION protect_artifact_record();
ALTER TABLE artifact_uploads DROP CONSTRAINT artifact_upload_worker_identity;
