DROP TABLE artifact_uploads;
DROP FUNCTION protect_upload_declaration();
ALTER TABLE attempts DROP CONSTRAINT attempts_upload_authority;
ALTER TABLE jobs DROP CONSTRAINT jobs_project_identity;
