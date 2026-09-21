DROP TRIGGER accepted_manifest_identity ON jobs;
DROP FUNCTION check_accepted_manifest();
ALTER TABLE jobs DROP CONSTRAINT accepted_completion;
DROP TABLE completion_artifacts;
DROP TABLE attempt_completions;
ALTER TABLE artifact_uploads DROP CONSTRAINT upload_result_identity;
ALTER TABLE artifacts DROP CONSTRAINT artifact_upload_reference;
ALTER TABLE attempts DROP CONSTRAINT attempt_terminal_identity;
