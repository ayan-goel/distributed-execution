DROP TABLE job_inputs;
DROP FUNCTION protect_job_input();
ALTER TABLE datasets DROP CONSTRAINT datasets_id_project_unique;
ALTER TABLE jobs DROP CONSTRAINT jobs_id_project_unique;
