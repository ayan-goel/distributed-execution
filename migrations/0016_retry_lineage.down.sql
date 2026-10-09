DROP TRIGGER protect_job_parent ON jobs;
DROP FUNCTION protect_job_parent();
ALTER TABLE jobs DROP CONSTRAINT jobs_parent_not_self;
ALTER TABLE jobs DROP CONSTRAINT jobs_parent_project;
ALTER TABLE jobs ADD CONSTRAINT jobs_parent_job_id_fkey
    FOREIGN KEY(parent_job_id) REFERENCES jobs(id);

-- Explicit development rollback discards sweep-level provenance. Job parent
-- references remain, but the previous schema no longer protects their project.
ALTER TABLE sweeps DROP CONSTRAINT sweeps_parent_not_self;
ALTER TABLE sweeps DROP CONSTRAINT sweeps_parent_project;
ALTER TABLE sweeps DROP COLUMN parent_sweep_id;
