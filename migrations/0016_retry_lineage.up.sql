ALTER TABLE sweeps ADD COLUMN parent_sweep_id uuid;
-- INVARIANT: retry lineage stays inside the admitted project's history.
-- Existing sweep immutability also protects this new parent reference.
ALTER TABLE sweeps ADD CONSTRAINT sweeps_parent_project
    FOREIGN KEY(project_id,parent_sweep_id) REFERENCES sweeps(project_id,id);
ALTER TABLE sweeps ADD CONSTRAINT sweeps_parent_not_self
    CHECK (parent_sweep_id IS NULL OR parent_sweep_id <> id);

ALTER TABLE jobs DROP CONSTRAINT jobs_parent_job_id_fkey;
ALTER TABLE jobs ADD CONSTRAINT jobs_parent_project
    FOREIGN KEY(project_id,parent_job_id) REFERENCES jobs(project_id,id);
ALTER TABLE jobs ADD CONSTRAINT jobs_parent_not_self
    CHECK (parent_job_id IS NULL OR parent_job_id <> id);

-- INVARIANT: provenance is fixed at admission. Rebinding it would erase retry
-- history without changing the already immutable execution specification.
CREATE FUNCTION protect_job_parent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.parent_job_id IS DISTINCT FROM NEW.parent_job_id THEN
        RAISE EXCEPTION 'job retry provenance is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_job_parent BEFORE UPDATE OF parent_job_id ON jobs
FOR EACH ROW EXECUTE FUNCTION protect_job_parent();
