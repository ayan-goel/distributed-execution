CREATE TABLE sweeps (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id uuid NOT NULL REFERENCES projects(id),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 128),
    spec jsonb NOT NULL CHECK (jsonb_typeof(spec)='object' AND octet_length(spec::text)<=2097152),
    spec_hash text NOT NULL CHECK (spec_hash ~ '^[0-9a-f]{64}$'),
    child_count integer NOT NULL CHECK (child_count BETWEEN 1 AND 1000),
    max_concurrent integer NOT NULL CHECK (max_concurrent BETWEEN 1 AND 1000),
    fail_fast boolean NOT NULL,
    cancel_running_on_failure boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(project_id,id),
    CHECK (NOT cancel_running_on_failure OR fail_fast)
);

ALTER TABLE jobs ADD COLUMN sweep_id uuid;
ALTER TABLE jobs ADD COLUMN sweep_index integer;
-- INVARIANT: a child index is meaningful only within its own project's sweep.
-- The pair and composite FK prevent cross-project links and orphan child slots.
ALTER TABLE jobs ADD CONSTRAINT jobs_sweep_pair
    CHECK ((sweep_id IS NULL) = (sweep_index IS NULL) AND (sweep_index IS NULL OR sweep_index BETWEEN 0 AND 999));
ALTER TABLE jobs ADD CONSTRAINT jobs_sweep_project
    FOREIGN KEY(project_id,sweep_id) REFERENCES sweeps(project_id,id);
ALTER TABLE jobs ADD CONSTRAINT jobs_sweep_index_unique UNIQUE(sweep_id,sweep_index);

CREATE FUNCTION protect_sweep_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- INVARIANT: admitted matrix and concurrency policy stay fixed while child
    -- state changes; a new sweep is required for a different experiment.
    RAISE EXCEPTION 'sweep records are immutable' USING ERRCODE='23514';
END $$;
CREATE TRIGGER protect_sweep_record BEFORE UPDATE OR DELETE ON sweeps
FOR EACH ROW EXECUTE FUNCTION protect_sweep_record();

CREATE OR REPLACE FUNCTION protect_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.id,OLD.project_id,OLD.spec,OLD.spec_hash,OLD.cpu_millis,OLD.memory_mib,OLD.scratch_mib,OLD.sweep_id,OLD.sweep_index)
        IS DISTINCT FROM (NEW.id,NEW.project_id,NEW.spec,NEW.spec_hash,NEW.cpu_millis,NEW.memory_mib,NEW.scratch_mib,NEW.sweep_id,NEW.sweep_index) THEN
        RAISE EXCEPTION 'job specifications are immutable; submit a new job' USING ERRCODE='23514';
    END IF;
    IF OLD.state IN ('SUCCEEDED','FAILED','CANCELLED') AND
        (OLD.state,OLD.accepted_attempt_id,OLD.accepted_manifest,OLD.cancel_requested,OLD.attempt_counter)
        IS DISTINCT FROM (NEW.state,NEW.accepted_attempt_id,NEW.accepted_manifest,NEW.cancel_requested,NEW.attempt_counter) THEN
        RAISE EXCEPTION 'terminal job outcomes are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
