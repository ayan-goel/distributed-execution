CREATE OR REPLACE FUNCTION protect_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.id,OLD.project_id,OLD.spec,OLD.spec_hash,OLD.cpu_millis,OLD.memory_mib,OLD.scratch_mib)
        IS DISTINCT FROM (NEW.id,NEW.project_id,NEW.spec,NEW.spec_hash,NEW.cpu_millis,NEW.memory_mib,NEW.scratch_mib) THEN
        RAISE EXCEPTION 'job specifications are immutable; submit a new job' USING ERRCODE='23514';
    END IF;
    IF OLD.state IN ('SUCCEEDED','FAILED','CANCELLED') AND
        (OLD.state,OLD.accepted_attempt_id,OLD.accepted_manifest,OLD.cancel_requested,OLD.attempt_counter)
        IS DISTINCT FROM (NEW.state,NEW.accepted_attempt_id,NEW.accepted_manifest,NEW.cancel_requested,NEW.attempt_counter) THEN
        RAISE EXCEPTION 'terminal job outcomes are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

ALTER TABLE jobs DROP CONSTRAINT jobs_sweep_index_unique;
ALTER TABLE jobs DROP CONSTRAINT jobs_sweep_project;
ALTER TABLE jobs DROP CONSTRAINT jobs_sweep_pair;
ALTER TABLE jobs DROP COLUMN sweep_index;
ALTER TABLE jobs DROP COLUMN sweep_id;
DROP TABLE sweeps;
DROP FUNCTION protect_sweep_record();
