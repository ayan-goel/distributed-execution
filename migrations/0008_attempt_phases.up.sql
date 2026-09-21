ALTER TABLE attempts ADD COLUMN container_id text CHECK (container_id ~ '^[0-9a-f]{64}$');

-- INVARIANT: once runtime identity is bound, a later report cannot redirect
-- supervision or cleanup to another container, including after terminalization.
CREATE FUNCTION protect_attempt_container() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.container_id IS NOT NULL AND NEW.container_id IS DISTINCT FROM OLD.container_id THEN
        RAISE EXCEPTION 'attempt container identity is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_attempt_container BEFORE UPDATE ON attempts FOR EACH ROW EXECUTE FUNCTION protect_attempt_container();

CREATE TABLE attempt_phase_reports (
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    event_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    phase text NOT NULL CHECK (phase IN ('STARTING','RUNNING','FINALIZING')),
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(worker_id,session_id,event_id),
    FOREIGN KEY(worker_id,session_id) REFERENCES worker_sessions(worker_id,id),
    FOREIGN KEY(job_id,attempt_id) REFERENCES attempts(job_id,id)
);
