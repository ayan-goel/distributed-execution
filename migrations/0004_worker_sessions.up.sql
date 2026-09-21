ALTER TABLE workers ADD COLUMN reconciliation_complete boolean NOT NULL DEFAULT false;
ALTER TABLE worker_sessions ADD COLUMN registration_hash text CHECK (registration_hash ~ '^[0-9a-f]{64}$');
ALTER TABLE worker_sessions ADD CONSTRAINT session_registration_identity UNIQUE(worker_id,id,registration_hash);

-- Request aliases can recover one incarnation, but cannot change its claims or
-- attach a request to another host. Old requests remain tied to fenced sessions.
CREATE TABLE worker_registrations (
    worker_id uuid NOT NULL,
    request_id uuid NOT NULL,
    session_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(worker_id,request_id),
    FOREIGN KEY(worker_id,session_id,request_hash) REFERENCES worker_sessions(worker_id,id,registration_hash)
);

CREATE FUNCTION protect_worker_session() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.id,OLD.worker_id,OLD.generation,OLD.registration_hash,OLD.created_at)
        IS DISTINCT FROM (NEW.id,NEW.worker_id,NEW.generation,NEW.registration_hash,NEW.created_at)
        OR (OLD.fenced_at IS NOT NULL AND OLD.fenced_at IS DISTINCT FROM NEW.fenced_at) THEN
        RAISE EXCEPTION 'session identity and fencing are irreversible' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_worker_session BEFORE UPDATE ON worker_sessions FOR EACH ROW EXECUTE FUNCTION protect_worker_session();
