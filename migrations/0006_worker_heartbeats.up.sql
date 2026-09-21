ALTER TABLE workers ADD COLUMN runtime_healthy boolean NOT NULL DEFAULT false;
ALTER TABLE workers ADD COLUMN disk_pressure boolean NOT NULL DEFAULT false;
ALTER TABLE worker_sessions ADD COLUMN heartbeat_sequence bigint NOT NULL DEFAULT 0 CHECK (heartbeat_sequence>=0);
ALTER TABLE worker_sessions ADD COLUMN heartbeat_request_id uuid;
ALTER TABLE worker_sessions ADD COLUMN heartbeat_hash text CHECK (heartbeat_hash ~ '^[0-9a-f]{64}$');
ALTER TABLE worker_sessions ADD CONSTRAINT heartbeat_identity CHECK (
    (heartbeat_sequence=0 AND heartbeat_request_id IS NULL AND heartbeat_hash IS NULL) OR
    (heartbeat_sequence>0 AND heartbeat_request_id IS NOT NULL AND heartbeat_hash IS NOT NULL)
);

-- A delayed report cannot replace newer liveness/cleanup evidence. Keeping only
-- the latest report identity bounds storage independently of session lifetime.
CREATE FUNCTION protect_heartbeat_order() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.heartbeat_sequence<OLD.heartbeat_sequence OR
        (NEW.heartbeat_sequence=OLD.heartbeat_sequence AND
        (NEW.heartbeat_request_id,NEW.heartbeat_hash) IS DISTINCT FROM (OLD.heartbeat_request_id,OLD.heartbeat_hash)) THEN
        RAISE EXCEPTION 'heartbeat reports are monotonically ordered' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_heartbeat_order BEFORE UPDATE ON worker_sessions FOR EACH ROW EXECUTE FUNCTION protect_heartbeat_order();
