DROP TRIGGER protect_heartbeat_order ON worker_sessions;
DROP FUNCTION protect_heartbeat_order();
ALTER TABLE worker_sessions DROP CONSTRAINT heartbeat_identity;
ALTER TABLE worker_sessions DROP COLUMN heartbeat_hash;
ALTER TABLE worker_sessions DROP COLUMN heartbeat_request_id;
ALTER TABLE worker_sessions DROP COLUMN heartbeat_sequence;
ALTER TABLE workers DROP COLUMN disk_pressure;
ALTER TABLE workers DROP COLUMN runtime_healthy;
