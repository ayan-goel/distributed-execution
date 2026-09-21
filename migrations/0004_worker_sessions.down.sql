DROP TRIGGER protect_worker_session ON worker_sessions;
DROP FUNCTION protect_worker_session();
DROP TABLE worker_registrations;
ALTER TABLE worker_sessions DROP CONSTRAINT session_registration_identity;
ALTER TABLE worker_sessions DROP COLUMN registration_hash;
ALTER TABLE workers DROP COLUMN reconciliation_complete;
