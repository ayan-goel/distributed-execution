DROP TABLE attempt_phase_reports;
DROP TRIGGER protect_attempt_container ON attempts;
DROP FUNCTION protect_attempt_container();
ALTER TABLE attempts DROP COLUMN container_id;
