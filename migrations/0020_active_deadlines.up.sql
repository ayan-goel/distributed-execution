-- Scan the earliest authority deadline without sorting every active attempt.
-- The partial index excludes terminal history from periodic reaper work.
CREATE INDEX active_attempt_deadlines ON attempts (LEAST(lease_expires_at,phase_deadline),id)
WHERE state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING');
