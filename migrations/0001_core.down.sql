-- Development rollback is destructive and must never run implicitly at startup.
-- Drop the mutually referencing core tables in one statement, without CASCADE.
DROP TABLE worker_requests,idempotency_keys,job_events,reservations,jobs,attempts,workers,worker_sessions,projects;
DROP FUNCTION check_job_ownership(),protect_attempt_identity(),protect_job_identity();
