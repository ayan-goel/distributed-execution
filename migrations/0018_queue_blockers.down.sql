-- Development rollback discards diagnostics only; execution authority and frozen
-- job identity remain in their existing tables.
DROP TABLE job_queue_blockers;
