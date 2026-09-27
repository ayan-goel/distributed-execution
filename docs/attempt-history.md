# Project-scoped attempt history (D16a)

`GET /v1/jobs/{id}/attempts` returns a job's attempts in attempt-number order,
including current state, failure reason, exit code, worker ID, cleanup status,
and timestamps. A job with no attempt returns an empty array. The lookup first
checks the caller's project, so unknown and foreign jobs both return 404.

The endpoint requires project read permission. The job spec caps retries at
ten attempts, so v0.1 returns the full history without a cursor. This read
path lets the CLI resolve an active attempt before requesting its log cursor;
that CLI command is still pending.

The Go client now validates the response's job identity, bounded ordered
attempt numbers, UUIDs, states, and timestamps before selecting an attempt.
