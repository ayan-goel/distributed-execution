# Project-scoped attempt history

```sh
bin/dispatch attempts list JOB_UUID
bin/dispatch attempts list JOB_UUID --json
```

Options follow the canonical job UUID. Human output starts with the job ID, then
prints each attempt's ID, number, state, worker ID, cleanup status, reason, exit
code, and UTC timestamps. Missing optional values print as `null`; reasons are
quoted to escape terminal control characters. An empty history prints the job ID
and `No attempts yet.`

JSON output matches the API envelope: `{"jobId":"…","attempts":[…]}`. It always
includes an array, including before the first assignment. The command reads the
full bounded history once; it does not follow subsequent transitions. A successful
read exits 0 even when an attempt failed or cleanup remains pending. Argument,
API, transport, and output errors exit 2. Pending cleanup means capacity can still
be quarantined; a terminal attempt alone does not prove its container was removed.

`GET /v1/jobs/{id}/attempts` returns a job's attempts in attempt-number order,
including current state, failure reason, exit code, worker ID, cleanup status,
and timestamps. A job with no attempt returns an empty array. The lookup first
checks the caller's project, so unknown and foreign jobs both return 404.

The endpoint requires project read permission. The job spec caps retries at
ten attempts, so v0.1 returns the full history without a cursor. This read
path also lets `dispatch logs` resolve an active attempt before requesting its
log cursor.

The Go client now validates the response's job identity, bounded ordered
attempt numbers, UUIDs, states, and timestamps before selecting an attempt.

CLI tests cover ordered evidence, empty history, JSON parity, safe reason output,
invalid arguments/responses, and write errors. Real CLI/HTTP/PostgreSQL coverage
uses acquisition, lease-loss reaping, and replacement acquisition to verify two
attempts and project isolation. Test-only clock advancement exercises the loss
transition; this is not a new live-worker or wall-clock lease-expiry gate.
