# Waiting for jobs

Use `dispatch wait` in scripts that depend on a job's final outcome:

```sh
bin/dispatch wait JOB_UUID
bin/dispatch wait JOB_UUID --timeout 30m --json
bin/dispatch wait JOB_UUID --timeout 30m --cancel-on-timeout
```

Options follow the job UUID. The default timeout is zero, meaning no overall
deadline. Status reads start immediately, then wait one second between completed
reads. `--poll-interval` accepts durations from `100ms` to `1m`.

| Observed outcome | Exit code | Output |
| --- | --- | --- |
| `SUCCEEDED` | 0 | Final job UUID and quoted state |
| `FAILED` or `CANCELLED` | 1 | Final job UUID and quoted state |
| Timeout, interruption, invalid response, or client/infrastructure error | 2 | Diagnostic on stderr |

`--json` replaces the terminal text with one job object in the same format as
`dispatch jobs get --json`. Polling emits no progress on stdout. An output write
failure returns 2 even when the job has reached a terminal state. API and transport
errors fail immediately; the command does not retry them. Each request retains
the client's independent 20-second timeout, bounded by the overall wait deadline.

## Timeout and cancellation

Ordinary timeout and interruption do not change the job. `--cancel-on-timeout`
requires a positive `--timeout` and sends one cancellation request only when that
wait deadline expires. Ctrl-C or a parent deadline does not trigger cancellation.

Explicit timeout cancellation can take up to 20 additional seconds under the live
parent context. The command still returns 2, reports the returned job state, and
does not wait for worker cleanup. Completion may race with cancellation, so the
returned state can already be terminal. If the response is lost or rejected,
stderr reports cancellation as unconfirmed; inspect the job before taking another
action. A lost response can follow a committed cancellation.

Read-only project tokens can wait but cannot cancel. Existing project ownership
checks apply to every read and cancellation request.

## Verification boundary

CLI tests cover all terminal exit codes, polling, timeouts, interruption, malformed
states, output failures, cancellation races, and lost responses. A real
CLI/HTTP/PostgreSQL test verifies queued-job cancellation, unchanged job/event
storage on ordinary timeout, and project/role enforcement. This slice adds no
worker runtime behavior or new live-container execution evidence.
