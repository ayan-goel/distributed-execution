# CLI log inspection (D15o)

`dispatch logs JOB_ID` selects the latest attempt and reads both stdout and
stderr catalog cursors. `--stream stdout|stderr` narrows the read. `--follow`
polls once per second, switches to a newer attempt after a retry, and stops
when the job and latest attempt are terminal.

The command prints only records whose object bytes passed the client's exact
version, size, hash, identity, and gap checks. Each record appears on one line
with stream and sequence. Every nonprintable byte, backslash, newline, invalid
UTF-8 byte, and ANSI escape is shown as `\xHH`; the CLI never emits those bytes
directly to the terminal. Frozen completion gaps appear in a single
`[logs incomplete ...]` line for each attempt.

The server cursor allows polling after empty pages. If the command stops, a
new invocation replays available history from the start. Live worker capture
and delivery still need runner integration, so a current deployment may have
no registered log objects to show.
