package cli

import (
	"fmt"
	"io"
	"time"

	"dispatch.local/dispatch/internal/client"
)

func writeQueueDiagnostics(out io.Writer, d *client.QueueDiagnostics) error {
	if d == nil {
		return nil
	}
	// Label retained checks as historical: one worker's constraint or an old
	// attempt cannot establish whether the current job fits every eligible host.
	if _, err := fmt.Fprintf(out, "Historical queue observations (read at %s; current attempt counter %d):\n", d.AsOf.Format(time.RFC3339Nano), d.AttemptCounter); err != nil {
		return err
	}
	if len(d.Observations) == 0 {
		_, err := fmt.Fprintln(out, "  No queue observations recorded.")
		return err
	}
	for _, o := range d.Observations {
		// The client validates enums, canonical IDs, and times before rendering.
		// Absolute server timestamps avoid inventing ages from the CLI host's clock.
		if _, err := fmt.Fprintf(out, "  %s  %s  worker=%s  attemptCounter=%d\n", o.ObservedAt.Format(time.RFC3339Nano), o.Reason, o.WorkerID, o.AttemptCounter); err != nil {
			return err
		}
	}
	return nil
}
