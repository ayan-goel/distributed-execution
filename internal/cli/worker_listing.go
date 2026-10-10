package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"dispatch.local/dispatch/internal/client"
)

func listWorkers(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("workers list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := fs.Int("limit", 50, "workers per page, 1–100")
	cursor := fs.String("cursor", "", "continue after the previous page")
	asJSON := fs.Bool("json", false, "emit one JSON page")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *limit < 1 || *limit > 100 || len(*cursor) > 256 {
		return errors.New("invalid worker listing arguments")
	}
	for _, r := range *cursor {
		if r < 0x21 || r > 0x7e {
			return errors.New("invalid worker listing cursor")
		}
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	page, err := c.ListWorkers(ctx, client.WorkerListOptions{Limit: *limit, Cursor: *cursor})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(page)
	}
	if _, err := fmt.Fprintf(out, "Project %q (%s) asOf=%s\n", page.Project, page.ProjectID, page.AsOf.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if len(page.Workers) == 0 {
		_, err = fmt.Fprintln(out, "No workers on this page.")
		return err
	}
	for _, worker := range page.Workers {
		heartbeat := "never"
		if worker.LastHeartbeatAt != nil {
			heartbeat = worker.LastHeartbeatAt.Format(time.RFC3339Nano)
		}
		// Recorded health and free headroom do not guarantee a fresh session.
		// Include observation times so operators can assess stale evidence.
		if _, err := fmt.Fprintf(out, "%s %q %s drain=%t runtimeHealthy=%t reconciliationComplete=%t diskPressure=%t lastHeartbeatAt=%s\n", worker.ID, worker.Name, worker.State, worker.DrainRequested, worker.RuntimeHealthy, worker.ReconciliationComplete, worker.DiskPressure, heartbeat); err != nil {
			return err
		}
		for _, row := range []struct {
			name  string
			value client.WorkerCapacity
		}{{"capacity", worker.Capacity}, {"reserved", worker.Reserved}, {"available", worker.Available}} {
			if _, err := fmt.Fprintf(out, "  %s cpuMillis=%d memoryMiB=%d scratchMiB=%d slots=%d\n", row.name, row.value.CPUMillis, row.value.MemoryMiB, row.value.ScratchMiB, row.value.Slots); err != nil {
				return err
			}
		}
		labels, err := json.Marshal(worker.Labels)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "  labels=%s\n", labels); err != nil {
			return err
		}
	}
	if page.HasMore {
		_, err = fmt.Fprintf(out, "nextCursor: %q\n", page.NextCursor)
	}
	return err
}
