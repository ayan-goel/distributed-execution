package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"dispatch.local/dispatch/internal/client"
	"github.com/google/uuid"
)

func runWorkers(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) >= 2 && args[1] == "list" {
		return listWorkers(ctx, args, getenv, out)
	}
	if len(args) < 3 || args[1] != "drain" {
		return errors.New("usage: dispatch workers list [--limit N] [--cursor CURSOR] [--json] | drain WORKER_ID [--json]")
	}
	fs := flag.NewFlagSet("workers drain", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "emit worker drain acknowledgement")
	if err := fs.Parse(args[3:]); err != nil {
		return err
	}
	id, err := uuid.Parse(args[2])
	if err != nil || id == uuid.Nil || id.String() != args[2] || fs.NArg() != 0 {
		return errors.New("worker drain requires a canonical worker UUID and no extra arguments")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	result, err := c.DrainWorker(ctx, id.String())
	if err != nil {
		// A committed drain can lose its reply. Repeating this fixed operation is
		// safe; do not claim either success or an unchanged host from this error.
		return fmt.Errorf("drain for worker %s unconfirmed: %w; inspect fleet status or repeat drain", id, err)
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(result)
	}
	_, err = fmt.Fprintf(out, "Worker %s drain requested; state=%s; existing attempts may finish\n", result.WorkerID, result.State)
	return err
}
