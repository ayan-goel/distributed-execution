package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"

	"dispatch.local/dispatch/internal/client"
	"github.com/google/uuid"
)

func listAttempts(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) < 3 || args[1] != "list" {
		return errors.New("usage: dispatch attempts list JOB_ID [--json]")
	}
	fs := flag.NewFlagSet("attempts list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "emit job identity and ordered attempts")
	if err := fs.Parse(args[3:]); err != nil {
		return err
	}
	id, err := uuid.Parse(args[2])
	if err != nil || id == uuid.Nil || id.String() != args[2] || fs.NArg() != 0 {
		return errors.New("attempts list requires a canonical job UUID and no extra arguments")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	attempts, err := c.ListAttempts(ctx, id.String())
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(struct {
			JobID    string           `json:"jobId"`
			Attempts []client.Attempt `json:"attempts"`
		}{id.String(), attempts})
	}
	if _, err := fmt.Fprintf(out, "Job %s\n", id); err != nil {
		return err
	}
	if len(attempts) == 0 {
		_, err := fmt.Fprintln(out, "No attempts yet.")
		return err
	}
	for _, attempt := range attempts {
		reason, exitCode, finished := "null", "null", "null"
		// Reasons originate in worker reports. Quote them so control characters
		// cannot alter the terminal or make one attempt appear as several rows.
		if attempt.Reason != nil {
			reason = strconv.Quote(*attempt.Reason)
		}
		if attempt.ExitCode != nil {
			exitCode = strconv.FormatInt(int64(*attempt.ExitCode), 10)
		}
		if attempt.FinishedAt != nil {
			finished = attempt.FinishedAt.UTC().Format(time.RFC3339Nano)
		}
		if _, err := fmt.Fprintf(out, "%s number=%d state=%s worker=%s cleanupPending=%t reason=%s exitCode=%s createdAt=%s finishedAt=%s\n",
			attempt.ID, attempt.Number, attempt.State, attempt.WorkerID, attempt.CleanupPending, reason, exitCode, attempt.CreatedAt.UTC().Format(time.RFC3339Nano), finished); err != nil {
			return err
		}
	}
	return nil
}
