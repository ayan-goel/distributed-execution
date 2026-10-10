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
	"github.com/google/uuid"
)

var errWaitJobFailed = errors.New("waited job failed or was cancelled")
var errWaitDeadline = errors.New("wait deadline expired")

func waitJob(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("wait", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	timeout := fs.Duration("timeout", 0, "maximum wait; zero waits until interrupted")
	interval := fs.Duration("poll-interval", time.Second, "delay between status reads, 100ms–1m")
	cancelOnTimeout := fs.Bool("cancel-on-timeout", false, "request job cancellation after the wait's own timeout")
	asJSON := fs.Bool("json", false, "emit one final job object")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	id, err := uuid.Parse(args[1])
	if err != nil || id == uuid.Nil || fs.NArg() != 0 || *timeout < 0 || *interval < 100*time.Millisecond || *interval > time.Minute || *cancelOnTimeout && *timeout == 0 {
		return errors.New("wait requires a job UUID, nonnegative timeout, poll interval 100ms–1m, and a positive timeout for --cancel-on-timeout")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	waitCtx := ctx
	if *timeout > 0 {
		var cancel context.CancelFunc
		// A distinct cause separates our deadline from parent interruption and
		// the HTTP client's timeout. Only this deadline can authorize cancellation.
		waitCtx, cancel = context.WithTimeoutCause(ctx, *timeout, errWaitDeadline)
		defer cancel()
	}
	job, err := pollJob(waitCtx, c, id.String(), *interval)
	if err != nil {
		if context.Cause(waitCtx) == errWaitDeadline {
			return waitTimedOut(ctx, c, id.String(), *timeout, *cancelOnTimeout)
		}
		return fmt.Errorf("wait for job %s: %w", id, err)
	}
	if *asJSON {
		err = json.NewEncoder(out).Encode(job)
	} else {
		_, err = fmt.Fprintf(out, "%s %q\n", job.ID, job.State)
	}
	if err != nil {
		return err
	}
	// A terminal failure is an observed job outcome, not an infrastructure error.
	// Preserve exit 2 if rendering that outcome failed before this point.
	if job.State != "SUCCEEDED" {
		return errWaitJobFailed
	}
	return nil
}

func pollJob(ctx context.Context, c *client.Client, id string, interval time.Duration) (client.Job, error) {
	for {
		if err := ctx.Err(); err != nil {
			return client.Job{}, err
		}
		job, err := c.GetJob(ctx, id)
		if err != nil {
			return client.Job{}, err
		}
		if err := ctx.Err(); err != nil {
			return client.Job{}, err
		}
		switch job.State {
		case "SUCCEEDED", "FAILED", "CANCELLED":
			return job, nil
		case "QUEUED", "RETRY_WAIT", "ACTIVE", "CANCELLING":
		default:
			return client.Job{}, errors.New("invalid job state in wait response")
		}
		// Delay after each completed read to avoid request bursts on a slow server.
		// Context cancellation interrupts the timer without changing the job.
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return client.Job{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func waitTimedOut(ctx context.Context, c *client.Client, id string, timeout time.Duration, cancelJob bool) error {
	timedOut := fmt.Errorf("wait for job %s timed out after %s", id, timeout)
	if !cancelJob || ctx.Err() != nil {
		return timedOut
	}
	// The expired wait context cannot send a cancellation. Keep this bounded
	// request under the live parent so Ctrl-C still stops it; never detach it.
	cancelCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	job, err := c.CancelJob(cancelCtx, id)
	if err != nil {
		// A lost response can follow a committed cancellation. Describe uncertainty
		// so callers inspect status instead of assuming the job is still running.
		return fmt.Errorf("%w; cancellation unconfirmed: %v; inspect job status", timedOut, err)
	}
	return fmt.Errorf("%w; cancellation requested; returned state %q; cleanup may still be pending", timedOut, job.State)
}
