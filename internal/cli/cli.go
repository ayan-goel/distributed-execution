package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

const usage = `usage:
  dispatch --version
  dispatch validate FILE [--json]
  dispatch submit FILE [--idempotency-key KEY] [--json]
  dispatch jobs get JOB_ID [--json]

Configure DISPATCH_URL and DISPATCH_TOKEN for server commands.
Set DISPATCH_DEV_INSECURE=1 only for literal-loopback HTTP development.
`

func Run(ctx context.Context, args []string, getenv func(string) string, out, errout io.Writer) int {
	if err := run(ctx, args, getenv, out, errout); err != nil {
		// Parser, filesystem, and server errors may contain user-controlled text.
		// Quote the whole error so it cannot inject terminal commands or new lines.
		fmt.Fprintf(errout, "dispatch: %q\n", err.Error())
		return 2
	}
	return 0
}

func run(ctx context.Context, args []string, getenv func(string) string, out, errout io.Writer) error {
	if len(args) == 1 && args[0] == "--version" {
		_, err := fmt.Fprintln(out, "dispatch 0.1.0-dev")
		return err
	}
	if len(args) == 1 && (args[0] == "--help" || args[0] == "help") {
		_, err := io.WriteString(out, usage)
		return err
	}
	if len(args) < 2 {
		return errors.New(usage)
	}
	command, operand, flags := args[0], args[1], args[2:]
	if command == "jobs" {
		if len(args) < 3 || args[1] != "get" {
			return errors.New(usage)
		}
		command, operand, flags = "get", args[2], args[3:]
	} else if command != "validate" && command != "submit" {
		return errors.New(usage)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "emit stable JSON")
	var key string
	if command == "submit" {
		fs.StringVar(&key, "idempotency-key", "", "reuse this key when retrying the same submission")
	}
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	var body []byte
	if command == "validate" || command == "submit" {
		file, err := os.Open(operand)
		if err != nil {
			return err
		}
		defer file.Close()
		job, err := spec.DecodeJob(file)
		if err != nil {
			return err
		}
		var hash string
		body, hash, err = job.Canonical()
		if err != nil {
			return err
		}
		if command == "validate" {
			if *asJSON {
				return json.NewEncoder(out).Encode(struct {
					Valid    bool   `json:"valid"`
					SpecHash string `json:"specHash"`
				}{true, hash})
			}
			_, err := fmt.Fprintf(out, "valid job specification (sha256:%s)\n", hash)
			return err
		}
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	var job client.Job
	if command == "submit" {
		if key == "" {
			key = uuid.NewString()
		}
		// Record the key before sending anything: a lost response or failed stdout
		// write cannot tell us whether admission committed. Reuse this key to recover.
		if _, err := fmt.Fprintf(errout, "Idempotency-Key: %q\n", key); err != nil {
			return errors.New("cannot record submission recovery key")
		}
		job, err = c.Submit(ctx, body, key)
	} else {
		job, err = c.GetJob(ctx, operand)
	}
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(job)
	}
	// ID has been validated by the client; quote the remote state for terminal safety.
	_, err = fmt.Fprintf(out, "%s %q\n", job.ID, job.State)
	return err
}
