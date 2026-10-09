package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func runSweep(ctx context.Context, args []string, getenv func(string) string, out, errout io.Writer) error {
	if len(args) >= 3 && args[1] == "retry" {
		return retrySweep(ctx, args, getenv, out, errout)
	}
	if len(args) >= 3 && args[1] == "get" {
		return getSweep(ctx, args, getenv, out)
	}
	if len(args) >= 3 && args[1] == "export" {
		return exportSweep(ctx, args, getenv, out)
	}
	if len(args) < 3 || (args[1] != "submit" && args[1] != "validate") {
		return errors.New(usage)
	}
	fs := flag.NewFlagSet("sweep "+args[1], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "emit stable JSON")
	var key string
	if args[1] == "submit" {
		fs.StringVar(&key, "idempotency-key", "", "reuse this key when retrying the same sweep")
	}
	if err := fs.Parse(args[3:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	file, err := os.Open(args[2])
	if err != nil {
		return err
	}
	defer file.Close()
	sweep, err := spec.DecodeLocalSweep(file, func(path string) (spec.Job, error) {
		// Resolve relative references beside the sweep file so invocation from a
		// different working directory cannot silently select another template.
		if !filepath.IsAbs(path) {
			path = filepath.Join(filepath.Dir(args[2]), path)
		}
		template, err := os.Open(path)
		if err != nil {
			return spec.Job{}, err
		}
		defer template.Close()
		return spec.DecodeJob(template)
	})
	if err != nil {
		return err
	}
	body, hash, err := sweep.Canonical()
	if err != nil {
		return err
	}
	if len(body) > spec.MaxDocumentBytes {
		return errors.New("resolved sweep exceeds 1 MiB")
	}
	count := 1
	for _, values := range sweep.Spec.Matrix {
		count *= len(values)
	}
	if args[1] == "validate" {
		if *asJSON {
			return json.NewEncoder(out).Encode(map[string]any{"valid": true, "specHash": hash, "childCount": count})
		}
		_, err = fmt.Fprintf(out, "valid sweep specification: %d children (sha256:%s)\n", count, hash)
		return err
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	if key == "" {
		key = uuid.NewString()
	}
	// Emit the recovery key before admission can commit, including when stdout
	// fails or the connection loses the response after all children were created.
	if _, err := fmt.Fprintf(errout, "Idempotency-Key: %q\n", key); err != nil {
		return errors.New("cannot record sweep submission recovery key")
	}
	result, err := c.SubmitSweep(ctx, body, key)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(result)
	}
	_, err = fmt.Fprintf(out, "%s %d children\n", result.ID, len(result.ChildIDs))
	return err
}
