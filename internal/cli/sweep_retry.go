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

func retrySweep(ctx context.Context, args []string, getenv func(string) string, out, errout io.Writer) error {
	fs := flag.NewFlagSet("sweep retry", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "emit stable JSON")
	key := fs.String("idempotency-key", "", "reuse this key to recover the same retry sweep")
	if err := fs.Parse(args[3:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	if *key == "" {
		*key = uuid.NewString()
	}
	// Admission may commit even when the reply is lost. Record the key first so
	// recovery reuses one retry instead of creating duplicate linked jobs.
	if _, err := fmt.Fprintf(errout, "Idempotency-Key: %q\n", *key); err != nil {
		return errors.New("cannot record sweep retry recovery key")
	}
	result, err := c.RetrySweep(ctx, args[2], *key)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(result)
	}
	_, err = fmt.Fprintf(out, "%s %d children\n", result.ID, len(result.Children))
	return err
}
