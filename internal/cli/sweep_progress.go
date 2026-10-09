package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"dispatch.local/dispatch/internal/client"
)

func getSweep(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("sweep get", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "emit stable JSON")
	limit := fs.Int("limit", 50, "children per page, 1–100")
	cursor := fs.String("cursor", "", "continue after the previous page")
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
	page, err := c.GetSweep(ctx, args[2], *cursor, *limit)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(page)
	}
	p := page.Sweep.Progress
	if _, err := fmt.Fprintf(out, "%s %q %s\n%d total: %d queued, %d retrying, %d active, %d cancelling, %d succeeded, %d failed, %d cancelled\n",
		page.Sweep.ID, page.Sweep.Name, page.Sweep.State, p.Total, p.Queued, p.RetryWait, p.Active, p.Cancelling, p.Succeeded, p.Failed, p.Cancelled); err != nil {
		return err
	}
	for _, child := range page.Children {
		// JSON escapes parameter controls before terminal display and preserves
		// exact metric decimals. Identity and state were validated by the client.
		parameters, err := json.Marshal(child.Parameters)
		if err != nil {
			return err
		}
		metrics, err := json.Marshal(child.Metrics)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "[%d] %s %s parameters=%s metrics=%s\n", child.Index, child.ID, child.State, parameters, metrics); err != nil {
			return err
		}
	}
	if page.HasMore {
		_, err = fmt.Fprintf(out, "nextCursor: %q\n", page.NextCursor)
	}
	return err
}
