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

func downloadArtifact(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) < 4 || args[1] != "download" {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("artifacts download", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	destination := flags.String("output", "", "new local destination file")
	asJSON := flags.Bool("json", false, "emit verified download receipt")
	if err := flags.Parse(args[4:]); err != nil {
		return err
	}
	if *destination == "" || flags.NArg() != 0 {
		return errors.New("artifact download requires --output FILE and no extra positional arguments")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	receipt, err := c.DownloadArtifact(ctx, args[2], args[3], *destination)
	if err != nil {
		return err
	}
	// Report only verified local publication. Receipts deliberately omit signed
	// capabilities, and quoted text keeps user-controlled filenames terminal-safe.
	if *asJSON {
		return json.NewEncoder(out).Encode(receipt)
	}
	_, err = fmt.Fprintf(out, "Downloaded %q to %q (%d bytes, sha256:%s)\n", receipt.Name, receipt.Path, receipt.SizeBytes, receipt.SHA256)
	return err
}
