package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	"dispatch.local/dispatch/internal/client"
	"github.com/google/uuid"
)

func uploadDataset(ctx context.Context, args []string, getenv func(string) string, out, errout io.Writer) error {
	if len(args) < 3 || args[1] != "upload" {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("dataset upload", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	name := flags.String("name", "", "registered dataset name")
	requestID := flags.String("request-id", "", "reuse this ID for an uncertain upload")
	resumeVersion := flags.String("resume-version", "", "complete an already uploaded object version")
	asJSON := flags.Bool("json", false, "emit registered dataset JSON")
	if err := flags.Parse(args[3:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *name == "" || *resumeVersion != "" && *requestID == "" {
		return errors.New("dataset upload requires --name; resume also requires --request-id")
	}
	archive, manifest, err := buildDatasetArchive(args[2])
	if err != nil {
		return err
	}
	if *requestID == "" {
		*requestID = uuid.NewString()
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	// Print durable recovery identities before their uncertain external actions.
	// A retry with the same request ID binds to this exact archive declaration.
	if _, err := fmt.Fprintf(errout, "Dataset-Request-ID: %q\n", *requestID); err != nil {
		return errors.New("cannot record dataset recovery ID")
	}
	digest := sha256.Sum256(archive)
	session, err := c.CreateDatasetUpload(ctx, *requestID, *name, int64(len(archive)), hex.EncodeToString(digest[:]))
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(errout, "Dataset-Upload-ID: %q\n", session.UploadID); err != nil {
		return errors.New("cannot record dataset upload ID")
	}
	version := *resumeVersion
	if version == "" {
		version, err = c.UploadDatasetBytes(ctx, session, archive)
		if err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(errout, "Dataset-Object-Version: %q\n", version); err != nil {
		return errors.New("cannot record dataset object version")
	}
	registered, err := c.CompleteDatasetUpload(ctx, session, version, manifest)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(registered)
	}
	_, err = fmt.Fprintf(out, "%s %q %q\n", registered.DatasetID, registered.Name, registered.ObjectVersion)
	return err
}
