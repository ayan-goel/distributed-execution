package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/client"
)

type jobLabelFlags map[string]string

func (labels *jobLabelFlags) String() string { return "" }
func (labels *jobLabelFlags) Set(value string) error {
	key, value, ok := strings.Cut(value, "=")
	if !ok {
		return errors.New("label must use key=value")
	}
	if *labels == nil {
		*labels = jobLabelFlags{}
	}
	if _, exists := (*labels)[key]; exists {
		return errors.New("duplicate label key")
	}
	(*labels)[key] = value
	return nil
}

func listJobs(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("jobs list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	project := fs.String("project", "", "project name; defaults to the token's project")
	state := fs.String("state", "", "filter by job state")
	limit := fs.Int("limit", 50, "jobs per page, 1–100")
	cursor := fs.String("cursor", "", "continue the same filters after the previous page")
	asJSON := fs.Bool("json", false, "emit stable JSON")
	var labels jobLabelFlags
	fs.Var(&labels, "label", "require metadata key=value; repeat for AND matching")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	page, err := c.ListJobs(ctx, client.JobListOptions{Project: *project, State: *state, Labels: labels, Limit: *limit, Cursor: *cursor})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(page)
	}
	if _, err := fmt.Fprintf(out, "Project %q (%s)\n", page.Project, page.ProjectID); err != nil {
		return err
	}
	if len(page.Jobs) == 0 {
		_, err = fmt.Fprintln(out, "No jobs matched.")
		return err
	}
	for _, job := range page.Jobs {
		// The client validates IDs/enums and quotes the remote name here. Print
		// submitted priority; effective queue priority can change with eligible age.
		if _, err := fmt.Fprintf(out, "%s %s %q priority=%d createdAt=%s\n", job.ID, job.State, job.Name, job.Priority, job.CreatedAt.Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	if page.HasMore {
		_, err = fmt.Fprintf(out, "nextCursor: %q\n", page.NextCursor)
	}
	return err
}
