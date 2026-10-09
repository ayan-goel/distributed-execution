package cli

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"slices"
	"strconv"

	"dispatch.local/dispatch/internal/client"
)

func exportSweep(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	fs := flag.NewFlagSet("sweep export", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	format := fs.String("format", "json", "json or csv")
	if err := fs.Parse(args[3:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || (*format != "json" && *format != "csv") {
		return errors.New("sweep export requires --format json|csv and no extra positional arguments")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	results, err := c.CollectSweep(ctx, args[2])
	if err != nil {
		return err
	}
	if *format == "json" {
		return json.NewEncoder(out).Encode(results)
	}
	return writeSweepCSV(out, results.Children)
}

func writeSweepCSV(out io.Writer, children []client.SweepChild) error {
	names := map[string]bool{}
	for _, child := range children {
		for name := range child.Metrics {
			names[name] = true
		}
	}
	// A metric per child must not multiply into an enormous sparse CSV table.
	// JSON remains available when a sweep uses more than 256 distinct metrics.
	if len(names) > 256 {
		return errors.New("CSV export exceeds 256 metric columns; use JSON instead")
	}
	metrics := make([]string, 0, len(names))
	for name := range names {
		metrics = append(metrics, name)
	}
	slices.Sort(metrics)
	header := []string{"index", "jobId", "state", "acceptedAttemptId", "parameters"}
	for _, name := range metrics {
		header = append(header, "metric."+name)
	}
	w := csv.NewWriter(out)
	if err := w.Write(header); err != nil {
		return err
	}
	for _, child := range children {
		// A JSON object preserves parameter strings and starts with '{', so
		// spreadsheet software cannot interpret a user value as a formula.
		parameters, err := json.Marshal(child.Parameters)
		if err != nil {
			return err
		}
		attempt := ""
		if child.AcceptedAttemptID != nil {
			attempt = *child.AcceptedAttemptID
		}
		row := []string{strconv.Itoa(child.Index), child.ID, child.State, attempt, string(parameters)}
		for _, name := range metrics {
			row = append(row, child.Metrics[name].String())
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
