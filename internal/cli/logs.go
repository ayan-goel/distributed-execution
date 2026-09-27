package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/client"
)

func showLogs(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) < 2 {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("logs", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	follow := flags.Bool("follow", false, "poll new log segments")
	stream := flags.String("stream", "both", "stdout, stderr, or both")
	if err := flags.Parse(args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *stream != "both" && *stream != "stdout" && *stream != "stderr" {
		return errors.New("logs requires a job ID and optional --follow or --stream")
	}
	c, err := client.New(getenv("DISPATCH_URL"), getenv("DISPATCH_TOKEN"), getenv("DISPATCH_DEV_INSECURE") == "1", nil)
	if err != nil {
		return err
	}
	streams := []string{"stdout", "stderr"}
	if *stream != "both" {
		streams = []string{*stream}
	}
	current := ""
	cursors := map[string]string{}
	lastSequence := map[string]uint64{}
	reportedCompletion := false
	for {
		attempts, err := c.ListAttempts(ctx, args[1])
		if err != nil {
			return err
		}
		if len(attempts) > 0 {
			latest := attempts[len(attempts)-1]
			if latest.ID != current {
				current = latest.ID
				cursors = map[string]string{}
				lastSequence = map[string]uint64{}
				reportedCompletion = false
				if _, err := fmt.Fprintf(out, "attempt %s\n", current); err != nil {
					return err
				}
			}
			var completion *client.LogCompletion
			for _, name := range streams {
				found, err := pollLogStream(ctx, c, current, name, cursors, lastSequence, out)
				if err != nil {
					return err
				}
				if found != nil {
					completion = found
				}
			}
			if completion != nil && !reportedCompletion {
				if err := renderCompletion(out, completion); err != nil {
					return err
				}
				reportedCompletion = true
			}
		} else if !*follow {
			return errors.New("job has no attempts yet")
		}
		if !*follow {
			return nil
		}
		job, err := c.GetJob(ctx, args[1])
		if err != nil {
			return err
		}
		if job.State == "SUCCEEDED" || job.State == "FAILED" || job.State == "CANCELLED" {
			if len(attempts) == 0 || terminalAttempt(attempts[len(attempts)-1].State) {
				return nil
			}
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func pollLogStream(ctx context.Context, c *client.Client, attemptID, stream string, cursors map[string]string, lastSequence map[string]uint64, out io.Writer) (*client.LogCompletion, error) {
	for {
		page, err := c.ListLogs(ctx, attemptID, stream, cursors[stream])
		if err != nil {
			return nil, err
		}
		for _, segment := range page.Segments {
			decoded, err := c.FetchLogSegment(ctx, attemptID, segment)
			if err != nil {
				return nil, err
			}
			for _, record := range decoded.Records {
				if record.Sequence <= lastSequence[stream] {
					continue
				}
				if _, err := fmt.Fprintf(out, "[%s #%d] %s\n", stream, record.Sequence, escapeLog(record.Payload)); err != nil {
					return nil, err
				}
				lastSequence[stream] = record.Sequence
			}
		}
		if page.HasMore && page.NextCursor == cursors[stream] {
			return nil, errors.New("log cursor did not advance")
		}
		cursors[stream] = page.NextCursor
		if !page.HasMore {
			return page.Completion, nil
		}
	}
}

func escapeLog(payload []byte) string {
	var out strings.Builder
	out.Grow(len(payload))
	for _, b := range payload {
		if b >= 0x20 && b <= 0x7e && b != '\\' {
			out.WriteByte(b)
		} else {
			fmt.Fprintf(&out, "\\x%02x", b)
		}
	}
	return out.String()
}

func renderCompletion(out io.Writer, completion *client.LogCompletion) error {
	if completion.LogsComplete {
		return nil
	}
	if _, err := io.WriteString(out, "[logs incomplete"); err != nil {
		return err
	}
	for _, gap := range completion.Gaps {
		if _, err := fmt.Fprintf(out, " %s:%d-%d", gap.Stream, gap.First, gap.Last); err != nil {
			return err
		}
	}
	_, err := io.WriteString(out, "]\n")
	return err
}

func terminalAttempt(state string) bool {
	return state == "SUCCEEDED" || state == "FAILED" || state == "LOST" || state == "CANCELLED"
}
