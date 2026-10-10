package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxJobListResponseBytes = 8 << 20
const MaxJobListQueryBytes = 12 << 10

type JobListOptions struct {
	Project string
	State   string
	Labels  map[string]string
	Limit   int
	Cursor  string
}

type JobSummary struct {
	ID        string            `json:"id"`
	ProjectID string            `json:"projectId"`
	Name      string            `json:"name"`
	State     string            `json:"state"`
	Labels    map[string]string `json:"labels"`
	Priority  int               `json:"priority"`
	CreatedAt time.Time         `json:"createdAt"`
}

type JobPage struct {
	Project    string       `json:"project"`
	ProjectID  string       `json:"projectId"`
	Jobs       []JobSummary `json:"jobs"`
	HasMore    bool         `json:"hasMore"`
	NextCursor string       `json:"nextCursor"`
}

func validListedState(state string) bool {
	switch state {
	case "QUEUED", "RETRY_WAIT", "ACTIVE", "CANCELLING", "SUCCEEDED", "FAILED", "CANCELLED":
		return true
	}
	return false
}

func validListedLabels(labels map[string]string) bool {
	if len(labels) > 128 {
		return false
	}
	for key, value := range labels {
		if !artifactName.MatchString(key) || len(value) > 8192 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return false
		}
	}
	return true
}

func (c *Client) ListJobs(ctx context.Context, options JobListOptions) (JobPage, error) {
	invalid := errors.New("invalid job listing options or encoded query exceeds 12 KiB")
	if options.Limit < 1 || options.Limit > 100 || options.Project != "" && !artifactName.MatchString(options.Project) || options.State != "" && !validListedState(options.State) || options.Cursor != "" && !visibleASCII(options.Cursor, 512) || !validListedLabels(options.Labels) {
		return JobPage{}, invalid
	}
	q := url.Values{"limit": {strconv.Itoa(options.Limit)}}
	if options.Project != "" {
		q.Set("project", options.Project)
	}
	if options.State != "" {
		q.Set("state", options.State)
	}
	if options.Cursor != "" {
		q.Set("cursor", options.Cursor)
	}
	for key, value := range options.Labels {
		q.Add("label", key+"="+value)
	}
	query := q.Encode()
	if len(query) > MaxJobListQueryBytes {
		return JobPage{}, invalid
	}
	// Escaped metadata can exceed the generic 4 MiB response allowance. Only
	// listing opts into the store's bounded 8 MiB page contract.
	body, err := c.requestBodyLimit(ctx, http.MethodGet, "/v1/jobs?"+query, nil, "", MaxJobListResponseBytes)
	if err != nil {
		return JobPage{}, err
	}
	return decodeJobPage(body, options)
}

func decodeJobPage(body []byte, options JobListOptions) (JobPage, error) {
	invalid := errors.New("invalid job listing response")
	var page JobPage
	var rows []json.RawMessage
	if !utf8.Valid(body) || requiredDiagnosticObject(body, map[string]any{"project": &page.Project, "projectId": &page.ProjectID, "jobs": &rows, "hasMore": &page.HasMore, "nextCursor": &page.NextCursor}) != nil || !artifactName.MatchString(page.Project) || !downloadUUID(page.ProjectID) || options.Project != "" && page.Project != options.Project || rows == nil || len(rows) > options.Limit || len(page.NextCursor) > 512 {
		return JobPage{}, invalid
	}
	if page.HasMore && (len(rows) == 0 || !visibleASCII(page.NextCursor, 512) || page.NextCursor == options.Cursor) || !page.HasMore && page.NextCursor != "" {
		return JobPage{}, invalid
	}
	page.Jobs = make([]JobSummary, len(rows))
	seen := make(map[string]bool, len(rows))
	for i, raw := range rows {
		job := &page.Jobs[i]
		var labels json.RawMessage
		if requiredDiagnosticObject(raw, map[string]any{"id": &job.ID, "projectId": &job.ProjectID, "name": &job.Name, "state": &job.State, "labels": &labels, "priority": &job.Priority, "createdAt": &job.CreatedAt}) != nil || !downloadUUID(job.ID) || seen[job.ID] || job.ProjectID != page.ProjectID || !artifactName.MatchString(job.Name) || !validListedState(job.State) || options.State != "" && job.State != options.State || job.Priority < 0 || job.Priority > 3 || !validDiagnosticTime(job.CreatedAt.UTC()) || json.Unmarshal(labels, &job.Labels) != nil || job.Labels == nil || !validListedLabels(job.Labels) {
			return JobPage{}, invalid
		}
		// Strict object decoding also rejects duplicate label keys and null values;
		// ordinary map unmarshalling would conceal ambiguous filter membership.
		fields := make(map[string]any, len(job.Labels))
		for key := range job.Labels {
			fields[key] = new(string)
		}
		if requiredDiagnosticObject(labels, fields) != nil {
			return JobPage{}, invalid
		}
		for key, value := range options.Labels {
			if got, ok := job.Labels[key]; !ok || got != value {
				return JobPage{}, invalid
			}
		}
		job.CreatedAt = job.CreatedAt.UTC()
		if i > 0 {
			previous := page.Jobs[i-1]
			if job.CreatedAt.After(previous.CreatedAt) || job.CreatedAt.Equal(previous.CreatedAt) && job.ID >= previous.ID {
				return JobPage{}, invalid
			}
		}
		seen[job.ID] = true
	}
	return page, nil
}
