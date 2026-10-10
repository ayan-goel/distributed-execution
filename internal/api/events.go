package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type eventCursor struct {
	Version int    `json:"v"`
	Project string `json:"p"`
	Job     string `json:"j"`
	After   int64  `json:"n"`
}

type EventPage struct {
	JobID string `json:"jobId"`
	store.JobEventPage
	NextCursor string `json:"nextCursor"`
}

func encodeEventCursor(project, job string, after int64) string {
	body, _ := json.Marshal(eventCursor{Version: 1, Project: project, Job: job, After: after})
	return base64.RawURLEncoding.EncodeToString(body)
}

func parseEventQuery(raw, project, job string) (int64, int, error) {
	// Two small query fields need no large allocation allowance. This also
	// leaves ample room for authentication under the listener's header limit.
	if len(raw) > 2048 {
		return 0, 0, store.ErrInvalid
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return 0, 0, store.ErrInvalid
	}
	for key, values := range q {
		if (key != "limit" && key != "cursor") || len(values) != 1 || values[0] == "" {
			return 0, 0, store.ErrInvalid
		}
	}
	limit := 50
	if value := q.Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 100 {
			return 0, 0, store.ErrInvalid
		}
	}
	value := q.Get("cursor")
	if value == "" {
		return 0, limit, nil
	}
	if len(value) > 256 {
		return 0, 0, store.ErrInvalid
	}
	body, err := base64.RawURLEncoding.Strict().DecodeString(value)
	var cursor eventCursor
	if err != nil || json.Unmarshal(body, &cursor) != nil || cursor.Version != 1 || cursor.Project != project || cursor.Job != job || cursor.After < 0 {
		return 0, 0, store.ErrInvalid
	}
	// Re-encoding requires exactly our versioned shape, rejecting duplicate,
	// unknown, missing, and null fields. The position grants no read authority.
	if encodeEventCursor(project, job, cursor.After) != value {
		return 0, 0, store.ErrInvalid
	}
	return cursor.After, limit, nil
}

func (s *Server) events(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "read permission required", false)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/jobs/"), "/events")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		fail(w, 404, "NOT_FOUND", "job not found", false)
		return
	}
	after, limit, err := parseEventQuery(r.URL.RawQuery, p.ProjectID, id)
	if err != nil {
		fail(w, 400, "INVALID_ARGUMENT", "invalid event query or cursor", false)
		return
	}
	page, err := store.ListJobEvents(r.Context(), s.pool, p.ProjectID, id, after, limit)
	if err != nil {
		s.storeError(w, err)
		return
	}
	response := EventPage{JobID: id, JobEventPage: page, NextCursor: encodeEventCursor(p.ProjectID, id, page.NextSequence)}
	// Check the entire envelope before headers. Even an empty page returns its
	// continuation so polling can resume without replaying observed events.
	body, err := json.Marshal(response)
	if err != nil || len(body)+1 > store.MaxEventPageBytes {
		fail(w, 503, "UNAVAILABLE", "event page unavailable", true)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(body, '\n'))
}
