package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

const maxJobQueryBytes = 12 << 10
const maxJobCursorBytes = 512

var errJobListProject = errors.New("project access denied")

type jobCursor struct {
	Version int       `json:"v"`
	Project string    `json:"p"`
	Filter  string    `json:"f"`
	Created time.Time `json:"t"`
	ID      string    `json:"id"`
}

type JobPage struct {
	Project   string `json:"project"`
	ProjectID string `json:"projectId"`
	store.JobListPage
	NextCursor string `json:"nextCursor"`
}

func jobFilterHash(filter store.JobListFilter) string {
	if filter.Labels == nil {
		filter.Labels = map[string]string{}
	}
	// Sorted JSON map keys make label order irrelevant. Page size is excluded
	// so callers can resize subsequent pages without changing membership.
	body, _ := json.Marshal(filter)
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

func encodeJobCursor(project string, filter store.JobListFilter, position store.JobListPosition) (string, error) {
	body, err := json.Marshal(jobCursor{Version: 1, Project: project, Filter: jobFilterHash(filter), Created: position.CreatedAt.UTC(), ID: position.ID})
	return base64.RawURLEncoding.EncodeToString(body), err
}

func parseJobQuery(raw string, p store.Principal) (store.JobListFilter, *store.JobListPosition, int, error) {
	filter := store.JobListFilter{Labels: map[string]string{}}
	invalid := func() (store.JobListFilter, *store.JobListPosition, int, error) {
		return filter, nil, 0, store.ErrInvalid
	}
	// Stay below the executable listener's 16 KiB header budget, leaving room
	// for the request line and credentials. Bound before allocating query maps.
	if len(raw) > maxJobQueryBytes {
		return invalid()
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return invalid()
	}
	for key, values := range q {
		if key == "label" {
			continue
		}
		if (key != "project" && key != "state" && key != "limit" && key != "cursor") || len(values) != 1 || values[0] == "" {
			return invalid()
		}
	}
	if project := q.Get("project"); project != "" && project != p.Project {
		return filter, nil, 0, errJobListProject
	}
	filter.State = q.Get("state")
	for _, label := range q["label"] {
		key, value, ok := strings.Cut(label, "=")
		if _, duplicate := filter.Labels[key]; !ok || duplicate {
			return invalid()
		}
		filter.Labels[key] = value
	}
	limit := 50
	if value := q.Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 100 {
			return invalid()
		}
	}
	value := q.Get("cursor")
	if value == "" {
		return filter, nil, limit, nil
	}
	if len(value) > maxJobCursorBytes {
		return invalid()
	}
	body, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil {
		return invalid()
	}
	var cursor jobCursor
	if json.Unmarshal(body, &cursor) != nil || cursor.Version != 1 || cursor.Project != p.ProjectID || cursor.Filter != jobFilterHash(filter) || cursor.Created.IsZero() || cursor.Created.Year() < 1 || cursor.Created.Year() > 9999 {
		return invalid()
	}
	id, err := uuid.Parse(cursor.ID)
	if err != nil || id == uuid.Nil || id.String() != cursor.ID {
		return invalid()
	}
	position := store.JobListPosition{CreatedAt: cursor.Created.UTC(), ID: cursor.ID}
	canonical, err := encodeJobCursor(p.ProjectID, filter, position)
	// Cursors are opaque server output. Requiring its canonical encoding rejects
	// unknown, duplicate, missing, and null fields without ambiguous JSON rules.
	if err != nil || canonical != value {
		return invalid()
	}
	return filter, &position, limit, nil
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "read permission required", false)
		return
	}
	filter, after, limit, err := parseJobQuery(r.URL.RawQuery, p)
	if errors.Is(err, errJobListProject) {
		fail(w, 403, "FORBIDDEN", "project access denied", false)
		return
	}
	if err != nil {
		fail(w, 400, "INVALID_ARGUMENT", "invalid job query or cursor", false)
		return
	}
	// A cursor is only a position. Ownership always comes from authentication,
	// even if a caller constructs another otherwise valid cursor themselves.
	page, err := store.ListJobs(r.Context(), s.pool, p.ProjectID, filter, after, limit)
	if errors.Is(err, store.ErrInvalid) {
		fail(w, 400, "INVALID_ARGUMENT", "invalid job filters", false)
		return
	}
	if err != nil {
		s.storeError(w, err)
		return
	}
	response := JobPage{Project: p.Project, ProjectID: p.ProjectID, JobListPage: page}
	if page.HasMore {
		response.NextCursor, err = encodeJobCursor(p.ProjectID, filter, *page.Next)
		if err != nil {
			fail(w, 503, "UNAVAILABLE", "job cursor unavailable", true)
			return
		}
	}
	// Check the actual envelope before committing headers. Store accounting
	// reserves 1 KiB for these bounded project/cursor fields and the newline.
	body, err := json.Marshal(response)
	if err != nil || len(response.NextCursor) > maxJobCursorBytes || len(body)+1 > store.MaxJobPageBytes {
		fail(w, 503, "UNAVAILABLE", "job page unavailable", true)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(body, '\n'))
}
