package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type workerCursor struct {
	Version int    `json:"v"`
	Project string `json:"p"`
	Next    string `json:"n"`
}

type WorkerPage struct {
	Project   string `json:"project"`
	ProjectID string `json:"projectId"`
	store.WorkerListPage
	NextCursor string `json:"nextCursor"`
}

func encodeWorkerCursor(project, after string) (string, error) {
	body, err := json.Marshal(workerCursor{Version: 1, Project: project, Next: after})
	return base64.RawURLEncoding.EncodeToString(body), err
}

func parseWorkerQuery(raw string, p store.Principal) (string, int, error) {
	if len(raw) > 2048 {
		return "", 0, store.ErrInvalid
	}
	q, err := url.ParseQuery(raw)
	if err != nil {
		return "", 0, store.ErrInvalid
	}
	for key, values := range q {
		if (key != "limit" && key != "cursor") || len(values) != 1 || values[0] == "" {
			return "", 0, store.ErrInvalid
		}
	}
	limit := 50
	if value := q.Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 100 {
			return "", 0, store.ErrInvalid
		}
	}
	value := q.Get("cursor")
	if value == "" {
		return "", limit, nil
	}
	if len(value) > 256 {
		return "", 0, store.ErrInvalid
	}
	body, err := base64.RawURLEncoding.Strict().DecodeString(value)
	var cursor workerCursor
	if err != nil || json.Unmarshal(body, &cursor) != nil || cursor.Version != 1 || cursor.Project != p.ProjectID {
		return "", 0, store.ErrInvalid
	}
	id, err := uuid.Parse(cursor.Next)
	if err != nil || id == uuid.Nil || id.String() != cursor.Next {
		return "", 0, store.ErrInvalid
	}
	canonical, err := encodeWorkerCursor(p.ProjectID, cursor.Next)
	// Canonical output rejects ambiguous JSON fields and encodings. The cursor
	// carries a position only; authentication independently defines host access.
	if err != nil || canonical != value {
		return "", 0, store.ErrInvalid
	}
	return cursor.Next, limit, nil
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "read permission required", false)
		return
	}
	after, limit, err := parseWorkerQuery(r.URL.RawQuery, p)
	if err != nil {
		fail(w, 400, "INVALID_ARGUMENT", "invalid worker query or cursor", false)
		return
	}
	page, err := store.ListWorkers(r.Context(), s.pool, p.ProjectID, after, limit)
	if err != nil {
		s.storeError(w, err)
		return
	}
	response := WorkerPage{Project: p.Project, ProjectID: p.ProjectID, WorkerListPage: page}
	if page.HasMore {
		response.NextCursor, err = encodeWorkerCursor(p.ProjectID, page.NextID)
		if err != nil {
			fail(w, 503, "UNAVAILABLE", "worker cursor unavailable", true)
			return
		}
	}
	// Include escaped labels and the public envelope in the bound before headers
	// are committed. Oversized stored evidence fails without a truncated page.
	body, err := json.Marshal(response)
	if err != nil || len(response.NextCursor) > 256 || len(body)+1 > store.MaxWorkerPageBytes {
		fail(w, 503, "UNAVAILABLE", "worker page unavailable", true)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(body, '\n'))
}
