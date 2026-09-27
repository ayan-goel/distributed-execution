package api

import (
	"net/http"
	"strings"

	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func (s *Server) attempts(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "read permission required", false)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/jobs/"), "/attempts")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		fail(w, 404, "NOT_FOUND", "job not found", false)
		return
	}
	history, err := store.ListAttempts(r.Context(), s.pool, p.ProjectID, id)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, history)
}
