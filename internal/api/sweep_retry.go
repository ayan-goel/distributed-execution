package api

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func (s *Server) retrySweep(w http.ResponseWriter, r *http.Request, p store.Principal) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/sweeps/"), "/retry")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		fail(w, 404, "NOT_FOUND", "sweep not found", false)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	validKey := len(key) >= 1 && len(key) <= 128
	for _, char := range key {
		validKey = validKey && char >= '!' && char <= '~'
	}
	if !validKey {
		fail(w, 400, "INVALID_ARGUMENT", "Idempotency-Key must contain 1–128 visible ASCII characters", false)
		return
	}
	// Retry has one fixed selection policy. Reject rather than ignore options:
	// the durable request identity binds only the source sweep, not a payload.
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		fail(w, 400, "INVALID_ARGUMENT", "sweep retry accepts no query parameters", false)
		return
	}
	if _, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 0)); err != nil {
		fail(w, 400, "INVALID_ARGUMENT", "sweep retry requires an empty body", false)
		return
	}
	// Authorization is checked on every request, including replay. Source and
	// idempotency lookups use the authenticated project, never caller-supplied scope.
	result, err := store.RetrySweep(r.Context(), s.pool, p.ProjectID, id, key)
	if errors.Is(err, store.ErrConflict) {
		fail(w, 409, "CONFLICT", "sweep must be terminal and contain failed or cancelled jobs", false)
		return
	}
	if err != nil {
		s.storeError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/v1/sweeps/"+result.ID)
	writeJSON(w, status, result)
}
