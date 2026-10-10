package api

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func (s *Server) drainWorker(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleOperator) {
		fail(w, 403, "FORBIDDEN", "operator permission required", false)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/workers/"), "/drain")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		fail(w, 404, "NOT_FOUND", "worker not found", false)
		return
	}
	// Drain has no policy variants. Reject ignored options so an operator cannot
	// mistake this maintenance intent for forced termination or cancellation.
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		fail(w, 400, "INVALID_ARGUMENT", "worker drain accepts no query parameters", false)
		return
	}
	if _, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 0)); err != nil {
		fail(w, 400, "INVALID_ARGUMENT", "worker drain requires an empty body", false)
		return
	}
	result, err := store.RequestWorkerDrain(r.Context(), s.pool, p, id)
	if errors.Is(err, store.ErrUnauthorized) {
		fail(w, 401, "UNAUTHENTICATED", "operator authorization no longer valid", false)
		return
	}
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
