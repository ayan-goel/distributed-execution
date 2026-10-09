package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type sweepCursor struct {
	Version int    `json:"v"`
	SweepID string `json:"s"`
	After   *int   `json:"i"`
}

type SweepPage struct {
	store.SweepPage
	NextCursor string `json:"nextCursor"`
}

func encodeSweepCursor(sweepID string, after int) string {
	body, _ := json.Marshal(sweepCursor{Version: 1, SweepID: sweepID, After: &after})
	return base64.RawURLEncoding.EncodeToString(body)
}

func parseSweepQuery(raw, sweepID string) (int, int, error) {
	q, err := url.ParseQuery(raw)
	if err != nil {
		return 0, 0, store.ErrInvalid
	}
	for key, values := range q {
		if (key != "limit" && key != "cursor") || len(values) != 1 {
			return 0, 0, store.ErrInvalid
		}
	}
	limit := 50
	if values, present := q["limit"]; present {
		limit, err = strconv.Atoi(values[0])
		if err != nil || limit < 1 || limit > store.MaxSweepPageSize {
			return 0, 0, store.ErrInvalid
		}
	}
	value, present := q["cursor"]
	if !present {
		return -1, limit, nil
	}
	if value[0] == "" || len(value[0]) > 256 {
		return 0, 0, store.ErrInvalid
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value[0])
	if err != nil {
		return 0, 0, store.ErrInvalid
	}
	var cursor sweepCursor
	// A cursor carries position, never authorization. Bind it to this sweep and
	// still scope every database lookup to the authenticated project's identity.
	if json.Unmarshal(decoded, &cursor) != nil || cursor.Version != 1 || cursor.SweepID != sweepID || cursor.After == nil || *cursor.After < 0 || *cursor.After >= spec.MaxSweepJobs {
		return 0, 0, store.ErrInvalid
	}
	return *cursor.After, limit, nil
}

func (s *Server) sweepProgress(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "read permission required", false)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/sweeps/")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		fail(w, 404, "NOT_FOUND", "sweep not found", false)
		return
	}
	after, limit, err := parseSweepQuery(r.URL.RawQuery, id)
	if err != nil {
		fail(w, 400, "INVALID_ARGUMENT", "invalid sweep query or cursor", false)
		return
	}
	page, err := store.GetSweep(r.Context(), s.pool, p.ProjectID, id, after, limit)
	if err != nil {
		s.storeError(w, err)
		return
	}
	response := SweepPage{SweepPage: page}
	if page.HasMore {
		response.NextCursor = encodeSweepCursor(id, page.NextIndex)
	}
	writeJSON(w, 200, response)
}
