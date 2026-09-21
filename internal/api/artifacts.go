package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type ArtifactList struct {
	JobID             string             `json:"jobId"`
	State             string             `json:"state"`
	AcceptedAttemptID *string            `json:"acceptedAttemptId"`
	Artifacts         []ArtifactDownload `json:"artifacts"`
}

type ArtifactDownload struct {
	Name            string               `json:"name"`
	ArtifactID      string               `json:"artifactId"`
	Object          store.ArtifactObject `json:"object"`
	DownloadURL     string               `json:"downloadUrl"`
	Method          string               `json:"method"`
	RequiredHeaders http.Header          `json:"requiredHeaders"`
	ExpiresAt       time.Time            `json:"expiresAt"`
}

func (s *Server) artifacts(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "read permission required", false)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/jobs/"), "/artifacts")
	parsed, err := uuid.Parse(id)
	if err != nil {
		fail(w, 404, "NOT_FOUND", "job not found", false)
		return
	}
	job, err := store.GetJob(r.Context(), s.pool, p.ProjectID, parsed.String())
	if err != nil {
		s.storeError(w, err)
		return
	}
	response := ArtifactList{JobID: job.ID, State: job.State, AcceptedAttemptID: job.AcceptedAttemptID, Artifacts: []ArtifactDownload{}}
	if job.AcceptedAttemptID != nil {
		// INVARIANT: capabilities come only from the accepted immutable manifest.
		// Never sign a caller-provided key or enumerate unaccepted attempt uploads.
		var manifest struct {
			Outputs []ArtifactDownload `json:"outputs"`
		}
		if err := json.Unmarshal(job.AcceptedManifest, &manifest); err != nil || job.State != "SUCCEEDED" || len(manifest.Outputs) > 64 {
			fail(w, 503, "UNAVAILABLE", "accepted result unavailable", true)
			return
		}
		if len(manifest.Outputs) > 0 && s.objects == nil {
			fail(w, 503, "OBJECT_STORAGE_NOT_CONFIGURED", "artifact downloads are not configured", false)
			return
		}
		for _, a := range manifest.Outputs {
			o := a.Object
			grant, err := s.objects.PresignDownload(r.Context(), objectstore.Object{Key: o.Key, Version: o.Version, Size: o.SizeBytes, SHA256: o.SHA256}, time.Minute)
			if err != nil {
				fail(w, 503, "OBJECT_STORAGE_UNAVAILABLE", "artifact download signing unavailable", true)
				return
			}
			a.DownloadURL = grant.URL
			a.Method = grant.Method
			a.RequiredHeaders = grant.Headers
			a.ExpiresAt = grant.ExpiresAt.UTC()
			response.Artifacts = append(response.Artifacts, a)
		}
	}
	// Signing can wait for bounded adapter capacity. Reauthenticate before exposing
	// any capability; already returned URLs remain bearer grants until their expiry.
	current, err := store.Authenticate(r.Context(), s.pool, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if err != nil {
		if errors.Is(err, store.ErrUnauthorized) {
			fail(w, 401, "UNAUTHENTICATED", "invalid credentials", false)
		} else {
			fail(w, 503, "UNAVAILABLE", "authentication unavailable", true)
		}
		return
	}
	if current.ProjectID != p.ProjectID || !current.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "project access denied", false)
		return
	}
	writeJSON(w, 200, response)
}
