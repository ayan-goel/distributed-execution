package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
)

type uploadSigner interface {
	PresignUpload(context.Context, string, int64, string, time.Duration) (objectstore.Grant, error)
}

type datasetUploadRequest struct {
	RequestID string `json:"requestId"`
	Name      string `json:"name"`
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
}

type DatasetUploadSession struct {
	UploadID        string      `json:"uploadId"`
	ObjectKey       string      `json:"objectKey"`
	UploadURL       string      `json:"uploadUrl"`
	Method          string      `json:"method"`
	RequiredHeaders http.Header `json:"requiredHeaders"`
	ExpiresAt       time.Time   `json:"expiresAt"`
	Replayed        bool        `json:"replayed"`
}

func (s *Server) datasetUpload(w http.ResponseWriter, r *http.Request, p store.Principal) {
	signer, ok := s.objects.(uploadSigner)
	if !ok {
		fail(w, 503, "OBJECT_STORAGE_NOT_CONFIGURED", "dataset uploads are not configured", false)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input datasetUploadRequest
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			fail(w, 413, "TOO_LARGE", "dataset upload declaration exceeds 4 KiB", false)
		} else {
			fail(w, 422, "INVALID_ARGUMENT", "invalid dataset upload declaration", false)
		}
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		fail(w, 422, "INVALID_ARGUMENT", "expected one dataset upload declaration", false)
		return
	}
	// INVARIANT: only the project-scoped, server-allocated key may be signed.
	// The declaration binds size and digest so a changed retry cannot reuse it.
	record, err := store.CreateDatasetUpload(r.Context(), s.pool, store.DatasetUploadRequest{
		ProjectID: p.ProjectID, RequestID: input.RequestID, Name: input.Name,
		SizeBytes: input.SizeBytes, SHA256: input.SHA256,
	})
	if err != nil {
		s.storeError(w, err)
		return
	}
	grant, err := signer.PresignUpload(r.Context(), record.ObjectKey, record.SizeBytes, record.SHA256, time.Minute)
	if err != nil {
		fail(w, 503, "OBJECT_STORAGE_UNAVAILABLE", "dataset upload signing unavailable", true)
		return
	}
	// Signing may wait on storage. Recheck revocation before returning the
	// short-lived bearer grant; the durable declaration remains replayable.
	current, err := store.Authenticate(r.Context(), s.pool, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if err != nil {
		if errors.Is(err, store.ErrUnauthorized) {
			fail(w, 401, "UNAUTHENTICATED", "invalid credentials", false)
		} else {
			fail(w, 503, "UNAVAILABLE", "authentication unavailable", true)
		}
		return
	}
	if current.ProjectID != p.ProjectID || !current.Allows(store.RoleSubmit) {
		fail(w, 403, "FORBIDDEN", "project access denied", false)
		return
	}
	status := 201
	if record.Replayed {
		status = 200
	}
	writeJSON(w, status, DatasetUploadSession{
		UploadID: record.ID, ObjectKey: record.ObjectKey, UploadURL: grant.URL,
		Method: grant.Method, RequiredHeaders: grant.Headers, ExpiresAt: grant.ExpiresAt.UTC(),
		Replayed: record.Replayed,
	})
}
