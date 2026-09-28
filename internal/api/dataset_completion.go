package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type datasetVerifier interface {
	Verify(context.Context, objectstore.Object) error
}

type datasetCompletionRequest struct {
	Version  string                `json:"version"`
	Manifest store.DatasetManifest `json:"manifest"`
}

type DatasetRegistration struct {
	DatasetID     string                `json:"datasetId"`
	Name          string                `json:"name"`
	UploadID      string                `json:"uploadId"`
	ObjectKey     string                `json:"objectKey"`
	ObjectVersion string                `json:"objectVersion"`
	SizeBytes     int64                 `json:"sizeBytes"`
	SHA256        string                `json:"sha256"`
	Manifest      store.DatasetManifest `json:"manifest"`
	Replayed      bool                  `json:"replayed"`
}

func (s *Server) datasetComplete(w http.ResponseWriter, r *http.Request, p store.Principal) {
	verifier, ok := s.objects.(datasetVerifier)
	if !ok {
		fail(w, 503, "OBJECT_STORAGE_NOT_CONFIGURED", "dataset verification is not configured", false)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/datasets/uploads/"), "/complete")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		fail(w, 404, "NOT_FOUND", "dataset upload not found", false)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20+4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input datasetCompletionRequest
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			fail(w, 413, "TOO_LARGE", "dataset manifest exceeds 2 MiB", false)
		} else {
			fail(w, 422, "INVALID_ARGUMENT", "invalid dataset completion request", false)
		}
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		fail(w, 422, "INVALID_ARGUMENT", "expected one dataset completion request", false)
		return
	}
	// INVARIANT: verify the exact version and digest from the server-owned
	// declaration. Never register the key's mutable latest version by name.
	verify := func(ctx context.Context, object objectstore.Object) error {
		if err := verifier.Verify(ctx, object); err != nil {
			return err
		}
		// Storage verification may be slow; a token revoked during the read must
		// not authorize this registration after verification finishes.
		current, err := store.Authenticate(ctx, s.pool, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if err != nil {
			return err
		}
		if current.ProjectID != p.ProjectID || !current.Allows(store.RoleSubmit) {
			return store.ErrUnauthorized
		}
		return nil
	}
	result, err := store.RegisterDataset(r.Context(), s.pool, store.DatasetRegistrationRequest{
		ProjectID: p.ProjectID, UploadID: parsed.String(), Version: input.Version, Manifest: input.Manifest,
	}, verify)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrUnauthorized):
			fail(w, 401, "UNAUTHENTICATED", "invalid credentials", false)
		case errors.Is(err, objectstore.ErrIntegrity):
			fail(w, 422, "INTEGRITY_ERROR", "dataset object does not match declaration", false)
		case errors.Is(err, objectstore.ErrUnavailable), errors.Is(err, context.DeadlineExceeded):
			fail(w, 503, "OBJECT_STORAGE_UNAVAILABLE", "dataset verification unavailable", true)
		case errors.Is(err, store.ErrConflict):
			fail(w, 409, "CONFLICT", "dataset name or version already registered", false)
		default:
			s.storeError(w, err)
		}
		return
	}
	status := 201
	if result.Replayed {
		status = 200
	}
	writeJSON(w, status, DatasetRegistration{DatasetID: result.ID, Name: result.Name, UploadID: result.UploadID,
		ObjectKey: result.Object.Key, ObjectVersion: result.Object.Version, SizeBytes: result.Object.Size,
		SHA256: result.Object.SHA256, Manifest: result.Manifest, Replayed: result.Replayed})
}
