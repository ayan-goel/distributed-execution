package api

import (
	"errors"
	"net/http"
	"strings"

	"dispatch.local/dispatch/internal/admission"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
)

func (s *Server) submitSweep(w http.ResponseWriter, r *http.Request, p store.Principal) {
	key := r.Header.Get("Idempotency-Key")
	if len(key) < 1 || len(key) > 128 || strings.TrimSpace(key) != key {
		fail(w, 400, "INVALID_ARGUMENT", "Idempotency-Key must contain 1–128 characters", false)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, spec.MaxDocumentBytes)
	sweep, err := spec.DecodeSweep(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			fail(w, 413, "TOO_LARGE", "specification exceeds 1 MiB", false)
		} else {
			fail(w, 422, "INVALID_ARGUMENT", err.Error(), false)
		}
		return
	}
	if sweep.Metadata.Project != p.Project {
		fail(w, 403, "FORBIDDEN", "project access denied", false)
		return
	}
	_, requestHash, err := sweep.Canonical()
	if err != nil {
		fail(w, 422, "INVALID_ARGUMENT", "invalid sweep", false)
		return
	}
	// Replay precedes external resolution so a lost response cannot change the
	// frozen template image or dataset identities of an already committed sweep.
	existing, err := store.LookupSweepSubmission(r.Context(), s.pool, p.Project, key, requestHash)
	if err == nil {
		w.Header().Set("Location", "/v1/sweeps/"+existing.ID)
		writeJSON(w, 200, existing)
		return
	}
	if !errors.Is(err, store.ErrNotFound) {
		s.storeError(w, err)
		return
	}
	template := &sweep.Spec.JobTemplate
	pinned, err := s.images.Resolve(r.Context(), template.Spec.Image)
	if err != nil {
		if errors.Is(err, admission.ErrRegistryDenied) {
			fail(w, 422, "REGISTRY_DENIED", "registry is not approved", false)
		} else {
			fail(w, 503, "UNAVAILABLE", "image resolution unavailable", true)
		}
		return
	}
	template.Spec.Image = pinned
	var bindings []store.DatasetBinding
	if len(template.Spec.Inputs) > 0 {
		names := make([]string, len(template.Spec.Inputs))
		for i, input := range template.Spec.Inputs {
			names[i] = input.Dataset
		}
		// Resolve through the authenticated project ID. A dataset name in the
		// document must never grant access to another project's registration.
		bindings, err = store.ResolveDatasetNames(r.Context(), s.pool, p.ProjectID, names)
		if err != nil {
			s.storeError(w, err)
			return
		}
	}
	result, err := store.SubmitSweepResolved(r.Context(), s.pool, key, requestHash, sweep, bindings)
	if err != nil {
		s.storeError(w, err)
		return
	}
	status := 201
	if result.Replayed {
		status = 200
	}
	w.Header().Set("Location", "/v1/sweeps/"+result.ID)
	writeJSON(w, status, result)
}
