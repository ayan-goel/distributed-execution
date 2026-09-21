package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"dispatch.local/dispatch/internal/admission"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ImageResolver interface {
	Resolve(context.Context, string) (string, error)
}
type DownloadSigner interface {
	PresignDownload(context.Context, objectstore.Object, time.Duration) (objectstore.Grant, error)
}
type Server struct {
	pool     *pgxpool.Pool
	images   ImageResolver
	objects  DownloadSigner
	slots    chan struct{}
	rateMu   sync.Mutex
	window   time.Time
	requests int
}

func New(pool *pgxpool.Pool, images ImageResolver, objects DownloadSigner) *Server {
	return &Server{pool: pool, images: images, objects: objects, slots: make(chan struct{}, 256)}
}

type APIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	RequestID string `json:"requestId"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Request-ID", rand.Text())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	if r.TLS != nil {
		w.Header().Set("Strict-Transport-Security", "max-age=31536000")
	}
	// A global fixed-window cap and bounded in-flight set protect the single
	// control plane without retaining attacker-controlled per-client state.
	s.rateMu.Lock()
	if time.Since(s.window) >= time.Second {
		s.window = time.Now()
		s.requests = 0
	}
	s.requests++
	allowed := s.requests <= 200
	s.rateMu.Unlock()
	if !allowed {
		w.Header().Set("Retry-After", "1")
		fail(w, 429, "RATE_LIMITED", "request rate exceeded", true)
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		fail(w, 503, "UNAVAILABLE", "server busy", true)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		if err := s.pool.Ping(ctx); err != nil {
			fail(w, 503, "UNAVAILABLE", "database unavailable", true)
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		fail(w, 401, "UNAUTHENTICATED", "bearer token required", false)
		return
	}
	p, err := store.Authenticate(ctx, s.pool, strings.TrimPrefix(auth, "Bearer "))
	if err != nil {
		if errors.Is(err, store.ErrUnauthorized) {
			fail(w, 401, "UNAUTHENTICATED", "invalid credentials", false)
		} else {
			fail(w, 503, "UNAVAILABLE", "authentication unavailable", true)
		}
		return
	}
	switch {
	case r.URL.Path == "/v1/jobs" && r.Method == http.MethodPost:
		if !p.Allows(store.RoleSubmit) {
			fail(w, 403, "FORBIDDEN", "submit permission required", false)
			return
		}
		s.submit(w, r, p)
	case strings.HasPrefix(r.URL.Path, "/v1/jobs/") && strings.HasSuffix(r.URL.Path, "/artifacts") && r.Method == http.MethodGet:
		s.artifacts(w, r, p)
	case strings.HasPrefix(r.URL.Path, "/v1/jobs/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
		if _, err := uuid.Parse(id); err != nil {
			fail(w, 404, "NOT_FOUND", "job not found", false)
			return
		}
		j, err := store.GetJob(ctx, s.pool, p.ProjectID, id)
		if err != nil {
			s.storeError(w, err)
			return
		}
		writeJSON(w, 200, j)
	default:
		fail(w, 404, "NOT_FOUND", "endpoint not found", false)
	}
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request, p store.Principal) {
	key := r.Header.Get("Idempotency-Key")
	if len(key) < 1 || len(key) > 128 || strings.TrimSpace(key) != key {
		fail(w, 400, "INVALID_ARGUMENT", "Idempotency-Key must contain 1–128 characters", false)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, spec.MaxDocumentBytes)
	j, err := spec.DecodeJob(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			fail(w, 413, "TOO_LARGE", "specification exceeds 1 MiB", false)
		} else {
			fail(w, 422, "INVALID_ARGUMENT", err.Error(), false)
		}
		return
	}
	if j.Metadata.Project != p.Project {
		fail(w, 403, "FORBIDDEN", "project access denied", false)
		return
	}
	_, requestHash, err := j.Canonical()
	if err != nil {
		fail(w, 422, "INVALID_ARGUMENT", "invalid job", false)
		return
	}
	existing, err := store.LookupSubmission(r.Context(), s.pool, p.Project, key, requestHash)
	if err == nil {
		writeJSON(w, 200, existing)
		return
	}
	if !errors.Is(err, store.ErrNotFound) {
		s.storeError(w, err)
		return
	}
	// Recover duplicate requests before registry access: losing an HTTP response
	// must not make an already committed submission depend on a healthy registry.
	if len(j.Spec.Inputs) > 0 {
		fail(w, 501, "UNIMPLEMENTED", "dataset admission is not available yet", false)
		return
	}
	pinned, err := s.images.Resolve(r.Context(), j.Spec.Image)
	if err != nil {
		if errors.Is(err, admission.ErrRegistryDenied) {
			fail(w, 422, "REGISTRY_DENIED", "registry is not approved", false)
		} else {
			fail(w, 503, "UNAVAILABLE", "image resolution unavailable", true)
		}
		return
	}
	j.Spec.Image = pinned
	result, err := store.SubmitJob(r.Context(), s.pool, key, requestHash, j)
	if err != nil {
		s.storeError(w, err)
		return
	}
	status := 201
	if result.Replayed {
		status = 200
	}
	w.Header().Set("Location", "/v1/jobs/"+result.ID)
	writeJSON(w, status, result)
}

func (s *Server) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrConflict):
		fail(w, 409, "CONFLICT", "idempotency key has a different request", false)
	case errors.Is(err, store.ErrNotFound):
		fail(w, 404, "NOT_FOUND", "resource not found", false)
	case errors.Is(err, store.ErrQuota):
		fail(w, 422, "QUOTA_EXCEEDED", "job exceeds project resource limits", false)
	case errors.Is(err, store.ErrDisabled):
		fail(w, 403, "FORBIDDEN", "project disabled", false)
	case errors.Is(err, store.ErrInvalid):
		fail(w, 422, "INVALID_ARGUMENT", "invalid resolved specification", false)
	default:
		fail(w, 503, "UNAVAILABLE", "database operation unavailable", true)
	}
}
func fail(w http.ResponseWriter, status int, code, message string, retryable bool) {
	writeJSON(w, status, struct {
		Error APIError `json:"error"`
	}{APIError{Code: code, Message: message, Retryable: retryable, RequestID: w.Header().Get("X-Request-ID")}})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
