package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

const MaxResponseBytes = 4 << 20

type Job struct {
	ID                string          `json:"id"`
	ProjectID         string          `json:"projectId"`
	State             string          `json:"state"`
	Spec              json.RawMessage `json:"spec"`
	SpecHash          string          `json:"specHash"`
	CreatedAt         time.Time       `json:"createdAt"`
	AcceptedAttemptID *string         `json:"acceptedAttemptId"`
	AcceptedManifest  json.RawMessage `json:"acceptedManifest"`
}

type APIError struct {
	Status    int    `json:"-"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	RequestID string `json:"requestId"`
}

func (e *APIError) Error() string {
	// Server text is untrusted terminal input, including validation errors that
	// may echo submitted values. Quoting prevents terminal control sequences.
	return fmt.Sprintf("HTTP %d: %q: %q (request %q)", e.Status, e.Code, e.Message, e.RequestID)
}

type Client struct {
	endpoint    string
	token       string
	http        *http.Client
	devInsecure bool
}

func New(endpoint, token string, devInsecure bool, transport http.RoundTripper) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("DISPATCH_URL must be a server origin without credentials, path, query, or fragment")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && devInsecure && ip != nil && ip.IsLoopback()) {
		return nil, errors.New("HTTPS required; development HTTP requires an explicit literal loopback address")
	}
	if !visibleASCII(token, 4096) {
		return nil, errors.New("DISPATCH_TOKEN must contain a nonempty bearer token")
	}
	return &Client{endpoint: strings.TrimSuffix(u.String(), "/"), token: token, devInsecure: devInsecure, http: &http.Client{
		Timeout: 20 * time.Second, Transport: transport,
		// Never forward credentials or a submission body to a redirect target,
		// even when net/http considers it within the same trust domain.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (c *Client) Submit(ctx context.Context, body []byte, key string) (Job, error) {
	if len(body) == 0 || len(body) > spec.MaxDocumentBytes || !visibleASCII(key, 128) {
		return Job{}, errors.New("submission requires a bounded specification and a 1–128 character visible ASCII idempotency key")
	}
	return c.request(ctx, http.MethodPost, "/v1/jobs", body, key)
}

func (c *Client) GetJob(ctx context.Context, id string) (Job, error) {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Job{}, errors.New("job ID must be a UUID")
	}
	j, err := c.request(ctx, http.MethodGet, "/v1/jobs/"+parsed.String(), nil, "")
	if err == nil && j.ID != parsed.String() {
		return Job{}, errors.New("server returned a different job ID")
	}
	return j, err
}

func (c *Client) request(ctx context.Context, method, path string, body []byte, key string) (Job, error) {
	b, err := c.requestBody(ctx, method, path, body, key)
	if err != nil {
		return Job{}, err
	}
	var j Job
	if json.Unmarshal(b, &j) != nil || j.State == "" {
		return Job{}, errors.New("invalid job response")
	}
	if _, err := uuid.Parse(j.ID); err != nil {
		return Job{}, errors.New("invalid job ID in response")
	}
	return j, nil
}

func (c *Client) requestBody(ctx context.Context, method, path string, body []byte, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("could not construct request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if key != "" {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Transport errors may contain proxy URLs or credentials. Submission
		// callers retain the key because a transport failure is an uncertain commit.
		return nil, errors.New("request failed; check connectivity and TLS configuration")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil || len(b) > MaxResponseBytes {
		return nil, errors.New("server response unreadable or exceeds 4 MiB")
	}
	if resp.StatusCode != http.StatusOK && !(method == http.MethodPost && resp.StatusCode == http.StatusCreated) {
		var envelope struct {
			Error APIError `json:"error"`
		}
		if json.Unmarshal(b, &envelope) != nil || envelope.Error.Code == "" {
			return nil, fmt.Errorf("HTTP %d: invalid API error response", resp.StatusCode)
		}
		envelope.Error.Status = resp.StatusCode
		return nil, &envelope.Error
	}
	return b, nil
}

func visibleASCII(value string, max int) bool {
	if len(value) == 0 || len(value) > max {
		return false
	}
	for _, b := range []byte(value) {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}
