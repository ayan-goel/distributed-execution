package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func TestSweepRetryClientValidatesOrderedFreshLineage(t *testing.T) {
	source := uuid.NewString()
	valid := SweepRetry{ID: uuid.NewString(), ProjectID: uuid.NewString(), ParentSweepID: source,
		SpecHash: strings.Repeat("a", 64), MaxConcurrent: 7, CreatedAt: time.Now().UTC(),
		Children: []SweepRetryChild{{ID: uuid.NewString(), Index: 0, ParentJobID: uuid.NewString()}, {ID: uuid.NewString(), Index: 1, ParentJobID: uuid.NewString()}}}
	for _, tc := range []struct {
		name   string
		change func(*SweepRetry)
	}{
		{"valid", func(*SweepRetry) {}},
		{"replay", func(*SweepRetry) {}},
		{"boundary", func(s *SweepRetry) {
			s.Children = make([]SweepRetryChild, spec.MaxSweepJobs)
			for i := range s.Children {
				s.Children[i] = SweepRetryChild{ID: uuid.NewString(), Index: i, ParentJobID: uuid.NewString()}
			}
		}},
		{"missing index", func(*SweepRetry) {}},
		{"bad id", func(s *SweepRetry) { s.ID = "bad" }},
		{"nil project", func(s *SweepRetry) { s.ProjectID = uuid.Nil.String() }},
		{"wrong source", func(s *SweepRetry) { s.ParentSweepID = uuid.NewString() }},
		{"same sweep", func(s *SweepRetry) { s.ID = source }},
		{"bad hash", func(s *SweepRetry) { s.SpecHash = strings.Repeat("z", 64) }},
		{"missing time", func(s *SweepRetry) { s.CreatedAt = time.Time{} }},
		{"zero cap", func(s *SweepRetry) { s.MaxConcurrent = 0 }},
		{"large cap", func(s *SweepRetry) { s.MaxConcurrent = spec.MaxSweepJobs + 1 }},
		{"empty children", func(s *SweepRetry) { s.Children = nil }},
		{"large children", func(s *SweepRetry) { s.Children = make([]SweepRetryChild, spec.MaxSweepJobs+1) }},
		{"bad child", func(s *SweepRetry) { s.Children[0].ID = "bad" }},
		{"bad parent", func(s *SweepRetry) { s.Children[0].ParentJobID = "bad" }},
		{"duplicate child", func(s *SweepRetry) { s.Children[1].ID = s.Children[0].ID }},
		{"duplicate parent", func(s *SweepRetry) { s.Children[1].ParentJobID = s.Children[0].ParentJobID }},
		{"same job", func(s *SweepRetry) { s.Children[0].ID = s.Children[0].ParentJobID }},
		{"cross alias", func(s *SweepRetry) { s.Children[1].ID = s.Children[0].ParentJobID }},
		{"source alias", func(s *SweepRetry) { s.Children[0].ID = source }},
		{"sweep alias", func(s *SweepRetry) { s.Children[0].ParentJobID = s.ID }},
		{"negative index", func(s *SweepRetry) { s.Children[0].Index = -1 }},
		{"sparse index", func(s *SweepRetry) { s.Children[1].Index = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := valid
			response.Children = append([]SweepRetryChild(nil), valid.Children...)
			tc.change(&response)
			body, _ := json.Marshal(response)
			if tc.name == "missing index" {
				body = bytes.Replace(body, []byte(`"index":0`), []byte(`"unused":0`), 1)
			}
			c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
				requestBody, err := io.ReadAll(r.Body)
				if err != nil || len(requestBody) != 0 || r.Method != "POST" || r.URL.Path != "/v1/sweeps/"+source+"/retry" || r.URL.RawQuery != "" || r.Header.Get("Idempotency-Key") != "same" {
					t.Error("incorrect retry request", r, err)
				}
				status := 201
				if tc.name == "replay" {
					status = 200
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(body)), Header: http.Header{}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			result, err := c.RetrySweep(context.Background(), source, "same")
			wantValid := tc.name == "valid" || tc.name == "replay" || tc.name == "boundary"
			if (err == nil) != wantValid || err == nil && (result.ID != valid.ID || len(result.Children) != len(response.Children)) {
				t.Fatal("retry response validation", result, err)
			}
		})
	}
}

func TestSweepRetryClientRejectsInvalidInputsBeforeNetworkAndPreservesErrors(t *testing.T) {
	requests := 0
	c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"CONFLICT","message":"sweep unfinished","requestId":"request"}}`)), Header: http.Header{}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	source := uuid.NewString()
	for _, tc := range []struct{ id, key string }{
		{"bad", "same"}, {uuid.Nil.String(), "same"}, {strings.ReplaceAll(source, "-", ""), "same"},
		{source, ""}, {source, "two words"}, {source, strings.Repeat("a", 129)}, {source, "\x1b"},
	} {
		if _, err := c.RetrySweep(context.Background(), tc.id, tc.key); err == nil {
			t.Fatal("invalid retry input accepted", tc)
		}
	}
	if requests != 0 {
		t.Fatal("invalid input reached network", requests)
	}
	_, err = c.RetrySweep(context.Background(), source, "same")
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Status != 409 || apiError.Code != "CONFLICT" || requests != 1 {
		t.Fatal("retry lost error or retried implicitly", err, requests)
	}
}
