package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func TestSweepClientRejectsChangedIdentityAndIncompleteChildren(t *testing.T) {
	example, err := os.ReadFile("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(bytes.NewReader(example))
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Image = "registry.example.org/eval@sha256:" + strings.Repeat("a", 64)
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: spec.Metadata{Name: "grid", Project: "research"},
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{"SEED": {"1", "2"}}, MaxConcurrent: 1}}
	body, hash, err := sweep.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	valid := Sweep{ID: uuid.NewString(), ProjectID: uuid.NewString(), Spec: body, SpecHash: hash,
		ChildIDs: []string{uuid.NewString(), uuid.NewString()}, MaxConcurrent: 1, CreatedAt: time.Now().UTC()}
	for _, tc := range []struct {
		name   string
		change func(*Sweep)
	}{
		{"valid", func(*Sweep) {}},
		{"bad id", func(s *Sweep) { s.ID = "bad" }},
		{"missing child", func(s *Sweep) { s.ChildIDs = s.ChildIDs[:1] }},
		{"duplicate child", func(s *Sweep) { s.ChildIDs[1] = s.ChildIDs[0] }},
		{"changed concurrency", func(s *Sweep) { s.MaxConcurrent++ }},
		{"changed hash", func(s *Sweep) { s.SpecHash = strings.Repeat("b", 64) }},
		{"changed matrix", func(s *Sweep) {
			s.Spec = bytes.ReplaceAll(s.Spec, []byte(`"SEED":["1","2"]`), []byte(`"SEED":["2","1"]`))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := valid
			response.ChildIDs = append([]string(nil), valid.ChildIDs...)
			tc.change(&response)
			encoded, _ := json.Marshal(response)
			c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/v1/sweeps" || r.Header.Get("Idempotency-Key") != "same" {
					t.Fatal("wrong sweep request")
				}
				return &http.Response{StatusCode: 201, Body: io.NopCloser(bytes.NewReader(encoded)), Header: http.Header{}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.SubmitSweep(context.Background(), body, "same")
			if (err == nil) != (tc.name == "valid") {
				t.Fatal("sweep response validation", err)
			}
		})
	}
}
