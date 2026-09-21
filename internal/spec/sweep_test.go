package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func sweepExample(t *testing.T) Sweep {
	t.Helper()
	j, err := DecodeJob(bytes.NewReader(example(t)))
	if err != nil {
		t.Fatal(err)
	}
	return Sweep{APIVersion: APIVersion, Kind: "Sweep", Metadata: Metadata{Name: "grid", Project: "research"}, Spec: SweepSpec{
		JobTemplate: j, Matrix: map[string][]string{"METHOD": {"random", "contact", "knn"}, "RATE": {"0.0001", "0.0003", "0.001"}, "SEED": {"1", "2", "3"}}, MaxConcurrent: 6,
	}}
}

func TestSweepStableExpansionAndIsolation(t *testing.T) {
	s := sweepExample(t)
	jobs, err := s.Expand()
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 27 {
		t.Fatalf("got %d jobs", len(jobs))
	}
	for i, j := range jobs {
		want := map[string]string{"METHOD": s.Spec.Matrix["METHOD"][i/9], "RATE": s.Spec.Matrix["RATE"][(i/3)%3], "SEED": s.Spec.Matrix["SEED"][i%3]}
		for k, v := range want {
			if j.Spec.Env[k] != v {
				t.Fatalf("child %d %s=%s want %s", i, k, j.Spec.Env[k], v)
			}
		}
		if err := j.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := json.Marshal(s)
	decoded, err := DecodeSweep(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	other, err := decoded.Expand()
	if err != nil || !reflect.DeepEqual(jobs, other) {
		t.Fatal("unstable child order", err)
	}
	jobs[0].Spec.Env["SEED"] = "changed"
	jobs[0].Spec.Command[0] = "changed"
	jobs[0].Metadata.Labels["experiment"] = "changed"
	if jobs[1].Spec.Command[0] == "changed" || s.Spec.JobTemplate.Spec.Env["SEED"] != "1" || jobs[1].Metadata.Labels["experiment"] == "changed" {
		t.Fatal("children alias template or each other")
	}
}

func TestSweepBoundsAndPolicy(t *testing.T) {
	for _, mutate := range []func(*Sweep){
		func(s *Sweep) { s.Spec.Matrix = nil },
		func(s *Sweep) { s.Spec.Matrix["SEED"] = nil },
		func(s *Sweep) { s.Spec.Matrix["DISPATCH_JOB_ID"] = []string{"spoof"} },
		func(s *Sweep) { s.Spec.MaxConcurrent = 0 },
		func(s *Sweep) { s.Spec.MaxConcurrent = 1001 },
		func(s *Sweep) { s.Spec.CancelRunningOnFailure = true },
		func(s *Sweep) { s.Metadata.Project = "another-project" },
		func(s *Sweep) { s.Spec.Matrix["SEED"] = make([]string, 1001) },
	} {
		s := sweepExample(t)
		mutate(&s)
		if _, err := s.Expand(); err == nil {
			t.Fatal("invalid sweep accepted")
		}
	}
	s := sweepExample(t)
	s.Spec.Matrix = map[string][]string{"SEED": make([]string, 1000)}
	for i := range s.Spec.Matrix["SEED"] {
		s.Spec.Matrix["SEED"][i] = fmt.Sprint(i)
	}
	if jobs, err := s.Expand(); err != nil || len(jobs) != 1000 {
		t.Fatal("boundary rejected", err)
	}
	// The server must never dereference paths supplied by a remote client.
	if _, err := DecodeSweep(bytes.NewBufferString(`{"spec":{"jobTemplateFile":"/etc/passwd"}}`)); err == nil {
		t.Fatal("server accepted a client path")
	}
}
