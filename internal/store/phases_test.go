package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestPhaseReportValidation(t *testing.T) {
	worker := uuid.NewString()
	base := PhaseReport{Authority: AttemptAuthority{JobID: uuid.NewString(), AttemptID: uuid.NewString(), WorkerID: worker, SessionID: uuid.NewString(), Generation: 1}, EventID: uuid.NewString(), Phase: "STARTING"}
	code := int32(0)
	for _, phase := range []string{"STARTING", "RUNNING", "FINALIZING"} {
		r := base
		r.Phase = phase
		if phase != "STARTING" {
			r.ContainerID = strings.Repeat("a", 64)
		}
		if phase == "FINALIZING" {
			r.ExitCode = &code
		}
		if _, err := r.hash(worker); err != nil {
			t.Fatal(phase, err)
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*PhaseReport)
	}{
		{"event", func(r *PhaseReport) { r.EventID = "bad" }},
		{"attempt", func(r *PhaseReport) { r.Authority.AttemptID = uuid.Nil.String() }},
		{"job", func(r *PhaseReport) { r.Authority.JobID = "bad" }},
		{"session", func(r *PhaseReport) { r.Authority.SessionID = "bad" }},
		{"worker", func(r *PhaseReport) { r.Authority.WorkerID = uuid.NewString() }},
		{"generation", func(r *PhaseReport) { r.Authority.Generation = 0 }},
		{"terminal phase", func(r *PhaseReport) { r.Phase = "SUCCEEDED" }},
		{"starting container", func(r *PhaseReport) { r.ContainerID = strings.Repeat("a", 64) }},
		{"starting exit", func(r *PhaseReport) { r.ExitCode = &code }},
		{"missing running container", func(r *PhaseReport) { r.Phase = "RUNNING" }},
		{"short container", func(r *PhaseReport) { r.Phase = "RUNNING"; r.ContainerID = "abcd" }},
		{"running exit", func(r *PhaseReport) { r.Phase = "RUNNING"; r.ContainerID = strings.Repeat("a", 64); r.ExitCode = &code }},
		{"missing final exit", func(r *PhaseReport) { r.Phase = "FINALIZING"; r.ContainerID = strings.Repeat("a", 64) }},
		{"invalid final exit", func(r *PhaseReport) {
			r.Phase = "FINALIZING"
			r.ContainerID = strings.Repeat("a", 64)
			bad := int32(256)
			r.ExitCode = &bad
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.change(&r)
			if _, err := r.hash(worker); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
}
