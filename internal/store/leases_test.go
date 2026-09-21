package store

import (
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
)

func TestLeaseRenewalValidation(t *testing.T) {
	worker, session := uuid.NewString(), uuid.NewString()
	base := LeaseRenewal{RequestID: uuid.NewString(), SessionID: session, Attempts: []AttemptAuthority{{JobID: uuid.NewString(), AttemptID: uuid.NewString(), WorkerID: worker, SessionID: session, Generation: 1}}}
	for _, tc := range []struct {
		name   string
		change func(*LeaseRenewal)
	}{
		{"empty", func(r *LeaseRenewal) { r.Attempts = nil }},
		{"oversized", func(r *LeaseRenewal) { r.Attempts = make([]AttemptAuthority, 65) }},
		{"request UUID", func(r *LeaseRenewal) { r.RequestID = "invalid" }},
		{"session UUID", func(r *LeaseRenewal) { r.SessionID = uuid.Nil.String() }},
		{"job UUID", func(r *LeaseRenewal) { r.Attempts[0].JobID = "invalid" }},
		{"attempt UUID", func(r *LeaseRenewal) { r.Attempts[0].AttemptID = uuid.Nil.String() }},
		{"worker mismatch", func(r *LeaseRenewal) { r.Attempts[0].WorkerID = uuid.NewString() }},
		{"session mismatch", func(r *LeaseRenewal) { r.Attempts[0].SessionID = uuid.NewString() }},
		{"generation", func(r *LeaseRenewal) { r.Attempts[0].Generation = 0 }},
		{"duplicate", func(r *LeaseRenewal) { r.Attempts = append(r.Attempts, r.Attempts[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			r.Attempts = slices.Clone(base.Attempts)
			tc.change(&r)
			if _, err := r.hash(worker); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
	for len(base.Attempts) < MaxLeaseRenewalBatch {
		a := base.Attempts[0]
		a.AttemptID = uuid.NewString()
		base.Attempts = append(base.Attempts, a)
	}
	if _, err := base.hash(worker); err != nil {
		t.Fatal("maximum valid batch rejected", err)
	}
}
