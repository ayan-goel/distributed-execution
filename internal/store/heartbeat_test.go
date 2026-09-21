package store

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestHeartbeatBoundsAndCanonicalInventory(t *testing.T) {
	worker := uuid.NewString()
	session := uuid.NewString()
	a := AttemptAuthority{JobID: uuid.NewString(), AttemptID: uuid.NewString(), Generation: 1, WorkerID: worker, SessionID: session}
	h := HeartbeatReport{RequestID: uuid.NewString(), SessionID: session, Sequence: 1, RuntimeHealthy: true, Inventory: []ExecutionRecord{{Authority: a, ContainerID: strings.Repeat("a", 64)}, {Authority: a, ContainerID: strings.Repeat("b", 64)}}}
	first, err := h.hash(worker)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(h.Inventory)
	if second, err := h.hash(worker); err != nil || first != second {
		t.Fatal("inventory enumeration order changed replay identity", err)
	}
	for _, change := range []func(*HeartbeatReport){
		func(h *HeartbeatReport) { h.Sequence = 0 },
		func(h *HeartbeatReport) { h.RequestID = "bad" },
		func(h *HeartbeatReport) { h.Inventory = make([]ExecutionRecord, 1025) },
		func(h *HeartbeatReport) { h.Inventory = append(h.Inventory, h.Inventory[0]) },
		func(h *HeartbeatReport) { h.Inventory[0].Authority.WorkerID = uuid.NewString() },
	} {
		bad := h
		bad.Inventory = slices.Clone(h.Inventory)
		change(&bad)
		if _, err := bad.hash(worker); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid heartbeat accepted", err)
		}
	}
}
