package store

import "testing"

func TestSweepStateWaitsForAllChildrenToFinish(t *testing.T) {
	for _, tc := range []struct {
		progress SweepProgress
		state    string
	}{
		{SweepProgress{Total: 2, Queued: 2}, "QUEUED"},
		{SweepProgress{Total: 2, Active: 1, Queued: 1}, "ACTIVE"},
		{SweepProgress{Total: 2, Failed: 1, Queued: 1}, "ACTIVE"},
		{SweepProgress{Total: 2, RetryWait: 2}, "ACTIVE"},
		{SweepProgress{Total: 2, Succeeded: 2}, "SUCCEEDED"},
		{SweepProgress{Total: 2, Succeeded: 1, Failed: 1}, "FAILED"},
		{SweepProgress{Total: 2, Failed: 1, Cancelled: 1}, "FAILED"},
		{SweepProgress{Total: 2, Succeeded: 1, Cancelled: 1}, "CANCELLED"},
	} {
		if state := sweepState(tc.progress); state != tc.state {
			t.Fatal(tc.progress, state, tc.state)
		}
	}
}
