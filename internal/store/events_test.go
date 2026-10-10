package store

import (
	"context"
	"errors"
	"testing"
)

func TestJobEventsRejectInvalidInputBeforeDatabaseAccess(t *testing.T) {
	const id = "00000000-0000-0000-0000-000000000001"
	for _, tc := range []struct {
		project, job string
		after        int64
		limit        int
	}{{"invalid", id, 0, 1}, {id, "invalid", 0, 1}, {id, id, -1, 1}, {id, id, 0, 0}, {id, id, 0, 101}} {
		if _, err := ListJobEvents(context.Background(), nil, tc.project, tc.job, tc.after, tc.limit); !errors.Is(err, ErrInvalid) {
			t.Fatal(tc, err)
		}
	}
}
