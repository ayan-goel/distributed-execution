package store

import (
	"fmt"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
)

func TestRetryDelayIsCappedStableAndJittered(t *testing.T) {
	policy := spec.Retry{InitialBackoffSeconds: 5, MaxBackoffSeconds: 60}
	for _, tc := range []struct {
		number    int
		low, high time.Duration
	}{{1, 2500 * time.Millisecond, 5 * time.Second}, {2, 5 * time.Second, 10 * time.Second}, {8, 30 * time.Second, 60 * time.Second}} {
		seen := map[time.Duration]bool{}
		for n := range 100 {
			id := fmt.Sprintf("attempt-%d", n)
			delay := retryDelay(policy, tc.number, id)
			if delay < tc.low || delay > tc.high || delay != retryDelay(policy, tc.number, id) {
				t.Fatal("unstable or out-of-range backoff", delay)
			}
			seen[delay] = true
		}
		if len(seen) < 90 {
			t.Fatal("retry jitter did not spread independent attempts")
		}
	}
}
