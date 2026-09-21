package workerapi

import (
	"context"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRenewalRequiresTransportIdentity(t *testing.T) {
	if _, err := NewService(nil, store.AcquisitionPolicy{}, nil).RenewLeases(context.Background(), &pb.RenewLeasesRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("renewal bypassed transport identity", err)
	}
}

func TestLeaseWireBoundsAndRejections(t *testing.T) {
	now := time.Unix(100, 0)
	base := store.LeaseGrant{Authority: store.AttemptAuthority{JobID: "job", AttemptID: "attempt", WorkerID: "worker", SessionID: "session", Generation: 2}, Decision: "ACCEPTED", ServerTime: now, LeaseExpiresAt: now.Add(12500*time.Millisecond + 900*time.Microsecond), PhaseDeadline: now.Add(42500*time.Millisecond + 900*time.Microsecond)}
	response, err := leaseResponse([]store.LeaseGrant{base})
	if err != nil {
		t.Fatal(err)
	}
	g := response.Results[0]
	if g.Decision != pb.Decision_ACCEPTED || g.RemainingMs != 12500 || g.PhaseRemainingMs != 42500 || g.ServerTimeUnixMs != now.UnixMilli() || g.Authority.Generation != 2 || g.Authority.AttemptId != "attempt" {
		t.Fatal(response)
	}
	for _, tc := range []struct {
		name     string
		change   func(*store.LeaseGrant)
		decision pb.Decision
	}{
		{"expired lease", func(g *store.LeaseGrant) { g.LeaseExpiresAt = now.Add(-time.Second) }, pb.Decision_FENCED},
		{"fractional lease", func(g *store.LeaseGrant) { g.LeaseExpiresAt = now.Add(time.Microsecond) }, pb.Decision_FENCED},
		{"fractional phase", func(g *store.LeaseGrant) { g.PhaseDeadline = now.Add(time.Microsecond) }, pb.Decision_STOP_REQUESTED},
		{"fenced", func(g *store.LeaseGrant) { g.Decision = "FENCED" }, pb.Decision_FENCED},
		{"cancelled", func(g *store.LeaseGrant) { g.Decision = "STOP_REQUESTED" }, pb.Decision_STOP_REQUESTED},
		{"terminal", func(g *store.LeaseGrant) { g.Decision = "ALREADY_TERMINAL" }, pb.Decision_ALREADY_TERMINAL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := base
			tc.change(&g)
			r, e := leaseResponse([]store.LeaseGrant{g})
			if e != nil || r.Results[0].Decision != tc.decision || r.Results[0].RemainingMs != 0 || r.Results[0].PhaseRemainingMs != 0 {
				t.Fatal(r, e)
			}
		})
	}
	for _, change := range []func(*store.LeaseGrant){
		func(g *store.LeaseGrant) { g.Decision = "NEW_UNKNOWN_DECISION" },
		func(g *store.LeaseGrant) { g.Decision = "REQUEST_CONFLICT" },
		func(g *store.LeaseGrant) { g.Authority.Generation = -1 },
		func(g *store.LeaseGrant) { g.LeaseExpiresAt = now.Add(31 * time.Second) },
	} {
		g := base
		change(&g)
		if _, err := leaseResponse([]store.LeaseGrant{g}); status.Code(err) != codes.Internal {
			t.Fatal("invalid store result became authority", g, err)
		}
	}
}
