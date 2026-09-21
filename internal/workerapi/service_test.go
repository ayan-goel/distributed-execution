package workerapi

import (
	"context"
	"testing"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerServiceRequiresTransportIdentity(t *testing.T) {
	s := NewService(nil, store.AcquisitionPolicy{}, nil)
	if _, err := s.RegisterWorker(context.Background(), &pb.RegisterWorkerRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("registration bypassed identity interceptor", err)
	}
	if _, err := s.Heartbeat(context.Background(), &pb.HeartbeatRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("heartbeat bypassed identity interceptor", err)
	}
}
