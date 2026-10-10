//go:build integration

package main

import (
	"context"
	"sync"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"google.golang.org/grpc/status"
)

type stalledOutputGrantService struct {
	pb.WorkerServiceServer
	entered chan struct{}
	once    sync.Once
}

func (s *stalledOutputGrantService) CreateUpload(ctx context.Context, r *pb.CreateUploadRequest) (*pb.CreateUploadResponse, error) {
	if r.GetName() != "result" || r.GetKind() != pb.ArtifactKind_OUTPUT {
		return s.WorkerServiceServer.CreateUpload(ctx, r)
	}
	// The real worker has exited and prepared its required output. Withhold its
	// grant until cancellation/deadline so finalization cannot borrow more time.
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}
