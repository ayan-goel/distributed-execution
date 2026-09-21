package workerapi

import (
	"context"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Service) ListAssignments(ctx context.Context, request *pb.ListAssignmentsRequest) (*pb.ListAssignmentsResponse, error) {
	identity, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(request, identity.WorkerID); err != nil {
		return nil, err
	}
	if request.GetPageSize() > store.MaxAssignmentPageSize {
		return nil, rpcError(store.ErrInvalid)
	}
	page, err := store.ListAssignments(ctx, s.pool, identity, request.GetSession().GetSessionId(), request.GetAfterJobId(), int(request.GetPageSize()))
	if err != nil {
		return nil, rpcError(err)
	}
	return assignmentPageResponse(page)
}

func assignmentPageResponse(page store.AssignmentPage) (*pb.ListAssignmentsResponse, error) {
	response := &pb.ListAssignmentsResponse{NextAfterJobId: page.NextAfterJobID}
	for n := range page.Assignments {
		result, err := acquisitionResponse(store.AcquisitionResult{Assignment: &page.Assignments[n]})
		if err != nil {
			return nil, err
		}
		if assignment := result.GetAssignment(); assignment != nil {
			response.Assignments = append(response.Assignments, assignment)
		}
	}
	// Store pages budget canonical specs conservatively; enforce exact protobuf
	// size as well so repeated argv fields cannot exceed the transport boundary.
	if proto.Size(response) > maxWorkerMessageBytes {
		return nil, status.Error(codes.ResourceExhausted, "ASSIGNMENT_PAGE_TOO_LARGE")
	}
	return response, nil
}
