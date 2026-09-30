package workerapi

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Service) AcquireWork(ctx context.Context, request *pb.AcquireWorkRequest) (*pb.AcquireWorkResponse, error) {
	identity, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(request, identity.WorkerID); err != nil {
		return nil, err
	}
	result, err := store.AcquireWork(ctx, s.pool, identity, store.AcquisitionRequest{SessionID: request.GetSession().GetSessionId(), RequestID: request.GetRequestId()}, s.policy)
	if err != nil {
		return nil, rpcError(err)
	}
	response, err := acquisitionResponse(result)
	if err != nil {
		return nil, err
	}
	if assignment := response.GetAssignment(); assignment != nil {
		if err := s.signInputGrants(ctx, assignment); err != nil {
			return nil, err
		}
		if proto.Size(response) > maxWorkerMessageBytes {
			return nil, status.Error(codes.ResourceExhausted, "ASSIGNMENT_TOO_LARGE")
		}
	}
	return response, nil
}

func acquisitionResponse(result store.AcquisitionResult) (*pb.AcquireWorkResponse, error) {
	outcomes := 0
	if result.Assignment != nil {
		outcomes++
	}
	if result.NoWorkReason != "" {
		outcomes++
	}
	if result.Decision != "" {
		outcomes++
	}
	// An unknown/ambiguous store result must never become a zero-valued grant.
	// Exactly one typed outcome is required at this authority boundary.
	invalid := status.Error(codes.Internal, "INVALID_ACQUISITION_RESULT")
	if outcomes != 1 {
		return nil, invalid
	}
	if result.NoWorkReason != "" {
		value, ok := pb.NoWorkReason_value[result.NoWorkReason]
		if !ok || value == 0 {
			return nil, invalid
		}
		return &pb.AcquireWorkResponse{Outcome: &pb.AcquireWorkResponse_NoWork{NoWork: pb.NoWorkReason(value)}}, nil
	}
	if result.Decision != "" {
		value, ok := pb.Decision_value[result.Decision]
		decision := pb.Decision(value)
		if !ok || !slices.Contains([]pb.Decision{pb.Decision_FENCED, pb.Decision_STOP_REQUESTED, pb.Decision_ALREADY_TERMINAL}, decision) {
			return nil, invalid
		}
		return &pb.AcquireWorkResponse{Outcome: &pb.AcquireWorkResponse_Rejected{Rejected: decision}}, nil
	}
	a := result.Assignment
	// Floor to milliseconds, never round an expired/submillisecond deadline up.
	// The worker must also subtract RPC elapsed time and its local safety margin.
	lease := a.LeaseExpiresAt.Sub(a.ServerTime).Milliseconds()
	phase := a.PhaseDeadline.Sub(a.ServerTime).Milliseconds()
	if lease <= 0 {
		return acquisitionResponse(store.AcquisitionResult{Decision: "FENCED"})
	}
	if phase <= 0 {
		return acquisitionResponse(store.AcquisitionResult{Decision: "STOP_REQUESTED"})
	}
	authority := a.Authority
	resources := a.Job.Spec.Resources
	assignment := &pb.Assignment{
		Authority:   &pb.AttemptAuthority{JobId: authority.JobID, AttemptId: authority.AttemptID, Generation: uint64(authority.Generation), WorkerId: authority.WorkerID, SessionId: authority.SessionID},
		ImageDigest: a.Job.Spec.Image, Argv: append(slices.Clone(a.Job.Spec.Command), a.Job.Spec.Args...),
		Resources:            &pb.Resources{CpuMillis: uint32(resources.CPUMillis), MemoryBytes: uint64(resources.MemoryMiB) << 20, ScratchBytes: uint64(resources.ScratchMiB) << 20},
		CanonicalJobSpecJson: a.CanonicalSpec, SpecSha256: a.SpecHash, LeaseDurationMs: uint64(lease), PhaseRemainingMs: uint64(phase), ServerTimeUnixMs: a.ServerTime.UnixMilli(),
	}
	if len(a.Inputs) != len(a.Job.Spec.Inputs) {
		return nil, invalid
	}
	for i, input := range a.Inputs {
		if input.Dataset.ID == "" || input.Dataset.Name != a.Job.Spec.Inputs[i].Dataset ||
			input.MountPath != a.Job.Spec.Inputs[i].MountPath || input.Dataset.Object.Size < 1 ||
			input.Dataset.Object.Size > objectstore.MaxSinglePartBytes {
			return nil, invalid
		}
		manifest, err := json.Marshal(input.Dataset.Manifest)
		if err != nil || len(manifest) > 2<<20 {
			return nil, invalid
		}
		assignment.Inputs = append(assignment.Inputs, &pb.InputManifest{
			DatasetId: input.Dataset.ID, DatasetName: input.Dataset.Name, MountPath: input.MountPath,
			Archive: &pb.ObjectVersion{Key: input.Dataset.Object.Key, VersionId: input.Dataset.Object.Version,
				SizeBytes: uint64(input.Dataset.Object.Size), Sha256: input.Dataset.Object.SHA256},
			FileManifestJson: manifest,
		})
	}
	return &pb.AcquireWorkResponse{Outcome: &pb.AcquireWorkResponse_Assignment{Assignment: assignment}}, nil
}

func (s *Service) signInputGrants(ctx context.Context, assignment *pb.Assignment) error {
	if len(assignment.Inputs) == 0 {
		return nil
	}
	if s.objects == nil {
		return status.Error(codes.Unavailable, "DATASET_GRANT_UNAVAILABLE")
	}
	for _, input := range assignment.Inputs {
		archive := input.GetArchive()
		if archive == nil || archive.SizeBytes == 0 || archive.SizeBytes > uint64(objectstore.MaxSinglePartBytes) {
			return status.Error(codes.Internal, "INVALID_DATASET_ASSIGNMENT")
		}
		// Sign after acquisition commits. An uncertain RPC reply can replay the
		// same durable binding while receiving a fresh short-lived capability.
		grant, err := s.objects.PresignDownload(ctx, objectstore.Object{Key: archive.Key,
			Version: archive.VersionId, Size: int64(archive.SizeBytes), SHA256: archive.Sha256}, 30*time.Second)
		if err != nil {
			return status.Error(codes.Unavailable, "DATASET_GRANT_UNAVAILABLE")
		}
		for name, values := range grant.Headers {
			if !strings.EqualFold(name, "Host") || len(values) != 1 {
				return status.Error(codes.Internal, "UNSUPPORTED_DATASET_GRANT")
			}
		}
		if grant.Method != "GET" || len(grant.URL) > 64<<10 || !grant.ExpiresAt.After(time.Now()) {
			return status.Error(codes.Internal, "INVALID_DATASET_GRANT")
		}
		input.DownloadUrl = grant.URL
		input.ExpiresUnixMs = grant.ExpiresAt.UnixMilli()
	}
	return nil
}
