package workerapi

import (
	"context"
	"errors"
	"math"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	pb.UnimplementedWorkerServiceServer
	pool   *pgxpool.Pool
	policy store.AcquisitionPolicy
}

func NewService(pool *pgxpool.Pool, policy store.AcquisitionPolicy) *Service {
	return &Service{pool: pool, policy: policy}
}

func (s *Service) RegisterWorker(ctx context.Context, r *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	identity, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, identity.WorkerID); err != nil {
		return nil, err
	}
	resources := r.GetAllocatable()
	const mib = 1 << 20
	// Database reservations use whole MiB. Reject fractions instead of rounding
	// worker capacity upward and allowing more memory/scratch than was advertised.
	if resources == nil || resources.MemoryBytes%mib != 0 || resources.ScratchBytes%mib != 0 || r.GetExecutionSlots() > 1000 {
		return nil, rpcError(store.ErrInvalid)
	}
	registration := store.Registration{RequestID: r.GetRequestId(), SessionID: r.GetRequestedSessionId(), ProtocolVersion: r.GetProtocolVersion(),
		Resources: spec.Resources{CPUMillis: int64(resources.CpuMillis), MemoryMiB: int64(resources.MemoryBytes / mib), ScratchMiB: int64(resources.ScratchBytes / mib)}, Slots: int(r.GetExecutionSlots()), Labels: r.GetLabels(), Capabilities: r.GetCapabilities()}
	session, err := store.RegisterSession(ctx, s.pool, identity, registration)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.RegisterWorkerResponse{Session: &pb.WorkerSession{WorkerId: session.WorkerID, SessionId: session.SessionID}, SessionGeneration: uint64(session.Generation), ProtocolVersion: 1, CleanupRequired: session.CleanupRequired}, nil
}

func (s *Service) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	identity, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, identity.WorkerID); err != nil {
		return nil, err
	}
	if r.GetReportSequence() > math.MaxInt64 || len(r.GetInventory()) > 1024 {
		return nil, rpcError(store.ErrInvalid)
	}
	report := store.HeartbeatReport{RequestID: r.GetRequestId(), SessionID: r.GetSession().GetSessionId(), Sequence: int64(r.GetReportSequence()), RuntimeHealthy: r.GetRuntimeHealthy(), DiskPressure: r.GetDiskPressure(), ReconciliationComplete: r.GetReconciliationComplete()}
	for _, entry := range r.GetInventory() {
		a := entry.GetAuthority()
		if a == nil || a.GetGeneration() > math.MaxInt64 {
			return nil, rpcError(store.ErrInvalid)
		}
		report.Inventory = append(report.Inventory, store.ExecutionRecord{Authority: store.AttemptAuthority{JobID: a.GetJobId(), AttemptID: a.GetAttemptId(), Generation: int64(a.GetGeneration()), WorkerID: a.GetWorkerId(), SessionID: a.GetSessionId()}, ContainerID: entry.GetContainerId(), Running: entry.GetRunning()})
	}
	result, err := store.RecordHeartbeat(ctx, s.pool, identity, report)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &pb.HeartbeatResponse{Drain: result.Drain, Reconcile: result.Reconcile}
	for _, a := range result.Stop {
		response.Stop = append(response.Stop, &pb.AttemptAuthority{JobId: a.JobID, AttemptId: a.AttemptID, Generation: uint64(a.Generation), WorkerId: a.WorkerID, SessionId: a.SessionID})
	}
	return response, nil
}

func rpcError(err error) error {
	// Keep wire reasons stable and independent of driver errors, which can
	// contain SQL, schema details, or connection configuration.
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "REQUEST_CANCELLED")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "DEADLINE_EXCEEDED")
	case errors.Is(err, store.ErrUnauthorized):
		return status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	case errors.Is(err, store.ErrInvalid):
		return status.Error(codes.InvalidArgument, "INVALID_ARGUMENT")
	case errors.Is(err, store.ErrConflict):
		return status.Error(codes.AlreadyExists, "REQUEST_CONFLICT")
	case errors.Is(err, store.ErrSessionActive):
		return status.Error(codes.FailedPrecondition, "SESSION_ACTIVE")
	case errors.Is(err, store.ErrFenced):
		return status.Error(codes.FailedPrecondition, "SESSION_FENCED")
	case errors.Is(err, store.ErrStaleHeartbeat):
		return status.Error(codes.Aborted, "STALE_HEARTBEAT")
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "NOT_FOUND")
	default:
		return status.Error(codes.Unavailable, "DATABASE_UNAVAILABLE")
	}
}
