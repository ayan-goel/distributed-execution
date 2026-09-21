package workerapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type identityKey struct{}

const maxWorkerMessageBytes = 4 << 20

func Identity(ctx context.Context) (store.WorkerIdentity, bool) {
	id, ok := ctx.Value(identityKey{}).(store.WorkerIdentity)
	return id, ok
}

func NewServer(pool *pgxpool.Pool, certificate tls.Certificate, clientRoots *x509.CertPool, service pb.WorkerServiceServer) (*grpc.Server, error) {
	if pool == nil || len(certificate.Certificate) == 0 || certificate.PrivateKey == nil || clientRoots == nil || service == nil {
		return nil, errors.New("worker server requires database, service, certificate/key, and client CA trust")
	}
	// INVARIANT: host identity comes from a verified client certificate, never
	// from gRPC metadata or a request field. No plaintext fallback is configured.
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientCAs: clientRoots.Clone(), ClientAuth: tls.RequireAndVerifyClientCert}
	slots := make(chan struct{}, 256)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.MaxRecvMsgSize(maxWorkerMessageBytes), grpc.MaxSendMsgSize(maxWorkerMessageBytes), grpc.MaxHeaderListSize(16<<10), grpc.MaxConcurrentStreams(64), grpc.ConnectionTimeout(5*time.Second), grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Bound database wait and handler work independently of client deadlines;
		// overload cannot create an unbounded queue of authentication operations.
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			return nil, status.Error(codes.ResourceExhausted, "worker RPC capacity exhausted")
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		remote, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
		}
		tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
		}
		leaf, err := verifiedLeaf(tlsInfo.State, time.Now())
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "valid client certificate required")
		}
		identity, err := store.AuthenticateWorker(ctx, pool, sha256.Sum256(leaf.Raw))
		if errors.Is(err, store.ErrUnauthorized) {
			return nil, status.Error(codes.Unauthenticated, "worker identity not authorized")
		}
		if err != nil {
			return nil, status.Error(codes.Unavailable, "worker authentication unavailable")
		}
		if err := bindWorker(request, identity.WorkerID); err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, identityKey{}, identity), request)
	}))
	pb.RegisterWorkerServiceServer(server, service)
	return server, nil
}

func verifiedLeaf(state tls.ConnectionState, now time.Time) (*x509.Certificate, error) {
	if !state.HandshakeComplete || state.Version < tls.VersionTLS13 || len(state.PeerCertificates) == 0 {
		return nil, errors.New("unverified TLS peer")
	}
	// TLS verification happens at handshake. An established gRPC connection can
	// outlive certificate validity, so every RPC must still have a currently valid
	// verified chain. Database revocation is also checked on every call.
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || !bytes.Equal(chain[0].Raw, state.PeerCertificates[0].Raw) {
			continue
		}
		valid := true
		for _, cert := range chain {
			if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
				valid = false
				break
			}
		}
		if valid {
			return state.PeerCertificates[0], nil
		}
	}
	return nil, errors.New("no currently valid verified client chain")
}

func bindWorker(request any, id string) error {
	claimed := requestWorkerID(request)
	if claimed == "" {
		return status.Error(codes.InvalidArgument, "worker identity required")
	}
	if claimed != id {
		return status.Error(codes.PermissionDenied, "worker identity mismatch")
	}
	// Inventory can describe an old session on this same host during cleanup.
	// Renewal batches, however, must all belong to their enclosing session.
	switch r := request.(type) {
	case *pb.HeartbeatRequest:
		for _, entry := range r.GetInventory() {
			if entry.GetAuthority().GetWorkerId() != id {
				return status.Error(codes.PermissionDenied, "inventory host mismatch")
			}
		}
	case *pb.RenewLeasesRequest:
		for _, a := range r.GetAttempts() {
			if a.GetWorkerId() != id || a.GetSessionId() != r.GetSession().GetSessionId() {
				return status.Error(codes.PermissionDenied, "renewal host or session mismatch")
			}
		}
	}
	return nil
}

func requestWorkerID(request any) string {
	switch r := request.(type) {
	case *pb.RegisterWorkerRequest:
		return r.GetWorkerId()
	case *pb.HeartbeatRequest:
		return r.GetSession().GetWorkerId()
	case *pb.AcquireWorkRequest:
		return r.GetSession().GetWorkerId()
	case *pb.ListAssignmentsRequest:
		return r.GetSession().GetWorkerId()
	case *pb.ReportPhaseRequest:
		return r.GetAuthority().GetWorkerId()
	case *pb.RenewLeasesRequest:
		return r.GetSession().GetWorkerId()
	case *pb.CreateUploadRequest:
		return r.GetAuthority().GetWorkerId()
	case *pb.FinalizeUploadRequest:
		return r.GetAuthority().GetWorkerId()
	case *pb.RegisterLogSegmentRequest:
		return r.GetAuthority().GetWorkerId()
	case *pb.CompleteAttemptRequest:
		return r.GetAuthority().GetWorkerId()
	default:
		return ""
	}
}
