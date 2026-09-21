package workerapi

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testCA(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

func TestNestedInventoryAndRenewalCannotImpersonateHosts(t *testing.T) {
	session := &pb.WorkerSession{WorkerId: "host", SessionId: "current"}
	for _, request := range []any{
		&pb.HeartbeatRequest{Session: session, Inventory: []*pb.ExecutionInventory{{Authority: &pb.AttemptAuthority{WorkerId: "other"}}}},
		&pb.RenewLeasesRequest{Session: session, Attempts: []*pb.AttemptAuthority{{WorkerId: "other", SessionId: "current"}}},
		&pb.RenewLeasesRequest{Session: session, Attempts: []*pb.AttemptAuthority{{WorkerId: "host", SessionId: "old"}}},
	} {
		if err := bindWorker(request, "host"); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("nested identity not bound: %T: %v", request, err)
		}
	}
	cleanup := &pb.HeartbeatRequest{Session: session, Inventory: []*pb.ExecutionInventory{{Authority: &pb.AttemptAuthority{WorkerId: "host", SessionId: "old"}}}}
	if err := bindWorker(cleanup, "host"); err != nil {
		t.Fatal("old local inventory must remain visible for cleanup", err)
	}
}

func testLeaf(t *testing.T, ca tls.Certificate, usage x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(30 * time.Minute), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.Leaf, pub, ca.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func TestVerifiedConnectionCannotOutliveCertificate(t *testing.T) {
	ca, _ := testCA(t)
	cert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	s := tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{cert.Leaf}, VerifiedChains: [][]*x509.Certificate{{cert.Leaf, ca.Leaf}}}
	if _, err := verifiedLeaf(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := verifiedLeaf(s, cert.Leaf.NotAfter.Add(time.Second)); err == nil {
		t.Fatal("expired connection accepted")
	}
	s.VerifiedChains = nil
	if _, err := verifiedLeaf(s, time.Now()); err == nil {
		t.Fatal("unverified peer accepted")
	}
}

func TestEveryWorkerRequestCarriesAuthenticatedHostIdentity(t *testing.T) {
	s := &pb.WorkerSession{WorkerId: "host"}
	a := &pb.AttemptAuthority{WorkerId: "host"}
	for _, request := range []any{
		&pb.RegisterWorkerRequest{WorkerId: "host"}, &pb.HeartbeatRequest{Session: s}, &pb.AcquireWorkRequest{Session: s}, &pb.ListAssignmentsRequest{Session: s}, &pb.ReportPhaseRequest{Authority: a}, &pb.RenewLeasesRequest{Session: s}, &pb.CreateUploadRequest{Authority: a}, &pb.FinalizeUploadRequest{Authority: a}, &pb.RegisterLogSegmentRequest{Authority: a}, &pb.CompleteAttemptRequest{Authority: a},
	} {
		if id := requestWorkerID(request); id != "host" {
			t.Fatalf("request lacks transport binding: %T", request)
		}
	}
	for _, request := range []any{nil, &pb.HeartbeatRequest{}, &pb.CompleteAttemptRequest{}} {
		if requestWorkerID(request) != "" {
			t.Fatal("missing authority accepted")
		}
	}
}
