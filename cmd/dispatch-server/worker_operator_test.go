package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type workerPKI struct {
	ca, serverCert, serverKey, clientCert, clientKey string
	client                                           tls.Certificate
	roots                                            *x509.CertPool
}

func testWorkerPKI(t *testing.T) workerPKI {
	t.Helper()
	dir := t.TempDir()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, root, root, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	p := workerPKI{ca: filepath.Join(dir, "ca.pem"), roots: x509.NewCertPool()}
	p.roots.AddCert(root)
	if err := os.WriteFile(p.ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	leaf := func(name string, usage x509.ExtKeyUsage) (string, string, tls.Certificate) {
		pub, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(int64(usage) + 2), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(30 * time.Minute), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		der, err := x509.CreateCertificate(rand.Reader, template, root, pub, key)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		certPath, keyPath := filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
		if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
			t.Fatal(err)
		}
		return certPath, keyPath, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}
	}
	p.serverCert, p.serverKey, _ = leaf("server", x509.ExtKeyUsageServerAuth)
	p.clientCert, p.clientKey, p.client = leaf("worker", x509.ExtKeyUsageClientAuth)
	return p
}

func TestWorkerEnrollmentReadsOnlyClientLeafCertificate(t *testing.T) {
	p := testWorkerPKI(t)
	fingerprint, err := workerCertificateFingerprint(p.clientCert)
	if err != nil || fingerprint != sha256.Sum256(p.client.Certificate[0]) {
		t.Fatal("wrong public certificate fingerprint", err)
	}
	for _, path := range []string{p.clientKey, p.serverCert, p.ca, "missing.pem"} {
		if _, err := workerCertificateFingerprint(path); err == nil {
			t.Fatal("invalid worker certificate enrolled", path)
		}
	}
	large := filepath.Join(t.TempDir(), "large.pem")
	if err := os.WriteFile(large, []byte(strings.Repeat("x", (1<<20)+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := workerCertificateFingerprint(large); err == nil {
		t.Fatal("oversized certificate accepted")
	}
}
