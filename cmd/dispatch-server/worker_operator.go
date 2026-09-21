package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
)

func workerOperator(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 2 {
		return errors.New("usage: dispatch-server worker create|revoke|takeover")
	}
	command := args[1]
	f := flags("worker " + command)
	var name, certificate, architecture, id, credential, from, to string
	var projects, labels stringsFlag
	var resources spec.Resources
	var slots int
	switch command {
	case "create":
		f.StringVar(&name, "name", "", "installed host name")
		f.StringVar(&certificate, "certificate", "", "public client leaf certificate PEM, optionally followed by intermediates")
		f.StringVar(&architecture, "architecture", "", "amd64 or arm64")
		f.Var(&projects, "project", "authorized project (repeatable)")
		f.Var(&labels, "label", "placement label key=value (repeatable)")
		f.Int64Var(&resources.CPUMillis, "cpu-millis", 4000, "authorized CPU ceiling")
		f.Int64Var(&resources.MemoryMiB, "memory-mib", 8192, "authorized memory ceiling")
		f.Int64Var(&resources.ScratchMiB, "scratch-mib", 16384, "authorized scratch ceiling")
		f.IntVar(&slots, "slots", 4, "execution slot ceiling")
	case "revoke":
		f.StringVar(&id, "id", "", "worker UUID")
		f.StringVar(&credential, "credential", "", "credential UUID")
	case "takeover":
		f.StringVar(&id, "id", "", "worker UUID")
		f.StringVar(&from, "from-session", "", "current session UUID")
		f.StringVar(&to, "to-session", "", "specific replacement session UUID")
	default:
		return errors.New("unknown worker operator command")
	}
	if err := f.Parse(args[2:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected worker arguments")
	}
	p := store.WorkerProvision{Name: name, Resources: resources, Slots: slots, Projects: projects, Labels: map[string]string{"os": "linux", "architecture": architecture}}
	if command == "create" {
		for _, label := range labels {
			key, value, ok := strings.Cut(label, "=")
			if _, duplicate := p.Labels[key]; !ok || duplicate {
				return errors.New("labels require unique key=value pairs; os/architecture use dedicated configuration")
			}
			p.Labels[key] = value
		}
		fingerprint, err := workerCertificateFingerprint(certificate)
		if err != nil {
			return err
		}
		p.CertificateSHA256 = fingerprint
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	pool, err := openPool(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	switch command {
	case "create":
		identity, err := store.ProvisionWorker(ctx, pool, p)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(identity)
	case "revoke":
		if err := store.RevokeWorkerCredential(ctx, pool, id, credential); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]any{"workerId": id, "credentialId": credential, "revoked": true})
	case "takeover":
		if err := store.ApproveSessionTakeover(ctx, pool, id, from, to); err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]any{"workerId": id, "fromSessionId": from, "toSessionId": to, "approved": true})
	}
	return nil
}

func workerCertificateFingerprint(path string) ([32]byte, error) {
	var zero [32]byte
	file, err := os.Open(path)
	if err != nil {
		return zero, errors.New("cannot read worker certificate")
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return zero, errors.New("worker certificate must be readable and at most 1 MiB")
	}
	var leaf *x509.Certificate
	// Enrollment accepts public certificates only. Never read/store a worker's
	// private key here; the listener separately verifies the CA chain via mTLS.
	for len(bytes.TrimSpace(body)) > 0 {
		block, rest := pem.Decode(body)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return zero, errors.New("worker certificate file must contain only certificate PEM blocks")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return zero, errors.New("invalid worker certificate")
		}
		if leaf == nil {
			leaf = certificate
		}
		body = rest
	}
	now := time.Now()
	if leaf == nil || leaf.IsCA || now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) || (!slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) && !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageAny)) {
		return zero, errors.New("worker certificate must be a currently valid client-authentication leaf")
	}
	return sha256.Sum256(leaf.Raw), nil
}
