//go:build integration

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/store"
)

type sweepVM struct {
	Port       int    `json:"port"`
	Key        string `json:"key"`
	KnownHosts string `json:"knownHosts"`
}

func (vm sweepVM) command(ctx context.Context, operation string, forwards ...string) *exec.Cmd {
	args := []string{"-i", vm.Key, "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + vm.KnownHosts, "-o", "ConnectTimeout=5", "-o", "ExitOnForwardFailure=yes", "-p", strconv.Itoa(vm.Port)}
	for _, endpoint := range forwards {
		args = append(args, "-R", endpoint+":"+endpoint)
	}
	args = append(args, "dispatch@127.0.0.1", "sudo sh /tmp/dispatch-worker-vm-fixture.sh "+operation)
	return exec.CommandContext(ctx, "ssh", args...)
}

func newIndependentSweepDaemonFixture(t *testing.T) *sweepDaemonFixture {
	t.Helper()
	path := os.Getenv("DISPATCH_TEST_WORKER_VMS")
	if path == "" {
		t.Skip("requires two dedicated Linux VMs; see docs/independent-workers.md")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var hosts []sweepVM
	if err := json.Unmarshal(data, &hosts); err != nil || len(hosts) != 2 {
		t.Fatal("exactly two VM descriptions required", err)
	}
	var identities [2][]string
	for index, vm := range hosts {
		if vm.Port < 1024 || vm.Port > 65535 || !filepath.IsAbs(vm.Key) || !filepath.IsAbs(vm.KnownHosts) {
			t.Fatal("invalid loopback VM SSH configuration")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		output, err := vm.command(ctx, "identity").CombinedOutput()
		cancel()
		identities[index] = strings.Fields(string(output))
		if err != nil || len(identities[index]) != 4 {
			t.Fatal("cannot identify independent VM", err, string(output))
		}
	}
	// Names alone do not establish independence. Require different boot and
	// daemon IDs before a multi-host test can report success.
	if identities[0][1] == identities[1][1] || identities[0][2] == identities[1][2] || identities[0][3] != identities[1][3] {
		t.Fatal("VMs must have independent kernels/daemons and matching architectures")
	}
	t.Logf("independent VM identities: %v; %v", identities[0], identities[1])
	fixture := newSweepDaemonFixture(t, hosts...)
	fixture.labels["architecture"] = identities[0][3]
	fixture.job.Spec.Placement.Labels["architecture"] = identities[0][3]
	return fixture
}

func (vm sweepVM) start(t *testing.T, f *sweepDaemonFixture, worker store.WorkerIdentity, pki workerPKI, ca, endpoint string) sweepTestDaemon {
	t.Helper()
	output, err := vm.command(f.ctx, "prepare").CombinedOutput()
	root := strings.TrimSpace(string(output))
	if err != nil || !regexp.MustCompile(`^/tmp/dispatch-sweep\.[a-zA-Z0-9]+$`).MatchString(root) {
		t.Fatal("cannot prepare owned VM filesystem", err, root)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if output, err := vm.command(ctx, "cleanup "+root).CombinedOutput(); err != nil {
			t.Error("VM cleanup retained fixture", root, err, string(output))
		}
	})
	config, err := json.Marshal(map[string]any{"worker_id": worker.WorkerID, "server_url": "https://" + endpoint, "ca_cert": root + "/ca.pem", "client_cert": root + "/client.pem", "client_key": root + "/client.key", "journal_dir": root + "/state", "workspace_root": root + "/fs", "docker_socket": "/var/run/docker.sock", "cpu_millis": 500, "memory_mib": 128, "scratch_mib": 64, "execution_slots": 1, "labels": f.labels})
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"worker.json": config, "worker-id": []byte(worker.WorkerID)}
	for name, path := range map[string]string{"ca.pem": ca, "client.pem": pki.clientCert, "client.key": pki.clientKey} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = content
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for name, content := range files {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	configure := vm.command(f.ctx, "configure "+root)
	configure.Stdin = &archive
	if output, err := configure.CombinedOutput(); err != nil {
		t.Fatal("cannot configure strict VM worker", err, string(output))
	}
	storage, err := url.Parse(os.Getenv("DISPATCH_TEST_S3_ENDPOINT"))
	if err != nil || storage.Hostname() != "127.0.0.1" || storage.Port() == "" {
		t.Fatal("VM gate requires loopback object storage", err)
	}
	// Preserve the signed URL's loopback origin through SSH. No Docker TCP
	// listener or unencrypted off-host object endpoint is exposed.
	command := vm.command(f.ctx, "run "+root, endpoint, net.JoinHostPort("127.0.0.1", storage.Port()))
	log := &lockedBuffer{}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	var stopped sync.Once
	stop := func() {
		stopped.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if output, err := vm.command(ctx, "stop "+root).CombinedOutput(); err != nil {
				t.Error("cannot stop owned VM agent", err, string(output))
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = command.Process.Kill()
				<-done
			}
		})
	}
	t.Cleanup(stop)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("VM worker:", log.String())
		}
	})
	return sweepTestDaemon{workerID: worker.WorkerID, stop: stop, endpoint: endpoint,
		tls: &tls.Config{RootCAs: pki.roots, Certificates: []tls.Certificate{pki.client}, MinVersion: tls.VersionTLS13},
		inspect: func(container string) string {
			if !regexp.MustCompile(`^[a-f0-9]+$`).MatchString(container) {
				t.Fatal("invalid fixture container ID")
			}
			output, err := vm.command(f.ctx, "inspect "+root+" "+container).CombinedOutput()
			if err != nil {
				t.Fatal("cannot inspect owned VM container", err, string(output))
			}
			return strings.TrimSpace(string(output))
		},
	}
}
