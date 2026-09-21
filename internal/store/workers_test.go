package store

import (
	"crypto/sha256"
	"errors"
	"testing"

	"dispatch.local/dispatch/internal/spec"
)

func workerProvision() WorkerProvision {
	return WorkerProvision{Name: "linux-a", CertificateSHA256: sha256.Sum256([]byte("test leaf certificate")),
		Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4,
		Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "arm64"}}
}

func TestWorkerProvisioningBounds(t *testing.T) {
	if err := workerProvision().validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*WorkerProvision){
		"empty name":           func(p *WorkerProvision) { p.Name = "" },
		"missing certificate":  func(p *WorkerProvision) { p.CertificateSHA256 = [32]byte{} },
		"zero cpu":             func(p *WorkerProvision) { p.Resources.CPUMillis = 0 },
		"cpu overflow":         func(p *WorkerProvision) { p.Resources.CPUMillis = 1_024_001 },
		"memory overflow":      func(p *WorkerProvision) { p.Resources.MemoryMiB = 16_777_217 },
		"scratch overflow":     func(p *WorkerProvision) { p.Resources.ScratchMiB = 1_073_741_825 },
		"slot overflow":        func(p *WorkerProvision) { p.Slots = 1001 },
		"no projects":          func(p *WorkerProvision) { p.Projects = nil },
		"duplicate project":    func(p *WorkerProvision) { p.Projects = append(p.Projects, p.Projects[0]) },
		"unsupported os":       func(p *WorkerProvision) { p.Labels["os"] = "darwin" },
		"missing architecture": func(p *WorkerProvision) { delete(p.Labels, "architecture") },
		"unsafe label":         func(p *WorkerProvision) { p.Labels["rack"] = "\x1b[31m" },
	} {
		t.Run(name, func(t *testing.T) {
			p := workerProvision()
			change(&p)
			if err := p.validate(); !errors.Is(err, ErrInvalid) {
				t.Fatal("unsafe worker provisioning accepted", err)
			}
		})
	}
}
