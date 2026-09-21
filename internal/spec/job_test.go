package spec

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func example(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestJobExampleAndCanonicalIdentity(t *testing.T) {
	j, err := DecodeJob(bytes.NewReader(example(t)))
	if err != nil {
		t.Fatal(err)
	}
	a, hash, err := j.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if len(hash) != 64 || j.Spec.Resources.CPUMillis != 2000 {
		t.Fatal("incorrect contract")
	}
	var object map[string]any
	if err := json.Unmarshal(a, &object); err != nil {
		t.Fatal(err)
	}
	pretty, _ := json.MarshalIndent(object, "", "  ")
	other, err := DecodeJob(bytes.NewReader(pretty))
	if err != nil {
		t.Fatal(err)
	}
	b, h, _ := other.Canonical()
	if !bytes.Equal(a, b) || hash != h {
		t.Fatal("formatting changed canonical identity")
	}
	other.Spec.Env["SEED"] = "2"
	_, changed, _ := other.Canonical()
	if changed == hash {
		t.Fatal("parameter change did not change identity")
	}
}

func TestRejectUnsafeJobSpecifications(t *testing.T) {
	base := string(example(t))
	cases := map[string]string{
		"unknown":             strings.Replace(base, "  network: disabled", "  privileged: true", 1),
		"gpu deferred":        strings.Replace(base, "    cpuMillis: 2000", "    gpuCount: 1\n    cpuMillis: 2000", 1),
		"zero cpu":            strings.Replace(base, "cpuMillis: 2000", "cpuMillis: 0", 1),
		"negative memory":     strings.Replace(base, "memoryMiB: 4096", "memoryMiB: -1", 1),
		"unbounded execution": strings.Replace(base, "executionSeconds: 1800", "executionSeconds: 0", 1),
		"host network":        strings.Replace(base, "network: disabled", "network: host", 1),
		"host input":          strings.Replace(base, "/inputs/antibodies", "/etc", -1),
		"traversal":           strings.Replace(base, "/outputs/metrics.json", "/outputs/../etc/passwd", 1),
		"overlapping output":  strings.Replace(base, "/outputs/predictions.parquet", "/outputs/metrics.json/child", 1),
		"duplicate name":      strings.Replace(base, "name: predictions", "name: metrics", 1),
		"oversized output":    strings.Replace(base, "maxBytes: 1048576", "maxBytes: 0", 1),
		"duplicate field":     base + "  network: disabled\n",
		"extra document":      base + "\n---\nkind: Job\n",
		"numeric env":         strings.Replace(base, `SEED: "1"`, "SEED: 1", 1),
		"shell string":        strings.Replace(base, "command: [python, evaluate.py]", "command: python evaluate.py", 1),
		"unknown retry":       strings.Replace(base, "WORKER_LOST", "ANYTHING", 1),
		"no attempts":         strings.Replace(base, "maxAttempts: 3", "maxAttempts: 0", 1),
		"inverted backoff":    strings.Replace(base, "maxBackoffSeconds: 60", "maxBackoffSeconds: 1", 1),
		"oversize document":   strings.Repeat(" ", MaxDocumentBytes+1),
		"null":                "null",
		"case mismatch":       strings.Replace(base, "apiVersion:", "APIVersion:", 1),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeJob(strings.NewReader(input)); err == nil {
				t.Fatal("unsafe job accepted")
			}
		})
	}
}

func TestJobWithoutRetryReasonsRoundTrips(t *testing.T) {
	j, err := DecodeJob(bytes.NewReader(example(t)))
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Retry.On = nil
	b, _, err := j.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeJob(bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
}

func FuzzJobDecoder(f *testing.F) {
	f.Add([]byte("{}"))
	f.Add([]byte("kind: Job\n"))
	f.Fuzz(func(t *testing.T, input []byte) {
		j, err := DecodeJob(bytes.NewReader(input))
		if err != nil {
			return
		}
		b, h, err := j.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		again, err := DecodeJob(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		_, h2, _ := again.Canonical()
		if h != h2 {
			t.Fatal("normalization not idempotent")
		}
	})
}
