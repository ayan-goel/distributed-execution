// Package spec defines the immutable, bounded submission contract. Admission must
// resolve image tags and dataset names before persisting the final execution hash.
package spec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"slices"
	"strings"
)

const APIVersion = "dispatch.dev/v1alpha1"

type Metadata struct {
	Name    string            `json:"name"`
	Project string            `json:"project"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type Resources struct {
	CPUMillis  int64 `json:"cpuMillis"`
	MemoryMiB  int64 `json:"memoryMiB"`
	ScratchMiB int64 `json:"scratchMiB"`
}

type Placement struct {
	Labels map[string]string `json:"labels,omitempty"`
}
type Input struct {
	Dataset   string `json:"dataset"`
	MountPath string `json:"mountPath"`
}
type Output struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Required bool   `json:"required"`
	MaxBytes int64  `json:"maxBytes"`
}
type Timeouts struct {
	StartupSeconds      int64 `json:"startupSeconds"`
	ExecutionSeconds    int64 `json:"executionSeconds"`
	FinalizationSeconds int64 `json:"finalizationSeconds"`
}
type Retry struct {
	MaxAttempts           int      `json:"maxAttempts"`
	On                    []string `json:"on,omitempty"`
	InitialBackoffSeconds int64    `json:"initialBackoffSeconds"`
	MaxBackoffSeconds     int64    `json:"maxBackoffSeconds"`
}
type JobSpec struct {
	Image                   string            `json:"image"`
	Command                 []string          `json:"command"`
	Args                    []string          `json:"args,omitempty"`
	Env                     map[string]string `json:"env,omitempty"`
	Resources               Resources         `json:"resources"`
	Placement               Placement         `json:"placement"`
	Inputs                  []Input           `json:"inputs,omitempty"`
	Outputs                 []Output          `json:"outputs,omitempty"`
	Timeouts                Timeouts          `json:"timeouts"`
	Retry                   Retry             `json:"retry"`
	TerminationGraceSeconds int64             `json:"terminationGraceSeconds"`
	Network                 string            `json:"network"`
}
type Job struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       JobSpec  `json:"spec"`
}

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
var envPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)

func DecodeJob(r io.Reader) (Job, error) {
	var j Job
	if err := decodeDocument(r, &j); err != nil {
		return j, err
	}
	if j.Spec.Network == "" {
		j.Spec.Network = "disabled"
	}
	return j, j.Validate()
}

func (j Job) Validate() error {
	if j.APIVersion != APIVersion || j.Kind != "Job" {
		return fmt.Errorf("expected %s Job", APIVersion)
	}
	if err := j.Metadata.validate(); err != nil {
		return err
	}
	s := j.Spec
	if !textWithin(s.Image, 1024) || strings.ContainsAny(s.Image, " \t\r\n") {
		return fmt.Errorf("invalid image reference")
	}
	if len(s.Command) == 0 || len(s.Command)+len(s.Args) > 256 || s.Command[0] == "" {
		return fmt.Errorf("command requires an executable and at most 256 arguments")
	}
	for _, a := range append(slices.Clone(s.Command), s.Args...) {
		if len(a) > 8192 || strings.ContainsRune(a, 0) {
			return fmt.Errorf("invalid command argument")
		}
	}
	if err := validateMap(s.Env, true); err != nil {
		return err
	}
	if err := validateMap(s.Placement.Labels, false); err != nil {
		return err
	}
	// Limits keep admission arithmetic and runtime conversion bounded; project
	// quotas further restrict these ceilings before a job can enter the queue.
	if !between(s.Resources.CPUMillis, 1, 1_024_000) || !between(s.Resources.MemoryMiB, 1, 16_777_216) || !between(s.Resources.ScratchMiB, 1, 1_073_741_824) {
		return fmt.Errorf("resources must be positive and within platform bounds")
	}
	if !between(s.Timeouts.StartupSeconds, 1, 3600) || !between(s.Timeouts.ExecutionSeconds, 1, 604800) || !between(s.Timeouts.FinalizationSeconds, 1, 3600) || !between(s.TerminationGraceSeconds, 0, 300) {
		return fmt.Errorf("timeouts must be finite and within platform bounds")
	}
	if s.Network != "disabled" {
		return fmt.Errorf("only disabled workload networking is supported")
	}
	if s.Retry.MaxAttempts < 1 || s.Retry.MaxAttempts > 10 || !between(s.Retry.InitialBackoffSeconds, 1, 3600) || !between(s.Retry.MaxBackoffSeconds, s.Retry.InitialBackoffSeconds, 3600) {
		return fmt.Errorf("invalid retry policy")
	}
	reasons := map[string]bool{}
	for _, reason := range s.Retry.On {
		if !slices.Contains([]string{"WORKER_LOST", "RUNTIME_UNAVAILABLE", "TRANSFER_FAILED"}, reason) || reasons[reason] {
			return fmt.Errorf("unsupported or duplicate retry reason: %s", reason)
		}
		reasons[reason] = true
	}
	if len(s.Inputs) > 64 || len(s.Outputs) > 64 {
		return fmt.Errorf("at most 64 inputs and outputs are allowed")
	}
	var mounts, outputs []string
	names := map[string]bool{}
	for _, input := range s.Inputs {
		if !namePattern.MatchString(input.Dataset) || !containedPath(input.MountPath, "/inputs") || overlaps(mounts, input.MountPath) {
			return fmt.Errorf("invalid or overlapping input mount")
		}
		mounts = append(mounts, input.MountPath)
	}
	for _, output := range s.Outputs {
		if !namePattern.MatchString(output.Name) || names[output.Name] || !containedPath(output.Path, "/outputs") || overlaps(outputs, output.Path) || !between(output.MaxBytes, 1, 1<<40) {
			return fmt.Errorf("invalid, duplicate, or overlapping output")
		}
		names[output.Name] = true
		outputs = append(outputs, output.Path)
	}
	return nil
}

func (m Metadata) validate() error {
	if !namePattern.MatchString(m.Name) || !namePattern.MatchString(m.Project) {
		return fmt.Errorf("name and project must be 1–128 identifier characters")
	}
	return validateMap(m.Labels, false)
}

func validateMap(m map[string]string, environment bool) error {
	if len(m) > 128 {
		return fmt.Errorf("at most 128 map entries are allowed")
	}
	for k, v := range m {
		valid := namePattern.MatchString(k)
		if environment {
			valid = envPattern.MatchString(k) && !strings.HasPrefix(k, "DISPATCH_")
		}
		if !valid || len(v) > 8192 || strings.ContainsRune(v, 0) {
			return fmt.Errorf("invalid map entry %q", k)
		}
	}
	return nil
}

func containedPath(p, root string) bool {
	// INVARIANT: user paths cannot request a host mount or escape the declared
	// root. Runtime collection must additionally prevent filesystem symlink races.
	return len(p) <= 4096 && path.Clean(p) == p && strings.HasPrefix(p, root+"/") && !strings.ContainsAny(p, "\\\x00")
}

func overlaps(existing []string, p string) bool {
	for _, q := range existing {
		if p == q || strings.HasPrefix(p, q+"/") || strings.HasPrefix(q, p+"/") {
			return true
		}
	}
	return false
}
func textWithin(s string, n int) bool {
	return len(s) > 0 && len(s) <= n && !strings.ContainsRune(s, 0)
}
func between(n, low, high int64) bool { return n >= low && n <= high }

func (j Job) Canonical() ([]byte, string, error) {
	if err := j.Validate(); err != nil {
		return nil, "", err
	}
	// JSON sorts string map keys. Sorting the retry set removes irrelevant order
	// without changing argument, input, or user-specified parameter value order.
	j.Spec.Retry.On = slices.Clone(j.Spec.Retry.On)
	slices.Sort(j.Spec.Retry.On)
	b, err := json.Marshal(j)
	if err != nil {
		return nil, "", err
	}
	h := sha256.Sum256(b)
	return b, hex.EncodeToString(h[:]), nil
}
