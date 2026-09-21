package spec

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
)

const MaxSweepJobs = 1000
const MaxExpandedBytes = 16 << 20

type Sweep struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Metadata   Metadata  `json:"metadata"`
	Spec       SweepSpec `json:"spec"`
}

type SweepSpec struct {
	JobTemplate            Job                 `json:"jobTemplate"`
	Matrix                 map[string][]string `json:"matrix"`
	MaxConcurrent          int                 `json:"maxConcurrent"`
	FailFast               bool                `json:"failFast"`
	CancelRunningOnFailure bool                `json:"cancelRunningOnFailure"`
}

func DecodeSweep(r io.Reader) (Sweep, error) {
	var s Sweep
	if err := decodeDocument(r, &s); err != nil {
		return s, err
	}
	if s.Spec.JobTemplate.Spec.Network == "" {
		s.Spec.JobTemplate.Spec.Network = "disabled"
	}
	_, err := s.Expand()
	return s, err
}

func (s Sweep) Expand() ([]Job, error) {
	if s.APIVersion != APIVersion || s.Kind != "Sweep" {
		return nil, fmt.Errorf("expected %s Sweep", APIVersion)
	}
	if err := s.Metadata.validate(); err != nil {
		return nil, err
	}
	if s.Metadata.Project != s.Spec.JobTemplate.Metadata.Project {
		return nil, fmt.Errorf("template and sweep projects must match")
	}
	if err := s.Spec.JobTemplate.Validate(); err != nil {
		return nil, err
	}
	if s.Spec.MaxConcurrent < 1 || s.Spec.MaxConcurrent > MaxSweepJobs || (s.Spec.CancelRunningOnFailure && !s.Spec.FailFast) {
		return nil, fmt.Errorf("invalid sweep concurrency or failure policy")
	}
	if len(s.Spec.Matrix) == 0 || len(s.Spec.Matrix) > 32 {
		return nil, fmt.Errorf("matrix requires 1–32 parameters")
	}
	keys := make([]string, 0, len(s.Spec.Matrix))
	count := 1
	for key, values := range s.Spec.Matrix {
		// Check before multiplying or allocating to prevent integer overflow and
		// unbounded child creation inside the eventual submission transaction.
		if !envPattern.MatchString(key) || strings.HasPrefix(key, "DISPATCH_") || len(values) == 0 || len(values) > MaxSweepJobs/count {
			return nil, fmt.Errorf("invalid matrix or sweep exceeds 1000 jobs")
		}
		for _, value := range values {
			if len(value) > 8192 || strings.ContainsRune(value, 0) {
				return nil, fmt.Errorf("invalid matrix value")
			}
		}
		count *= len(values)
		keys = append(keys, key)
	}
	// A stable key order preserves child indices across retries and languages;
	// values retain the user's order, including intentional repeated parameters.
	slices.Sort(keys)
	template, _, err := s.Spec.JobTemplate.Canonical()
	if err != nil {
		return nil, err
	}
	jobs := make([]Job, 0, count)
	total := 0
	for i := 0; i < count; i++ {
		var child Job
		if err := json.Unmarshal(template, &child); err != nil {
			return nil, err
		}
		if child.Spec.Env == nil {
			child.Spec.Env = map[string]string{}
		}
		index := i
		for k := len(keys) - 1; k >= 0; k-- {
			values := s.Spec.Matrix[keys[k]]
			child.Spec.Env[keys[k]] = values[index%len(values)]
			index /= len(values)
		}
		canonical, _, err := child.Canonical()
		if err != nil {
			return nil, err
		}
		total += len(canonical)
		if total > MaxExpandedBytes {
			return nil, fmt.Errorf("expanded sweep exceeds 16 MiB")
		}
		jobs = append(jobs, child)
	}
	return jobs, nil
}
