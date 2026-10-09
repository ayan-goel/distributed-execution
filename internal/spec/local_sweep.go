package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// DecodeLocalSweep is client-only: the caller resolves local template paths.
// The server's DecodeSweep continues to reject every jobTemplateFile field.
func DecodeLocalSweep(r io.Reader, loadTemplate func(string) (Job, error)) (Sweep, error) {
	var document struct {
		APIVersion string   `json:"apiVersion"`
		Kind       string   `json:"kind"`
		Metadata   Metadata `json:"metadata"`
		Spec       struct {
			JobTemplate            json.RawMessage     `json:"jobTemplate"`
			JobTemplateFile        string              `json:"jobTemplateFile"`
			Matrix                 map[string][]string `json:"matrix"`
			MaxConcurrent          int                 `json:"maxConcurrent"`
			FailFast               bool                `json:"failFast"`
			CancelRunningOnFailure bool                `json:"cancelRunningOnFailure"`
		} `json:"spec"`
	}
	if err := decodeDocument(r, &document); err != nil {
		return Sweep{}, err
	}
	s := document.Spec
	if (len(s.JobTemplate) > 0) == (s.JobTemplateFile != "") {
		return Sweep{}, fmt.Errorf("provide exactly one of jobTemplate or jobTemplateFile")
	}
	var job Job
	var err error
	if s.JobTemplateFile != "" {
		if loadTemplate == nil || len(s.JobTemplateFile) > 4096 || strings.ContainsRune(s.JobTemplateFile, 0) {
			return Sweep{}, fmt.Errorf("invalid local template reference")
		}
		job, err = loadTemplate(s.JobTemplateFile)
	} else {
		// Decode the embedded document through the strict Job parser as well;
		// raw JSON must not bypass exact field names or template validation.
		job, err = DecodeJob(bytes.NewReader(s.JobTemplate))
	}
	if err != nil {
		return Sweep{}, err
	}
	sweep := Sweep{APIVersion: document.APIVersion, Kind: document.Kind, Metadata: document.Metadata,
		Spec: SweepSpec{JobTemplate: job, Matrix: s.Matrix, MaxConcurrent: s.MaxConcurrent,
			FailFast: s.FailFast, CancelRunningOnFailure: s.CancelRunningOnFailure}}
	_, err = sweep.Expand()
	return sweep, err
}
