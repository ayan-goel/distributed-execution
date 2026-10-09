package spec

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestLocalSweepAcceptsOneStrictTemplateSource(t *testing.T) {
	sweep := sweepExample(t)
	body, _ := json.Marshal(sweep)
	inline, err := DecodeLocalSweep(bytes.NewReader(body), func(string) (Job, error) {
		t.Fatal("inline sweep consulted filesystem")
		return Job{}, nil
	})
	if err != nil || inline.Spec.JobTemplate.Kind != "Job" {
		t.Fatal("inline sweep rejected", err)
	}
	var document map[string]any
	if err := json.Unmarshal(body, &document); err != nil {
		t.Fatal(err)
	}
	fields := document["spec"].(map[string]any)
	fields["jobTemplateFile"] = "job.yaml"
	both, _ := json.Marshal(document)
	if _, err := DecodeLocalSweep(bytes.NewReader(both), nil); err == nil {
		t.Fatal("ambiguous template sources accepted")
	}
	delete(fields, "jobTemplate")
	reference, _ := json.Marshal(document)
	loaded := 0
	resolved, err := DecodeLocalSweep(bytes.NewReader(reference), func(path string) (Job, error) {
		loaded++
		if path != "job.yaml" {
			t.Fatal("reference changed", path)
		}
		return sweep.Spec.JobTemplate, nil
	})
	if err != nil || loaded != 1 || resolved.Metadata.Name != sweep.Metadata.Name {
		t.Fatal("referenced template did not resolve", loaded, err)
	}
	if _, err := DecodeSweep(bytes.NewReader(reference)); err == nil {
		t.Fatal("server parser accepted a local path")
	}
	for _, invalid := range []string{
		strings.Replace(string(body), `"jobTemplate":{"apiVersion":`, `"jobTemplate":{"APIVersion":`, 1),
		strings.Replace(string(reference), `"jobTemplateFile":"job.yaml"`, `"jobTemplateFile":"job.yaml","unknown":true`, 1),
		strings.Replace(string(reference), `"jobTemplateFile":"job.yaml"`, `"jobTemplateFile":""`, 1),
	} {
		if _, err := DecodeLocalSweep(strings.NewReader(invalid), nil); err == nil {
			t.Fatal("invalid local document accepted", invalid)
		}
	}
}
