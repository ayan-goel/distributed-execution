package store

import (
	"strings"
	"testing"
)

func TestAssignmentBudgetIncludesInputManifestsAndGrantHeadroom(t *testing.T) {
	base := WorkAssignment{CanonicalSpec: []byte(strings.Repeat("x", 100))}
	without, err := assignmentBudgetBytes(base)
	if err != nil {
		t.Fatal(err)
	}
	base.Inputs = []JobInputBinding{{Dataset: DatasetBinding{Manifest: DatasetManifest{
		Format: "tar.v1", Files: []DatasetFile{{Path: "data.txt", SizeBytes: 4, SHA256: strings.Repeat("a", 64)}},
	}}}}
	with, err := assignmentBudgetBytes(base)
	if err != nil {
		t.Fatal(err)
	}
	if with <= without+64*1024 {
		t.Fatalf("manifest or envelope missing from recovery budget: %d -> %d", without, with)
	}
}
