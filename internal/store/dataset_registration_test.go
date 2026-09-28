package store

import (
	"strings"
	"testing"
)

func TestDatasetManifestRejectsUnsafeOrAmbiguousPaths(t *testing.T) {
	file := func(path string) DatasetFile {
		return DatasetFile{Path: path, SizeBytes: 3, SHA256: strings.Repeat("a", 64)}
	}
	for _, paths := range [][]string{
		{"../escape"}, {".."}, {"/absolute"}, {"a/../b"}, {"a\\b"},
		{"a", "a"}, {"a", "a/b"}, {"z", "a"},
	} {
		manifest := DatasetManifest{Format: "tar.v1"}
		for _, path := range paths {
			manifest.Files = append(manifest.Files, file(path))
		}
		if err := manifest.validate(10240); err == nil {
			t.Fatal("unsafe manifest was accepted", paths)
		}
	}
	valid := DatasetManifest{Format: "tar.v1", Files: []DatasetFile{file("a/input.txt"), file("b/input.txt")}}
	if err := valid.validate(10240); err != nil {
		t.Fatal("sorted regular files were rejected", err)
	}
}
