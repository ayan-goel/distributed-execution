package cli

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestBuildDatasetArchiveIsStableAndManifestMatchesFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "b.txt"), []byte("second"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("first"), 0600); err != nil {
		t.Fatal(err)
	}
	first, manifest, err := buildDatasetArchive(root)
	if err != nil || manifest.Format != "tar.v1" || len(manifest.Files) != 2 ||
		manifest.Files[0].Path != "a.txt" || manifest.Files[1].Path != "nested/b.txt" {
		t.Fatal("archive manifest is not stable and sorted", manifest, err)
	}
	if err := os.Chtimes(filepath.Join(root, "a.txt"), time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	second, repeated, err := buildDatasetArchive(root)
	if err != nil || !bytes.Equal(first, second) || !reflect.DeepEqual(manifest, repeated) {
		t.Fatal("timestamps changed the archive identity", err)
	}
	reader := tar.NewReader(bytes.NewReader(first))
	for _, want := range []struct{ name, body string }{{"a.txt", "first"}, {"nested/b.txt", "second"}} {
		header, err := reader.Next()
		if err != nil || header.Name != want.name || header.Typeflag != tar.TypeReg || header.ModTime.Unix() != 0 {
			t.Fatal("archive entry metadata changed", header, err)
		}
		body, err := io.ReadAll(reader)
		if err != nil || string(body) != want.body {
			t.Fatal("archive content differs from manifest", err)
		}
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatal("archive included undeclared entries", err)
	}
}

func TestBuildDatasetArchiveRejectsUnsafeEntries(t *testing.T) {
	root := t.TempDir()
	if _, _, err := buildDatasetArchive(root); err == nil {
		t.Fatal("empty dataset accepted")
	}
	file := filepath.Join(root, "input.txt")
	if err := os.WriteFile(file, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(file, filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildDatasetArchive(root); err == nil {
		t.Fatal("symlink accepted")
	}
	if err := os.Remove(filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(file, filepath.Join(root, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildDatasetArchive(root); err == nil {
		t.Fatal("hardlink accepted")
	}
	if err := os.Remove(filepath.Join(root, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(file, 64<<20); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildDatasetArchive(root); err == nil {
		t.Fatal("oversized archive accepted")
	}
}
