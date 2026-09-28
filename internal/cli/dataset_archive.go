package cli

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"dispatch.local/dispatch/internal/client"
)

const maxDatasetArchiveBytes int64 = 64 << 20

type boundedArchiveWriter struct {
	buffer bytes.Buffer
	limit  int64
}

func (w *boundedArchiveWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-int64(w.buffer.Len()) {
		return 0, errors.New("dataset archive exceeds 64 MiB")
	}
	return w.buffer.Write(p)
}

func buildDatasetArchive(directory string) ([]byte, client.DatasetManifest, error) {
	var manifest client.DatasetManifest
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, manifest, err
	}
	rootInfo, err := os.Lstat(absolute)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, manifest, errors.New("dataset source must be a directory, not a symlink")
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, manifest, errors.New("cannot open dataset source directory")
	}
	defer root.Close()
	var paths []string
	err = filepath.WalkDir(absolute, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == absolute {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() && !entry.Type().IsRegular() {
			return errors.New("dataset source contains a link or special file")
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(absolute, path)
		if err != nil || relative == "." || len(filepath.ToSlash(relative)) > 512 {
			return errors.New("dataset relative path is invalid or too long")
		}
		paths = append(paths, relative)
		if len(paths) > 1024 {
			return errors.New("dataset exceeds 1024 files")
		}
		return nil
	})
	if err != nil {
		return nil, manifest, err
	}
	if len(paths) == 0 {
		return nil, manifest, errors.New("dataset directory contains no regular files")
	}
	slices.Sort(paths)
	bounded := &boundedArchiveWriter{limit: maxDatasetArchiveBytes}
	writer := tar.NewWriter(bounded)
	manifest.Format = "tar.v1"
	for _, relative := range paths {
		info, err := root.Lstat(relative)
		if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxDatasetArchiveBytes {
			return nil, client.DatasetManifest{}, errors.New("dataset file changed or is unsupported")
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Nlink > 1 {
			return nil, client.DatasetManifest{}, errors.New("dataset hard links are unsupported")
		}
		file, err := root.Open(relative)
		if err != nil {
			return nil, client.DatasetManifest{}, errors.New("cannot open dataset file")
		}
		opened, err := file.Stat()
		if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) ||
			opened.Size() > maxDatasetArchiveBytes-int64(bounded.buffer.Len())-2048 {
			_ = file.Close()
			return nil, client.DatasetManifest{}, errors.New("dataset file changed or archive exceeds 64 MiB")
		}
		name := filepath.ToSlash(relative)
		header := &tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0644,
			Size: opened.Size(), ModTime: time.Unix(0, 0), Format: tar.FormatPAX}
		if err := writer.WriteHeader(header); err != nil {
			_ = file.Close()
			return nil, client.DatasetManifest{}, err
		}
		hash := sha256.New()
		_, err = io.CopyN(io.MultiWriter(writer, hash), file, opened.Size())
		if err == nil {
			var extra [1]byte
			count, readErr := file.Read(extra[:])
			if count != 0 || readErr != io.EOF {
				err = errors.New("dataset file changed while archiving")
			}
		}
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return nil, client.DatasetManifest{}, errors.New("cannot read a stable dataset file")
		}
		manifest.Files = append(manifest.Files, client.DatasetFile{Path: name,
			SizeBytes: opened.Size(), SHA256: hex.EncodeToString(hash.Sum(nil))})
	}
	if err := writer.Close(); err != nil {
		return nil, client.DatasetManifest{}, err
	}
	return bounded.buffer.Bytes(), manifest, nil
}
