package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxDownloadBytes int64 = 64 << 20

var artifactName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var objectKey = regexp.MustCompile(`^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$`)
var objectSHA = regexp.MustCompile(`^[0-9a-f]{64}$`)
var storageHeader = regexp.MustCompile(`^x-amz-[a-z0-9-]+$`)

type downloadObject struct {
	Key       string `json:"key"`
	Version   string `json:"version"`
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
}
type artifactGrant struct {
	Name            string         `json:"name"`
	ArtifactID      string         `json:"artifactId"`
	Object          downloadObject `json:"object"`
	DownloadURL     string         `json:"downloadUrl"`
	Method          string         `json:"method"`
	RequiredHeaders http.Header    `json:"requiredHeaders"`
	ExpiresAt       time.Time      `json:"expiresAt"`
}

func (artifactGrant) String() string { return "artifact download grant (redacted)" }

type DownloadReceipt struct {
	JobID      string `json:"jobId"`
	AttemptID  string `json:"attemptId"`
	ArtifactID string `json:"artifactId"`
	Name       string `json:"name"`
	Path       string `json:"path"`
	Version    string `json:"version"`
	SizeBytes  int64  `json:"sizeBytes"`
	SHA256     string `json:"sha256"`
}

func (c *Client) DownloadArtifact(ctx context.Context, jobID, name, destination string) (DownloadReceipt, error) {
	parsed, err := uuid.Parse(jobID)
	if err != nil || parsed == uuid.Nil || !artifactName.MatchString(name) || destination == "" || strings.HasSuffix(destination, string(os.PathSeparator)) || filepath.Base(destination) == "." || filepath.Base(destination) == ".." {
		return DownloadReceipt{}, errors.New("download requires a job UUID, output name, and destination file")
	}
	root, err := os.OpenRoot(filepath.Dir(destination))
	if err != nil {
		return DownloadReceipt{}, errors.New("cannot open destination directory")
	}
	defer root.Close()
	base := filepath.Base(destination)
	if _, err := root.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		return DownloadReceipt{}, errors.New("destination already exists or cannot be inspected")
	}
	artifact, attempt, err := c.acceptedArtifact(ctx, parsed.String(), name)
	if err != nil {
		return DownloadReceipt{}, err
	}
	if err := c.downloadFile(ctx, root, base, artifact); err != nil {
		return DownloadReceipt{}, err
	}
	return DownloadReceipt{JobID: parsed.String(), AttemptID: attempt, ArtifactID: artifact.ArtifactID, Name: name, Path: destination, Version: artifact.Object.Version, SizeBytes: artifact.Object.SizeBytes, SHA256: artifact.Object.SHA256}, nil
}

func (c *Client) acceptedArtifact(ctx context.Context, jobID, name string) (artifactGrant, string, error) {
	body, err := c.requestBody(ctx, http.MethodGet, "/v1/jobs/"+jobID+"/artifacts", nil, "")
	if err != nil {
		return artifactGrant{}, "", err
	}
	var list struct {
		JobID             string          `json:"jobId"`
		State             string          `json:"state"`
		AcceptedAttemptID *string         `json:"acceptedAttemptId"`
		Artifacts         []artifactGrant `json:"artifacts"`
	}
	if json.Unmarshal(body, &list) != nil || list.JobID != jobID || len(list.Artifacts) > 64 {
		return artifactGrant{}, "", errors.New("invalid artifact metadata response")
	}
	if list.State != "SUCCEEDED" {
		if list.AcceptedAttemptID != nil || len(list.Artifacts) != 0 || !slices.Contains([]string{"QUEUED", "ACTIVE", "RETRY_WAIT", "CANCELLING", "CANCELLED", "FAILED"}, list.State) {
			return artifactGrant{}, "", errors.New("invalid accepted artifact state")
		}
		return artifactGrant{}, "", errors.New("job has no accepted successful result")
	}
	if list.AcceptedAttemptID == nil || !downloadUUID(*list.AcceptedAttemptID) {
		return artifactGrant{}, "", errors.New("invalid accepted attempt identity")
	}
	names, ids := map[string]bool{}, map[string]bool{}
	var selected artifactGrant
	for _, a := range list.Artifacts {
		if names[a.Name] || ids[a.ArtifactID] || c.validateGrant(a) != nil {
			return artifactGrant{}, "", errors.New("invalid or expired artifact download grant")
		}
		names[a.Name], ids[a.ArtifactID] = true, true
		if a.Name == name {
			selected = a
		}
	}
	if selected.ArtifactID == "" {
		return artifactGrant{}, "", errors.New("accepted output name not found")
	}
	return selected, *list.AcceptedAttemptID, nil
}

func downloadUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func (c *Client) validateGrant(a artifactGrant) error {
	invalid := errors.New("invalid artifact grant")
	o := a.Object
	if !artifactName.MatchString(a.Name) || !downloadUUID(a.ArtifactID) || len(o.Key) > 1024 || !objectKey.MatchString(o.Key) || !visibleASCII(o.Version, 1024) || o.Version == "null" || o.SizeBytes < 0 || o.SizeBytes > maxDownloadBytes || !objectSHA.MatchString(o.SHA256) || a.Method != http.MethodGet || !a.ExpiresAt.After(time.Now()) || a.ExpiresAt.After(time.Now().Add(5*time.Minute)) || len(a.DownloadURL) > 64<<10 || len(a.RequiredHeaders) > 16 {
		return invalid
	}
	u, err := url.Parse(a.DownloadURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" || strings.Contains(a.DownloadURL, "#") || !strings.HasSuffix(u.Path, "/"+o.Key) {
		return invalid
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && c.devInsecure && ip != nil && ip.IsLoopback()) {
		return invalid
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query["versionId"]) != 1 || query.Get("versionId") != o.Version {
		return invalid
	}
	seen := map[string]bool{}
	for key, values := range a.RequiredHeaders {
		lower := strings.ToLower(key)
		// Only storage-signature headers may cross this boundary. In particular, a
		// response cannot cause the project token, cookies, or a Range request to leak.
		if seen[lower] || len(values) != 1 || !visibleASCII(values[0], 8192) || lower != "host" && !storageHeader.MatchString(lower) {
			return invalid
		}
		if lower == "host" && values[0] != u.Host {
			return invalid
		}
		seen[lower] = true
	}
	return nil
}

func (c *Client) downloadFile(ctx context.Context, root *os.Root, base string, a artifactGrant) error {
	if err := c.validateGrant(a); err != nil {
		return errors.New("artifact grant expired or is invalid; retry the download")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, a.DownloadURL, nil)
	if err != nil {
		return errors.New("invalid download request")
	}
	for key, values := range a.RequiredHeaders {
		if strings.EqualFold(key, "Host") {
			request.Host = values[0]
		} else {
			request.Header.Set(key, values[0])
		}
	}
	// Transfer traffic has no project credentials, cookies, proxies, automatic
	// decompression, or redirects. Bounds apply even when the backend misbehaves.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = 64 << 10
	transport.ResponseHeaderTimeout = 10 * time.Second
	defer transport.CloseIdleConnections()
	transfer := &http.Client{Transport: transport, Timeout: 2 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := transfer.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("artifact transfer failed; retry to obtain a fresh grant")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("X-Amz-Version-Id") != a.Object.Version || response.Header.Get("Content-Encoding") != "" || response.ContentLength >= 0 && response.ContentLength != a.Object.SizeBytes {
		return errors.New("artifact response does not match the accepted version and size")
	}
	temp := ".dispatch-" + rand.Text() + ".partial"
	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("cannot create temporary output")
	}
	defer root.Remove(temp)
	defer file.Close()
	hash := sha256.New()
	count, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, a.Object.SizeBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("artifact transfer interrupted or local output write failed")
	}
	if count != a.Object.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != a.Object.SHA256 {
		return errors.New("artifact size or SHA-256 does not match accepted metadata")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return errors.New("cannot sync verified output")
	}
	if err := file.Close(); err != nil {
		return errors.New("cannot close verified output")
	}
	// Link publishes a fully verified file atomically without replacing an existing
	// file or symlink. All operations stay relative to the opened destination folder.
	if err := root.Link(temp, base); err != nil {
		return errors.New("cannot publish verified output; destination may already exist")
	}
	if err := root.Remove(temp); err != nil {
		return errors.New("verified output published, but temporary-file cleanup failed")
	}
	directory, err := root.Open(".")
	if err != nil {
		return errors.New("verified output published, but directory sync unavailable")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errors.New("verified output published, but directory sync failed")
	}
	return nil
}
