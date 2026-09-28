package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

type DatasetUploadSession struct {
	UploadID        string      `json:"uploadId"`
	ObjectKey       string      `json:"objectKey"`
	UploadURL       string      `json:"uploadUrl"`
	Method          string      `json:"method"`
	RequiredHeaders http.Header `json:"requiredHeaders"`
	ExpiresAt       time.Time   `json:"expiresAt"`
	Replayed        bool        `json:"replayed"`
	declaredSize    int64       `json:"-"`
	declaredSHA256  string      `json:"-"`
}

func (DatasetUploadSession) String() string { return "dataset upload grant (redacted)" }

type DatasetFile struct {
	Path      string `json:"path"`
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
}

type DatasetManifest struct {
	Format string        `json:"format"`
	Files  []DatasetFile `json:"files"`
}

type DatasetRegistration struct {
	DatasetID     string          `json:"datasetId"`
	Name          string          `json:"name"`
	UploadID      string          `json:"uploadId"`
	ObjectKey     string          `json:"objectKey"`
	ObjectVersion string          `json:"objectVersion"`
	SizeBytes     int64           `json:"sizeBytes"`
	SHA256        string          `json:"sha256"`
	Manifest      DatasetManifest `json:"manifest"`
	Replayed      bool            `json:"replayed"`
}

func (c *Client) CreateDatasetUpload(ctx context.Context, requestID, name string, size int64, sha256 string) (DatasetUploadSession, error) {
	if !downloadUUID(requestID) || !artifactName.MatchString(name) || size < 1 || size > maxDownloadBytes || !objectSHA.MatchString(sha256) {
		return DatasetUploadSession{}, errors.New("invalid dataset upload declaration")
	}
	body, _ := json.Marshal(struct {
		RequestID string `json:"requestId"`
		Name      string `json:"name"`
		SizeBytes int64  `json:"sizeBytes"`
		SHA256    string `json:"sha256"`
	}{requestID, name, size, sha256})
	response, err := c.requestBody(ctx, http.MethodPost, "/v1/datasets/uploads", body, "")
	if err != nil {
		return DatasetUploadSession{}, err
	}
	var session DatasetUploadSession
	if json.Unmarshal(response, &session) != nil || c.validateUploadSession(session) != nil {
		return DatasetUploadSession{}, errors.New("invalid dataset upload session")
	}
	session.declaredSize = size
	session.declaredSHA256 = sha256
	return session, nil
}

func validDatasetSessionIdentity(session DatasetUploadSession) bool {
	if !downloadUUID(session.UploadID) || len(session.ObjectKey) > 1024 || !objectKey.MatchString(session.ObjectKey) {
		return false
	}
	parts := strings.Split(session.ObjectKey, "/")
	return len(parts) == 5 && parts[0] == "projects" && downloadUUID(parts[1]) &&
		parts[2] == "datasets" && parts[3] == "uploads" && parts[4] == session.UploadID
}

func (c *Client) validateUploadSession(session DatasetUploadSession) error {
	invalid := errors.New("invalid dataset upload session")
	if !validDatasetSessionIdentity(session) || session.Method != http.MethodPut || !session.ExpiresAt.After(time.Now()) ||
		session.ExpiresAt.After(time.Now().Add(5*time.Minute)) || len(session.UploadURL) > 64<<10 ||
		len(session.RequiredHeaders) > 16 {
		return invalid
	}
	u, err := url.Parse(session.UploadURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" ||
		u.Fragment != "" || !strings.HasSuffix(u.Path, "/"+session.ObjectKey) {
		return invalid
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && c.devInsecure && ip != nil && ip.IsLoopback()) {
		return invalid
	}
	for key, values := range session.RequiredHeaders {
		lower := strings.ToLower(key)
		if len(values) != 1 || !visibleASCII(values[0], 8192) || lower != "host" &&
			lower != "content-type" && lower != "content-length" && !storageHeader.MatchString(lower) {
			return invalid
		}
		if lower == "host" && values[0] != u.Host {
			return invalid
		}
	}
	return nil
}

func (c *Client) CompleteDatasetUpload(ctx context.Context, session DatasetUploadSession, version string, manifest DatasetManifest) (DatasetRegistration, error) {
	// Completion uses the durable upload identity, not the short-lived grant.
	// A large but valid transfer may finish after its URL has expired.
	if !validDatasetSessionIdentity(session) || !visibleASCII(version, 1024) || version == "null" ||
		manifest.Format != "tar.v1" || len(manifest.Files) == 0 || len(manifest.Files) > 1024 {
		return DatasetRegistration{}, errors.New("invalid dataset completion request")
	}
	body, err := json.Marshal(struct {
		Version  string          `json:"version"`
		Manifest DatasetManifest `json:"manifest"`
	}{version, manifest})
	if err != nil || len(body) > 2<<20 {
		return DatasetRegistration{}, errors.New("dataset manifest exceeds 2 MiB")
	}
	response, err := c.requestBody(ctx, http.MethodPost, "/v1/datasets/uploads/"+session.UploadID+"/complete", body, "")
	if err != nil {
		return DatasetRegistration{}, err
	}
	var registered DatasetRegistration
	if json.Unmarshal(response, &registered) != nil || !downloadUUID(registered.DatasetID) ||
		registered.UploadID != session.UploadID || registered.ObjectKey != session.ObjectKey ||
		registered.ObjectVersion != version || registered.SizeBytes < 1 || registered.SizeBytes > maxDownloadBytes ||
		!artifactName.MatchString(registered.Name) || !objectSHA.MatchString(registered.SHA256) ||
		!reflect.DeepEqual(registered.Manifest, manifest) {
		return DatasetRegistration{}, errors.New("invalid dataset registration response")
	}
	if session.declaredSize != 0 && (registered.SizeBytes != session.declaredSize || registered.SHA256 != session.declaredSHA256) {
		return DatasetRegistration{}, errors.New("dataset registration disagrees with upload declaration")
	}
	return registered, nil
}
