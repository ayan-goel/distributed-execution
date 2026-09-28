package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (c *Client) UploadDatasetBytes(ctx context.Context, session DatasetUploadSession, archive []byte) (string, error) {
	if c.validateUploadSession(session) != nil || session.declaredSize < 1 ||
		session.declaredSize > maxDownloadBytes || int64(len(archive)) != session.declaredSize ||
		!objectSHA.MatchString(session.declaredSHA256) {
		return "", errors.New("dataset upload grant or archive size is invalid")
	}
	sum := sha256.Sum256(archive)
	if hex.EncodeToString(sum[:]) != session.declaredSHA256 {
		return "", errors.New("dataset archive SHA-256 differs from upload declaration")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, session.UploadURL, bytes.NewReader(archive))
	if err != nil {
		return "", errors.New("invalid dataset upload request")
	}
	for key, values := range session.RequiredHeaders {
		switch strings.ToLower(key) {
		case "host":
			request.Host = values[0]
		case "content-length":
			if values[0] != strconv.FormatInt(session.declaredSize, 10) {
				return "", errors.New("signed dataset size differs from declaration")
			}
		case "content-type":
			if values[0] != "application/octet-stream" {
				return "", errors.New("signed dataset content type is invalid")
			}
			request.Header.Set(key, values[0])
		case "x-amz-checksum-sha256":
			if values[0] != base64.StdEncoding.EncodeToString(sum[:]) {
				return "", errors.New("signed dataset checksum differs from declaration")
			}
			request.Header.Set(key, values[0])
		default:
			request.Header.Set(key, values[0])
		}
	}
	// This data-plane client has no project token, proxy, cookies, decompression,
	// or redirects. A storage grant cannot cause credentials to cross origins.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = 64 << 10
	transport.ResponseHeaderTimeout = 10 * time.Second
	defer transport.CloseIdleConnections()
	transfer := &http.Client{Transport: transport, Timeout: 2 * time.Minute,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := transfer.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", errors.New("dataset transfer failed; retry to obtain a fresh grant")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	version := response.Header.Get("X-Amz-Version-Id")
	if response.StatusCode != http.StatusOK || !visibleASCII(version, 1024) || version == "null" {
		return "", errors.New("dataset transfer did not return an immutable object version")
	}
	return version, nil
}
