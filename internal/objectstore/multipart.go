package objectstore

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const MinMultipartPartBytes int64 = 5 << 20
const MaxMultipartParts = 10000

var ErrMultipartGone = errors.New("multipart upload no longer exists")

type MultipartUpload struct {
	Key, UploadID  string
	Size, PartSize int64
}

func (MultipartUpload) String() string { return "multipart upload (redacted)" }

type CompletedPart struct {
	Number       int32
	ETag, SHA256 string
}

func (s *Store) validMultipartPlan(u MultipartUpload) bool {
	// Bound both part grants and completion XML; the final part alone may be small.
	// The total still obeys the configured object cap, including after replay.
	return validKey(u.Key) && u.Size > 0 && u.Size <= s.maxBytes && u.PartSize >= MinMultipartPartBytes && u.PartSize <= MaxSinglePartBytes && 1+(u.Size-1)/u.PartSize <= MaxMultipartParts
}

func validOpaque(value string) bool {
	return len(value) > 0 && len(value) <= 1024 && strings.IndexFunc(value, func(c rune) bool { return c < 33 || c > 126 }) == -1
}

// BeginMultipart creates storage state, not an accepted artifact. The caller must
// persist its returned identity before exposing grants or attempting completion.
func (s *Store) BeginMultipart(ctx context.Context, key string, size, partSize int64) (MultipartUpload, error) {
	u := MultipartUpload{Key: key, Size: size, PartSize: partSize}
	if !s.validMultipartPlan(u) {
		return MultipartUpload{}, ErrInvalid
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return MultipartUpload{}, err
	}
	defer release()
	if err := s.checkVersioning(ctx); err != nil {
		return MultipartUpload{}, err
	}
	result, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(key), ContentType: aws.String("application/octet-stream"), ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ChecksumType: types.ChecksumTypeComposite})
	if err != nil {
		return MultipartUpload{}, storageError(err)
	}
	u.UploadID = aws.ToString(result.UploadId)
	if !validOpaque(u.UploadID) {
		return MultipartUpload{}, ErrIntegrity
	}
	return u, nil
}

func (s *Store) PresignPart(ctx context.Context, u MultipartUpload, number int32, checksum string, ttl time.Duration) (Grant, error) {
	if !s.validMultipartPlan(u) || !validOpaque(u.UploadID) || number < 1 || int64(number) > 1+(u.Size-1)/u.PartSize || !validHash(checksum) || !validTTL(ttl) {
		return Grant{}, ErrInvalid
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return Grant{}, err
	}
	defer release()
	if err := s.checkVersioning(ctx); err != nil {
		return Grant{}, err
	}
	size := min(u.PartSize, u.Size-int64(number-1)*u.PartSize)
	hash, _ := hex.DecodeString(checksum)
	issued := time.Now().Truncate(time.Second)
	// Each bearer capability binds one key, backend upload ID, part number, exact
	// byte count, and checksum. It cannot complete or accept an attempt result.
	signed, err := s.signer.PresignUploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(s.bucket), Key: aws.String(u.Key), UploadId: aws.String(u.UploadID), PartNumber: aws.Int32(number), ContentLength: aws.Int64(size), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(hash))}, s3.WithPresignExpires(ttl))
	if err != nil {
		return Grant{}, storageError(err)
	}
	return Grant{URL: signed.URL, Method: signed.Method, Headers: signed.SignedHeader.Clone(), ExpiresAt: issued.Add(ttl)}, nil
}

// CompleteMultipart returns storage's exact immutable version. Full-object Verify
// and the fenced metadata transaction remain required before accepting artifacts.
func (s *Store) CompleteMultipart(ctx context.Context, u MultipartUpload, parts []CompletedPart) (string, error) {
	if !s.validMultipartPlan(u) || !validOpaque(u.UploadID) || int64(len(parts)) != 1+(u.Size-1)/u.PartSize {
		return "", ErrInvalid
	}
	completed := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		if p.Number != int32(i+1) || !validOpaque(p.ETag) || !validHash(p.SHA256) {
			return "", ErrInvalid
		}
		hash, _ := hex.DecodeString(p.SHA256)
		completed[i] = types.CompletedPart{PartNumber: aws.Int32(p.Number), ETag: aws.String(p.ETag), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(hash))}
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	if err := s.checkVersioning(ctx); err != nil {
		return "", err
	}
	// The SDK parses embedded errors even when HTTP status is 200. No automatic
	// retry or latest-version fallback may hide an uncertain completion response.
	result, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(u.Key), UploadId: aws.String(u.UploadID), MultipartUpload: &types.CompletedMultipartUpload{Parts: completed}, ChecksumType: types.ChecksumTypeComposite})
	if err != nil {
		return "", multipartError(err)
	}
	version := aws.ToString(result.VersionId)
	if !validOpaque(version) || version == "null" || result.Key != nil && *result.Key != u.Key {
		return "", ErrIntegrity
	}
	return version, nil
}

func (s *Store) AbortMultipart(ctx context.Context, u MultipartUpload) error {
	// Lowering admission limits must not strand uploads created under older limits.
	if !validKey(u.Key) || !validOpaque(u.UploadID) {
		return ErrInvalid
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return err
	}
	defer release()
	// Cleanup targets only this upload, never other uploads or completed versions.
	// Callers must stop in-flight transfers and repeat after grant expiry as needed.
	_, err = s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(s.bucket), Key: aws.String(u.Key), UploadId: aws.String(u.UploadID)})
	if err != nil && !errors.Is(multipartError(err), ErrMultipartGone) {
		return storageError(err)
	}
	result, err := s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(s.bucket), Key: aws.String(u.Key), UploadId: aws.String(u.UploadID), MaxParts: aws.Int32(1)})
	if err != nil {
		if errors.Is(multipartError(err), ErrMultipartGone) {
			return nil
		}
		return storageError(err)
	}
	if len(result.Parts) != 0 || aws.ToBool(result.IsTruncated) {
		return ErrUnavailable
	}
	return nil
}

func multipartError(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "NoSuchUpload" {
		return ErrMultipartGone
	}
	return storageError(err)
}
