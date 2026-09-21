// Package objectstore provides bounded transfers of exact S3 object versions.
package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const MaxSinglePartBytes int64 = 64 << 20
const operationTimeout = 30 * time.Second

var (
	ErrInvalid     = errors.New("invalid object storage request or configuration")
	ErrUnavailable = errors.New("object storage unavailable")
	ErrVersioning  = errors.New("object bucket versioning is not enabled")
	ErrIntegrity   = errors.New("object version, size, or checksum does not match")
)

type Object struct {
	Key     string
	Version string
	Size    int64
	SHA256  string
}

type Grant struct {
	URL       string
	Method    string
	Headers   http.Header
	ExpiresAt time.Time
}

func (Grant) String() string { return "scoped object transfer grant (redacted)" }

type Store struct {
	client   *s3.Client
	signer   *s3.PresignClient
	bucket   string
	maxBytes int64
	slots    chan struct{}
}

func (s *Store) slot(parent context.Context) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, nil, err
	}
	select {
	case s.slots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-s.slots
			cancel()
			return nil, nil, err
		}
		return ctx, func() { <-s.slots; cancel() }, nil
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	}
}

func (s *Store) CheckVersioning(ctx context.Context) error {
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return err
	}
	defer release()
	return s.checkVersioning(ctx)
}
func (s *Store) checkVersioning(ctx context.Context) error {
	result, err := s.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		return storageError(err)
	}
	if result.Status != types.BucketVersioningStatusEnabled {
		return ErrVersioning
	}
	return nil
}

func (s *Store) PresignUpload(ctx context.Context, key string, size int64, checksum string, ttl time.Duration) (Grant, error) {
	if !validKey(key) || size < 0 || size > s.maxBytes || !validHash(checksum) || !validTTL(ttl) {
		return Grant{}, ErrInvalid
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return Grant{}, err
	}
	defer release()
	// A suspended bucket can yield replaceable null versions. Recheck before
	// issuing a capability; verification independently rejects null versions.
	if err := s.checkVersioning(ctx); err != nil {
		return Grant{}, err
	}
	hash, _ := hex.DecodeString(checksum)
	// SigV4 timestamps use whole seconds. Round down so the advertised expiry
	// never promises capability lifetime beyond the actual signature.
	issued := time.Now().Truncate(time.Second)
	signed, err := s.signer.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), ContentLength: aws.Int64(size),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(hash)),
		ContentType:    aws.String("application/octet-stream"),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return Grant{}, storageError(err)
	}
	// Only this key, byte count, and SHA-256 are signed. Workers receive this
	// short-lived capability, never the server's static storage credentials.
	return Grant{URL: signed.URL, Method: signed.Method, Headers: signed.SignedHeader.Clone(), ExpiresAt: issued.Add(ttl)}, nil
}

func (s *Store) PresignDownload(ctx context.Context, object Object, ttl time.Duration) (Grant, error) {
	if !s.validObject(object) || !validTTL(ttl) {
		return Grant{}, ErrInvalid
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return Grant{}, err
	}
	defer release()
	issued := time.Now().Truncate(time.Second)
	signed, err := s.signer.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(object.Key), VersionId: aws.String(object.Version),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return Grant{}, storageError(err)
	}
	return Grant{URL: signed.URL, Method: signed.Method, Headers: signed.SignedHeader.Clone(), ExpiresAt: issued.Add(ttl)}, nil
}

func (s *Store) Verify(ctx context.Context, object Object) error {
	if !s.validObject(object) {
		return ErrInvalid
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return err
	}
	defer release()
	// Exact-version reads happen outside state transactions. An accepted version
	// must never silently fall back to the key's current object after overwrite.
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(object.Key), VersionId: aws.String(object.Version),
	})
	if err != nil {
		return storageError(err)
	}
	defer result.Body.Close()
	if aws.ToString(result.VersionId) != object.Version || result.ContentLength == nil || *result.ContentLength != object.Size {
		return ErrIntegrity
	}
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(result.Body, object.Size+1))
	if err != nil {
		return storageError(err)
	}
	// ETag is not a universal content digest, and metadata can originate from the
	// uploader. Streaming SHA-256 establishes byte integrity without buffering it.
	if count != object.Size || hex.EncodeToString(hash.Sum(nil)) != object.SHA256 {
		return ErrIntegrity
	}
	return nil
}

func storageError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	// SDK errors can contain presigned URLs and credential identifiers. Only
	// stable categories cross this boundary or reach caller diagnostics.
	return ErrUnavailable
}
