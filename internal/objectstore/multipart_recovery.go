package objectstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

const initializationMetadataKey = "dispatch-initialization-id"
const maxRecoveryVersions = 16

func validInitializationID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

// RecoverMultipartVersion locates one exact version carrying server-bound
// initialization metadata. It neither verifies bytes nor accepts a job result.
func (s *Store) RecoverMultipartVersion(ctx context.Context, u MultipartUpload, initializationID string) (string, error) {
	if !s.validMultipartPlan(u) || !validOpaque(u.UploadID) || !validInitializationID(initializationID) {
		return "", ErrInvalid
	}
	ctx, release, err := s.slot(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	if err := s.checkVersioning(ctx); err != nil {
		return "", err
	}
	// A generated upload key normally has one version. Refuse a truncated or
	// oversized inventory instead of guessing among unseen versions or doing
	// unbounded backend work while a finalization deadline is running.
	listed, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(s.bucket), Prefix: aws.String(u.Key), MaxKeys: aws.Int32(maxRecoveryVersions)})
	if err != nil {
		return "", storageError(err)
	}
	if aws.ToBool(listed.IsTruncated) || len(listed.Versions)+len(listed.DeleteMarkers) > maxRecoveryVersions {
		return "", ErrUnavailable
	}
	found := ""
	seen := make(map[string]bool, len(listed.Versions))
	for _, candidate := range listed.Versions {
		if aws.ToString(candidate.Key) != u.Key {
			continue
		}
		version := aws.ToString(candidate.VersionId)
		if !validOpaque(version) || version == "null" || seen[version] {
			return "", ErrIntegrity
		}
		seen[version] = true
		// Every metadata read binds a listed immutable version. Latest-object HEAD
		// cannot establish which upload completed after an ambiguous response.
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(u.Key), VersionId: aws.String(version)})
		if err != nil {
			return "", storageError(err)
		}
		if aws.ToString(head.VersionId) != version {
			return "", ErrIntegrity
		}
		if head.Metadata[initializationMetadataKey] != initializationID {
			continue
		}
		if found != "" || head.ContentLength == nil || *head.ContentLength != u.Size {
			return "", ErrIntegrity
		}
		found = version
	}
	if found == "" {
		return "", ErrMultipartGone
	}
	return found, nil
}
