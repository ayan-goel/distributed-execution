package main

import (
	"context"
	"errors"
	"os"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
)

func configuredObjectStore(ctx context.Context, c serveConfig) (*objectstore.Store, error) {
	if c.objectEndpoint == "" {
		return nil, nil
	}
	// Storage credentials stay in the server environment. Never forward them to
	// workers, include them in flags/logs, or fall back to ambient AWS credentials.
	objects, err := objectstore.New(objectstore.Config{Endpoint: c.objectEndpoint, Region: c.objectRegion, Bucket: c.objectBucket, AllowLoopbackHTTP: c.objectLoopback, AccessKey: os.Getenv("DISPATCH_S3_ACCESS_KEY"), SecretKey: os.Getenv("DISPATCH_S3_SECRET_KEY"), SessionToken: os.Getenv("DISPATCH_S3_SESSION_TOKEN")})
	if err != nil {
		return nil, errors.New("invalid object storage configuration or missing DISPATCH_S3 credentials")
	}
	// Fail before either listener opens; a configured but unversioned bucket
	// cannot preserve accepted exact versions. Each upload rechecks this later.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := objects.CheckVersioning(ctx); err != nil {
		return nil, errors.New("object storage must be reachable with bucket versioning enabled")
	}
	return objects, nil
}
