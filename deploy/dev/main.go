// Command dev enables versioning only on the local Compose development bucket.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	if err := initialize(); err != nil {
		// Storage errors may contain signed requests; keep capabilities out of output.
		fmt.Fprintln(os.Stderr, "local storage initialization failed; check the dispatch-dev Compose services")
		os.Exit(1)
	}
}

func initialize() error {
	cfg := objectstore.Config{Endpoint: os.Getenv("DISPATCH_OBJECT_ENDPOINT"), Region: "us-east-1", Bucket: "dispatch-dev",
		AccessKey: os.Getenv("DISPATCH_S3_ACCESS_KEY"), SecretKey: os.Getenv("DISPATCH_S3_SECRET_KEY"), AllowLoopbackHTTP: true}
	origin, err := url.Parse(cfg.Endpoint)
	if err != nil || origin.Scheme != "http" || !net.ParseIP(origin.Hostname()).IsLoopback() {
		return errors.New("development storage must use a literal loopback HTTP endpoint")
	}
	store, err := objectstore.New(cfg)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := s3.New(s3.Options{BaseEndpoint: aws.String(cfg.Endpoint), Region: cfg.Region, UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""), Retryer: aws.NopRetryer{},
		HTTPClient: &http.Client{Transport: transport, Timeout: 5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.Bucket)}); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	// Exact-version publication depends on this setting; recheck after every startup.
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(cfg.Bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		return err
	}
	return store.CheckVersioning(ctx)
}
