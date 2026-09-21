package objectstore

import (
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	Endpoint          string
	Region            string
	Bucket            string
	AccessKey         string
	SecretKey         string
	SessionToken      string
	AllowLoopbackHTTP bool
	MaxObjectBytes    int64
	MaxConcurrent     int
}

func (Config) String() string { return "object storage configuration (redacted)" }

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
var regionPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
var keyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$`)
var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func New(cfg Config) (*Store, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || (endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.Opaque != "" {
		return nil, ErrInvalid
	}
	ip := net.ParseIP(endpoint.Hostname())
	loopback := endpoint.Hostname() == "localhost" || ip != nil && ip.IsLoopback()
	if endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && cfg.AllowLoopbackHTTP && loopback) {
		return nil, ErrInvalid
	}
	if !bucketPattern.MatchString(cfg.Bucket) || !regionPattern.MatchString(cfg.Region) || !credential(cfg.AccessKey, false) || !credential(cfg.SecretKey, false) || !credential(cfg.SessionToken, true) {
		return nil, ErrInvalid
	}
	if cfg.MaxObjectBytes == 0 {
		cfg.MaxObjectBytes = MaxSinglePartBytes
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 4
	}
	if cfg.MaxObjectBytes < 1 || cfg.MaxObjectBytes > MaxSinglePartBytes || cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 64 {
		return nil, ErrInvalid
	}
	endpoint.Path = ""
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.Proxy = nil
	base.MaxConnsPerHost = cfg.MaxConcurrent
	base.MaxIdleConnsPerHost = cfg.MaxConcurrent
	base.MaxResponseHeaderBytes = 64 << 10
	base.ResponseHeaderTimeout = 10 * time.Second
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint.String()), Region: cfg.Region, UsePathStyle: true,
		// Explicit credentials and endpoint avoid ambient AWS profiles, metadata
		// services, or accidental use of the operator's unrelated cloud account.
		Credentials:                credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		Retryer:                    aws.NopRetryer{},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		HTTPClient: &http.Client{
			Transport:     guardedTransport{base: base, origin: *endpoint, maxBytes: cfg.MaxObjectBytes},
			Timeout:       operationTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return ErrUnavailable },
		},
	})
	return &Store{client: client, signer: s3.NewPresignClient(client), bucket: cfg.Bucket, maxBytes: cfg.MaxObjectBytes, slots: make(chan struct{}, cfg.MaxConcurrent)}, nil
}

func credential(value string, optional bool) bool {
	if len(value) > 4096 || value == "" && !optional {
		return false
	}
	for _, char := range value {
		if char < 33 || char > 126 {
			return false
		}
	}
	return true
}
func validKey(key string) bool   { return len(key) <= 1024 && keyPattern.MatchString(key) }
func validHash(hash string) bool { return hashPattern.MatchString(hash) }
func validTTL(ttl time.Duration) bool {
	return ttl >= time.Second && ttl <= 5*time.Minute && ttl%time.Second == 0
}
func (s *Store) validObject(object Object) bool {
	return validKey(object.Key) && object.Size >= 0 && object.Size <= s.maxBytes && validHash(object.SHA256) && object.Version != "" && object.Version != "null" && len(object.Version) <= 1024 && strings.IndexFunc(object.Version, func(c rune) bool { return c < 33 || c > 126 }) == -1
}
