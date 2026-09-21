package store

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

const MaxCompletionMetricsBytes = 64 << 10
const MaxCompletionMetrics = 256
const MaxCompletionGaps = 1024

var metricNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$`)

type CompletionOutput struct {
	Name       string `json:"name"`
	ArtifactID string `json:"artifactId"`
}

type CompletionLogGap struct {
	Stream string `json:"stream"`
	First  int64  `json:"first"`
	Last   int64  `json:"last"`
}

type CompletionRequest struct {
	Authority                   AttemptAuthority
	CompletionID, PayloadSHA256 string
	ExitCode                    *int32
	Reason                      string
	Stopped                     bool
	Outputs                     []CompletionOutput
	LogsComplete                bool
	Gaps                        []CompletionLogGap
	MetricsJSON                 []byte
}

type completionPayload struct {
	Authority           AttemptAuthority       `json:"authority"`
	ExitCode            *int32                 `json:"exitCode"`
	Reason              string                 `json:"reason"`
	Stopped             bool                   `json:"stopped"`
	Outputs             []CompletionOutput     `json:"outputs"`
	LogsComplete        bool                   `json:"logsComplete"`
	Gaps                []CompletionLogGap     `json:"gaps"`
	Metrics             map[string]json.Number `json:"metrics"`
	MetricsSourceSHA256 string                 `json:"metricsSourceSha256"`
}

// CompletionDigest excludes the request ID and claimed digest, while binding all
// result evidence. Set-like output/gap ordering is normalized without modifying
// the caller's buffers. Metric source bytes stay bound to their verified artifact.
func CompletionDigest(r CompletionRequest) (string, error) {
	payload, err := normalizeCompletion(r)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", ErrInvalid
	}
	hash := sha256.Sum256(append([]byte("dispatch.worker.v1.CompleteAttempt\n"), body...))
	return hex.EncodeToString(hash[:]), nil
}

func normalizeCompletion(r CompletionRequest) (completionPayload, error) {
	a := r.Authority
	if !canonicalUUID(r.CompletionID) || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.WorkerID) || !canonicalUUID(a.SessionID) || a.Generation < 1 || len(r.Outputs) > 64 || len(r.Gaps) > MaxCompletionGaps || r.ExitCode != nil && (*r.ExitCode < 0 || *r.ExitCode > 255) {
		return completionPayload{}, ErrInvalid
	}
	if !slices.Contains([]string{"", "RUNTIME_UNAVAILABLE", "TRANSFER_FAILED", "APPLICATION_EXIT", "OOM", "STARTUP_TIMEOUT", "EXECUTION_TIMEOUT", "OUTPUT_INVALID", "USER_CANCELLED", "FINALIZATION_TIMEOUT"}, r.Reason) {
		return completionPayload{}, ErrInvalid
	}
	if r.Reason == "" && (r.ExitCode == nil || *r.ExitCode != 0 || !r.Stopped) || r.Reason == "APPLICATION_EXIT" && (r.ExitCode == nil || *r.ExitCode == 0) {
		return completionPayload{}, ErrInvalid
	}
	p := completionPayload{Authority: a, ExitCode: r.ExitCode, Reason: r.Reason, Stopped: r.Stopped, Outputs: append([]CompletionOutput{}, r.Outputs...), LogsComplete: r.LogsComplete, Gaps: append([]CompletionLogGap{}, r.Gaps...)}
	slices.SortFunc(p.Outputs, func(a, b CompletionOutput) int { return cmp.Compare(a.Name, b.Name) })
	ids := map[string]bool{}
	for i, o := range p.Outputs {
		if !uploadNamePattern.MatchString(o.Name) || !canonicalUUID(o.ArtifactID) || ids[o.ArtifactID] || i > 0 && p.Outputs[i-1].Name == o.Name {
			return completionPayload{}, ErrInvalid
		}
		ids[o.ArtifactID] = true
	}
	if r.LogsComplete && len(r.Gaps) != 0 {
		return completionPayload{}, ErrInvalid
	}
	slices.SortFunc(p.Gaps, func(a, b CompletionLogGap) int {
		if c := cmp.Compare(a.Stream, b.Stream); c != 0 {
			return c
		}
		return cmp.Compare(a.First, b.First)
	})
	for i, g := range p.Gaps {
		if !slices.Contains([]string{"stdout", "stderr"}, g.Stream) || g.First < 1 || g.Last < g.First || i > 0 && p.Gaps[i-1].Stream == g.Stream && p.Gaps[i-1].Last >= g.First {
			return completionPayload{}, ErrInvalid
		}
	}
	metrics, err := completionMetrics(r.MetricsJSON)
	if err != nil {
		return completionPayload{}, err
	}
	p.Metrics = metrics
	if len(r.MetricsJSON) != 0 {
		hash := sha256.Sum256(r.MetricsJSON)
		p.MetricsSourceSHA256 = hex.EncodeToString(hash[:])
	}
	return p, nil
}

func completionMetrics(body []byte) (map[string]json.Number, error) {
	metrics := map[string]json.Number{}
	if len(body) == 0 {
		return metrics, nil
	}
	if len(body) > MaxCompletionMetricsBytes || !utf8.Valid(body) {
		return nil, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, ErrInvalid
	}
	for decoder.More() {
		name, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalid
		}
		key, ok := name.(string)
		if !ok || !metricNamePattern.MatchString(key) || len(metrics) >= MaxCompletionMetrics {
			return nil, ErrInvalid
		}
		if _, exists := metrics[key]; exists {
			return nil, ErrInvalid
		}
		token, err := decoder.Token()
		if err != nil {
			return nil, ErrInvalid
		}
		number, ok := token.(json.Number)
		if !ok || len(number.String()) > 1024 {
			return nil, ErrInvalid
		}
		// Finite float64 values are the interoperable export range. Preserve JSON
		// number text so large integer metrics are not silently rounded in storage.
		value, err := number.Float64()
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, ErrInvalid
		}
		if value == 0 {
			mantissa, _, _ := strings.Cut(strings.ToLower(number.String()), "e")
			if strings.Trim(mantissa, "-+.0") != "" {
				return nil, ErrInvalid
			}
			// Reject nonzero underflow rather than silently reporting zero. True
			// zero uses a bounded representation even with an extreme exponent.
			number = json.Number("0")
		}
		metrics[key] = number
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, ErrInvalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	return metrics, nil
}
