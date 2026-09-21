package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCompletionDigestGoldenAndMetricBounds(t *testing.T) {
	id := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }
	r := completionRequest()
	r.Authority = AttemptAuthority{JobID: id(1), AttemptID: id(2), Generation: 7, WorkerID: id(3), SessionID: id(4)}
	r.CompletionID = id(5)
	r.Outputs = []CompletionOutput{{Name: "result", ArtifactID: id(6)}}
	r.LogsComplete = false
	r.Gaps = []CompletionLogGap{{Stream: "stderr", First: 1, Last: 2}}
	r.MetricsJSON = []byte(`{"loss":0.25}`)
	if hash, err := CompletionDigest(r); err != nil || hash != "10bae250e1af14636713d63053f8b8939cd09e3267514d40fa1ac43ebd8c81f8" {
		t.Fatal("completion digest contract changed", hash, err)
	}
	entries := make([]string, MaxCompletionMetrics+1)
	for i := range entries {
		entries[i] = fmt.Sprintf(`"metric%d":%d`, i, i)
	}
	r.MetricsJSON = []byte("{" + strings.Join(entries[:MaxCompletionMetrics], ",") + "}")
	if _, err := CompletionDigest(r); err != nil {
		t.Fatal("maximum metric count rejected", err)
	}
	r.MetricsJSON = []byte("{" + strings.Join(entries, ",") + "}")
	if _, err := CompletionDigest(r); !errors.Is(err, ErrInvalid) {
		t.Fatal("metric count exceeded", err)
	}
	r.MetricsJSON = []byte(`{"zero":0e-99999}`)
	if payload, err := normalizeCompletion(r); err != nil || payload.Metrics["zero"] != "0" {
		t.Fatal("zero representation not bounded", err)
	}
}

func completionRequest() CompletionRequest {
	code := int32(0)
	return CompletionRequest{Authority: uploadRequest().Authority, CompletionID: uuid.NewString(), ExitCode: &code, Stopped: true, LogsComplete: true, Outputs: []CompletionOutput{{Name: "result", ArtifactID: uuid.NewString()}}}
}

func TestCompletionDigestNormalizesSetsAndBindsEvidence(t *testing.T) {
	r := completionRequest()
	r.Outputs = append(r.Outputs, CompletionOutput{Name: "other", ArtifactID: uuid.NewString()})
	r.LogsComplete = false
	r.Gaps = []CompletionLogGap{{Stream: "stdout", First: 1, Last: 3}, {Stream: "stderr", First: 5, Last: 9}}
	r.MetricsJSON = []byte(`{"loss":0.25,"accuracy":0.9}`)
	first, err := CompletionDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	replay := r
	replay.CompletionID = uuid.NewString()
	replay.PayloadSHA256 = strings.Repeat("f", 64)
	replay.Outputs = []CompletionOutput{r.Outputs[1], r.Outputs[0]}
	replay.Gaps = []CompletionLogGap{r.Gaps[1], r.Gaps[0]}
	if hash, err := CompletionDigest(replay); err != nil || hash != first {
		t.Fatal("ordering or request envelope changed completion digest", err)
	}
	for _, change := range []func(*CompletionRequest){
		func(r *CompletionRequest) { r.Stopped = false },
		func(r *CompletionRequest) { r.Authority.Generation++ },
		func(r *CompletionRequest) { r.MetricsJSON = []byte(`{"loss":0.5}`) },
	} {
		changed := r
		change(&changed)
		if hash, err := CompletionDigest(changed); err == nil && hash == first {
			t.Fatal("changed completion evidence retained digest")
		}
	}
}

func TestCompletionRejectsMalformedEvidenceAndUnboundedMetrics(t *testing.T) {
	for _, change := range []func(*CompletionRequest){
		func(r *CompletionRequest) { r.CompletionID = "bad" },
		func(r *CompletionRequest) { r.ExitCode = nil },
		func(r *CompletionRequest) { r.Stopped = false },
		func(r *CompletionRequest) { r.Reason = "WORKER_LOST" },
		func(r *CompletionRequest) { r.Reason = "APPLICATION_EXIT" },
		func(r *CompletionRequest) { r.Outputs = append(r.Outputs, r.Outputs[0]) },
		func(r *CompletionRequest) { r.Gaps = []CompletionLogGap{{Stream: "stdout", First: 1, Last: 2}} },
		func(r *CompletionRequest) {
			r.LogsComplete = false
			r.Gaps = []CompletionLogGap{{Stream: "stdout", First: 3, Last: 2}}
		},
		func(r *CompletionRequest) {
			r.LogsComplete = false
			r.Gaps = []CompletionLogGap{{Stream: "stdout", First: 1, Last: 3}, {Stream: "stdout", First: 3, Last: 4}}
		},
	} {
		r := completionRequest()
		change(&r)
		if _, err := CompletionDigest(r); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid completion accepted", err)
		}
	}
	for _, metrics := range []string{`null`, `[]`, `{"loss":"0.5"}`, `{"x":1,"x":2}`, `{"x":1e999}`, `{"x":1e-999}`, `{"x":{}}`, `{"x":1} {}`, `{"":1}`, strings.Repeat(" ", MaxCompletionMetricsBytes+1), `{"x":0.` + strings.Repeat("0", 1024) + `}`} {
		r := completionRequest()
		r.MetricsJSON = []byte(metrics)
		if _, err := CompletionDigest(r); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid metrics accepted", metrics[:min(40, len(metrics))], err)
		}
	}
}
