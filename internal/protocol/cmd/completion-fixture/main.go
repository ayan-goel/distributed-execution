package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/protobuf/proto"
)

func fixtures() []*pb.CompleteAttemptRequest {
	id := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }
	exit := int32(0)
	base := &pb.CompleteAttemptRequest{Authority: &pb.AttemptAuthority{JobId: id(1), AttemptId: id(2), WorkerId: id(3), SessionId: id(4), Generation: 7}, CompletionId: id(5), ExitCode: &exit, Stopped: true, Outputs: []*pb.OutputReference{{Name: "result", ArtifactId: id(6)}}, Gaps: []*pb.LogGap{{Stream: pb.LogStream_STDERR, FirstSequence: 1, LastSequence: 2}}, MetricsJson: []byte(`{"loss":0.25}`)}
	cases := []*pb.CompleteAttemptRequest{base}
	for _, body := range []string{
		"", `{}`, `{"big":9007199254740993,"small":5e-324,"upper":1E+03,"scale":1.00}`,
		`{"zero":-0.00e-99999,"positiveZero":0e+99999}`, `{"max":1.7976931348623157e308}`,
		`{"a/b":-3.50E-004,"a:b":2,"a-b":1}`, `{ "space" : 2 }`,
		`{"x":1,"x":2}`, `{"x":1,"\u0078":2}`, `{"x":1e99999}`, `{"x":1e-99999}`,
		`{"x":1.7976931348623159e308}`, `{"x":true}`, `{"x":[]}`, `{"x":"1"}`, `null`, `[]`, `{} {}`,
		strings.Repeat(" ", store.MaxCompletionMetricsBytes+1), string([]byte{0xff}),
	} {
		r := proto.Clone(base).(*pb.CompleteAttemptRequest)
		r.MetricsJson = []byte(body)
		cases = append(cases, r)
	}
	for _, mutate := range []func(*pb.CompleteAttemptRequest){
		func(r *pb.CompleteAttemptRequest) {
			r.Authority.Generation = math.MaxInt64
			r.Gaps[0].LastSequence = math.MaxInt64
		},
		func(r *pb.CompleteAttemptRequest) { r.Authority.Generation = math.MaxUint64 },
		func(r *pb.CompleteAttemptRequest) { r.Gaps[0].LastSequence = math.MaxUint64 },
		func(r *pb.CompleteAttemptRequest) {
			r.Outputs = append(r.Outputs, &pb.OutputReference{Name: "another", ArtifactId: id(8)})
			r.Gaps = append(r.Gaps, &pb.LogGap{Stream: pb.LogStream_STDOUT, FirstSequence: 2, LastSequence: 3})
		},
		func(r *pb.CompleteAttemptRequest) {
			r.Outputs = []*pb.OutputReference{{Name: "another", ArtifactId: id(8)}, r.Outputs[0]}
			r.Gaps = append([]*pb.LogGap{{Stream: pb.LogStream_STDOUT, FirstSequence: 2, LastSequence: 3}}, r.Gaps...)
		},
		func(r *pb.CompleteAttemptRequest) {
			r.Reason = pb.FailureReason_RUNTIME_UNAVAILABLE
			r.ExitCode = nil
			r.Stopped = false
		},
		func(r *pb.CompleteAttemptRequest) { r.ExitCode = nil },
		func(r *pb.CompleteAttemptRequest) { r.Reason = pb.FailureReason_WORKER_LOST },
		func(r *pb.CompleteAttemptRequest) { r.Reason = 999 },
		func(r *pb.CompleteAttemptRequest) { r.Gaps[0].Stream = 999 },
		func(r *pb.CompleteAttemptRequest) { r.Gaps = append(r.Gaps, r.Gaps[0]) },
		func(r *pb.CompleteAttemptRequest) { r.LogsComplete = true },
		func(r *pb.CompleteAttemptRequest) { r.Outputs = append(r.Outputs, r.Outputs[0]) },
		func(r *pb.CompleteAttemptRequest) { r.PayloadSha256 = strings.Repeat("f", 64); r.CompletionId = id(99) },
	} {
		r := proto.Clone(base).(*pb.CompleteAttemptRequest)
		mutate(r)
		cases = append(cases, r)
	}
	for _, count := range []int{256, 257} {
		r := proto.Clone(base).(*pb.CompleteAttemptRequest)
		pairs := make([]string, count)
		for i := range pairs {
			pairs[i] = fmt.Sprintf(`"m%d":%d`, i, i)
		}
		r.MetricsJson = []byte("{" + strings.Join(pairs, ",") + "}")
		cases = append(cases, r)
	}
	return cases
}

func expected(w *pb.CompleteAttemptRequest) string {
	a := w.Authority
	if a.Generation > math.MaxInt64 {
		return "INVALID"
	}
	r := store.CompletionRequest{Authority: store.AttemptAuthority{JobID: a.JobId, AttemptID: a.AttemptId, WorkerID: a.WorkerId, SessionID: a.SessionId, Generation: int64(a.Generation)}, CompletionID: w.CompletionId, ExitCode: w.ExitCode, Stopped: w.Stopped, LogsComplete: w.LogsComplete, MetricsJSON: w.MetricsJson}
	if w.Reason != pb.FailureReason_FAILURE_REASON_UNSPECIFIED {
		r.Reason = w.Reason.String()
	}
	for _, o := range w.Outputs {
		r.Outputs = append(r.Outputs, store.CompletionOutput{Name: o.Name, ArtifactID: o.ArtifactId})
	}
	for _, g := range w.Gaps {
		if g.FirstSequence > math.MaxInt64 || g.LastSequence > math.MaxInt64 {
			return "INVALID"
		}
		stream := strings.ToLower(g.Stream.String())
		r.Gaps = append(r.Gaps, store.CompletionLogGap{Stream: stream, First: int64(g.FirstSequence), Last: int64(g.LastSequence)})
	}
	hash, err := store.CompletionDigest(r)
	if err != nil {
		return "INVALID"
	}
	return hash
}

func main() {
	if len(os.Args) != 2 {
		panic("expected create or check")
	}
	switch os.Args[1] {
	case "create":
		for _, r := range fixtures() {
			body, err := proto.Marshal(r)
			if err != nil {
				panic(err)
			}
			if err := binary.Write(os.Stdout, binary.LittleEndian, uint32(len(body))); err != nil {
				panic(err)
			}
			if _, err := os.Stdout.Write(body); err != nil {
				panic(err)
			}
		}
	case "check":
		scanner := bufio.NewScanner(os.Stdin)
		for i, r := range fixtures() {
			if !scanner.Scan() || scanner.Text() != expected(r) {
				panic(fmt.Sprintf("completion digest disagreement at fixture %d", i))
			}
		}
		if scanner.Scan() || scanner.Err() != nil {
			panic("unexpected completion digest output")
		}
	default:
		panic("expected create or check")
	}
}
