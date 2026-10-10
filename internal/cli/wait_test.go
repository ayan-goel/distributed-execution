package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func waitEnv(endpoint string) func(string) string {
	return func(key string) string {
		return map[string]string{"DISPATCH_URL": endpoint, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
}

func TestWaitReturnsTerminalExitCodesAndOneFinalOutput(t *testing.T) {
	for _, tc := range []struct {
		state string
		code  int
	}{{"SUCCEEDED", 0}, {"FAILED", 1}, {"CANCELLED", 1}} {
		for _, asJSON := range []bool{false, true} {
			t.Run(tc.state+map[bool]string{false: "/text", true: "/json"}[asJSON], func(t *testing.T) {
				var reads atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "GET" || r.URL.Path != "/v1/jobs/"+cliJob || r.Header.Get("Authorization") != "Bearer private" {
						t.Error("incorrect wait request", r.Method, r.URL.Path)
					}
					state := tc.state
					switch reads.Add(1) {
					case 1:
						state = "QUEUED"
					case 2:
						state = "ACTIVE"
					case 3:
						state = "RETRY_WAIT"
					case 4:
						state = "CANCELLING"
					}
					json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": state})
				}))
				defer server.Close()
				args := []string{"wait", cliJob, "--poll-interval", "100ms", "--timeout", "5s"}
				if asJSON {
					args = append(args, "--json")
				}
				var out, errs bytes.Buffer
				if code := Run(context.Background(), args, waitEnv(server.URL), &out, &errs); code != tc.code || errs.Len() != 0 || reads.Load() != 5 {
					t.Fatal(code, tc.code, reads.Load(), errs.String())
				}
				if asJSON {
					var job struct{ ID, State string }
					if json.Unmarshal(out.Bytes(), &job) != nil || job.ID != cliJob || job.State != tc.state {
						t.Fatal("wait did not emit one final job", out.String())
					}
				} else if out.String() != cliJob+" "+`"`+tc.state+`"`+"\n" {
					t.Fatal(out.String())
				}
			})
		}
	}
}

func TestWaitTimeoutNeverCancelsWithoutExplicitOption(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		var mutations atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" {
				mutations.Add(1)
				if r.URL.Path != "/v1/jobs/"+cliJob+"/cancel" || r.Context().Err() != nil {
					t.Error("wrong or expired cancellation request")
				}
				json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": "CANCELLING"})
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": "ACTIVE"})
		}))
		defer server.Close()
		args := []string{"wait", cliJob, "--timeout", "150ms", "--poll-interval", "100ms", "--json"}
		if explicit {
			args = append(args, "--cancel-on-timeout")
		}
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, waitEnv(server.URL), &out, &errs); code != 2 || out.Len() != 0 || !strings.Contains(errs.String(), "timed out") || mutations.Load() != map[bool]int64{false: 0, true: 1}[explicit] {
			t.Fatal(explicit, code, out.String(), errs.String(), mutations.Load())
		}
		if explicit && !strings.Contains(errs.String(), "cancellation requested") {
			t.Fatal("timeout did not distinguish request from completed cleanup", errs.String())
		}
	}
}

func TestWaitParentCancellationAndInfrastructureErrorsExitTwo(t *testing.T) {
	for _, tc := range []struct {
		state  string
		status int
	}{{"unknown", 200}, {"\x1b[31m", 200}, {"", 503}, {"", 401}} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			if tc.status != 200 {
				json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "UNAVAILABLE", "message": "fixture", "retryable": true, "requestId": "fixture"}})
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": tc.state})
		}))
		defer server.Close()
		var out, errs bytes.Buffer
		if code := Run(context.Background(), []string{"wait", cliJob}, waitEnv(server.URL), &out, &errs); code != 2 || out.Len() != 0 || strings.ContainsRune(errs.String(), '\x1b') {
			t.Fatal(code, out.String(), errs.String())
		}
	}
	var mutations atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			mutations.Add(1)
		}
		json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": "ACTIVE"})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var out, errs bytes.Buffer
	if code := Run(ctx, []string{"wait", cliJob, "--timeout", "5s", "--cancel-on-timeout"}, waitEnv(server.URL), &out, &errs); code != 2 || out.Len() != 0 || mutations.Load() != 0 {
		t.Fatal("parent deadline cancelled job", code, mutations.Load(), errs.String())
	}
}

func TestWaitRejectsInvalidOptionsAndPropagatesOutputFailure(t *testing.T) {
	for _, args := range [][]string{{"wait", "bad"}, {"wait", cliJob, "--timeout", "-1s"}, {"wait", cliJob, "--timeout", "bad"}, {"wait", cliJob, "--poll-interval", "0s"}, {"wait", cliJob, "--poll-interval", "99ms"}, {"wait", cliJob, "--poll-interval", "2m"}, {"wait", cliJob, "--cancel-on-timeout"}, {"wait", cliJob, "extra"}} {
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, func(string) string { t.Fatal("invalid wait consulted credentials"); return "" }, &out, &errs); code != 2 || out.Len() != 0 {
			t.Fatal(args, code)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": "FAILED"})
	}))
	defer server.Close()
	for _, args := range [][]string{{"wait", cliJob}, {"wait", cliJob, "--json"}} {
		var errs bytes.Buffer
		if code := Run(context.Background(), args, waitEnv(server.URL), failedExportWriter{}, &errs); code != 2 || !strings.Contains(errs.String(), "output unavailable") {
			t.Fatal("output failure incorrectly returned job failure", code, errs.String())
		}
	}
}

func TestWaitTimeoutReportsUnconfirmedCancellationAndTerminalRaces(t *testing.T) {
	for _, state := range []string{"lost-response", "SUCCEEDED", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			var mutations atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					mutations.Add(1)
					if state == "lost-response" {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						conn.Close()
						return
					}
					json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": state})
					return
				}
				json.NewEncoder(w).Encode(map[string]string{"id": cliJob, "state": "ACTIVE"})
			}))
			defer server.Close()
			var out, errs bytes.Buffer
			if code := Run(context.Background(), []string{"wait", cliJob, "--timeout", "150ms", "--cancel-on-timeout"}, waitEnv(server.URL), &out, &errs); code != 2 || out.Len() != 0 || mutations.Load() != 1 {
				t.Fatal(code, out.String(), errs.String(), mutations.Load())
			}
			if state == "lost-response" {
				if !strings.Contains(errs.String(), "cancellation unconfirmed") || strings.Contains(errs.String(), "cancellation requested") {
					t.Fatal("lost response claimed cancellation outcome", errs.String())
				}
			} else if !strings.Contains(errs.String(), state) {
				t.Fatal("cancellation race omitted returned terminal state", errs.String())
			}
		})
	}
}

func TestWaitInterruptsAnInFlightReadWithoutCancellingJob(t *testing.T) {
	started := make(chan struct{})
	var mutations atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			mutations.Add(1)
			return
		}
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out, errs bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, []string{"wait", cliJob, "--timeout", "5s", "--cancel-on-timeout"}, waitEnv(server.URL), &out, &errs)
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("wait never started HTTP read")
	}
	select {
	case code := <-done:
		if code != 2 || out.Len() != 0 || mutations.Load() != 0 {
			t.Fatal(code, out.String(), errs.String(), mutations.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("interruption did not stop in-flight read")
	}
}
