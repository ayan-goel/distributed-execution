//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestHTTPAndCLIWorkerDrainAuthorizationAndIdempotency(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	worker, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{Name: "drain-api", CertificateSHA256: sha256.Sum256([]byte("drain-api")), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "amd64"}})
	if err != nil {
		t.Fatal(err)
	}
	issue := func(project string, role store.Role) string {
		t.Helper()
		token, _, err := store.IssueToken(ctx, pool, project, role)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	operator, reader, submit, foreign := issue("research", store.RoleOperator), issue("research", store.RoleRead), issue("research", store.RoleSubmit), issue("other", store.RoleOperator)
	h := New(pool, nil, nil)
	path := "/v1/workers/" + worker.WorkerID + "/drain"
	for _, tc := range []struct {
		token, path, body string
		status            int
	}{
		{reader, path, "", 403}, {submit, path, "", 403}, {foreign, path, "", 404}, {"", path, "", 401},
		{operator, "/v1/workers/" + uuid.NewString() + "/drain", "", 404}, {operator, "/v1/workers/bad/drain", "", 404},
		{operator, path, "{}", 400}, {operator, path, " ", 400}, {operator, path + "?", "", 400}, {operator, path + "?force=1", "", 400},
	} {
		w := call(h, "POST", tc.path, tc.token, "", []byte(tc.body))
		if w.Code != tc.status {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
	}
	var drain bool
	if err := pool.QueryRow(ctx, "SELECT drain_requested FROM workers WHERE id=$1", worker.WorkerID).Scan(&drain); err != nil || drain {
		t.Fatal("rejected request changed drain", drain, err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	for _, asJSON := range []bool{false, true, true} {
		args := []string{"workers", "drain", worker.WorkerID}
		if asJSON {
			args = append(args, "--json")
		}
		var out, errs bytes.Buffer
		code := cli.Run(ctx, args, func(key string) string {
			return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_DEV_INSECURE": "1", "DISPATCH_TOKEN": operator}[key]
		}, &out, &errs)
		if code != 0 || errs.Len() != 0 || !strings.Contains(out.String(), worker.WorkerID) {
			t.Fatal(code, out.String(), errs.String())
		}
		if asJSON {
			var result store.WorkerDrain
			if json.Unmarshal(out.Bytes(), &result) != nil || result.WorkerID != worker.WorkerID || !result.DrainRequested || result.State != "REGISTERING" {
				t.Fatal("CLI result changed", out.String())
			}
		} else if !strings.Contains(out.String(), "existing attempts may finish") {
			t.Fatal("human output omitted drain meaning", out.String())
		}
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_audit_events WHERE worker_id=$1 AND action='WORKER_DRAIN_REQUESTED'", worker.WorkerID).Scan(&count); err != nil || count != 1 {
		t.Fatal("repeated CLI request duplicated audit", count, err)
	}
}
