//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
)

func TestHTTPWorkerListingAuthorizationPaginationAndDrain(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	for _, name := range []string{"fleet-a", "fleet-b", "fleet-c"} {
		_, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{Name: name, CertificateSHA256: sha256.Sum256([]byte(name)), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "amd64"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	issue := func(project string, role store.Role) string {
		t.Helper()
		token, _, err := store.IssueToken(ctx, pool, project, role)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	reader, submit, operator, foreign := issue("research", store.RoleRead), issue("research", store.RoleSubmit), issue("research", store.RoleOperator), issue("other", store.RoleRead)
	h := New(pool, nil, nil)
	for _, token := range []string{reader, submit, operator} {
		w := call(h, "GET", "/v1/workers", token, "", nil)
		var page WorkerPage
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Workers) != 3 || page.HasMore || page.NextCursor != "" || page.Project != "research" {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, worker := range page.Workers {
			if worker.LastHeartbeatAt != nil || worker.RuntimeHealthy || worker.ReconciliationComplete || worker.State != "REGISTERING" || worker.Capacity != worker.Available {
				t.Fatal(worker)
			}
		}
		for _, secret := range []string{"credentialId", "certificate", "sessionId", "currentSessionId"} {
			if strings.Contains(w.Body.String(), secret) {
				t.Fatal("private worker field exposed", secret)
			}
		}
	}
	if w := call(h, "GET", "/v1/workers", "", "", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	w := call(h, "GET", "/v1/workers", foreign, "", nil)
	var empty WorkerPage
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &empty) != nil || empty.Workers == nil || len(empty.Workers) != 0 {
		t.Fatal(w.Code, w.Body.String())
	}
	first := call(h, "GET", "/v1/workers?limit=1", reader, "", nil)
	var page WorkerPage
	if first.Code != 200 || json.Unmarshal(first.Body.Bytes(), &page) != nil || len(page.Workers) != 1 || !page.HasMore || page.NextCursor == "" {
		t.Fatal(first.Code, first.Body.String())
	}
	anchor := page.Workers[0].ID
	for _, query := range []string{"limit=0", "limit=101", "limit=1&limit=2", "cursor=", "cursor=bad", "unknown=1", "cursor=" + url.QueryEscape(page.NextCursor) + "&project=research"} {
		if w := call(h, "GET", "/v1/workers?"+query, reader, "", nil); w.Code != 400 {
			t.Fatal(query, w.Code, w.Body.String())
		}
	}
	if w := call(h, "GET", "/v1/workers?cursor="+url.QueryEscape(page.NextCursor), foreign, "", nil); w.Code != 400 {
		t.Fatal("foreign cursor accepted", w.Code)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM worker_projects WHERE worker_id=$1", anchor); err != nil {
		t.Fatal(err)
	}
	next := call(h, "GET", "/v1/workers?limit=100&cursor="+url.QueryEscape(page.NextCursor), reader, "", nil)
	if next.Code != 200 || json.Unmarshal(next.Body.Bytes(), &page) != nil || len(page.Workers) != 2 || page.HasMore || page.NextCursor != "" || page.Workers[0].ID <= anchor {
		t.Fatal(next.Code, next.Body.String())
	}
	drained := page.Workers[0].ID
	if w := call(h, "POST", "/v1/workers/"+drained+"/drain", operator, "", nil); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(h, "GET", "/v1/workers", reader, "", nil)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || !page.Workers[0].DrainRequested {
		t.Fatal(w.Code, w.Body.String())
	}
	server := httptest.NewServer(h)
	defer server.Close()
	getenv := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": reader, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	for _, asJSON := range []bool{false, true} {
		args := []string{"workers", "list", "--limit=1"}
		if asJSON {
			args = append(args, "--json")
		}
		var out, errs bytes.Buffer
		code := cli.Run(ctx, args, getenv, &out, &errs)
		if code != 0 || errs.Len() != 0 || !strings.Contains(out.String(), drained) {
			t.Fatal(code, out.String(), errs.String())
		}
		if asJSON {
			var result WorkerPage
			if json.Unmarshal(out.Bytes(), &result) != nil || len(result.Workers) != 1 || !result.HasMore || !result.Workers[0].DrainRequested {
				t.Fatal(out.String())
			}
			out.Reset()
			if code := cli.Run(ctx, []string{"workers", "list", "--cursor=" + result.NextCursor, "--json"}, getenv, &out, &errs); code != 0 {
				t.Fatal(code, errs.String())
			}
			if json.Unmarshal(out.Bytes(), &result) != nil || len(result.Workers) != 1 || result.HasMore || result.NextCursor != "" {
				t.Fatal(out.String())
			}
		} else {
			for _, fragment := range []string{"REGISTERING", "lastHeartbeatAt=never", "reserved", "available", "drain=true", "nextCursor:"} {
				if !strings.Contains(out.String(), fragment) {
					t.Fatal("missing human fleet evidence", fragment, out.String())
				}
			}
		}
	}
}
