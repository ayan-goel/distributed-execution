//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestHTTPSweepInspectionScopesProgressAndCursors(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	issue := func(project string, role store.Role) string {
		t.Helper()
		token, _, err := store.IssueToken(ctx, pool, project, role)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	submit, reader, foreign := issue("research", store.RoleSubmit), issue("research", store.RoleRead), issue("other", store.RoleRead)
	job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: spec.Metadata{Name: "http-grid", Project: "research"},
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{"SEED": {"1", "2", "3"}}, MaxConcurrent: 2}}
	body, _ := json.Marshal(sweep)
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	response := call(h, "POST", "/v1/sweeps", submit, "progress", body)
	if response.Code != 201 {
		t.Fatal(response.Code, response.Body.String())
	}
	var created store.SweepRecord
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	path := "/v1/sweeps/" + created.ID
	first := call(h, "GET", path+"?limit=2", reader, "", nil)
	if first.Code != 200 {
		t.Fatal("sweep inspection unavailable", first.Code, first.Body.String())
	}
	var page struct {
		Sweep      store.SweepSummary `json:"sweep"`
		Children   []store.SweepChild `json:"children"`
		HasMore    bool               `json:"hasMore"`
		NextCursor string             `json:"nextCursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.Sweep.ID != created.ID || page.Sweep.Progress.Queued != 3 || len(page.Children) != 2 || !page.HasMore || page.NextCursor == "" {
		t.Fatal("incorrect first page", page, err)
	}
	if _, err := store.RequestCancellation(ctx, pool, created.ProjectID, created.ChildIDs[2]); err != nil {
		t.Fatal(err)
	}
	second := call(h, "GET", path+"?limit=2&cursor="+url.QueryEscape(page.NextCursor), submit, "", nil)
	if second.Code != 200 {
		t.Fatal(second.Code, second.Body.String())
	}
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil || page.Sweep.Progress.Cancelled != 1 || len(page.Children) != 1 || page.Children[0].ID != created.ChildIDs[2] || page.Children[0].State != "CANCELLED" || page.HasMore || page.NextCursor != "" {
		t.Fatal("incorrect continuation page", page, err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	c, err := client.New(server.URL, reader, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientPage, err := c.GetSweep(ctx, created.ID, "", 2)
	if err != nil || len(clientPage.Children) != 2 || !clientPage.HasMore {
		t.Fatal("client rejected real HTTP first page", clientPage, err)
	}
	clientPage, err = c.GetSweep(ctx, created.ID, clientPage.NextCursor, 2)
	if err != nil || len(clientPage.Children) != 1 || clientPage.Children[0].ID != created.ChildIDs[2] || clientPage.HasMore {
		t.Fatal("client rejected real HTTP continuation", clientPage, err)
	}
	for _, tc := range []struct {
		path, token string
		code        int
	}{
		{path, "", 401}, {path, foreign, 404}, {"/v1/sweeps/bad", reader, 404},
		{"/v1/sweeps/" + uuid.Nil.String(), reader, 404},
		{path + "?limit=0", reader, 400}, {path + "?limit=101", reader, 400},
		{path + "?cursor=bad", reader, 400}, {path + "?cursor=", reader, 400},
		{path + "?limit=1&limit=2", reader, 400}, {path + "?unknown=1", reader, 400},
		{path + "?cursor=" + encodeSweepCursor(uuid.NewString(), 1), reader, 400},
	} {
		if got := call(h, "GET", tc.path, tc.token, "", nil); got.Code != tc.code {
			t.Fatal("scope or query status", tc.path, got.Code, tc.code, got.Body.String())
		}
	}
}
