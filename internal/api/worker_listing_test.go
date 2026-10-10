package api

import (
	"encoding/base64"
	"net/http/httptest"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/store"
)

func TestWorkerListingQueryAndCursorValidation(t *testing.T) {
	p := store.Principal{ProjectID: "00000000-0000-0000-0000-000000000001"}
	id := "00000000-0000-0000-0000-000000000002"
	cursor, err := encodeWorkerCursor(p.ProjectID, id)
	if err != nil || len(cursor) > 256 {
		t.Fatal(cursor, err)
	}
	after, limit, err := parseWorkerQuery("cursor="+cursor+"&limit=100", p)
	if err != nil || after != id || limit != 100 {
		t.Fatal(after, limit, err)
	}
	for _, raw := range []string{"limit=0", "limit=101", "limit=", "limit=1&limit=1", "cursor=", "other=x", "project=research", "%zz", "cursor=" + strings.Repeat("a", 257), strings.Repeat("x", 2049)} {
		if _, _, err := parseWorkerQuery(raw, p); err == nil {
			t.Fatal("invalid query accepted", raw)
		}
	}
	body, _ := base64.RawURLEncoding.DecodeString(cursor)
	for _, bad := range []string{
		string(body) + " ", strings.Replace(string(body), `"v":1`, `"v":1,"v":1`, 1),
		strings.Replace(string(body), `"v":1`, `"v":1,"extra":1`, 1),
		strings.Replace(string(body), `"v":1`, `"v":null`, 1),
		strings.Replace(string(body), `"v":1`, `"v":2`, 1),
		strings.Replace(string(body), id, "00000000-0000-0000-0000-000000000000", 1),
		strings.Replace(string(body), `"n":"`+id+`"`, `"n":null`, 1),
		strings.Replace(string(body), p.ProjectID, "00000000-0000-0000-0000-000000000003", 1),
	} {
		if _, _, err := parseWorkerQuery("cursor="+base64.RawURLEncoding.EncodeToString([]byte(bad)), p); err == nil {
			t.Fatal("invalid cursor accepted", bad)
		}
	}
	if after, limit, err := parseWorkerQuery("", p); err != nil || after != "" || limit != 50 {
		t.Fatal(after, limit, err)
	}
	w := httptest.NewRecorder()
	New(nil, nil, nil).listWorkers(w, httptest.NewRequest("GET", "/v1/workers", nil), store.Principal{Role: "unknown"})
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
}
