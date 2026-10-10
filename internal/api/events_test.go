package api

import (
	"encoding/base64"
	"math"
	"net/url"
	"strings"
	"testing"
)

func TestEventQueryBoundsAndCanonicalScopedCursor(t *testing.T) {
	const project = "00000000-0000-0000-0000-000000000001"
	const job = "00000000-0000-0000-0000-000000000002"
	for _, sequence := range []int64{0, 7, math.MaxInt64} {
		cursor := encodeEventCursor(project, job, sequence)
		if len(cursor) > 256 {
			t.Fatal("cursor exceeds contract")
		}
		for _, limit := range []string{"1", "100"} {
			after, n, err := parseEventQuery("limit="+limit+"&cursor="+url.QueryEscape(cursor), project, job)
			if err != nil || after != sequence || n < 1 || n > 100 {
				t.Fatal(after, n, err)
			}
		}
		for _, scope := range [][2]string{{job, job}, {project, project}} {
			if _, _, err := parseEventQuery("cursor="+cursor, scope[0], scope[1]); err == nil {
				t.Fatal("cross-scope cursor accepted")
			}
		}
	}
	if after, limit, err := parseEventQuery("", project, job); err != nil || after != 0 || limit != 50 {
		t.Fatal(after, limit, err)
	}
	for _, raw := range []string{"limit=0", "limit=101", "limit=-1", "limit=1&limit=2", "limit=", "cursor=", "cursor=x", "cursor=" + strings.Repeat("x", 257), "unknown=1", "%xx=1", "limit=1;cursor=x", strings.Repeat("x", 2049)} {
		if _, _, err := parseEventQuery(raw, project, job); err == nil {
			t.Fatal("invalid query accepted", raw)
		}
	}
	valid := `{"v":1,"p":"` + project + `","j":"` + job + `","n":7}`
	for _, body := range []string{
		strings.Replace(valid, `"v":1`, `"v":2`, 1),
		strings.Replace(valid, `"n":7`, `"n":-1`, 1),
		strings.Replace(valid, `"n":7`, `"n":9223372036854775808`, 1),
		strings.Replace(valid, `"n":7`, `"n":7,"n":7`, 1),
		strings.Replace(valid, `"n":7`, `"n":null`, 1),
		strings.Replace(valid, `,"n":7`, "", 1),
		strings.Replace(valid, `"n":7`, `"n":7,"extra":1`, 1),
	} {
		cursor := base64.RawURLEncoding.EncodeToString([]byte(body))
		if _, _, err := parseEventQuery("cursor="+cursor, project, job); err == nil {
			t.Fatal("noncanonical cursor accepted", body)
		}
	}
}
