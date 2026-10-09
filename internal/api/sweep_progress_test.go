package api

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSweepQueryRequiresBoundedScopedCursor(t *testing.T) {
	id := uuid.NewString()
	if after, limit, err := parseSweepQuery("", id); err != nil || after != -1 || limit != 50 {
		t.Fatal("incorrect first-page defaults", after, limit, err)
	}
	value := url.QueryEscape(encodeSweepCursor(id, 7))
	if after, limit, err := parseSweepQuery("limit=100&cursor="+value, id); err != nil || after != 7 || limit != 100 {
		t.Fatal("valid cursor rejected", after, limit, err)
	}
	encoded := func(body string) string { return base64.RawURLEncoding.EncodeToString([]byte(body)) }
	for _, query := range []string{
		"limit=", "limit=-1", "limit=101", "limit=1&limit=2", "cursor=", "cursor=bad", "cursor=" + strings.Repeat("a", 257),
		"unknown=1", "limit=1;cursor=x", "cursor=" + encodeSweepCursor(uuid.NewString(), 1),
		"cursor=" + encodeSweepCursor(id, -1), "cursor=" + encodeSweepCursor(id, 1000),
		"cursor=" + encoded(`{"v":1,"s":"`+id+`"}`),
		"cursor=" + encoded(`{"v":2,"s":"`+id+`","i":0}`),
		"cursor=" + encoded(`{"v":1,"s":"`+id+`","i":null}`),
	} {
		if _, _, err := parseSweepQuery(query, id); err == nil {
			t.Fatal("invalid cursor or query accepted", query)
		}
	}
}
