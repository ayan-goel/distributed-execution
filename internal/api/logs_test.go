package api

import (
	"math"
	"testing"

	"github.com/google/uuid"
)

func TestLogCursorBindsAttemptAndStream(t *testing.T) {
	attempt := uuid.NewString()
	cursor := encodeLogCursor(attempt, "STDOUT", 42)
	stream, after, limit, echoed, err := parseLogQuery("stream=stdout&cursor="+cursor+"&limit=3", attempt)
	if err != nil || stream != "STDOUT" || after != 42 || limit != 3 || echoed != cursor {
		t.Fatal(stream, after, limit, echoed, err)
	}
	for _, query := range []string{
		"stream=stderr&cursor=" + cursor,
		"stream=stdout&cursor=" + encodeLogCursor(uuid.NewString(), "STDOUT", 42),
		"stream=stdout&cursor=" + encodeLogCursor(attempt, "STDOUT", math.MaxUint64),
		"stream=stdout&cursor=bad",
		"stream=stdout&cursor=",
		"stream=stdout&limit=101",
		"stream=stdout&limit=0",
		"stream=stdout&limit=",
		"stream=stdout&stream=stderr",
		"stream=stdout&unknown=x",
		"stream=stdout&cursor=%ZZ",
	} {
		if _, _, _, _, err := parseLogQuery(query, attempt); err == nil {
			t.Fatal("invalid log query accepted", query)
		}
	}
}
