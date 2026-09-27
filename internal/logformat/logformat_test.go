package logformat

import (
	"encoding/hex"
	"testing"
)

const vector = "4453504c4f47303131323365343536372d653839622d313264332d613435362d34323636313431373430303001000000000000000000000117979cfe362a00000000000500ff1b5b41000000000000000317979cfe362a002a000000010a"

func fixture(t *testing.T) []byte {
	t.Helper()
	body, err := hex.DecodeString(vector)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestDecodePreservesBinaryRecordsAndCaptureEvidence(t *testing.T) {
	segment, err := Decode(fixture(t))
	if err != nil || segment.AttemptID != "123e4567-e89b-12d3-a456-426614174000" || segment.Stream != "stdout" || len(segment.Records) != 2 {
		t.Fatal(segment, err)
	}
	if segment.Records[0].Sequence != 1 || segment.Records[0].CaptureUnixNanos != 1_700_000_000_000_000_000 || string(segment.Records[0].Payload) != "\x00\xff\x1b[A" || segment.Records[1].Sequence != 3 || segment.Records[1].CaptureUnixNanos != 1_700_000_000_000_000_042 || string(segment.Records[1].Payload) != "\n" {
		t.Fatal("record bytes or timestamps changed", segment)
	}
}

func TestDecodeRejectsCorruptFraming(t *testing.T) {
	if _, err := Decode(make([]byte, MaxSegmentBytes+1)); err == nil {
		t.Fatal("oversized object accepted")
	}
	for _, change := range []func([]byte) []byte{
		func(b []byte) []byte { return b[:len(b)-1] },
		func(b []byte) []byte { b[0] = 'X'; return b },
		func(b []byte) []byte { b[44] = 3; return b },
		func(b []byte) []byte { b[46] = 1; return b },
		func(b []byte) []byte { b[55] = 0; return b },
		func(b []byte) []byte { b[64] = 0xff; return b },
		func(b []byte) []byte { return append(b, 0) },
	} {
		if _, err := Decode(change(fixture(t))); err == nil {
			t.Fatal("corrupt log segment accepted")
		}
	}
}
