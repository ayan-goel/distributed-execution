package logformat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/google/uuid"
)

const MaxSegmentBytes = 1 << 20
const headerBytes = 48
const recordHeaderBytes = 20

var ErrInvalid = errors.New("invalid log segment")

type Record struct {
	Sequence         uint64
	CaptureUnixNanos int64
	Payload          []byte
}

type Segment struct {
	AttemptID string
	Stream    string
	Records   []Record
}

// Decode retains untrusted payload bytes without interpreting terminal control
// characters or UTF-8. The caller must sanitize before printing to a terminal.
func Decode(body []byte) (Segment, error) {
	if len(body) < headerBytes+recordHeaderBytes+1 || len(body) > MaxSegmentBytes || !bytes.Equal(body[:8], []byte("DSPLOG01")) || !bytes.Equal(body[45:48], []byte{0, 0, 0}) {
		return Segment{}, ErrInvalid
	}
	id := string(body[8:44])
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return Segment{}, ErrInvalid
	}
	segment := Segment{AttemptID: id, Records: []Record{}}
	switch body[44] {
	case 1:
		segment.Stream = "stdout"
	case 2:
		segment.Stream = "stderr"
	default:
		return Segment{}, ErrInvalid
	}
	var previous uint64
	for offset := headerBytes; offset < len(body); {
		if len(body)-offset < recordHeaderBytes {
			return Segment{}, ErrInvalid
		}
		sequence := binary.BigEndian.Uint64(body[offset : offset+8])
		capture := int64(binary.BigEndian.Uint64(body[offset+8 : offset+16]))
		size := uint64(binary.BigEndian.Uint32(body[offset+16 : offset+20]))
		offset += recordHeaderBytes
		if sequence == 0 || sequence > math.MaxInt64 || sequence <= previous || capture < 0 || size == 0 || size > uint64(len(body)-offset) {
			return Segment{}, ErrInvalid
		}
		payload := bytes.Clone(body[offset : offset+int(size)])
		segment.Records = append(segment.Records, Record{Sequence: sequence, CaptureUnixNanos: capture, Payload: payload})
		offset += int(size)
		previous = sequence
	}
	return segment, nil
}
