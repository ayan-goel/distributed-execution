package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"
)

type QueueDiagnostics struct {
	AsOf           time.Time          `json:"asOf"`
	AttemptCounter int64              `json:"attemptCounter"`
	Observations   []QueueObservation `json:"observations"`
}

type QueueObservation struct {
	Sequence       int64     `json:"sequence"`
	WorkerID       string    `json:"workerId"`
	SessionID      string    `json:"sessionId"`
	RequestID      string    `json:"requestId"`
	AttemptCounter int64     `json:"attemptCounter"`
	Reason         string    `json:"reason"`
	ObservedAt     time.Time `json:"observedAt"`
}

var errQueueDiagnostics = errors.New("invalid queue diagnostics response")

func decodeQueueDiagnostics(body []byte) (*QueueDiagnostics, error) {
	d := &QueueDiagnostics{}
	var observations []json.RawMessage
	if err := requiredDiagnosticObject(body, map[string]any{
		"asOf": &d.AsOf, "attemptCounter": &d.AttemptCounter, "observations": &observations,
	}); err != nil || !validDiagnosticTime(d.AsOf) || d.AttemptCounter < 0 || len(observations) > 16 {
		return nil, errQueueDiagnostics
	}
	// Bound parsing/rendering and reject incomplete context before it reaches the
	// terminal. Retained checks are historical, so clock rollback may put them
	// after asOf; attempt counters and sequence order still establish provenance.
	d.Observations = make([]QueueObservation, len(observations))
	seen := make(map[string]bool, len(observations))
	for i, body := range observations {
		o := &d.Observations[i]
		if err := requiredDiagnosticObject(body, map[string]any{
			"sequence": &o.Sequence, "workerId": &o.WorkerID, "sessionId": &o.SessionID,
			"requestId": &o.RequestID, "attemptCounter": &o.AttemptCounter,
			"reason": &o.Reason, "observedAt": &o.ObservedAt,
		}); err != nil || o.Sequence < 1 || i > 0 && o.Sequence >= d.Observations[i-1].Sequence ||
			!downloadUUID(o.WorkerID) || !downloadUUID(o.SessionID) || !downloadUUID(o.RequestID) ||
			o.AttemptCounter < 0 || o.AttemptCounter > d.AttemptCounter || !validDiagnosticTime(o.ObservedAt) {
			return nil, errQueueDiagnostics
		}
		switch o.Reason {
		case "PLACEMENT_MISMATCH", "NO_RESOURCE_FIT", "PROJECT_QUOTA", "SWEEP_CONCURRENCY", "PROJECT_DISABLED", "RETRY_BACKOFF":
		default:
			return nil, errQueueDiagnostics
		}
		identity := o.WorkerID + "/" + o.SessionID + "/" + o.RequestID
		if seen[identity] {
			return nil, errQueueDiagnostics
		}
		seen[identity] = true
	}
	return d, nil
}

func validDiagnosticTime(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999
}

// Require exact non-null fields, including zero-valued counters. Ordinary JSON
// decoding accepts duplicate members and null integers, hiding ambiguous evidence.
// Strictness stays within the new diagnostic objects for old-server compatibility.
func requiredDiagnosticObject(body []byte, fields map[string]any) error {
	return requiredNullableDiagnosticObject(body, fields, "")
}

// Some evidence, such as a never-observed heartbeat, is explicitly nullable.
// The named exception still requires the field and preserves duplicate checks.
func requiredNullableDiagnosticObject(body []byte, fields map[string]any, nullable string) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return errQueueDiagnostics
	}
	seen := make(map[string]bool, len(fields))
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		destination, exists := fields[key]
		if err != nil || !ok || !exists || seen[key] {
			return errQueueDiagnostics
		}
		var raw json.RawMessage
		if decoder.Decode(&raw) != nil || (nullable == "" || key != nullable) && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, destination) != nil {
			return errQueueDiagnostics
		}
		seen[key] = true
	}
	closing, err := decoder.Token()
	var trailing any
	if err != nil || closing != json.Delim('}') || len(seen) != len(fields) || decoder.Decode(&trailing) != io.EOF {
		return errQueueDiagnostics
	}
	return nil
}
