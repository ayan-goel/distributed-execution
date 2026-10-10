package cli

import (
	"errors"
	"io"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/client"
)

type diagnosticOutputFailure struct{ remaining int }

func (w *diagnosticOutputFailure) Write(p []byte) (int, error) {
	if w.remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	w.remaining--
	return len(p), nil
}

func TestQueueDiagnosticsPropagatesOutputFailures(t *testing.T) {
	d := &client.QueueDiagnostics{AsOf: time.Now(), Observations: make([]client.QueueObservation, 2)}
	for _, allowed := range []int{0, 1, 2} {
		if err := writeQueueDiagnostics(&diagnosticOutputFailure{remaining: allowed}, d); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal("lost status/header/observation output failure", allowed, err)
		}
	}
	d.Observations = nil
	if err := writeQueueDiagnostics(&diagnosticOutputFailure{remaining: 1}, d); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("lost empty-history output failure", err)
	}
}
