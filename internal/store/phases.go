package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PhaseReport struct {
	Authority   AttemptAuthority `json:"authority"`
	EventID     string           `json:"-"`
	Phase       string           `json:"phase"`
	ContainerID string           `json:"containerId"`
	ExitCode    *int32           `json:"exitCode"`
}

type MutationResult struct{ Decision, State string }

func (r PhaseReport) hash(workerID string) (string, error) {
	a := r.Authority
	if !canonicalUUID(r.EventID) || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.SessionID) || a.WorkerID != workerID || a.Generation < 1 {
		return "", ErrInvalid
	}
	switch r.Phase {
	case "STARTING":
		if r.ContainerID != "" || r.ExitCode != nil {
			return "", ErrInvalid
		}
	case "RUNNING":
		if !hashPattern.MatchString(r.ContainerID) || r.ExitCode != nil {
			return "", ErrInvalid
		}
	case "FINALIZING":
		if !hashPattern.MatchString(r.ContainerID) || r.ExitCode == nil || *r.ExitCode < 0 || *r.ExitCode > 255 {
			return "", ErrInvalid
		}
	default:
		return "", ErrInvalid
	}
	body, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("dispatch.worker.v1.ReportPhase\n"), body...))
	return hex.EncodeToString(digest[:]), nil
}

func nextPhaseDeadline(r PhaseReport, attempt leaseAttempt, container *string, exit *int32, job spec.Job, now time.Time) (time.Time, error) {
	if container != nil && r.ContainerID != *container {
		return time.Time{}, ErrConflict
	}
	if attempt.state == r.Phase {
		if (exit == nil) != (r.ExitCode == nil) || (exit != nil && *exit != *r.ExitCode) {
			return time.Time{}, ErrConflict
		}
		// A fresh event UUID for an already acknowledged phase is harmless only
		// when its runtime evidence matches. It must not restart the timeout.
		return attempt.phase, nil
	}
	switch {
	case attempt.state == "ASSIGNED" && r.Phase == "STARTING":
		// Startup time begins at assignment, including delivery and staging. An
		// acknowledgement cannot give a slow startup an additional full budget.
		return attempt.phase, nil
	case attempt.state == "STARTING" && r.Phase == "RUNNING":
		return now.Add(time.Duration(job.Spec.Timeouts.ExecutionSeconds) * time.Second), nil
	case slices.Contains([]string{"STARTING", "RUNNING"}, attempt.state) && r.Phase == "FINALIZING":
		// A short process may already have exited at the first runtime inspect;
		// its full container ID and exit status still permit finalization.
		return now.Add(time.Duration(job.Spec.Timeouts.FinalizationSeconds) * time.Second), nil
	default:
		return time.Time{}, ErrConflict
	}
}

// ReportPhase records a forward lifecycle transition and its event atomically.
// Neither acknowledgement nor replay renews execution authority or releases capacity.
func ReportPhase(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r PhaseReport) (MutationResult, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) {
		return MutationResult{}, ErrUnauthorized
	}
	hash, err := r.hash(id.WorkerID)
	if err != nil {
		return MutationResult{}, err
	}
	// Like renewal, phase changes do not alter capacity. Keep them independent
	// of the scheduler cluster lock while retaining job-before-attempt ordering.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return MutationResult{}, err
	}
	if err = authorizeWorkerTx(ctx, tx, id); err != nil {
		return MutationResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return MutationResult{}, err
	}
	jobs, attempts, err := lockLeaseBatch(ctx, tx, []AttemptAuthority{r.Authority})
	if err != nil {
		return MutationResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return MutationResult{}, err
	}
	attempt := attempts[r.Authority.AttemptID]
	if attempt.authority != r.Authority {
		return MutationResult{Decision: "FENCED"}, nil
	}
	var container *string
	var exit *int32
	var body []byte
	if err = tx.QueryRow(ctx, "SELECT a.container_id,a.exit_code,j.spec FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=$1", r.Authority.AttemptID).Scan(&container, &exit, &body); err != nil {
		return MutationResult{}, err
	}
	var job spec.Job
	if json.Unmarshal(body, &job) != nil || job.Validate() != nil {
		return MutationResult{}, ErrInvalid
	}
	// Claim the event after its ownership locks: the attempt FK must not acquire
	// an attempt lock ahead of its job. Conflicting cross-attempt UUIDs serialize
	// here before the fresh time sample, so waiting cannot revive expired authority.
	claim, err := tx.Exec(ctx, `INSERT INTO attempt_phase_reports(worker_id,session_id,event_id,job_id,attempt_id,phase,request_hash) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, id.WorkerID, r.Authority.SessionID, r.EventID, r.Authority.JobID, r.Authority.AttemptID, r.Phase, hash)
	if err != nil {
		return MutationResult{}, err
	}
	replay := claim.RowsAffected() == 0
	if replay {
		var oldHash string
		if err = tx.QueryRow(ctx, "SELECT request_hash FROM attempt_phase_reports WHERE worker_id=$1 AND session_id=$2 AND event_id=$3", id.WorkerID, r.Authority.SessionID, r.EventID).Scan(&oldHash); err != nil {
			return MutationResult{}, err
		}
		if oldHash != hash {
			return MutationResult{}, ErrConflict
		}
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return MutationResult{}, err
	}
	result := MutationResult{Decision: leaseDecision(r.Authority, jobs[r.Authority.JobID], attempt, now), State: attempt.state}
	if result.Decision != "ACCEPTED" {
		return result, nil
	}
	if !replay {
		deadline, err := nextPhaseDeadline(r, attempt, container, exit, job, now)
		if err != nil {
			return MutationResult{}, err
		}
		if attempt.state != r.Phase {
			// The job stays ACTIVE and its reservation stays charged through
			// finalization. Only the later completion path can accept a result.
			if _, err = tx.Exec(ctx, `UPDATE attempts SET state=$2,container_id=COALESCE(container_id,NULLIF($3,'')),exit_code=$4,phase_deadline=$5 WHERE id=$1`, r.Authority.AttemptID, r.Phase, r.ContainerID, r.ExitCode, deadline); err != nil {
				return MutationResult{}, err
			}
			if err = appendPhaseEvent(ctx, tx, r, attempt.state, deadline); err != nil {
				return MutationResult{}, err
			}
		}
		result.State = r.Phase
	}
	if err = tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	return result, nil
}

func appendPhaseEvent(ctx context.Context, tx pgx.Tx, r PhaseReport, from string, deadline time.Time) error {
	var sequence int64
	if err := tx.QueryRow(ctx, "UPDATE jobs SET event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence", r.Authority.JobID).Scan(&sequence); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{"previousState": from, "state": r.Phase, "phaseDeadline": deadline, "containerId": r.ContainerID, "exitCode": r.ExitCode})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,attempt_id,type,payload) VALUES($1,$2,$3,'PHASE_CHANGED',$4)", r.Authority.JobID, sequence, r.Authority.AttemptID, body)
	return err
}
