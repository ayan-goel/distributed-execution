package store

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CompletionResult struct {
	Decision, State string
	Manifest        json.RawMessage
}

type publishedArtifact struct {
	Name           string         `json:"name"`
	ArtifactID     string         `json:"artifactId"`
	Object         ArtifactObject `json:"object"`
	UploadID, Kind string         `json:"-"`
}

type resultAttempt struct {
	AttemptID  string     `json:"attemptId"`
	Number     int64      `json:"number"`
	State      string     `json:"state"`
	Reason     *string    `json:"reason"`
	ExitCode   *int32     `json:"exitCode"`
	CreatedAt  time.Time  `json:"createdAt"`
	FinishedAt *time.Time `json:"finishedAt"`
}

type resultManifest struct {
	Version           int                    `json:"version"`
	Authority         AttemptAuthority       `json:"authority"`
	ProjectID         string                 `json:"projectId"`
	Spec              spec.Job               `json:"spec"`
	SpecSHA256        string                 `json:"specSha256"`
	State             string                 `json:"state"`
	ExitCode          *int32                 `json:"exitCode"`
	Reason            string                 `json:"reason"`
	CompletedAt       time.Time              `json:"completedAt"`
	CleanupPending    bool                   `json:"cleanupPending"`
	Outputs           []publishedArtifact    `json:"outputs"`
	LogArtifacts      []publishedArtifact    `json:"logArtifacts"`
	LogsComplete      bool                   `json:"logsComplete"`
	Gaps              []CompletionLogGap     `json:"gaps"`
	Metrics           map[string]json.Number `json:"metrics"`
	MetricsArtifactID string                 `json:"metricsArtifactId"`
	Attempts          []resultAttempt        `json:"attempts"`
}

// CompleteAttempt atomically terminalizes an authorized attempt and its capacity.
// Exact accepted retries return durable evidence, including after lease expiry or
// session replacement; they never create new execution or publication authority.
func CompleteAttempt(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r CompletionRequest) (CompletionResult, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) || r.Authority.WorkerID != id.WorkerID {
		return CompletionResult{}, ErrUnauthorized
	}
	digest, err := CompletionDigest(r)
	if err != nil || !hashPattern.MatchString(r.PayloadSHA256) || digest != r.PayloadSHA256 {
		return CompletionResult{}, ErrInvalid
	}
	payload, err := normalizeCompletion(r)
	if err != nil {
		return CompletionResult{}, err
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return CompletionResult{}, err
	}
	defer rollback(tx)
	if err = authorizeWorkerTx(ctx, tx, id); err != nil {
		return CompletionResult{}, err
	}
	jobs, attempts, err := lockLeaseBatch(ctx, tx, []AttemptAuthority{r.Authority})
	if err != nil {
		return CompletionResult{}, err
	}
	attempt := attempts[r.Authority.AttemptID]
	if attempt.authority != r.Authority {
		return CompletionResult{Decision: "FENCED"}, nil
	}
	var savedID, savedHash, savedState string
	var savedManifest []byte
	err = tx.QueryRow(ctx, "SELECT completion_id::text,payload_digest,state,manifest_json FROM attempt_completions WHERE attempt_id=$1", r.Authority.AttemptID).Scan(&savedID, &savedHash, &savedState, &savedManifest)
	if err == nil {
		if savedID != r.CompletionID || savedHash != digest {
			return CompletionResult{}, ErrConflict
		}
		result := CompletionResult{Decision: "ACCEPTED", State: savedState}
		if savedState == "SUCCEEDED" {
			result.Manifest = savedManifest
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CompletionResult{}, err
	}
	var claimed bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM attempt_completions WHERE worker_id=$1 AND session_id=$2 AND completion_id=$3)", id.WorkerID, r.Authority.SessionID, r.CompletionID).Scan(&claimed); err != nil {
		return CompletionResult{}, err
	}
	if claimed {
		return CompletionResult{}, ErrConflict
	}
	if !slices.Contains([]string{"ASSIGNED", "STARTING", "RUNNING", "FINALIZING"}, attempt.state) {
		return CompletionResult{Decision: "ALREADY_TERMINAL", State: attempt.state}, nil
	}
	var job spec.Job
	var body []byte
	var projectID, specHash string
	var savedExit *int32
	if err = tx.QueryRow(ctx, "SELECT project_id::text,spec,spec_hash FROM jobs WHERE id=$1", r.Authority.JobID).Scan(&projectID, &body, &specHash); err != nil {
		return CompletionResult{}, err
	}
	if json.Unmarshal(body, &job) != nil || job.Validate() != nil {
		return CompletionResult{}, ErrInvalid
	}
	if err = tx.QueryRow(ctx, "SELECT exit_code FROM attempts WHERE id=$1", r.Authority.AttemptID).Scan(&savedExit); err != nil {
		return CompletionResult{}, err
	}
	// Reservation transitions share the cluster lock and take worker/project
	// accounting locks only after job/attempt locks, matching acquisition/recovery.
	if _, err = tx.Exec(ctx, "SELECT id FROM workers WHERE id=$1 FOR UPDATE", id.WorkerID); err != nil {
		return CompletionResult{}, err
	}
	if _, err = tx.Exec(ctx, "SELECT id FROM projects WHERE id=$1 FOR SHARE", projectID); err != nil {
		return CompletionResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return CompletionResult{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return CompletionResult{}, err
	}
	decision := completionDecision(r, jobs[r.Authority.JobID], attempt, now)
	cancelled := jobs[r.Authority.JobID].cancelled
	if decision != "ACCEPTED" {
		return CompletionResult{Decision: decision, State: attempt.state}, nil
	}
	if err = validateCompletionPhase(r, attempt.state, savedExit, cancelled); err != nil {
		return CompletionResult{}, err
	}
	state, jobState := "FAILED", "FAILED"
	nextEligible := now
	if r.Reason == "" {
		state, jobState = "SUCCEEDED", "SUCCEEDED"
	} else if r.Reason == "USER_CANCELLED" {
		state, jobState = "CANCELLED", "CANCELLED"
	} else if int(r.Authority.Generation) < job.Spec.Retry.MaxAttempts && slices.Contains(job.Spec.Retry.On, r.Reason) {
		jobState = "RETRY_WAIT"
		nextEligible = now.Add(retryDelay(job.Spec.Retry, int(r.Authority.Generation), r.Authority.AttemptID))
	}
	manifest, refs, err := completionManifest(ctx, tx, r, payload, job, projectID, specHash, state, now)
	if err != nil {
		return CompletionResult{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO attempt_completions(attempt_id,job_id,worker_id,session_id,generation,completion_id,payload_digest,state,manifest_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.Authority.AttemptID, r.Authority.JobID, id.WorkerID, r.Authority.SessionID, r.Authority.Generation, r.CompletionID, digest, state, manifest); err != nil {
		return CompletionResult{}, err
	}
	for _, ref := range refs {
		if _, err = tx.Exec(ctx, `INSERT INTO completion_artifacts(attempt_id,artifact_id,upload_id,logical_name,kind) VALUES($1,$2,$3,$4,$5)`, r.Authority.AttemptID, ref.ArtifactID, ref.UploadID, ref.Name, ref.Kind); err != nil {
			return CompletionResult{}, err
		}
	}
	// Result/reference validation can take time or wait on foreign-key locks.
	// Recheck immediately before terminal mutations; expiry rolls back the intent
	// and references too, rather than publishing from the earlier time sample.
	var publicationTime time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&publicationTime); err != nil {
		return CompletionResult{}, err
	}
	if decision = completionDecision(r, jobs[r.Authority.JobID], attempt, publicationTime); decision != "ACCEPTED" {
		return CompletionResult{Decision: decision, State: attempt.state}, nil
	}
	if jobState == "RETRY_WAIT" {
		nextEligible = publicationTime.Add(retryDelay(job.Spec.Retry, int(r.Authority.Generation), r.Authority.AttemptID))
	}
	// Success/failure becomes durable with result references and reservation state.
	// Unknown physical stop retains quarantined capacity until reconciliation.
	if _, err = tx.Exec(ctx, `UPDATE attempts SET state=$2,exit_code=$3,reason=NULLIF($4,''),completion_id=$5,completion_digest=$6,cleanup_pending=$7,finished_at=$8 WHERE id=$1`, r.Authority.AttemptID, state, r.ExitCode, r.Reason, r.CompletionID, digest, !r.Stopped, now); err != nil {
		return CompletionResult{}, err
	}
	reservation := "released"
	if !r.Stopped {
		reservation = "quarantined"
	}
	changed, err := tx.Exec(ctx, "UPDATE reservations SET state=$2 WHERE attempt_id=$1 AND state='active'", r.Authority.AttemptID, reservation)
	if err != nil {
		return CompletionResult{}, err
	}
	if changed.RowsAffected() != 1 {
		return CompletionResult{}, ErrConflict
	}
	var acceptedID *string
	var acceptedManifest any
	if state == "SUCCEEDED" {
		acceptedID = &r.Authority.AttemptID
		acceptedManifest = string(manifest)
	}
	var sequence int64
	if err = tx.QueryRow(ctx, `UPDATE jobs SET state=$2,current_attempt_id=NULL,accepted_attempt_id=$3,accepted_manifest=$4::jsonb,next_eligible_at=$5,event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence`, r.Authority.JobID, jobState, acceptedID, acceptedManifest, nextEligible).Scan(&sequence); err != nil {
		return CompletionResult{}, err
	}
	event, _ := json.Marshal(map[string]any{"completionId": r.CompletionID, "state": state, "nextState": jobState, "reason": r.Reason, "cleanupPending": !r.Stopped, "nextEligibleAt": nextEligible})
	if _, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,attempt_id,type,payload) VALUES($1,$2,$3,'ATTEMPT_COMPLETED',$4)", r.Authority.JobID, sequence, r.Authority.AttemptID, event); err != nil {
		return CompletionResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return CompletionResult{}, err
	}
	result := CompletionResult{Decision: "ACCEPTED", State: state}
	if state == "SUCCEEDED" {
		result.Manifest = manifest
	}
	return result, nil
}

func completionDecision(r CompletionRequest, job leaseJob, attempt leaseAttempt, now time.Time) string {
	decision := leaseDecision(r.Authority, job, attempt, now)
	// A confirmed cancellation acknowledgement resolves stop intent, but cannot
	// bypass expired authority or an ownership change.
	if decision == "STOP_REQUESTED" && job.cancelled && r.Reason == "USER_CANCELLED" && r.Stopped && attempt.expires.After(now) && attempt.phase.After(now) {
		return "ACCEPTED"
	}
	return decision
}

func validateCompletionPhase(r CompletionRequest, state string, exit *int32, cancelled bool) error {
	if (exit == nil) != (r.ExitCode == nil) || exit != nil && *exit != *r.ExitCode {
		return ErrConflict
	}
	switch r.Reason {
	case "", "APPLICATION_EXIT", "OOM", "OUTPUT_INVALID", "FINALIZATION_TIMEOUT":
		if state != "FINALIZING" {
			return ErrConflict
		}
	case "STARTUP_TIMEOUT":
		if state != "ASSIGNED" && state != "STARTING" {
			return ErrConflict
		}
	case "EXECUTION_TIMEOUT":
		if state != "RUNNING" && state != "FINALIZING" {
			return ErrConflict
		}
	case "USER_CANCELLED":
		if !cancelled || !r.Stopped {
			return ErrConflict
		}
	}
	return nil
}

func completionManifest(ctx context.Context, tx pgx.Tx, r CompletionRequest, p completionPayload, job spec.Job, projectID, specHash, state string, now time.Time) ([]byte, []publishedArtifact, error) {
	m := resultManifest{Version: 1, Authority: r.Authority, ProjectID: projectID, Spec: job, SpecSHA256: specHash, State: state, ExitCode: r.ExitCode, Reason: r.Reason, CompletedAt: now.UTC(), CleanupPending: !r.Stopped, Outputs: []publishedArtifact{}, LogArtifacts: []publishedArtifact{}, LogsComplete: r.LogsComplete, Gaps: p.Gaps, Metrics: p.Metrics, Attempts: []resultAttempt{}}
	declared := map[string]spec.Output{}
	for _, o := range job.Spec.Outputs {
		declared[o.Name] = o
	}
	for _, o := range p.Outputs {
		definition, ok := declared[o.Name]
		if !ok {
			return nil, nil, ErrInvalid
		}
		var a publishedArtifact
		err := tx.QueryRow(ctx, `SELECT a.id::text,u.upload_id::text,u.logical_name,u.kind,u.object_key,a.object_version,u.size_bytes,u.sha256 FROM artifacts a JOIN artifact_uploads u ON u.upload_id=a.upload_id WHERE a.id=$1 AND u.attempt_id=$2`, o.ArtifactID, r.Authority.AttemptID).Scan(&a.ArtifactID, &a.UploadID, &a.Name, &a.Kind, &a.Object.Key, &a.Object.Version, &a.Object.SizeBytes, &a.Object.SHA256)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrInvalid
		}
		if err != nil {
			return nil, nil, err
		}
		if a.Kind != "OUTPUT" || a.Name != o.Name || a.Object.SizeBytes > definition.MaxBytes {
			return nil, nil, ErrInvalid
		}
		m.Outputs = append(m.Outputs, a)
		delete(declared, o.Name)
		if o.Name == "metrics" {
			if len(r.MetricsJSON) == 0 || a.Object.SHA256 != p.MetricsSourceSHA256 || a.Object.SizeBytes != int64(len(r.MetricsJSON)) {
				return nil, nil, ErrInvalid
			}
			m.MetricsArtifactID = a.ArtifactID
		}
	}
	if r.Reason == "" {
		for _, o := range declared {
			if o.Required {
				return nil, nil, ErrInvalid
			}
		}
	}
	if len(r.MetricsJSON) > 0 && m.MetricsArtifactID == "" {
		return nil, nil, ErrInvalid
	}
	rows, err := tx.Query(ctx, `SELECT a.id::text,u.upload_id::text,u.logical_name,u.kind,u.object_key,a.object_version,u.size_bytes,u.sha256 FROM artifacts a JOIN artifact_uploads u ON u.upload_id=a.upload_id WHERE u.attempt_id=$1 AND u.kind='LOG' ORDER BY u.logical_name,u.created_at,u.upload_id`, r.Authority.AttemptID)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var a publishedArtifact
		if err = rows.Scan(&a.ArtifactID, &a.UploadID, &a.Name, &a.Kind, &a.Object.Key, &a.Object.Version, &a.Object.SizeBytes, &a.Object.SHA256); err != nil {
			rows.Close()
			return nil, nil, err
		}
		m.LogArtifacts = append(m.LogArtifacts, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	if r.LogsComplete {
		var pending bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_uploads u LEFT JOIN artifacts a ON a.upload_id=u.upload_id WHERE u.attempt_id=$1 AND u.kind='LOG' AND a.id IS NULL)`, r.Authority.AttemptID).Scan(&pending); err != nil {
			return nil, nil, err
		}
		if pending {
			return nil, nil, ErrInvalid
		}
	}
	rows, err = tx.Query(ctx, "SELECT id::text,attempt_number,state,reason,exit_code,created_at,finished_at FROM attempts WHERE job_id=$1 ORDER BY attempt_number", r.Authority.JobID)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var a resultAttempt
		if err = rows.Scan(&a.AttemptID, &a.Number, &a.State, &a.Reason, &a.ExitCode, &a.CreatedAt, &a.FinishedAt); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if a.AttemptID == r.Authority.AttemptID {
			a.State = state
			a.Reason = &r.Reason
			a.ExitCode = r.ExitCode
			a.FinishedAt = &now
		}
		m.Attempts = append(m.Attempts, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	body, err := json.Marshal(m)
	if err != nil || len(body) > 2<<20 {
		return nil, nil, ErrInvalid
	}
	return body, append(m.Outputs, m.LogArtifacts...), nil
}
