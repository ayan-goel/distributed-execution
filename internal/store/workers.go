package store

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"time"
	"unicode"
	"unicode/utf8"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerIdentity struct {
	WorkerID     string `json:"workerId"`
	CredentialID string `json:"credentialId"`
}

type WorkerProvision struct {
	Name              string
	CertificateSHA256 [32]byte
	Resources         spec.Resources
	Slots             int
	Projects          []string
	Labels            map[string]string
}

var workerName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

func (p WorkerProvision) validate() error {
	if !workerName.MatchString(p.Name) || p.CertificateSHA256 == ([32]byte{}) || len(p.Projects) < 1 || len(p.Projects) > 64 {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, name := range p.Projects {
		if !workerName.MatchString(name) || seen[name] {
			return ErrInvalid
		}
		seen[name] = true
	}
	return validateWorkerConfiguration(p.Resources, p.Slots, p.Labels)
}

func validateWorkerConfiguration(r spec.Resources, slots int, labels map[string]string) error {
	// Bound capacities before SQL and future byte conversions. These are host
	// ceilings, not worker claims; scheduling must also respect project quotas.
	if r.CPUMillis < 1 || r.CPUMillis > 1_024_000 || r.MemoryMiB < 1 || r.MemoryMiB > 16_777_216 || r.ScratchMiB < 1 || r.ScratchMiB > 1_073_741_824 || slots < 1 || slots > 1000 || len(labels) > 64 {
		return ErrInvalid
	}
	if labels["os"] != "linux" || (labels["architecture"] != "amd64" && labels["architecture"] != "arm64") {
		return ErrInvalid
	}
	for key, value := range labels {
		if !workerName.MatchString(key) || len(value) == 0 || len(value) > 256 || !utf8.ValidString(value) {
			return ErrInvalid
		}
		for _, r := range value {
			if unicode.IsControl(r) {
				return ErrInvalid
			}
		}
	}
	return nil
}

func ProvisionWorker(ctx context.Context, pool *pgxpool.Pool, p WorkerProvision) (identity WorkerIdentity, err error) {
	if err = p.validate(); err != nil {
		return identity, err
	}
	// Certificate uniqueness covers concurrent provisioning and revoked identities.
	// A conflict never leaves an unauthenticated, partially configured host behind.
	defer func() {
		var dbErr *pgconn.PgError
		if errors.As(err, &dbErr) && dbErr.Code == "23505" {
			identity = WorkerIdentity{}
			err = ErrConflict
		}
	}()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return WorkerIdentity{}, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return WorkerIdentity{}, err
	}
	names := slices.Clone(p.Projects)
	slices.Sort(names)
	projectIDs := make([]string, 0, len(names))
	for _, name := range names {
		var id string
		var enabled bool
		err = tx.QueryRow(ctx, "SELECT id::text,enabled FROM projects WHERE name=$1 FOR SHARE", name).Scan(&id, &enabled)
		if errors.Is(err, pgx.ErrNoRows) {
			return WorkerIdentity{}, ErrNotFound
		}
		if err != nil {
			return WorkerIdentity{}, err
		}
		if !enabled {
			return WorkerIdentity{}, ErrDisabled
		}
		projectIDs = append(projectIDs, id)
	}
	labels, err := json.Marshal(p.Labels)
	if err != nil {
		return WorkerIdentity{}, err
	}
	// New hosts start REGISTERING, with no session and no schedulable capacity.
	// A later authenticated reconciliation handshake is required to reach READY.
	err = tx.QueryRow(ctx, `INSERT INTO workers(name,cpu_millis,memory_mib,scratch_mib,slots,labels) VALUES($1,$2,$3,$4,$5,$6) RETURNING id::text`, p.Name, p.Resources.CPUMillis, p.Resources.MemoryMiB, p.Resources.ScratchMiB, p.Slots, labels).Scan(&identity.WorkerID)
	if err != nil {
		return WorkerIdentity{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO worker_authorizations(worker_id,cpu_limit,memory_limit_mib,scratch_limit_mib,slot_limit,labels) VALUES($1,$2,$3,$4,$5,$6)`, identity.WorkerID, p.Resources.CPUMillis, p.Resources.MemoryMiB, p.Resources.ScratchMiB, p.Slots, labels); err != nil {
		return WorkerIdentity{}, err
	}
	if err = tx.QueryRow(ctx, "INSERT INTO worker_credentials(worker_id,certificate_sha256) VALUES($1,$2) RETURNING id::text", identity.WorkerID, p.CertificateSHA256[:]).Scan(&identity.CredentialID); err != nil {
		return WorkerIdentity{}, err
	}
	for _, id := range projectIDs {
		if _, err = tx.Exec(ctx, "INSERT INTO worker_projects(worker_id,project_id) VALUES($1,$2)", identity.WorkerID, id); err != nil {
			return WorkerIdentity{}, err
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO worker_audit_events(worker_id,credential_id,action) VALUES($1,$2,'WORKER_PROVISIONED')", identity.WorkerID, identity.CredentialID); err != nil {
		return WorkerIdentity{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return WorkerIdentity{}, err
	}
	return identity, nil
}

// AuthenticateWorker requires a fingerprint from an already verified mTLS leaf
// certificate. A caller-provided fingerprint is not proof of host identity.
func AuthenticateWorker(ctx context.Context, pool *pgxpool.Pool, fingerprint [32]byte) (WorkerIdentity, error) {
	var identity WorkerIdentity
	err := pool.QueryRow(ctx, "SELECT worker_id::text,id::text FROM worker_credentials WHERE certificate_sha256=$1 AND revoked_at IS NULL", fingerprint[:]).Scan(&identity.WorkerID, &identity.CredentialID)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkerIdentity{}, ErrUnauthorized
	}
	return identity, err
}

func RevokeWorkerCredential(ctx context.Context, pool *pgxpool.Pool, workerID, credentialID string) error {
	if _, err := uuid.Parse(workerID); err != nil {
		return ErrInvalid
	}
	if _, err := uuid.Parse(credentialID); err != nil {
		return ErrInvalid
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return err
	}
	var revoked *time.Time
	err = tx.QueryRow(ctx, "SELECT revoked_at FROM worker_credentials WHERE worker_id=$1 AND id=$2 FOR UPDATE", workerID, credentialID).Scan(&revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if revoked != nil {
		return nil
	}
	// Revocation prevents future authenticated calls; it does not assert that
	// containers stopped. Lease fencing and cleanup remain separate transitions.
	if _, err = tx.Exec(ctx, "UPDATE worker_credentials SET revoked_at=clock_timestamp() WHERE id=$1", credentialID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO worker_audit_events(worker_id,credential_id,action) VALUES($1,$2,'WORKER_CREDENTIAL_REVOKED')", workerID, credentialID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
