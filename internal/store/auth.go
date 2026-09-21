package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Role string

const (
	RoleRead     Role = "read"
	RoleSubmit   Role = "submit"
	RoleOperator Role = "operator"
)

var ErrUnauthorized = errors.New("invalid or revoked credentials")

type Principal struct {
	ProjectID string
	Project   string
	TokenID   string
	Role      Role
}

func (p Principal) Allows(required Role) bool {
	switch required {
	case RoleRead:
		return p.Role == RoleRead || p.Role == RoleSubmit || p.Role == RoleOperator
	case RoleSubmit:
		return p.Role == RoleSubmit || p.Role == RoleOperator
	case RoleOperator:
		return p.Role == RoleOperator
	default:
		return false
	}
}

func Authenticate(ctx context.Context, pool *pgxpool.Pool, token string) (Principal, error) {
	var p Principal
	if len(token) != 47 || !strings.HasPrefix(token, "dsp_") {
		return p, ErrUnauthorized
	}
	hash := sha256.Sum256([]byte(token))
	// No token cache: each request observes committed revocation and project
	// disabling. Never put this token or its hash into request logs or errors.
	err := pool.QueryRow(ctx, `SELECT p.id::text,p.name,t.id::text,t.role FROM api_tokens t
		JOIN projects p ON p.id=t.project_id WHERE t.token_hash=$1 AND t.revoked_at IS NULL AND p.enabled`, hash[:]).Scan(&p.ProjectID, &p.Project, &p.TokenID, &p.Role)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrUnauthorized
	}
	return p, err
}

func IssueToken(ctx context.Context, pool *pgxpool.Pool, project string, role Role) (string, string, error) {
	if role != RoleRead && role != RoleSubmit && role != RoleOperator {
		return "", "", ErrInvalid
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", "", err
	}
	token := "dsp_" + base64.RawURLEncoding.EncodeToString(random)
	// SHA-256 is appropriate for 256-bit random API tokens, not user passwords.
	// Provisioning is a local operator action; raw tokens never enter SQL storage.
	hash := sha256.Sum256([]byte(token))
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return "", "", err
	}
	var projectID string
	var enabled bool
	err = tx.QueryRow(ctx, "SELECT id::text,enabled FROM projects WHERE name=$1 FOR SHARE", project).Scan(&projectID, &enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	if !enabled {
		return "", "", ErrDisabled
	}
	var id string
	if err = tx.QueryRow(ctx, "INSERT INTO api_tokens(project_id,token_hash,role) VALUES ($1,$2,$3) RETURNING id::text", projectID, hash[:], role).Scan(&id); err != nil {
		return "", "", err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO audit_events(project_id,token_id,action) VALUES ($1,$2,'TOKEN_CREATED')", projectID, id); err != nil {
		return "", "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", "", err
	}
	return token, id, nil
}

func RevokeToken(ctx context.Context, pool *pgxpool.Pool, project, id string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return err
	}
	var projectID string
	var revoked *time.Time
	err = tx.QueryRow(ctx, `SELECT t.project_id::text,t.revoked_at FROM api_tokens t JOIN projects p ON p.id=t.project_id
		WHERE t.id=$1 AND p.name=$2 FOR UPDATE OF t`, id, project).Scan(&projectID, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if revoked != nil {
		return nil
	}
	// Revocation and its audit event commit together. Repeated requests return
	// success without manufacturing additional revocation events.
	if _, err = tx.Exec(ctx, "UPDATE api_tokens SET revoked_at=clock_timestamp() WHERE id=$1", id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO audit_events(project_id,token_id,action) VALUES ($1,$2,'TOKEN_REVOKED')", projectID, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
