package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var datasetNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type DatasetUploadRequest struct {
	ProjectID string
	RequestID string
	Name      string
	SizeBytes int64
	SHA256    string
}

type DatasetUploadRecord struct {
	ID        string
	ProjectID string
	Name      string
	SizeBytes int64
	SHA256    string
	ObjectKey string
	CreatedAt time.Time
	Replayed  bool
}

func CreateDatasetUpload(ctx context.Context, pool *pgxpool.Pool, request DatasetUploadRequest) (DatasetUploadRecord, error) {
	var record DatasetUploadRecord
	if !canonicalUUID(request.ProjectID) || !canonicalUUID(request.RequestID) ||
		!datasetNamePattern.MatchString(request.Name) || request.SizeBytes < 1 ||
		request.SizeBytes > objectstore.MaxSinglePartBytes || !hashPattern.MatchString(request.SHA256) {
		return record, ErrInvalid
	}
	claim := request.Name + "\n" + strconv.FormatInt(request.SizeBytes, 10) + "\n" + request.SHA256
	sum := sha256.Sum256([]byte(claim))
	requestHash := hex.EncodeToString(sum[:])
	tx, err := pool.Begin(ctx)
	if err != nil {
		return record, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return record, err
	}
	var enabled bool
	// A shared project lock prevents disablement from racing key allocation;
	// unrelated projects and concurrent same-key replays remain independent.
	err = tx.QueryRow(ctx, "SELECT enabled FROM projects WHERE id=$1 FOR SHARE", request.ProjectID).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return record, ErrNotFound
	}
	if err != nil {
		return record, err
	}
	if !enabled {
		return record, ErrDisabled
	}
	// INVARIANT: the server allocates one key per project/request identity. An
	// uncertain response may replay, but changed bytes must never reuse that key.
	result, err := tx.Exec(ctx, `INSERT INTO dataset_uploads(id,project_id,request_id,request_hash,name,size_bytes,sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(project_id,request_id) DO NOTHING`,
		uuid.NewString(), request.ProjectID, request.RequestID, requestHash,
		request.Name, request.SizeBytes, request.SHA256)
	if err != nil {
		return record, err
	}
	var savedHash string
	err = tx.QueryRow(ctx, `SELECT id::text,project_id::text,name,size_bytes,sha256,object_key,created_at,request_hash
		FROM dataset_uploads WHERE project_id=$1 AND request_id=$2`, request.ProjectID, request.RequestID).
		Scan(&record.ID, &record.ProjectID, &record.Name, &record.SizeBytes, &record.SHA256,
			&record.ObjectKey, &record.CreatedAt, &savedHash)
	if err != nil {
		return DatasetUploadRecord{}, err
	}
	if savedHash != requestHash {
		return DatasetUploadRecord{}, ErrConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return DatasetUploadRecord{}, err
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.Replayed = result.RowsAffected() == 0
	return record, nil
}
