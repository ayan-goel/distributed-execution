package store

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"reflect"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DatasetFile struct {
	Path      string `json:"path"`
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
}

type DatasetManifest struct {
	Format string        `json:"format"`
	Files  []DatasetFile `json:"files"`
}

type DatasetRegistrationRequest struct {
	ProjectID string
	UploadID  string
	Version   string
	Manifest  DatasetManifest
}

type DatasetRecord struct {
	ID        string
	ProjectID string
	Name      string
	UploadID  string
	Object    objectstore.Object
	Manifest  DatasetManifest
	CreatedAt time.Time
	Replayed  bool
}

func (m DatasetManifest) validate(archiveBytes int64) error {
	if m.Format != "tar.v1" || len(m.Files) == 0 || len(m.Files) > 1024 {
		return ErrInvalid
	}
	var total int64
	previous := ""
	for _, file := range m.Files {
		if file.Path == "" || file.Path == "." || file.Path == ".." || strings.HasPrefix(file.Path, "../") ||
			len(file.Path) > 512 || path.IsAbs(file.Path) ||
			path.Clean(file.Path) != file.Path || strings.ContainsAny(file.Path, "\\\x00") ||
			file.Path <= previous || strings.HasPrefix(file.Path, previous+"/") && previous != "" ||
			file.SizeBytes < 0 || file.SizeBytes > archiveBytes-total || !hashPattern.MatchString(file.SHA256) {
			return ErrInvalid
		}
		previous = file.Path
		total += file.SizeBytes
	}
	return nil
}

func validDatasetVersion(version string) bool {
	if version == "" || version == "null" || len(version) > 1024 {
		return false
	}
	return strings.IndexFunc(version, func(r rune) bool { return r < 33 || r > 126 }) == -1
}

// RegisterDataset verifies a single immutable object version outside locks,
// then checks project ownership again before freezing the name and manifest.
func RegisterDataset(
	ctx context.Context,
	pool *pgxpool.Pool,
	request DatasetRegistrationRequest,
	verify func(context.Context, objectstore.Object) error,
) (DatasetRecord, error) {
	if !canonicalUUID(request.ProjectID) || !canonicalUUID(request.UploadID) ||
		!validDatasetVersion(request.Version) || verify == nil {
		return DatasetRecord{}, ErrInvalid
	}
	record, registered, err := lookupDataset(ctx, pool, request.ProjectID, request.UploadID)
	if err != nil {
		return DatasetRecord{}, err
	}
	if record.Object.Size > objectstore.MaxSinglePartBytes {
		return DatasetRecord{}, ErrInvalid
	}
	if err := request.Manifest.validate(record.Object.Size); err != nil {
		return DatasetRecord{}, err
	}
	manifest, err := json.Marshal(request.Manifest)
	if err != nil || len(manifest) > 2<<20 {
		return DatasetRecord{}, ErrInvalid
	}
	if registered {
		return replayDataset(record, request)
	}
	record.Object.Version = request.Version
	// INVARIANT: no PostgreSQL transaction spans object I/O. Verification reads
	// the exact version and digest; a later PUT to this key cannot change it.
	if err := verify(ctx, record.Object); err != nil {
		return DatasetRecord{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return DatasetRecord{}, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return DatasetRecord{}, err
	}
	var enabled bool
	err = tx.QueryRow(ctx, "SELECT enabled FROM projects WHERE id=$1 FOR SHARE", request.ProjectID).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return DatasetRecord{}, ErrNotFound
	}
	if err != nil {
		return DatasetRecord{}, err
	}
	if !enabled {
		return DatasetRecord{}, ErrDisabled
	}
	current, registered, err := lookupDataset(ctx, tx, request.ProjectID, request.UploadID)
	if err != nil {
		return DatasetRecord{}, err
	}
	if registered {
		return replayDataset(current, request)
	}
	// The declaration is immutable, but recheck the exact scope after external
	// verification so a racing registration cannot bind a different upload.
	if current.Object.Key != record.Object.Key || current.Object.Size != record.Object.Size || current.Object.SHA256 != record.Object.SHA256 {
		return DatasetRecord{}, ErrConflict
	}
	err = tx.QueryRow(ctx, `INSERT INTO datasets(project_id,name,upload_id,object_version,manifest)
		VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING id::text,created_at`,
		request.ProjectID, record.Name, request.UploadID, request.Version, manifest).
		Scan(&record.ID, &record.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		current, registered, err = lookupDataset(ctx, tx, request.ProjectID, request.UploadID)
		if err != nil {
			return DatasetRecord{}, err
		}
		if !registered {
			return DatasetRecord{}, ErrConflict
		}
		return replayDataset(current, request)
	}
	if err != nil {
		return DatasetRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DatasetRecord{}, err
	}
	record.Manifest = request.Manifest
	record.CreatedAt = record.CreatedAt.UTC()
	return record, nil
}

func replayDataset(record DatasetRecord, request DatasetRegistrationRequest) (DatasetRecord, error) {
	if record.Object.Version != request.Version || !reflect.DeepEqual(record.Manifest, request.Manifest) {
		return DatasetRecord{}, ErrConflict
	}
	record.Replayed = true
	return record, nil
}

func lookupDataset(ctx context.Context, db rowQuerier, projectID, uploadID string) (DatasetRecord, bool, error) {
	var record DatasetRecord
	var enabled bool
	var registeredID, registeredVersion *string
	var savedManifest []byte
	var registeredAt *time.Time
	err := db.QueryRow(ctx, `SELECT u.project_id::text,u.id::text,u.name,u.object_key,u.size_bytes,u.sha256,
		p.enabled,d.id::text,d.object_version,d.manifest,d.created_at
		FROM dataset_uploads u JOIN projects p ON p.id=u.project_id
		LEFT JOIN datasets d ON d.upload_id=u.id
		WHERE u.project_id=$1 AND u.id=$2`, projectID, uploadID).
		Scan(&record.ProjectID, &record.UploadID, &record.Name, &record.Object.Key,
			&record.Object.Size, &record.Object.SHA256, &enabled, &registeredID,
			&registeredVersion, &savedManifest, &registeredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return DatasetRecord{}, false, ErrNotFound
	}
	if err != nil {
		return DatasetRecord{}, false, err
	}
	if !enabled {
		return DatasetRecord{}, false, ErrDisabled
	}
	if registeredID == nil {
		return record, false, nil
	}
	record.ID = *registeredID
	record.Object.Version = *registeredVersion
	if err := json.Unmarshal(savedManifest, &record.Manifest); err != nil {
		return DatasetRecord{}, false, err
	}
	record.CreatedAt = registeredAt.UTC()
	return record, true, nil
}
