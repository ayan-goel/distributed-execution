package store

import (
	"context"
	"encoding/json"
	"errors"

	"dispatch.local/dispatch/internal/objectstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DatasetBinding struct {
	ID       string
	Name     string
	Object   objectstore.Object
	Manifest DatasetManifest
}

func ResolveDatasetNames(ctx context.Context, pool *pgxpool.Pool, projectID string, names []string) ([]DatasetBinding, error) {
	if !canonicalUUID(projectID) || len(names) == 0 || len(names) > 64 {
		return nil, ErrInvalid
	}
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		if !datasetNamePattern.MatchString(name) {
			return nil, ErrInvalid
		}
		unique[name] = struct{}{}
	}
	requested := make([]string, 0, len(unique))
	for name := range unique {
		requested = append(requested, name)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return nil, err
	}
	var enabled bool
	// A short shared lock makes the project enablement decision consistent with
	// this lookup. Submission independently rechecks it before queue insertion.
	err = tx.QueryRow(ctx, "SELECT enabled FROM projects WHERE id=$1 FOR SHARE", projectID).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, ErrDisabled
	}
	rows, err := tx.Query(ctx, `SELECT d.id::text,d.name,u.object_key,u.size_bytes,u.sha256,d.object_version,d.manifest
		FROM datasets d JOIN dataset_uploads u ON u.id=d.upload_id AND u.project_id=d.project_id
		WHERE d.project_id=$1 AND d.name=ANY($2::text[])`, projectID, requested)
	if err != nil {
		return nil, err
	}
	found := make(map[string]DatasetBinding, len(unique))
	for rows.Next() {
		var binding DatasetBinding
		var manifest []byte
		if err := rows.Scan(&binding.ID, &binding.Name, &binding.Object.Key, &binding.Object.Size,
			&binding.Object.SHA256, &binding.Object.Version, &manifest); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal(manifest, &binding.Manifest); err != nil {
			rows.Close()
			return nil, err
		}
		found[binding.Name] = binding
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(found) != len(unique) {
		return nil, ErrNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	// Preserve input order and repeated mounts while fetching each registered
	// name once; every returned reference is already immutable in the catalog.
	bindings := make([]DatasetBinding, len(names))
	for i, name := range names {
		bindings[i] = found[name]
	}
	return bindings, nil
}
