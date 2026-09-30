package store

import (
	"context"
	"encoding/json"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobInputBinding struct {
	Dataset   DatasetBinding
	MountPath string
}

type inputQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func LoadJobInputs(ctx context.Context, pool *pgxpool.Pool, projectID, jobID string, expected []spec.Input) ([]JobInputBinding, error) {
	return loadJobInputs(ctx, pool, projectID, jobID, expected)
}

func loadJobInputs(ctx context.Context, q inputQuerier, projectID, jobID string, expected []spec.Input) ([]JobInputBinding, error) {
	if !canonicalUUID(projectID) || !canonicalUUID(jobID) || len(expected) > 64 {
		return nil, ErrInvalid
	}
	rows, err := q.Query(ctx, `SELECT i.position,i.mount_path,d.id::text,d.name,
		u.object_key,u.size_bytes,u.sha256,d.object_version,d.manifest
		FROM job_inputs i JOIN datasets d ON d.id=i.dataset_id AND d.project_id=i.project_id
		JOIN dataset_uploads u ON u.id=d.upload_id AND u.project_id=d.project_id
		WHERE i.job_id=$1 AND i.project_id=$2 ORDER BY i.position`, jobID, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bindings := make([]JobInputBinding, 0, len(expected))
	for rows.Next() {
		var position int16
		var binding JobInputBinding
		var manifest []byte
		if err := rows.Scan(&position, &binding.MountPath, &binding.Dataset.ID,
			&binding.Dataset.Name, &binding.Dataset.Object.Key, &binding.Dataset.Object.Size,
			&binding.Dataset.Object.SHA256, &binding.Dataset.Object.Version, &manifest); err != nil {
			return nil, err
		}
		if int(position) != len(bindings) || len(bindings) >= len(expected) ||
			binding.Dataset.Name != expected[position].Dataset || binding.MountPath != expected[position].MountPath ||
			json.Unmarshal(manifest, &binding.Dataset.Manifest) != nil ||
			binding.Dataset.Manifest.validate(binding.Dataset.Object.Size) != nil {
			return nil, ErrInvalid
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(bindings) != len(expected) {
		return nil, ErrNotFound
	}
	return bindings, nil
}
