CREATE TABLE sweeps (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id uuid NOT NULL REFERENCES projects(id),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 128),
    spec jsonb NOT NULL CHECK (jsonb_typeof(spec)='object' AND octet_length(spec::text)<=2097152),
    spec_hash text NOT NULL CHECK (spec_hash ~ '^[0-9a-f]{64}$'),
    child_count integer NOT NULL CHECK (child_count BETWEEN 1 AND 1000),
    max_concurrent integer NOT NULL CHECK (max_concurrent BETWEEN 1 AND 1000),
    fail_fast boolean NOT NULL,
    cancel_running_on_failure boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(project_id,id),
    CHECK (NOT cancel_running_on_failure OR fail_fast)
);

ALTER TABLE jobs ADD COLUMN sweep_id uuid;
ALTER TABLE jobs ADD COLUMN sweep_index integer;
-- INVARIANT: a child index is meaningful only within its own project's sweep.
-- The pair and composite FK prevent cross-project links and orphan child slots.
ALTER TABLE jobs ADD CONSTRAINT jobs_sweep_pair
    CHECK ((sweep_id IS NULL) = (sweep_index IS NULL) AND (sweep_index IS NULL OR sweep_index BETWEEN 0 AND 999));
ALTER TABLE jobs ADD CONSTRAINT jobs_sweep_project
    FOREIGN KEY(project_id,sweep_id) REFERENCES sweeps(project_id,id);
ALTER TABLE jobs ADD CONSTRAINT jobs_sweep_index_unique UNIQUE(sweep_id,sweep_index);
