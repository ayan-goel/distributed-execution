-- Project-scoped keyset traversal avoids sorting the entire project history.
-- This transactional build may block writes while an existing table is indexed.
CREATE INDEX jobs_project_created_id_idx ON jobs(project_id,created_at DESC,id DESC);
