ALTER TABLE jobs DROP CONSTRAINT jobs_sweep_index_unique;
ALTER TABLE jobs DROP CONSTRAINT jobs_sweep_project;
ALTER TABLE jobs DROP CONSTRAINT jobs_sweep_pair;
ALTER TABLE jobs DROP COLUMN sweep_index;
ALTER TABLE jobs DROP COLUMN sweep_id;
DROP TABLE sweeps;
