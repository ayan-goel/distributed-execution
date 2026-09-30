ALTER TABLE jobs ADD CONSTRAINT jobs_id_project_unique UNIQUE(id,project_id);
ALTER TABLE datasets ADD CONSTRAINT datasets_id_project_unique UNIQUE(id,project_id);

CREATE TABLE job_inputs (
    job_id uuid NOT NULL,
    project_id uuid NOT NULL,
    position smallint NOT NULL CHECK (position BETWEEN 0 AND 63),
    dataset_id uuid NOT NULL,
    mount_path text NOT NULL CHECK (
        octet_length(mount_path) BETWEEN 9 AND 4096 AND
        mount_path LIKE '/inputs/%' AND
        position(chr(92) in mount_path)=0
    ),
    PRIMARY KEY(job_id,position),
    UNIQUE(job_id,mount_path),
    FOREIGN KEY(job_id,project_id) REFERENCES jobs(id,project_id),
    FOREIGN KEY(dataset_id,project_id) REFERENCES datasets(id,project_id)
);
CREATE INDEX job_inputs_dataset_id ON job_inputs(dataset_id);

CREATE FUNCTION protect_job_input() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- INVARIANT: retries and replays keep the exact registered dataset version
    -- admitted with the job, even if a control-plane write is attempted later.
    RAISE EXCEPTION 'job input bindings are immutable' USING ERRCODE='23514';
END $$;
CREATE TRIGGER protect_job_input_binding BEFORE UPDATE OR DELETE ON job_inputs
FOR EACH ROW EXECUTE FUNCTION protect_job_input();
