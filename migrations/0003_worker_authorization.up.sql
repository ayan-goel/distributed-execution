-- Host identity and capacity are operator-provisioned. Registration may reduce
-- advertised resources but cannot enlarge these limits or grant project access.
CREATE TABLE worker_authorizations (
    worker_id uuid PRIMARY KEY REFERENCES workers(id),
    cpu_limit bigint NOT NULL CHECK (cpu_limit BETWEEN 1 AND 1024000),
    memory_limit_mib bigint NOT NULL CHECK (memory_limit_mib BETWEEN 1 AND 16777216),
    scratch_limit_mib bigint NOT NULL CHECK (scratch_limit_mib BETWEEN 1 AND 1073741824),
    slot_limit integer NOT NULL CHECK (slot_limit BETWEEN 1 AND 1000),
    labels jsonb NOT NULL CHECK (jsonb_typeof(labels)='object' AND octet_length(labels::text)<=65536)
);
CREATE TABLE worker_projects (
    worker_id uuid NOT NULL REFERENCES workers(id),
    project_id uuid NOT NULL REFERENCES projects(id),
    PRIMARY KEY(worker_id,project_id)
);
CREATE INDEX project_workers ON worker_projects(project_id,worker_id);

-- Store a fingerprint of the public leaf certificate, never a private key.
-- TLS chain/client-use verification is still required before this lookup.
CREATE TABLE worker_credentials (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_id uuid NOT NULL REFERENCES workers(id),
    certificate_sha256 bytea NOT NULL UNIQUE CHECK (octet_length(certificate_sha256)=32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at timestamptz,
    UNIQUE(worker_id,id)
);
CREATE TABLE worker_audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    worker_id uuid NOT NULL REFERENCES workers(id),
    credential_id uuid,
    action text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(worker_id,credential_id) REFERENCES worker_credentials(worker_id,id)
);
