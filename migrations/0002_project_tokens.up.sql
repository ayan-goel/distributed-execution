-- Project tokens are high-entropy bearer secrets. Persist only their SHA-256
-- digest; raw values are returned once to the provisioning operator.
CREATE TABLE api_tokens (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id uuid NOT NULL REFERENCES projects(id),
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash)=32),
    role text NOT NULL CHECK (role IN ('read','submit','operator')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at timestamptz
);
CREATE TABLE audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    token_id uuid REFERENCES api_tokens(id),
    action text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
