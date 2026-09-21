-- Core ownership changes commit together. Deferred constraints permit inserting
-- an attempt/reservation before its job pointer, but never expose a partial owner.
CREATE TABLE projects (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL UNIQUE,
    cpu_quota bigint NOT NULL CHECK (cpu_quota > 0),
    memory_quota_mib bigint NOT NULL CHECK (memory_quota_mib > 0),
    concurrency_quota integer NOT NULL CHECK (concurrency_quota > 0),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE workers (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    state text NOT NULL DEFAULT 'REGISTERING' CHECK (state IN ('REGISTERING','READY','DRAINING','SUSPECT','OFFLINE','QUARANTINED')),
    labels jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(labels)='object'),
    capabilities jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(capabilities)='object'),
    cpu_millis bigint NOT NULL CHECK (cpu_millis > 0),
    memory_mib bigint NOT NULL CHECK (memory_mib > 0),
    scratch_mib bigint NOT NULL CHECK (scratch_mib > 0),
    slots integer NOT NULL CHECK (slots > 0),
    current_session_id uuid,
    drain_requested boolean NOT NULL DEFAULT false,
    last_heartbeat_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE worker_sessions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_id uuid NOT NULL REFERENCES workers(id),
    generation bigint NOT NULL CHECK (generation > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    fenced_at timestamptz,
    UNIQUE(worker_id,generation),
    UNIQUE(worker_id,id)
);
ALTER TABLE workers ADD FOREIGN KEY(id,current_session_id)
REFERENCES worker_sessions(worker_id,id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id uuid NOT NULL REFERENCES projects(id),
    parent_job_id uuid REFERENCES jobs(id),
    spec jsonb NOT NULL CHECK (jsonb_typeof(spec)='object' AND octet_length(spec::text)<=2097152),
    spec_hash text NOT NULL CHECK (spec_hash ~ '^[0-9a-f]{64}$'),
    state text NOT NULL DEFAULT 'QUEUED' CHECK (state IN ('QUEUED','ACTIVE','RETRY_WAIT','CANCELLING','SUCCEEDED','FAILED','CANCELLED')),
    cpu_millis bigint NOT NULL CHECK (cpu_millis > 0),
    memory_mib bigint NOT NULL CHECK (memory_mib > 0),
    scratch_mib bigint NOT NULL CHECK (scratch_mib > 0),
    priority smallint NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 3),
    next_eligible_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(next_eligible_at)),
    attempt_counter bigint NOT NULL DEFAULT 0 CHECK (attempt_counter >= 0),
    current_attempt_id uuid,
    cancel_requested boolean NOT NULL DEFAULT false,
    accepted_attempt_id uuid,
    accepted_manifest jsonb,
    event_sequence bigint NOT NULL DEFAULT 0 CHECK (event_sequence >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state IN ('ACTIVE','CANCELLING')) = (current_attempt_id IS NOT NULL)),
    CHECK ((state='SUCCEEDED') = (accepted_attempt_id IS NOT NULL AND accepted_manifest IS NOT NULL)),
    CHECK (state='SUCCEEDED' OR (accepted_attempt_id IS NULL AND accepted_manifest IS NULL)),
    CHECK (state<>'SUCCEEDED' OR NOT cancel_requested)
);

CREATE TABLE attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL REFERENCES jobs(id),
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    worker_id uuid NOT NULL REFERENCES workers(id),
    session_id uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation=attempt_number),
    state text NOT NULL DEFAULT 'ASSIGNED' CHECK (state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING','SUCCEEDED','FAILED','LOST','CANCELLED')),
    lease_expires_at timestamptz NOT NULL CHECK (isfinite(lease_expires_at)),
    phase_deadline timestamptz NOT NULL CHECK (isfinite(phase_deadline)),
    exit_code integer,
    reason text,
    completion_id uuid,
    completion_digest text CHECK (completion_digest ~ '^[0-9a-f]{64}$'),
    cleanup_pending boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    FOREIGN KEY(worker_id,session_id) REFERENCES worker_sessions(worker_id,id),
    UNIQUE(job_id,attempt_number),
    UNIQUE(job_id,id),
    UNIQUE(worker_id,id)
);
-- INVARIANT: a job has at most one authoritative active attempt, independently
-- of scheduler bugs or concurrent acquisitions using different request IDs.
CREATE UNIQUE INDEX one_active_attempt_per_job ON attempts(job_id)
WHERE state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING');
ALTER TABLE jobs ADD FOREIGN KEY(id,current_attempt_id) REFERENCES attempts(job_id,id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE jobs ADD FOREIGN KEY(id,accepted_attempt_id) REFERENCES attempts(job_id,id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE reservations (
    attempt_id uuid PRIMARY KEY REFERENCES attempts(id),
    worker_id uuid NOT NULL REFERENCES workers(id),
    cpu_millis bigint NOT NULL CHECK (cpu_millis > 0),
    memory_mib bigint NOT NULL CHECK (memory_mib > 0),
    scratch_mib bigint NOT NULL CHECK (scratch_mib > 0),
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','released','quarantined')),
    FOREIGN KEY(worker_id,attempt_id) REFERENCES attempts(worker_id,id)
);

CREATE TABLE job_events (
    job_id uuid NOT NULL REFERENCES jobs(id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    attempt_id uuid,
    type text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(payload)='object' AND octet_length(payload::text)<=65536),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id,sequence),
    FOREIGN KEY(job_id,attempt_id) REFERENCES attempts(job_id,id)
);

CREATE TABLE idempotency_keys (
    project_id uuid NOT NULL REFERENCES projects(id),
    endpoint text NOT NULL,
    key text NOT NULL CHECK (length(key) BETWEEN 1 AND 128),
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    response_reference uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(project_id,endpoint,key)
);

CREATE TABLE worker_requests (
    worker_id uuid NOT NULL,
    session_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    attempt_id uuid REFERENCES attempts(id),
    no_work_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(worker_id,session_id,request_id),
    FOREIGN KEY(worker_id,session_id) REFERENCES worker_sessions(worker_id,id),
    CHECK ((attempt_id IS NULL) = (no_work_reason IS NOT NULL))
);

CREATE INDEX queued_jobs ON jobs(project_id,state,next_eligible_at,priority,created_at);
CREATE INDEX active_leases ON attempts(lease_expires_at) WHERE state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING');
CREATE INDEX worker_reservations ON reservations(worker_id,state);

CREATE FUNCTION protect_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.id,OLD.project_id,OLD.spec,OLD.spec_hash,OLD.cpu_millis,OLD.memory_mib,OLD.scratch_mib)
        IS DISTINCT FROM (NEW.id,NEW.project_id,NEW.spec,NEW.spec_hash,NEW.cpu_millis,NEW.memory_mib,NEW.scratch_mib) THEN
        RAISE EXCEPTION 'job specifications are immutable; submit a new job' USING ERRCODE='23514';
    END IF;
    IF OLD.state IN ('SUCCEEDED','FAILED','CANCELLED') AND
        (OLD.state,OLD.accepted_attempt_id,OLD.accepted_manifest,OLD.cancel_requested,OLD.attempt_counter)
        IS DISTINCT FROM (NEW.state,NEW.accepted_attempt_id,NEW.accepted_manifest,NEW.cancel_requested,NEW.attempt_counter) THEN
        RAISE EXCEPTION 'terminal job outcomes are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_job BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION protect_job_identity();

CREATE FUNCTION protect_attempt_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.id,OLD.job_id,OLD.worker_id,OLD.session_id,OLD.attempt_number,OLD.generation)
        IS DISTINCT FROM (NEW.id,NEW.job_id,NEW.worker_id,NEW.session_id,NEW.attempt_number,NEW.generation) OR
        (OLD.state IN ('SUCCEEDED','FAILED','LOST','CANCELLED') AND
        (OLD.state,OLD.exit_code,OLD.reason,OLD.completion_id,OLD.completion_digest,OLD.lease_expires_at)
        IS DISTINCT FROM (NEW.state,NEW.exit_code,NEW.reason,NEW.completion_id,NEW.completion_digest,NEW.lease_expires_at)) THEN
        RAISE EXCEPTION 'attempt identity and terminal outcome are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER protect_attempt BEFORE UPDATE ON attempts FOR EACH ROW EXECUTE FUNCTION protect_attempt_identity();

CREATE FUNCTION check_job_ownership() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target uuid; j jobs%ROWTYPE; a attempts%ROWTYPE; r reservations%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME='jobs' THEN target := COALESCE(NEW.id,OLD.id);
    ELSIF TG_TABLE_NAME='attempts' THEN target := COALESCE(NEW.job_id,OLD.job_id);
    ELSE SELECT job_id INTO target FROM attempts WHERE id=COALESCE(NEW.attempt_id,OLD.attempt_id);
    END IF;
    SELECT * INTO j FROM jobs WHERE id=target;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT * INTO a FROM attempts WHERE job_id=target AND state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING');
    IF j.current_attempt_id IS DISTINCT FROM a.id THEN
        RAISE EXCEPTION 'current attempt must match authoritative active attempt' USING ERRCODE='23514';
    END IF;
    IF a.id IS NOT NULL THEN
        SELECT * INTO r FROM reservations WHERE attempt_id=a.id;
        IF r.attempt_id IS NULL OR r.state<>'active' OR
            (r.worker_id,r.cpu_millis,r.memory_mib,r.scratch_mib) IS DISTINCT FROM
            (a.worker_id,j.cpu_millis,j.memory_mib,j.scratch_mib) OR j.attempt_counter<>a.attempt_number THEN
            RAISE EXCEPTION 'active attempt requires a matching reservation and counter' USING ERRCODE='23514';
        END IF;
    END IF;
    IF j.state='SUCCEEDED' AND NOT EXISTS (SELECT 1 FROM attempts WHERE id=j.accepted_attempt_id AND state='SUCCEEDED') THEN
        RAISE EXCEPTION 'accepted result must belong to a successful attempt' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER job_ownership AFTER INSERT OR UPDATE OR DELETE ON jobs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_job_ownership();
CREATE CONSTRAINT TRIGGER attempt_ownership AFTER INSERT OR UPDATE OR DELETE ON attempts
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_job_ownership();
CREATE CONSTRAINT TRIGGER reservation_ownership AFTER INSERT OR UPDATE OR DELETE ON reservations
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_job_ownership();
