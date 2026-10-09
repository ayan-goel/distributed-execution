-- A single durable cursor lets all control-plane connections share admission
-- fairness. It grants no execution authority; attempts and reservations do that.
CREATE TABLE scheduler_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    project_cursor uuid REFERENCES projects(id) ON DELETE SET NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(updated_at))
);
INSERT INTO scheduler_state(singleton) VALUES(true);
