CREATE OR REPLACE FUNCTION pg_temp.expect_error(statement text, expected text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE actual text;
BEGIN
    BEGIN
        EXECUTE statement;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual = RETURNED_SQLSTATE;
        IF actual = expected THEN RETURN; END IF;
        RAISE EXCEPTION 'Expected SQLSTATE %, got %: %', expected, actual, SQLERRM;
    END;
    RAISE EXCEPTION 'Expected SQLSTATE % but statement succeeded: %', expected, statement;
END $$;

BEGIN;
INSERT INTO projects(id,name,cpu_quota,memory_quota_mib,concurrency_quota)
VALUES ('00000000-0000-0000-0000-000000000001','research',4000,8192,4);
INSERT INTO workers(id,name,cpu_millis,memory_mib,scratch_mib,slots)
VALUES ('00000000-0000-0000-0000-000000000002','worker-a',4000,8192,16384,4);
INSERT INTO worker_sessions(id,worker_id,generation)
VALUES ('00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000002',1);
INSERT INTO jobs(id,project_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib)
VALUES ('00000000-0000-0000-0000-000000000004','00000000-0000-0000-0000-000000000001','{}',repeat('a',64),1000,1024,1024),
('00000000-0000-0000-0000-000000000005','00000000-0000-0000-0000-000000000001','{}',repeat('b',64),1000,1024,1024);
INSERT INTO attempts(id,job_id,attempt_number,worker_id,session_id,generation,lease_expires_at,phase_deadline)
VALUES ('00000000-0000-0000-0000-000000000006','00000000-0000-0000-0000-000000000004',1,
'00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000003',1,clock_timestamp()+interval '30 seconds',clock_timestamp()+interval '300 seconds');
INSERT INTO reservations(attempt_id,worker_id,cpu_millis,memory_mib,scratch_mib)
VALUES ('00000000-0000-0000-0000-000000000006','00000000-0000-0000-0000-000000000002',1000,1024,1024);
UPDATE jobs SET state='ACTIVE',current_attempt_id='00000000-0000-0000-0000-000000000006',attempt_counter=1
WHERE id='00000000-0000-0000-0000-000000000004';
COMMIT;

SELECT pg_temp.expect_error($q$UPDATE jobs SET cpu_millis=-1 WHERE id='00000000-0000-0000-0000-000000000005'$q$,'23514');
SELECT pg_temp.expect_error($q$UPDATE jobs SET state='MADE_UP' WHERE id='00000000-0000-0000-0000-000000000005'$q$,'23514');
SELECT pg_temp.expect_error($q$INSERT INTO attempts(job_id,attempt_number,worker_id,session_id,generation,lease_expires_at,phase_deadline)
VALUES ('00000000-0000-0000-0000-000000000004',2,'00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000003',2,clock_timestamp(),clock_timestamp())$q$,'23505');
SELECT pg_temp.expect_error($q$INSERT INTO attempts(job_id,attempt_number,worker_id,session_id,generation,state,lease_expires_at,phase_deadline)
VALUES ('00000000-0000-0000-0000-000000000004',1,'00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000003',1,'LOST',clock_timestamp(),clock_timestamp())$q$,'23505');

-- Immediate constraint checks expose deferred violations inside a savepoint, so
-- the test can assert their SQLSTATE without corrupting later test fixtures.
SELECT pg_temp.expect_error($q$DO $body$ BEGIN
UPDATE jobs SET state='ACTIVE',current_attempt_id='00000000-0000-0000-0000-000000000006' WHERE id='00000000-0000-0000-0000-000000000005';
SET CONSTRAINTS ALL IMMEDIATE;
END $body$$q$,'23503');
SELECT pg_temp.expect_error($q$DO $body$ BEGIN
UPDATE jobs SET state='QUEUED',current_attempt_id=NULL WHERE id='00000000-0000-0000-0000-000000000004';
SET CONSTRAINTS ALL IMMEDIATE;
END $body$$q$,'23514');
SELECT pg_temp.expect_error($q$DO $body$ BEGIN
DELETE FROM reservations WHERE attempt_id='00000000-0000-0000-0000-000000000006';
SET CONSTRAINTS ALL IMMEDIATE;
END $body$$q$,'23514');

BEGIN;
UPDATE attempts SET state='FAILED',reason='APPLICATION_EXIT' WHERE id='00000000-0000-0000-0000-000000000006';
UPDATE reservations SET state='released' WHERE attempt_id='00000000-0000-0000-0000-000000000006';
UPDATE jobs SET state='FAILED',current_attempt_id=NULL WHERE id='00000000-0000-0000-0000-000000000004';
COMMIT;
SELECT pg_temp.expect_error($q$UPDATE jobs SET state='QUEUED' WHERE id='00000000-0000-0000-0000-000000000004'$q$,'23514');
SELECT pg_temp.expect_error($q$UPDATE jobs SET spec='{"changed":true}' WHERE id='00000000-0000-0000-0000-000000000005'$q$,'23514');
