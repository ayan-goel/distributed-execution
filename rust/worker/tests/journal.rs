#![cfg(unix)]

use dispatch_protocol::v1::{Assignment, AttemptAuthority, AttemptState, Resources};
use dispatch_protocol::v1::{RegisterWorkerRequest, RegisterWorkerResponse, WorkerSession};
use dispatch_worker::journal::{Journal, JournalError, JournalLimits};
use ring::digest::{digest, SHA256};
use std::{
    fs,
    os::unix::fs::{symlink, DirBuilderExt, PermissionsExt},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const WORKER: &str = "00000000-0000-0000-0000-000000000001";
const ATTEMPT: &str = "00000000-0000-0000-0000-000000000004";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "dispatch-journal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }
    fn open(&self) -> Journal {
        Journal::open(&self.0, WORKER, JournalLimits::default()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn assignment() -> Assignment {
    let image = format!("example.org/job@sha256:{}", "a".repeat(64));
    let raw=serde_json::to_vec(&serde_json::json!({
        "apiVersion":"dispatch.dev/v1alpha1","kind":"Job","metadata":{"name":"journal-test","project":"research"},
        "spec":{"image":image,"command":["true"],"env":{"SECRET":"not-for-diagnostics"},"resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},"placement":{},"network":"disabled",
        "timeouts":{"startupSeconds":30,"executionSeconds":30,"finalizationSeconds":30},"retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},"terminationGraceSeconds":1}
    })).unwrap();
    Assignment {
        authority: Some(AttemptAuthority {
            worker_id: WORKER.into(),
            session_id: "00000000-0000-0000-0000-000000000002".into(),
            job_id: "00000000-0000-0000-0000-000000000003".into(),
            attempt_id: ATTEMPT.into(),
            generation: 1,
        }),
        image_digest: image,
        argv: vec!["true".into()],
        resources: Some(Resources {
            cpu_millis: 1000,
            memory_bytes: 128 << 20,
            scratch_bytes: 64 << 20,
        }),
        spec_sha256: digest(&SHA256, &raw)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        canonical_job_spec_json: raw,
        lease_duration_ms: 30000,
        phase_remaining_ms: 30000,
        server_time_unix_ms: 100,
        ..Default::default()
    }
}

fn registration_claims() -> RegisterWorkerRequest {
    RegisterWorkerRequest {
        worker_id: WORKER.into(),
        protocol_version: dispatch_protocol::VERSION,
        allocatable: Some(Resources {
            cpu_millis: 2000,
            memory_bytes: 512 << 20,
            scratch_bytes: 1024 << 20,
        }),
        execution_slots: 2,
        labels: [
            ("os".into(), "linux".into()),
            ("architecture".into(), "arm64".into()),
            ("rack".into(), "lab-a".into()),
        ]
        .into(),
        capabilities: [
            "docker.v1",
            "cpu.hard",
            "memory.hard",
            "pids.hard",
            "scratch.soft",
        ]
        .map(str::to_string)
        .to_vec(),
        ..Default::default()
    }
}

fn registration_reply(request: &RegisterWorkerRequest, generation: u64) -> RegisterWorkerResponse {
    RegisterWorkerResponse {
        session: Some(WorkerSession {
            worker_id: request.worker_id.clone(),
            session_id: request.requested_session_id.clone(),
        }),
        session_generation: generation,
        protocol_version: dispatch_protocol::VERSION,
        cleanup_required: true,
    }
}

#[test]
fn session_retries_are_durable_but_reopening_requires_a_fresh_incarnation() {
    let f = Fixture::new();
    let mut journal = f.open();
    assert!(journal.load_session().unwrap().is_none());
    journal.persist_assignment(&assignment()).unwrap();
    let first = journal.begin_incarnation(registration_claims()).unwrap();
    let request = first.registration().clone();
    assert_ne!(request.request_id, request.requested_session_id);
    assert!(first.generation().is_none());
    assert!(first.previous_session_id().is_none());
    assert_eq!(
        journal.load_session().unwrap().unwrap().registration(),
        &request
    );
    let reply = registration_reply(&request, 1);
    journal.record_registration(&reply).unwrap();
    journal.record_registration(&reply).unwrap();
    assert_eq!(
        journal.load_session().unwrap().unwrap().generation(),
        Some(1)
    );
    assert!(matches!(
        journal.begin_incarnation(registration_claims()),
        Err(JournalError::Conflict)
    ));
    drop(journal);

    let mut next = f.open();
    assert_eq!(
        next.load_session().unwrap().unwrap().registration(),
        &request
    );
    assert!(matches!(
        next.record_registration(&reply),
        Err(JournalError::Conflict)
    ));
    let second = next.begin_incarnation(registration_claims()).unwrap();
    assert_ne!(
        second.registration().requested_session_id,
        request.requested_session_id
    );
    assert_ne!(second.registration().request_id, request.request_id);
    assert_eq!(
        second.previous_session_id(),
        Some(request.requested_session_id.as_str())
    );
    assert_eq!(second.generation(), None);
    assert!(matches!(
        next.record_registration(&reply),
        Err(JournalError::Conflict)
    ));
    next.record_registration(&registration_reply(second.registration(), 2))
        .unwrap();
    assert_eq!(next.attempt_ids().unwrap(), vec![ATTEMPT.to_string()]);
}

#[test]
fn corrupt_or_conflicting_session_evidence_is_not_replaced() {
    let f = Fixture::new();
    let mut journal = f.open();
    let mut claims = registration_claims();
    claims.requested_session_id = "00000000-0000-0000-0000-000000000099".into();
    assert!(matches!(
        journal.begin_incarnation(claims),
        Err(JournalError::Invalid)
    ));
    assert!(journal.load_session().unwrap().is_none());
    let session = journal.begin_incarnation(registration_claims()).unwrap();
    let mut reply = registration_reply(session.registration(), 1);
    reply.protocol_version = 99;
    assert!(matches!(
        journal.record_registration(&reply),
        Err(JournalError::Invalid)
    ));
    reply.protocol_version = dispatch_protocol::VERSION;
    reply.session_generation = 0;
    assert!(matches!(
        journal.record_registration(&reply),
        Err(JournalError::Invalid)
    ));
    reply.session_generation = 1;
    journal.record_registration(&reply).unwrap();
    reply.session_generation = 2;
    assert!(matches!(
        journal.record_registration(&reply),
        Err(JournalError::Conflict)
    ));
    drop(journal);
    let path = f.0.join(".session");
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&path, &bytes).unwrap();
    let mut journal = f.open();
    assert!(matches!(journal.load_session(), Err(JournalError::Corrupt)));
    assert!(matches!(
        journal.begin_incarnation(registration_claims()),
        Err(JournalError::Corrupt)
    ));
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn invalid_registration_claims_do_not_consume_an_incarnation() {
    let f = Fixture::new();
    let mut journal = f.open();
    for case in 0..12 {
        let mut claims = registration_claims();
        match case {
            0 => claims.worker_id = "00000000-0000-0000-0000-000000000099".into(),
            1 => claims.protocol_version = 2,
            2 => claims.execution_slots = 1001,
            3 => claims.allocatable.as_mut().unwrap().memory_bytes += 1,
            4 => claims.allocatable.as_mut().unwrap().scratch_bytes = u64::MAX,
            5 => claims.allocatable.as_mut().unwrap().cpu_millis = 0,
            6 => {
                claims.labels.insert("os".into(), "darwin".into());
            }
            7 => {
                claims.labels.insert("bad/key".into(), "x".into());
            }
            8 => {
                claims
                    .labels
                    .insert("rack".into(), "hidden\ncontrol".into());
            }
            9 => claims.capabilities.push("scratch.quota".into()),
            10 => claims.capabilities[0] = "memory.hard".into(),
            _ => claims.allocatable = None,
        }
        assert!(
            matches!(
                journal.begin_incarnation(claims),
                Err(JournalError::Invalid)
            ),
            "case {case}"
        );
        assert!(journal.load_session().unwrap().is_none());
    }
    let mut claims = registration_claims();
    claims
        .labels
        .insert("A".repeat(128), "accepted-by-server".into());
    let session = journal.begin_incarnation(claims.clone()).unwrap();
    assert_eq!(session.registration().labels, claims.labels);
    let reply = registration_reply(session.registration(), 1);
    journal.record_registration(&reply).unwrap();
    let mut replay = reply;
    replay.cleanup_required = false;
    journal.record_registration(&replay).unwrap();
    drop(journal);
    fs::remove_file(f.0.join(".identity")).unwrap();
    assert!(matches!(
        Journal::open(&f.0, WORKER, JournalLimits::default()),
        Err(JournalError::Corrupt)
    ));
}

#[test]
fn persists_exact_attempt_evidence_and_phase_identities_without_authority() {
    let f = Fixture::new();
    let mut journal = f.open();
    let a = assignment();
    journal.persist_assignment(&a).unwrap();
    let starting = journal
        .prepare_phase(ATTEMPT, AttemptState::Starting)
        .unwrap();
    assert_eq!(
        journal
            .prepare_phase(ATTEMPT, AttemptState::Starting)
            .unwrap(),
        starting
    );
    journal.bind_container(ATTEMPT, &"b".repeat(64)).unwrap();
    let running = journal
        .prepare_phase(ATTEMPT, AttemptState::Running)
        .unwrap();
    journal.record_exit(ATTEMPT, 0, false).unwrap();
    let finalizing = journal
        .prepare_phase(ATTEMPT, AttemptState::Finalizing)
        .unwrap();
    drop(journal);
    let mut recovered = f.open();
    let record = recovered.load_attempt(ATTEMPT).unwrap().unwrap();
    assert_eq!(record.assignment().authority, a.authority);
    assert_eq!(
        record.assignment().canonical_job_spec_json,
        a.canonical_job_spec_json
    );
    assert_eq!(record.assignment().lease_duration_ms, 0);
    assert_eq!(record.assignment().phase_remaining_ms, 0);
    assert_eq!(record.assignment().server_time_unix_ms, 0);
    assert_eq!(record.container_id(), Some("b".repeat(64).as_str()));
    assert_eq!(record.exit().unwrap().exit_code, 0);
    assert!(!record.exit().unwrap().oom_killed);
    assert_eq!(
        record.phase_reports(),
        &[starting.clone(), running, finalizing.clone()]
    );
    assert_eq!(
        recovered
            .prepare_phase(ATTEMPT, AttemptState::Starting)
            .unwrap(),
        starting
    );
    assert_eq!(
        recovered
            .prepare_phase(ATTEMPT, AttemptState::Finalizing)
            .unwrap(),
        finalizing
    );
    let mut replay = a;
    replay.lease_duration_ms = 20000;
    replay.server_time_unix_ms = 200;
    recovered.persist_assignment(&replay).unwrap();
    assert_eq!(recovered.attempt_ids().unwrap(), vec![ATTEMPT.to_string()]);
    assert!(!format!("{record:?}").contains("not-for-diagnostics"));
}

#[test]
fn refuses_identity_changes_invalid_evidence_and_unsafe_phase_order() {
    let f = Fixture::new();
    let mut j = f.open();
    let a = assignment();
    j.persist_assignment(&a).unwrap();
    assert!(matches!(
        j.prepare_phase(ATTEMPT, AttemptState::Running),
        Err(JournalError::Conflict)
    ));
    assert!(matches!(
        j.bind_container(ATTEMPT, &"a".repeat(64)),
        Err(JournalError::Conflict)
    ));
    j.prepare_phase(ATTEMPT, AttemptState::Starting).unwrap();
    j.bind_container(ATTEMPT, &"a".repeat(64)).unwrap();
    assert!(matches!(
        j.bind_container(ATTEMPT, &"b".repeat(64)),
        Err(JournalError::Conflict)
    ));
    assert!(matches!(
        j.record_exit(ATTEMPT, 256, false),
        Err(JournalError::Invalid)
    ));
    assert!(matches!(
        j.prepare_phase(ATTEMPT, AttemptState::Finalizing),
        Err(JournalError::Conflict)
    ));
    j.record_exit(ATTEMPT, 137, true).unwrap();
    j.record_exit(ATTEMPT, 137, true).unwrap();
    assert!(matches!(
        j.record_exit(ATTEMPT, 137, false),
        Err(JournalError::Conflict)
    ));
    assert!(matches!(
        j.prepare_phase(ATTEMPT, AttemptState::Running),
        Err(JournalError::Conflict)
    ));
    j.prepare_phase(ATTEMPT, AttemptState::Finalizing).unwrap();
    let mut changed = a.clone();
    changed.authority.as_mut().unwrap().generation = 2;
    assert!(matches!(
        j.persist_assignment(&changed),
        Err(JournalError::Conflict)
    ));
    changed.authority.as_mut().unwrap().worker_id = "00000000-0000-0000-0000-000000000099".into();
    assert!(matches!(
        j.persist_assignment(&changed),
        Err(JournalError::Identity)
    ));
    assert!(matches!(
        j.load_attempt("../other"),
        Err(JournalError::Invalid)
    ));
    assert!(j
        .load_attempt("00000000-0000-0000-0000-000000000099")
        .unwrap()
        .is_none());
}

#[test]
fn rejects_concurrent_owners_wrong_worker_and_corruption() {
    let f = Fixture::new();
    let mut j = f.open();
    j.persist_assignment(&assignment()).unwrap();
    assert!(matches!(
        Journal::open(&f.0, WORKER, JournalLimits::default()),
        Err(JournalError::Busy)
    ));
    drop(j);
    assert!(matches!(
        Journal::open(
            &f.0,
            "00000000-0000-0000-0000-000000000099",
            JournalLimits::default()
        ),
        Err(JournalError::Identity)
    ));
    let path = f.0.join(format!("{ATTEMPT}.attempt"));
    let mut bytes = fs::read(&path).unwrap();
    let n = bytes.len() - 1;
    bytes[n] ^= 1;
    fs::write(&path, bytes).unwrap();
    let mut j = f.open();
    assert!(matches!(
        j.load_attempt(ATTEMPT),
        Err(JournalError::Corrupt)
    ));
    assert!(matches!(
        j.persist_assignment(&assignment()),
        Err(JournalError::Corrupt)
    ));
}

#[test]
fn enforces_private_files_and_bounded_inventory() {
    let f = Fixture::new();
    fs::set_permissions(&f.0, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        Journal::open(&f.0, WORKER, JournalLimits::default()),
        Err(JournalError::UnsafePath)
    ));
    fs::set_permissions(&f.0, fs::Permissions::from_mode(0o700)).unwrap();
    let mut j = Journal::open(
        &f.0,
        WORKER,
        JournalLimits {
            max_attempts: 1,
            max_bytes: 8 << 20,
        },
    )
    .unwrap();
    j.persist_assignment(&assignment()).unwrap();
    let mut second = assignment();
    second.authority.as_mut().unwrap().attempt_id = "00000000-0000-0000-0000-000000000005".into();
    assert!(matches!(
        j.persist_assignment(&second),
        Err(JournalError::Limit)
    ));
    let path = f.0.join(format!("{ATTEMPT}.attempt"));
    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    fs::remove_file(&path).unwrap();
    symlink("/etc/passwd", &path).unwrap();
    assert!(matches!(
        j.load_attempt(ATTEMPT),
        Err(JournalError::UnsafePath)
    ));
}

#[test]
fn budget_rejection_and_hard_links_do_not_bypass_storage_bounds() {
    let f = Fixture::new();
    let mut limited = Journal::open(
        &f.0,
        WORKER,
        JournalLimits {
            max_attempts: 10,
            max_bytes: 512,
        },
    )
    .unwrap();
    assert!(matches!(
        limited.persist_assignment(&assignment()),
        Err(JournalError::Limit)
    ));
    assert!(limited.attempt_ids().unwrap().is_empty());
    drop(limited);
    let mut j = f.open();
    j.persist_assignment(&assignment()).unwrap();
    let path = f.0.join(format!("{ATTEMPT}.attempt"));
    let link = f.0.with_extension("hardlink");
    fs::hard_link(&path, &link).unwrap();
    let result = j.load_attempt(ATTEMPT);
    fs::remove_file(link).unwrap();
    assert!(matches!(result, Err(JournalError::UnsafePath)));
}

#[test]
#[ignore = "subprocess fixture invoked only by the process-death recovery test"]
fn journal_process_child() {
    let Some(root) = std::env::var_os("DISPATCH_JOURNAL_CHILD_ROOT") else {
        return;
    };
    let marker = std::env::var_os("DISPATCH_JOURNAL_CHILD_MARKER").unwrap();
    let mut j = Journal::open(PathBuf::from(root), WORKER, JournalLimits::default()).unwrap();
    let session = j.begin_incarnation(registration_claims()).unwrap();
    j.persist_assignment(&assignment()).unwrap();
    let request = j.prepare_phase(ATTEMPT, AttemptState::Starting).unwrap();
    let marker = PathBuf::from(marker);
    let pending = marker.with_extension("pending");
    // Publish readiness only after the complete event ID is visible; the parent
    // may kill this process immediately after observing the marker.
    fs::write(
        &pending,
        format!(
            "{}\n{}",
            request.event_id,
            session.registration().requested_session_id
        ),
    )
    .unwrap();
    fs::rename(pending, marker).unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn killed_owner_releases_lock_and_preserves_acknowledged_retry_identity() {
    use std::{
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };
    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let f = Fixture::new();
    let root = f.0.join("state");
    let marker = f.0.join("ready");
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "journal_process_child",
                "--ignored",
                "--nocapture",
            ])
            .env("DISPATCH_JOURNAL_CHILD_ROOT", &root)
            .env("DISPATCH_JOURNAL_CHILD_MARKER", &marker)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !marker.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "journal child exited before committing"
        );
        assert!(
            Instant::now() < deadline,
            "journal child did not finish committing"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(matches!(
        Journal::open(&root, WORKER, JournalLimits::default()),
        Err(JournalError::Busy)
    ));
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let marker = fs::read_to_string(marker).unwrap();
    let (event, old_session) = marker.split_once('\n').unwrap();
    let mut reopened = Journal::open(root, WORKER, JournalLimits::default()).unwrap();
    let saved = reopened.load_session().unwrap().unwrap();
    assert_eq!(saved.registration().requested_session_id, old_session);
    assert_eq!(saved.generation(), None);
    let fresh = reopened.begin_incarnation(registration_claims()).unwrap();
    assert_ne!(fresh.registration().requested_session_id, old_session);
    assert_eq!(fresh.previous_session_id(), Some(old_session));
    assert_eq!(
        reopened
            .prepare_phase(ATTEMPT, AttemptState::Starting)
            .unwrap()
            .event_id,
        event
    );
    assert_eq!(
        reopened
            .load_attempt(ATTEMPT)
            .unwrap()
            .unwrap()
            .assignment()
            .lease_duration_ms,
        0
    );
}
