use super::*;
use crate::{
    journal::{Journal, JournalLimits},
    lease::{AuthorityWindow, MonoTime},
    runtime::{ContainerLogs, ContainerStatus},
    supervisor::authority_channel,
};
use dispatch_protocol::v1::{
    AttemptAuthority, RegisterWorkerRequest, RegisterWorkerResponse, Resources,
};
use ring::digest::{digest, SHA256};
use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

const WORKER: &str = "00000000-0000-0000-0000-000000000001";
const ATTEMPT: &str = "00000000-0000-0000-0000-000000000004";
struct Fixture {
    root: PathBuf,
    journal: AsyncJournal,
    session: WorkerSession,
    assignment: Assignment,
    execution: ExecutionSpec,
    workspace: PreparedWorkspace,
}
impl Fixture {
    fn new(acknowledged: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "dispatch-launch-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let state = root.join("state");
        fs::DirBuilder::new().mode(0o700).create(&state).unwrap();
        let mut journal = Journal::open(state, WORKER, JournalLimits::default()).unwrap();
        let saved = journal
            .begin_incarnation(RegisterWorkerRequest {
                worker_id: WORKER.into(),
                protocol_version: 1,
                allocatable: Some(Resources {
                    cpu_millis: 1000,
                    memory_bytes: 128 << 20,
                    scratch_bytes: 64 << 20,
                }),
                execution_slots: 1,
                labels: [
                    ("os".into(), "linux".into()),
                    ("architecture".into(), "arm64".into()),
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
            })
            .unwrap();
        let session = WorkerSession {
            worker_id: WORKER.into(),
            session_id: saved.registration().requested_session_id.clone(),
        };
        if acknowledged {
            journal
                .record_registration(&RegisterWorkerResponse {
                    session: Some(session.clone()),
                    session_generation: 1,
                    protocol_version: 1,
                    cleanup_required: true,
                })
                .unwrap();
        }
        let image = format!("example.org/test@sha256:{}", "a".repeat(64));
        let raw = serde_json::to_vec(&serde_json::json!({
            "apiVersion":"dispatch.dev/v1alpha1", "kind":"Job", "metadata":{"name":"launch", "project":"research"},
            "spec":{"image":image,"command":["true"], "resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},
            "placement":{},"network":"disabled", "timeouts":{"startupSeconds":30,"executionSeconds":30,"finalizationSeconds":30},
            "retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},"terminationGraceSeconds":1}
        })).unwrap();
        let assignment = Assignment {
            authority: Some(AttemptAuthority {
                worker_id: WORKER.into(),
                session_id: session.session_id.clone(),
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
            ..Default::default()
        };
        let execution = ExecutionSpec::from_assignment(&assignment).unwrap();
        let work = root.join(ATTEMPT);
        fs::DirBuilder::new().mode(0o700).create(&work).unwrap();
        for name in ["inputs", "outputs", "scratch"] {
            fs::create_dir(work.join(name)).unwrap();
        }
        Self {
            root,
            journal: AsyncJournal::new(journal),
            session,
            assignment,
            execution,
            workspace: PreparedWorkspace::soft_development(work).unwrap(),
        }
    }
    fn input(&self) -> LaunchInput<'_> {
        LaunchInput {
            assignment: &self.assignment,
            execution: &self.execution,
        }
    }
    fn runtime(&self, mode: Mode) -> FakeRuntime {
        FakeRuntime {
            journal: self.journal.clone(),
            mode,
            starts: AtomicUsize::new(0),
            creates: AtomicUsize::new(0),
            killed: AtomicUsize::new(0),
            exited: AtomicBool::new(false),
            oom: false,
        }
    }
    fn phase(&self) -> FakePhase {
        FakePhase {
            journal: self.journal.clone(),
            requests: Vec::new(),
            retry_once: false,
            stall: false,
            decision: Decision::Accepted,
            state: AttemptState::Starting,
        }
    }
    fn authority(&self, ms: u64) -> (crate::supervisor::AuthorityController, SupervisedAuthority) {
        let now = MonoTime::now().unwrap();
        authority_channel(
            self.assignment.authority.clone().unwrap(),
            AuthorityWindow::from_grant(now, now, 30000, ms).unwrap(),
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct FakePhase {
    journal: AsyncJournal,
    requests: Vec<ReportPhaseRequest>,
    retry_once: bool,
    stall: bool,
    decision: Decision,
    state: AttemptState,
}
impl PhaseReporter for FakePhase {
    async fn phase(&mut self, request: &ReportPhaseRequest) -> Result<PhaseStatus, ClientError> {
        assert_eq!(
            self.journal
                .load_attempt(ATTEMPT.into())
                .await
                .unwrap()
                .unwrap()
                .phase_reports(),
            [request.clone()]
        );
        self.requests.push(request.clone());
        if self.stall {
            std::future::pending::<()>().await;
        }
        if self.retry_once && self.requests.len() == 1 {
            return Err(ClientError::Connection);
        }
        Ok(PhaseStatus {
            decision: self.decision,
            state: self.state,
        })
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Normal,
    LostStart,
    FastExit,
    CreateFailure,
    PoisonJournal,
    StalledStart,
}
struct FakeRuntime {
    journal: AsyncJournal,
    mode: Mode,
    creates: AtomicUsize,
    starts: AtomicUsize,
    killed: AtomicUsize,
    exited: AtomicBool,
    oom: bool,
}
impl Runtime for FakeRuntime {
    type Handle = String;
    fn container_id(handle: &String) -> &str {
        handle
    }
    async fn create(
        &self,
        _: &AttemptAuthority,
        _: &ExecutionSpec,
        _: &PreparedWorkspace,
        _: &AuthorityWindow,
    ) -> Result<String, RuntimeError> {
        let saved = self
            .journal
            .load_attempt(ATTEMPT.into())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            saved.phase_reports()[0].phase,
            AttemptState::Starting as i32
        );
        self.creates.fetch_add(1, Ordering::SeqCst);
        if matches!(self.mode, Mode::CreateFailure) {
            return Err(RuntimeError::Transport);
        }
        if matches!(self.mode, Mode::PoisonJournal) {
            let _ = self
                .journal
                .apply::<(), _>(|_| panic!("injected journal failure"))
                .await;
        }
        Ok("a".repeat(64))
    }
    async fn start(&self, handle: &String, _: &AuthorityWindow) -> Result<(), RuntimeError> {
        assert_eq!(
            self.journal
                .load_attempt(ATTEMPT.into())
                .await
                .unwrap()
                .unwrap()
                .container_id(),
            Some(handle.as_str())
        );
        self.starts.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            Mode::StalledStart => std::future::pending().await,
            Mode::LostStart => Err(RuntimeError::Transport),
            _ => Ok(()),
        }
    }
    async fn inspect(&self, _: &String) -> Result<ContainerStatus, RuntimeError> {
        let exited = self.killed.load(Ordering::SeqCst) > 0
            || self.exited.load(Ordering::SeqCst)
            || matches!(self.mode, Mode::FastExit);
        Ok(ContainerStatus {
            running: !exited,
            exit_code: exited.then_some(if self.oom { 137 } else { 0 }),
            oom_killed: exited && self.oom,
            state: if exited {
                ContainerState::Exited
            } else {
                ContainerState::Running
            },
        })
    }
    async fn kill(&self, _: &String) -> Result<(), RuntimeError> {
        self.killed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn wait(&self, _: &String) -> Result<ContainerStatus, RuntimeError> {
        panic!("launch must not wait for completion")
    }
    async fn stop(&self, _: &String, _: u32) -> Result<(), RuntimeError> {
        panic!("launch cleanup must not extend authority")
    }
    async fn logs(&self, _: &String, _: usize) -> Result<ContainerLogs, RuntimeError> {
        panic!("launch must preserve logs")
    }
    async fn remove(&self, _: &String) -> Result<(), RuntimeError> {
        panic!("launch must preserve container evidence")
    }
}

mod execution;

#[tokio::test]
async fn durable_launch_replays_starting_and_never_launches_duplicate_delivery() {
    let f = Fixture::new(true);
    let runtime = f.runtime(Mode::Normal);
    let mut phase = f.phase();
    phase.retry_once = true;
    let (_control, mut authority) = f.authority(5000);
    let handle = launch_inner(
        &runtime,
        &mut phase,
        &f.journal,
        f.input(),
        &f.session,
        &f.workspace,
        &mut authority,
    )
    .await
    .unwrap();
    assert_eq!(handle, "a".repeat(64));
    assert_eq!(phase.requests.len(), 2);
    assert_eq!(phase.requests[0], phase.requests[1]);
    let error = launch_inner(
        &runtime,
        &mut phase,
        &f.journal,
        f.input(),
        &f.session,
        &f.workspace,
        &mut authority,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error.cause,
        LaunchCause::Journal(JournalError::Conflict)
    ));
    assert_eq!(error.cleanup, CleanupEvidence::NotCreated);
    assert_eq!(runtime.creates.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.starts.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.killed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn lost_start_reply_and_fast_exit_are_inspected_without_start_replay() {
    for mode in [Mode::LostStart, Mode::FastExit] {
        let f = Fixture::new(true);
        let runtime = f.runtime(mode);
        let mut phase = f.phase();
        let (_c, mut a) = f.authority(5000);
        launch_inner(
            &runtime,
            &mut phase,
            &f.journal,
            f.input(),
            &f.session,
            &f.workspace,
            &mut a,
        )
        .await
        .unwrap();
        assert_eq!(runtime.starts.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.killed.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn expired_phase_rpc_prevents_creation_and_expired_start_triggers_cleanup() {
    for stall_phase in [true, false] {
        let f = Fixture::new(true);
        let runtime = f.runtime(Mode::StalledStart);
        let mut phase = f.phase();
        phase.stall = stall_phase;
        let (_c, mut a) = f.authority(300);
        let error = launch_inner(
            &runtime,
            &mut phase,
            &f.journal,
            f.input(),
            &f.session,
            &f.workspace,
            &mut a,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.cause,
            LaunchCause::Authority(StopReason::AuthorityExpired)
        ));
        assert_eq!(
            runtime.creates.load(Ordering::SeqCst),
            usize::from(!stall_phase)
        );
        assert_eq!(
            runtime.killed.load(Ordering::SeqCst),
            usize::from(!stall_phase)
        );
    }
}

#[tokio::test]
async fn uncertain_create_and_failed_container_binding_do_not_start_execution() {
    for mode in [Mode::CreateFailure, Mode::PoisonJournal] {
        let f = Fixture::new(true);
        let runtime = f.runtime(mode);
        let mut phase = f.phase();
        let (_c, mut a) = f.authority(5000);
        let error = launch_inner(
            &runtime,
            &mut phase,
            &f.journal,
            f.input(),
            &f.session,
            &f.workspace,
            &mut a,
        )
        .await
        .unwrap_err();
        assert_eq!(runtime.starts.load(Ordering::SeqCst), 0);
        if matches!(mode, Mode::CreateFailure) {
            assert_eq!(error.cleanup, CleanupEvidence::Uncertain);
        } else {
            assert!(matches!(
                error.cause,
                LaunchCause::Journal(JournalError::Poisoned)
            ));
            assert_eq!(runtime.killed.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn unacknowledged_or_mismatched_session_cannot_reach_docker() {
    for acknowledged in [false, true] {
        let f = Fixture::new(acknowledged);
        let runtime = f.runtime(Mode::Normal);
        let mut phase = f.phase();
        let (_c, mut a) = f.authority(5000);
        let mut session = f.session.clone();
        if acknowledged {
            session.session_id = WORKER.into();
        }
        assert!(launch_inner(
            &runtime,
            &mut phase,
            &f.journal,
            f.input(),
            &session,
            &f.workspace,
            &mut a
        )
        .await
        .is_err());
        assert!(phase.requests.is_empty());
        assert_eq!(runtime.creates.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn server_rejection_or_already_advanced_phase_prevents_launch() {
    for decision in [
        Decision::Fenced,
        Decision::StopRequested,
        Decision::AlreadyTerminal,
        Decision::Accepted,
    ] {
        let f = Fixture::new(true);
        let runtime = f.runtime(Mode::Normal);
        let mut phase = f.phase();
        phase.decision = decision;
        phase.state = AttemptState::Running;
        let (_c, mut authority) = f.authority(5000);
        let error = launch_inner(
            &runtime,
            &mut phase,
            &f.journal,
            f.input(),
            &f.session,
            &f.workspace,
            &mut authority,
        )
        .await
        .unwrap_err();
        assert_eq!(error.cleanup, CleanupEvidence::NotCreated);
        assert_eq!(runtime.creates.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn authority_from_another_generation_cannot_be_used_for_launch() {
    let f = Fixture::new(true);
    let runtime = f.runtime(Mode::Normal);
    let mut phase = f.phase();
    let mut identity = f.assignment.authority.clone().unwrap();
    identity.generation += 1;
    let now = MonoTime::now().unwrap();
    let (_c, mut authority) = authority_channel(
        identity,
        AuthorityWindow::from_grant(now, now, 30000, 5000).unwrap(),
    )
    .unwrap();
    let error = launch_inner(
        &runtime,
        &mut phase,
        &f.journal,
        f.input(),
        &f.session,
        &f.workspace,
        &mut authority,
    )
    .await
    .unwrap_err();
    assert!(matches!(error.cause, LaunchCause::Identity));
    assert!(f
        .journal
        .load_attempt(ATTEMPT.into())
        .await
        .unwrap()
        .is_none());
}
