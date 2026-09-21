use super::*;
use crate::{
    control::{RenewalOutcome, RenewedLease},
    launch::run::{execute_inner, ExecutionError},
    supervisor::AuthorityController,
};

struct Reporter<'a> {
    journal: &'a AsyncJournal,
    runtime: &'a FakeRuntime,
    requests: Vec<ReportPhaseRequest>,
    phase: &'a AtomicUsize,
    stall_running: bool,
    exit_during_running: bool,
    retry_finalizing: bool,
    reject_finalizing: bool,
}
impl PhaseReporter for Reporter<'_> {
    async fn phase(&mut self, request: &ReportPhaseRequest) -> Result<PhaseStatus, ClientError> {
        let saved = self
            .journal
            .load_attempt(ATTEMPT.into())
            .await
            .unwrap()
            .unwrap();
        assert!(saved.phase_reports().contains(request));
        if request.phase == AttemptState::Finalizing as i32 {
            assert_eq!(saved.exit().unwrap().exit_code, request.exit_code.unwrap());
            assert_eq!(saved.exit().unwrap().oom_killed, self.runtime.oom);
        }
        self.requests.push(request.clone());
        self.phase.store(request.phase as usize, Ordering::SeqCst);
        if request.phase == AttemptState::Running as i32 {
            if self.exit_during_running {
                self.runtime.exited.store(true, Ordering::SeqCst);
            }
            if self.stall_running {
                std::future::pending::<()>().await;
            }
        }
        if request.phase == AttemptState::Finalizing as i32 {
            if self.reject_finalizing {
                return Ok(PhaseStatus {
                    decision: Decision::Fenced,
                    state: AttemptState::Finalizing,
                });
            }
            if self.retry_finalizing {
                self.retry_finalizing = false;
                return Err(ClientError::Connection);
            }
        }
        Ok(PhaseStatus {
            decision: Decision::Accepted,
            state: AttemptState::try_from(request.phase).unwrap(),
        })
    }
}

impl Fixture {
    fn reporter<'a>(&'a self, runtime: &'a FakeRuntime, phase: &'a AtomicUsize) -> Reporter<'a> {
        Reporter {
            journal: &self.journal,
            runtime,
            requests: Vec::new(),
            phase,
            stall_running: false,
            exit_during_running: false,
            retry_finalizing: false,
            reject_finalizing: false,
        }
    }

    fn execution_timeout(&mut self, seconds: u64) {
        let mut raw: serde_json::Value =
            serde_json::from_slice(&self.assignment.canonical_job_spec_json).unwrap();
        raw["spec"]["timeouts"]["executionSeconds"] = seconds.into();
        let bytes = serde_json::to_vec(&raw).unwrap();
        self.assignment.spec_sha256 = digest(&SHA256, &bytes)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        self.assignment.canonical_job_spec_json = bytes;
        self.execution = ExecutionSpec::from_assignment(&self.assignment).unwrap();
    }
}

async fn refreshes(
    controller: &AuthorityController,
    phase: &AtomicUsize,
    runtime: &FakeRuntime,
    exit_after_running: bool,
) {
    loop {
        controller.refresh_requested().await;
        let revision = controller.refresh_revision();
        renew(controller);
        controller.acknowledge_refresh(revision);
        if exit_after_running && phase.load(Ordering::SeqCst) == AttemptState::Running as usize {
            runtime.exited.store(true, Ordering::SeqCst);
        }
    }
}

fn renew(controller: &AuthorityController) {
    let now = MonoTime::now().unwrap();
    controller
        .apply(&RenewalOutcome::Renewed(RenewedLease::fixture(
            controller.identity().clone(),
            AuthorityWindow::from_grant(now, now, 30000, 30000).unwrap(),
        )))
        .unwrap();
}

#[tokio::test]
async fn execution_journals_observed_exit_and_replays_finalizing_before_fresh_authority() {
    let f = Fixture::new(true);
    let mut runtime = f.runtime(Mode::Normal);
    runtime.oom = true;
    let phase = AtomicUsize::new(0);
    let mut reporter = f.reporter(&runtime, &phase);
    reporter.retry_finalizing = true;
    let (controller, authority) = f.authority(5000);
    let result = tokio::select! {
        result = execute_inner(&runtime, &mut reporter, &f.journal, f.input(), &f.session, &f.workspace, authority) => result.unwrap(),
        _ = refreshes(&controller, &phase, &runtime, true) => unreachable!(),
    };
    assert_eq!(result.exit().exit_code, 137);
    assert!(result.exit().oom_killed);
    assert_eq!(result.identity(), f.assignment.authority.as_ref().unwrap());
    assert_eq!(result.handle(), &"a".repeat(64));
    assert_eq!(
        reporter
            .requests
            .iter()
            .map(|r| r.phase)
            .collect::<Vec<_>>(),
        vec![
            AttemptState::Starting as i32,
            AttemptState::Running as i32,
            AttemptState::Finalizing as i32,
            AttemptState::Finalizing as i32
        ]
    );
    assert_eq!(reporter.requests[2], reporter.requests[3]);
    assert_eq!(runtime.killed.load(Ordering::SeqCst), 0);
    assert!(controller.active());
    drop(result);
    assert!(!controller.active());
}

#[tokio::test]
async fn fast_exit_skips_running_and_still_obtains_finalization_authority() {
    let f = Fixture::new(true);
    let runtime = f.runtime(Mode::FastExit);
    let phase = AtomicUsize::new(0);
    let mut reporter = f.reporter(&runtime, &phase);
    let (controller, authority) = f.authority(5000);
    let result = tokio::select! {
        result = execute_inner(&runtime, &mut reporter, &f.journal, f.input(), &f.session, &f.workspace, authority) => result.unwrap(),
        _ = refreshes(&controller, &phase, &runtime, false) => unreachable!(),
    };
    assert_eq!(result.exit().exit_code, 0);
    assert!(!result.exit().oom_killed);
    assert_eq!(
        reporter
            .requests
            .iter()
            .map(|r| r.phase)
            .collect::<Vec<_>>(),
        vec![
            AttemptState::Starting as i32,
            AttemptState::Finalizing as i32
        ]
    );
}

#[tokio::test]
async fn observed_exit_interrupts_a_lost_running_reply_and_proceeds_to_finalizing() {
    let f = Fixture::new(true);
    let runtime = f.runtime(Mode::Normal);
    let phase = AtomicUsize::new(0);
    let mut reporter = f.reporter(&runtime, &phase);
    reporter.stall_running = true;
    reporter.exit_during_running = true;
    let (controller, authority) = f.authority(5000);
    let result = tokio::select! {
        result = execute_inner(&runtime, &mut reporter, &f.journal, f.input(), &f.session, &f.workspace, authority) => result.unwrap(),
        _ = refreshes(&controller, &phase, &runtime, false) => unreachable!(),
    };
    assert_eq!(result.exit().exit_code, 0);
    assert_eq!(
        reporter.requests.last().unwrap().phase,
        AttemptState::Finalizing as i32
    );
    assert_eq!(runtime.killed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn uncertain_running_commit_cannot_outlive_short_execution_timeout() {
    let mut f = Fixture::new(true);
    f.execution_timeout(1);
    let runtime = f.runtime(Mode::Normal);
    let phase = AtomicUsize::new(0);
    let mut reporter = f.reporter(&runtime, &phase);
    reporter.stall_running = true;
    let (controller, authority) = f.authority(10000);
    let renewals = AtomicUsize::new(0);
    let run = tokio::time::timeout(
        Duration::from_secs(2),
        execute_inner(
            &runtime,
            &mut reporter,
            &f.journal,
            f.input(),
            &f.session,
            &f.workspace,
            authority,
        ),
    );
    let error = tokio::select! {
        result = run => result.expect("phase deadline did not bound stalled acknowledgement").unwrap_err(),
        _ = async {
            loop {
                tokio::time::sleep(Duration::from_millis(25)).await;
                renew(&controller);
                renewals.fetch_add(1, Ordering::SeqCst);
            }
        } => unreachable!(),
    };
    assert!(matches!(
        error,
        ExecutionError::AfterLaunch {
            cause: LaunchCause::Authority(StopReason::AuthorityExpired),
            cleanup: CleanupEvidence::Stopped
        }
    ));
    assert_eq!(runtime.killed.load(Ordering::SeqCst), 1);
    assert!(!controller.active());
    assert!(renewals.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn finalizing_acknowledgement_without_a_fresh_grant_cannot_return_success() {
    let f = Fixture::new(true);
    let runtime = f.runtime(Mode::FastExit);
    let phase = AtomicUsize::new(0);
    let mut reporter = f.reporter(&runtime, &phase);
    let (controller, authority) = f.authority(1000);
    let error = execute_inner(
        &runtime,
        &mut reporter,
        &f.journal,
        f.input(),
        &f.session,
        &f.workspace,
        authority,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ExecutionError::AfterLaunch {
            cause: LaunchCause::Authority(StopReason::AuthorityExpired),
            cleanup: CleanupEvidence::Stopped
        }
    ));
    assert_eq!(
        reporter.requests.last().unwrap().phase,
        AttemptState::Finalizing as i32
    );
    assert!(!controller.active());
}

#[tokio::test]
async fn finalizing_rejection_retains_exit_evidence_and_closes_renewal_consumer() {
    let f = Fixture::new(true);
    let runtime = f.runtime(Mode::FastExit);
    let phase = AtomicUsize::new(0);
    let mut reporter = f.reporter(&runtime, &phase);
    reporter.reject_finalizing = true;
    let (controller, authority) = f.authority(5000);
    let error = execute_inner(
        &runtime,
        &mut reporter,
        &f.journal,
        f.input(),
        &f.session,
        &f.workspace,
        authority,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ExecutionError::AfterLaunch {
            cause: LaunchCause::Authority(StopReason::Rejected(Decision::Fenced)),
            cleanup: CleanupEvidence::Stopped
        }
    ));
    assert_eq!(
        f.journal
            .load_attempt(ATTEMPT.into())
            .await
            .unwrap()
            .unwrap()
            .exit()
            .unwrap()
            .exit_code,
        0
    );
    assert!(!controller.active());
}
