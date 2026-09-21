//! Live-container authority enforcement; exit evidence alone never publishes a result.
use crate::{
    control::{canonical_uuid, RenewalOutcome},
    lease::AuthorityWindow,
    runtime::{ContainerState, ContainerStatus, Runtime},
};
use dispatch_protocol::v1::{AttemptAuthority, Decision};
use std::{fmt, future::Future, time::Duration};
use tokio::sync::watch;

const OBSERVATION_TICK: Duration = Duration::from_millis(100);
const CLEANUP_BUDGET: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    AuthorityExpired,
    Rejected(Decision),
    ControllerLost,
    RenewalFailed,
    RuntimeUnavailable,
}

#[derive(Debug)]
pub enum SupervisionOutcome {
    Exited(ContainerStatus),
    Stopped { reason: StopReason, confirmed: bool },
}

#[derive(Clone, Copy)]
enum AuthorityState {
    Live(AuthorityWindow),
    Stop(StopReason),
}

#[derive(Clone)]
pub struct AuthorityController {
    identity: AttemptAuthority,
    state: watch::Sender<AuthorityState>,
}
pub struct SupervisedAuthority {
    identity: AttemptAuthority,
    state: watch::Receiver<AuthorityState>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AuthorityUpdateError {
    Identity,
    InvalidDecision,
    Expired,
}
impl fmt::Display for AuthorityUpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid supervisor authority update: {self:?}")
    }
}
impl std::error::Error for AuthorityUpdateError {}

pub fn authority_channel(
    identity: AttemptAuthority,
    window: AuthorityWindow,
) -> Result<(AuthorityController, SupervisedAuthority), AuthorityUpdateError> {
    if [
        &identity.worker_id,
        &identity.session_id,
        &identity.job_id,
        &identity.attempt_id,
    ]
    .iter()
    .any(|v| !canonical_uuid(v))
        || identity.generation == 0
        || identity.generation > i64::MAX as u64
    {
        return Err(AuthorityUpdateError::Identity);
    }
    window
        .remaining()
        .map_err(|_| AuthorityUpdateError::Expired)?;
    let (state, receiver) = watch::channel(AuthorityState::Live(window));
    Ok((
        AuthorityController {
            identity: identity.clone(),
            state,
        },
        SupervisedAuthority {
            identity,
            state: receiver,
        },
    ))
}

impl AuthorityController {
    pub fn identity(&self) -> &AttemptAuthority {
        &self.identity
    }

    pub(crate) fn active(&self) -> bool {
        !self.state.is_closed() && live(&self.state.borrow()).is_ok()
    }

    pub(crate) fn stop(&self, reason: StopReason) {
        let _ = self.update(&self.identity, AuthorityState::Stop(reason));
    }

    pub fn apply(&self, outcome: &RenewalOutcome) -> Result<(), AuthorityUpdateError> {
        match outcome {
            RenewalOutcome::Renewed(grant) => {
                self.update(grant.identity(), AuthorityState::Live(*grant.authority()))
            }
            RenewalOutcome::Expired(identity) => {
                self.update(identity, AuthorityState::Stop(StopReason::AuthorityExpired))
            }
            RenewalOutcome::Rejected { identity, decision }
                if matches!(
                    decision,
                    Decision::Fenced | Decision::StopRequested | Decision::AlreadyTerminal
                ) =>
            {
                self.update(
                    identity,
                    AuthorityState::Stop(StopReason::Rejected(*decision)),
                )
            }
            _ => Err(AuthorityUpdateError::InvalidDecision),
        }
    }

    fn update(
        &self,
        identity: &AttemptAuthority,
        next: AuthorityState,
    ) -> Result<(), AuthorityUpdateError> {
        if identity != &self.identity {
            return Err(AuthorityUpdateError::Identity);
        }
        self.state.send_if_modified(|current| {
            if matches!(current, AuthorityState::Stop(_)) {
                return false;
            }
            // A grant received after local authority lapsed cannot resurrect the
            // same execution. Concurrent rejection also permanently wins over renewal.
            *current = if live(current).is_err()
                || live(&next).is_err() && matches!(next, AuthorityState::Live(_))
            {
                AuthorityState::Stop(StopReason::AuthorityExpired)
            } else {
                next
            };
            true
        });
        Ok(())
    }
}

fn live(state: &AuthorityState) -> Result<Duration, StopReason> {
    match state {
        AuthorityState::Stop(reason) => Err(*reason),
        AuthorityState::Live(window) => {
            window.remaining().map_err(|_| StopReason::AuthorityExpired)
        }
    }
}

impl SupervisedAuthority {
    pub(crate) fn identity(&self) -> &AttemptAuthority {
        &self.identity
    }

    pub(crate) fn window(&mut self) -> Result<AuthorityWindow, StopReason> {
        self.check()?;
        match *self.state.borrow() {
            AuthorityState::Live(window) => {
                window
                    .remaining()
                    .map_err(|_| StopReason::AuthorityExpired)?;
                Ok(window)
            }
            AuthorityState::Stop(reason) => Err(reason),
        }
    }

    fn check(&mut self) -> Result<Duration, StopReason> {
        let current = *self.state.borrow_and_update();
        if let AuthorityState::Stop(reason) = current {
            return Err(reason);
        }
        if self.state.has_changed().is_err() {
            return Err(StopReason::ControllerLost);
        }
        live(&current)
    }

    pub(crate) async fn while_live<T>(
        &mut self,
        operation: impl Future<Output = T>,
    ) -> Result<T, StopReason> {
        tokio::pin!(operation);
        loop {
            let until_check = self.check()?.min(OBSERVATION_TICK);
            tokio::select! {
                result = &mut operation => { self.check()?; return Ok(result); },
                _ = self.state.changed() => {},
                _ = tokio::time::sleep(until_check) => {},
            }
        }
    }
}

pub async fn supervise_running<R: Runtime>(
    runtime: &R,
    handle: &R::Handle,
    mut authority: SupervisedAuthority,
) -> SupervisionOutcome {
    let reason = loop {
        match authority.while_live(runtime.inspect(handle)).await {
            Ok(Ok(status))
                if matches!(status.state, ContainerState::Exited | ContainerState::Dead)
                    && !status.running
                    && status
                        .exit_code
                        .is_some_and(|code| (0..=255).contains(&code)) =>
            {
                return SupervisionOutcome::Exited(status);
            }
            Ok(Ok(status)) if status.running => {}
            Ok(_) => break StopReason::RuntimeUnavailable,
            Err(reason) => break reason,
        }
        if let Err(reason) = authority
            .while_live(tokio::time::sleep(OBSERVATION_TICK))
            .await
        {
            break reason;
        }
    };
    let confirmed = terminate(runtime, handle).await;
    SupervisionOutcome::Stopped { reason, confirmed }
}

pub(crate) async fn terminate<R: Runtime>(runtime: &R, handle: &R::Handle) -> bool {
    // Loss of authority uses immediate kill, not a grace period that could extend
    // execution. A stalled daemon is uncertainty: never release local reservations
    // merely because the kill request was sent or timed out.
    let _ = tokio::time::timeout(CLEANUP_BUDGET, runtime.kill(handle)).await;
    match tokio::time::timeout(CLEANUP_BUDGET, runtime.inspect(handle)).await {
        Ok(Ok(status)) => {
            !status.running && matches!(status.state, ContainerState::Exited | ContainerState::Dead)
        }
        Ok(Err(crate::runtime::RuntimeError::Daemon(404))) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::ExecutionSpec,
        lease::MonoTime,
        runtime::{ContainerLogs, PreparedWorkspace, RuntimeError},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Fake {
        natural_exit: bool,
        stall: bool,
        kill_fails: bool,
        inspections: AtomicUsize,
        kills: AtomicUsize,
    }
    impl Runtime for Fake {
        type Handle = ();
        fn container_id(_: &()) -> &str {
            "unused-watchdog-handle"
        }
        async fn create(
            &self,
            _: &AttemptAuthority,
            _: &ExecutionSpec,
            _: &PreparedWorkspace,
            _: &AuthorityWindow,
        ) -> Result<(), RuntimeError> {
            panic!("watchdog must not create")
        }
        async fn start(&self, _: &(), _: &AuthorityWindow) -> Result<(), RuntimeError> {
            panic!("watchdog must not start")
        }
        async fn inspect(&self, _: &()) -> Result<ContainerStatus, RuntimeError> {
            self.inspections.fetch_add(1, Ordering::SeqCst);
            let killed = self.kills.load(Ordering::SeqCst) > 0 && !self.kill_fails;
            if self.stall && !killed {
                std::future::pending::<()>().await;
            }
            if self.natural_exit {
                tokio::time::sleep(Duration::from_millis(180)).await;
            }
            let running = !killed && !self.natural_exit;
            Ok(ContainerStatus {
                running,
                state: if running {
                    ContainerState::Running
                } else {
                    ContainerState::Exited
                },
                exit_code: (!running).then_some(if killed { 137 } else { 0 }),
                oom_killed: false,
            })
        }
        async fn kill(&self, _: &()) -> Result<(), RuntimeError> {
            self.kills.fetch_add(1, Ordering::SeqCst);
            if self.kill_fails {
                Err(RuntimeError::Transport)
            } else {
                Ok(())
            }
        }
        async fn wait(&self, _: &()) -> Result<ContainerStatus, RuntimeError> {
            panic!("watchdog uses bounded observation")
        }
        async fn stop(&self, _: &(), _: u32) -> Result<(), RuntimeError> {
            panic!("expired authority cannot wait for grace")
        }
        async fn logs(&self, _: &(), _: usize) -> Result<ContainerLogs, RuntimeError> {
            panic!("watchdog must not collect logs")
        }
        async fn remove(&self, _: &()) -> Result<(), RuntimeError> {
            panic!("watchdog must preserve evidence")
        }
    }
    fn identity() -> AttemptAuthority {
        AttemptAuthority {
            worker_id: "00000000-0000-0000-0000-000000000001".into(),
            session_id: "00000000-0000-0000-0000-000000000002".into(),
            job_id: "00000000-0000-0000-0000-000000000003".into(),
            attempt_id: "00000000-0000-0000-0000-000000000004".into(),
            generation: 1,
        }
    }
    fn window(ms: u64) -> AuthorityWindow {
        let now = MonoTime::now().unwrap();
        AuthorityWindow::from_grant(now, now, 30_000, ms).unwrap()
    }

    #[tokio::test]
    async fn stalled_inspection_cannot_block_expiry_and_unknown_cleanup_stays_unknown() {
        let runtime = Fake {
            stall: true,
            ..Default::default()
        };
        let (_controller, authority) = authority_channel(identity(), window(40)).unwrap();
        let started = std::time::Instant::now();
        assert!(matches!(
            supervise_running(&runtime, &(), authority).await,
            SupervisionOutcome::Stopped {
                reason: StopReason::AuthorityExpired,
                confirmed: true
            }
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(runtime.kills.load(Ordering::SeqCst), 1);
        let unknown = Fake {
            kill_fails: true,
            ..Default::default()
        };
        let (controller, authority) = authority_channel(identity(), window(1000)).unwrap();
        drop(controller);
        assert!(matches!(
            supervise_running(&unknown, &(), authority).await,
            SupervisionOutcome::Stopped {
                reason: StopReason::ControllerLost,
                confirmed: false
            }
        ));
    }

    #[tokio::test]
    async fn identity_mismatch_and_late_renewal_cannot_extend_authority() {
        let id = identity();
        let (controller, mut authority) = authority_channel(id.clone(), window(10)).unwrap();
        let mut other = id.clone();
        other.generation += 1;
        assert_eq!(
            controller.update(&other, AuthorityState::Live(window(1000))),
            Err(AuthorityUpdateError::Identity)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller
            .update(&id, AuthorityState::Live(window(1000)))
            .unwrap();
        assert_eq!(authority.check(), Err(StopReason::AuthorityExpired));
    }

    #[tokio::test]
    async fn live_renewals_keep_running_but_rejection_is_irreversible() {
        let runtime = Fake::default();
        let id = identity();
        let (controller, authority) = authority_channel(id.clone(), window(500)).unwrap();
        let (result, ()) = tokio::join!(supervise_running(&runtime, &(), authority), async {
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                controller
                    .update(&id, AuthorityState::Live(window(500)))
                    .unwrap();
                assert_eq!(runtime.kills.load(Ordering::SeqCst), 0);
            }
            controller
                .apply(&RenewalOutcome::Rejected {
                    identity: id.clone(),
                    decision: Decision::Fenced,
                })
                .unwrap();
            controller
                .update(&id, AuthorityState::Live(window(1000)))
                .unwrap();
        });
        assert!(matches!(
            result,
            SupervisionOutcome::Stopped {
                reason: StopReason::Rejected(Decision::Fenced),
                confirmed: true
            }
        ));
    }

    #[tokio::test]
    async fn every_server_rejection_survives_controller_exit_and_stops_execution() {
        for decision in [
            Decision::Fenced,
            Decision::StopRequested,
            Decision::AlreadyTerminal,
        ] {
            let runtime = Fake::default();
            let (controller, authority) = authority_channel(identity(), window(1000)).unwrap();
            controller
                .apply(&RenewalOutcome::Rejected {
                    identity: identity(),
                    decision,
                })
                .unwrap();
            drop(controller);
            assert!(
                matches!(supervise_running(&runtime,&(),authority).await,SupervisionOutcome::Stopped {reason:StopReason::Rejected(d),confirmed:true} if d==decision)
            );
        }
    }

    #[tokio::test]
    async fn slow_inspection_is_not_restarted_on_each_watchdog_tick() {
        let runtime = Fake {
            natural_exit: true,
            ..Default::default()
        };
        let (_controller, authority) = authority_channel(identity(), window(1000)).unwrap();
        assert!(
            matches!(supervise_running(&runtime,&(),authority).await,SupervisionOutcome::Exited(status) if status.exit_code==Some(0))
        );
        assert_eq!(runtime.inspections.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.kills.load(Ordering::SeqCst), 0);
    }
}
