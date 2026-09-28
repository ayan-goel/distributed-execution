//! Own one launched container through observed exit and fresh finalization authority.
use super::*;
use crate::{
    journal::ExitEvidence,
    lease::PhaseDeadline,
    log_live::{capture_running, LiveCapture, LiveLogError},
    runtime::ContainerStatus,
};
use dispatch_protocol::v1::AttemptAuthority;

const OBSERVATION_PERIOD: Duration = Duration::from_millis(100);

pub struct FinalizingAttempt<H> {
    handle: H,
    identity: AttemptAuthority,
    exit: ExitEvidence,
    authority: SupervisedAuthority,
    deadline: PhaseDeadline,
    workspace: std::path::PathBuf,
    capture: Option<Result<LiveCapture, LiveLogError>>,
}
impl<H> FinalizingAttempt<H> {
    pub fn handle(&self) -> &H {
        &self.handle
    }
    pub fn identity(&self) -> &AttemptAuthority {
        &self.identity
    }
    pub fn exit(&self) -> &ExitEvidence {
        &self.exit
    }
    pub fn authority_mut(&mut self) -> &mut SupervisedAuthority {
        &mut self.authority
    }
    pub(crate) fn owns_workspace(&self, workspace: &PreparedWorkspace) -> bool {
        self.workspace == workspace.root()
    }
    pub(crate) fn take_capture(&mut self) -> Option<Result<LiveCapture, LiveLogError>> {
        self.capture.take()
    }
    pub(crate) async fn while_finalizing<T>(
        &mut self,
        operation: impl Future<Output = T>,
    ) -> Result<T, StopReason> {
        // Carry the deadline started before FINALIZING across collection and
        // uploads. New grants or retries cannot restart this phase's budget.
        bounded(&mut self.authority, &self.deadline, operation).await
    }
}
impl<H> fmt::Debug for FinalizingAttempt<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FinalizingAttempt")
            .field("identity", &self.identity)
            .field("exit", &self.exit)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum ExecutionError {
    Launch(LaunchError),
    AfterLaunch {
        cause: LaunchCause,
        cleanup: CleanupEvidence,
    },
}
impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Launch(error) => write!(f, "worker execution failed during launch: {error}"),
            // Preserve categories without including workload or RPC diagnostics.
            Self::AfterLaunch { cleanup, .. } => write!(
                f,
                "worker execution failed after launch; cleanup: {cleanup:?}"
            ),
        }
    }
}
impl std::error::Error for ExecutionError {}

/// Launch and supervise one assignment, retaining authority for artifact finalization.
/// The caller must poll through cleanup and run its lease producer independently.
pub async fn execute<R: Runtime>(
    runtime: &R,
    client: &mut ControlClient,
    journal: &AsyncJournal,
    grant: &GrantedAssignment,
    session: &WorkerSession,
    workspace: &PreparedWorkspace,
    authority: SupervisedAuthority,
) -> Result<FinalizingAttempt<R::Handle>, ExecutionError> {
    execute_inner(
        runtime,
        client,
        journal,
        LaunchInput {
            assignment: grant.assignment(),
            execution: grant.execution(),
            capture_logs: false,
        },
        session,
        workspace,
        authority,
    )
    .await
}

/// Run the same supervised attempt while draining Docker output into a bounded
/// private spool. Publication still needs current finalization authority.
pub async fn execute_with_logs<R: Runtime>(
    runtime: &R,
    client: &mut ControlClient,
    journal: &AsyncJournal,
    grant: &GrantedAssignment,
    session: &WorkerSession,
    workspace: &PreparedWorkspace,
    authority: SupervisedAuthority,
) -> Result<FinalizingAttempt<R::Handle>, ExecutionError> {
    execute_inner_impl(
        runtime,
        client,
        journal,
        LaunchInput {
            assignment: grant.assignment(),
            execution: grant.execution(),
            capture_logs: true,
        },
        session,
        workspace,
        authority,
    )
    .await
}

pub(super) async fn execute_inner<R: Runtime>(
    runtime: &R,
    client: &mut impl PhaseReporter,
    journal: &AsyncJournal,
    input: LaunchInput<'_>,
    session: &WorkerSession,
    workspace: &PreparedWorkspace,
    authority: SupervisedAuthority,
) -> Result<FinalizingAttempt<R::Handle>, ExecutionError> {
    execute_inner_impl(
        runtime, client, journal, input, session, workspace, authority,
    )
    .await
}

async fn execute_inner_impl<R: Runtime>(
    runtime: &R,
    client: &mut impl PhaseReporter,
    journal: &AsyncJournal,
    input: LaunchInput<'_>,
    session: &WorkerSession,
    workspace: &PreparedWorkspace,
    mut authority: SupervisedAuthority,
) -> Result<FinalizingAttempt<R::Handle>, ExecutionError> {
    let identity = authority.identity().clone();
    let timeouts = &input.execution.job().spec.timeouts;
    let grace_seconds = input.execution.job().spec.termination_grace_seconds;
    let execution_seconds = timeouts.execution_seconds;
    let finalization_seconds = timeouts.finalization_seconds;
    let capture_logs = input.capture_logs;
    // The handle comes only from this successful launch. Callers cannot pair an
    // unrelated runtime handle with a valid attempt and cause incorrect cleanup.
    let handle = launch_inner(
        runtime,
        client,
        journal,
        input,
        session,
        workspace,
        &mut authority,
    )
    .await
    .map_err(ExecutionError::Launch)?;
    let outcome = async {
        let first = authority.while_live(runtime.inspect(&handle)).await??;
        let status = if exited(&first) {
            first
        } else if first.running && first.state == ContainerState::Running {
            let deadline = local_deadline(execution_seconds)?;
            let mut refresh = authority.observer();
            let running = report(
                journal,
                client,
                &identity,
                AttemptState::Running,
                &mut refresh,
            );
            // Runtime observation continues while journal writes, retries, and
            // acknowledgements are pending. Fast exit can bypass an uncertain RUNNING.
            let early_exit = bounded(&mut authority, &deadline, async {
                tokio::select! {
                    result = running => { result?; Ok(None) },
                    result = observe_exit(runtime, &handle) => result.map(Some),
                }
            })
            .await??;
            match early_exit {
                Some(status) => status,
                None => {
                    authority
                        .while_live(observe_exit(runtime, &handle))
                        .await??
                }
            }
        } else {
            return Err(LaunchCause::Runtime(RuntimeError::Terminal));
        };

        let exit = ExitEvidence {
            exit_code: status.exit_code.ok_or(RuntimeError::Terminal)? as i32,
            oom_killed: status.oom_killed,
        };
        let deadline = local_deadline(finalization_seconds)?;
        let mut refresh = authority.observer();
        bounded(&mut authority, &deadline, async {
            // Durable runtime evidence precedes FINALIZING, including explicit
            // nonzero exits and OOM. Neither phase acceptance nor exit means success.
            journal
                .record_exit(identity.attempt_id.clone(), exit.exit_code, exit.oom_killed)
                .await?;
            report(
                journal,
                client,
                &identity,
                AttemptState::Finalizing,
                &mut refresh,
            )
            .await
        })
        .await??;
        Ok((exit, deadline))
    };
    let (outcome, capture) = if capture_logs {
        let collector = capture_running(runtime, &handle, workspace);
        tokio::pin!(collector);
        tokio::pin!(outcome);
        let mut early_capture = None;
        let result = loop {
            tokio::select! {
                result = &mut outcome => break result,
                captured = &mut collector, if early_capture.is_none() => early_capture = Some(captured),
            }
        };
        let captured = if result.is_ok() {
            if let Some(early) = early_capture {
                early
            } else {
                tokio::time::timeout(Duration::from_secs(2), collector)
                    .await
                    .unwrap_or(Err(LiveLogError::Capture))
            }
        } else {
            Err(LiveLogError::Capture)
        };
        // The follower starts after Docker start. A burst may rotate daemon
        // files before attachment, so even a clean follower cannot prove a
        // complete source stream until pre-start attachment is implemented.
        let captured = captured.map(|mut value: LiveCapture| {
            value.complete = false;
            value
        });
        (result, Some(captured))
    } else {
        (outcome.await, None)
    };
    match outcome {
        Ok((exit, deadline)) => Ok(FinalizingAttempt {
            handle,
            identity,
            exit,
            authority,
            deadline,
            workspace: workspace.root().to_owned(),
            capture,
        }),
        Err(cause) => {
            // Retain evidence even after uncertain phase commits. Cleanup confirms
            // physical stop only; capacity release and terminal publication are separate.
            let cleanup =
                if terminate_for_cause(runtime, &handle, &cause, &authority, grace_seconds).await {
                    CleanupEvidence::Stopped
                } else {
                    CleanupEvidence::Uncertain
                };
            Err(ExecutionError::AfterLaunch { cause, cleanup })
        }
    }
}

fn exited(status: &ContainerStatus) -> bool {
    !status.running
        && matches!(status.state, ContainerState::Exited | ContainerState::Dead)
        && status
            .exit_code
            .is_some_and(|code| (0..=255).contains(&code))
}

async fn observe_exit<R: Runtime>(
    runtime: &R,
    handle: &R::Handle,
) -> Result<ContainerStatus, LaunchCause> {
    loop {
        let status = runtime.inspect(handle).await?;
        if exited(&status) {
            return Ok(status);
        }
        if !status.running || status.state != ContainerState::Running {
            return Err(RuntimeError::Terminal.into());
        }
        tokio::time::sleep(OBSERVATION_PERIOD).await;
    }
}

async fn report(
    journal: &AsyncJournal,
    client: &mut impl PhaseReporter,
    identity: &AttemptAuthority,
    phase: AttemptState,
    authority: &mut SupervisedAuthority,
) -> Result<(), LaunchCause> {
    let request = journal
        .prepare_phase(identity.attempt_id.clone(), phase)
        .await?;
    loop {
        match client.phase(&request).await {
            Ok(status) if status.decision == Decision::Accepted && status.state == phase => break,
            Ok(status)
                if matches!(
                    status.decision,
                    Decision::Fenced | Decision::StopRequested | Decision::AlreadyTerminal
                ) =>
            {
                return Err(StopReason::Rejected(status.decision).into())
            }
            Ok(_) => return Err(LaunchCause::UnexpectedPhase),
            Err(error) if error.retryable() => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(error) => return Err(error.into()),
        }
    }
    authority.refresh().await?;
    Ok(())
}

fn local_deadline(seconds: u64) -> Result<PhaseDeadline, LaunchCause> {
    PhaseDeadline::start(seconds).map_err(|_| StopReason::AuthorityExpired.into())
}

async fn bounded<T>(
    authority: &mut SupervisedAuthority,
    deadline: &PhaseDeadline,
    operation: impl Future<Output = T>,
) -> Result<T, StopReason> {
    authority
        .while_live(async {
            tokio::pin!(operation);
            loop {
                let remaining = deadline
                    .remaining()
                    .map_err(|_| StopReason::AuthorityExpired)?;
                tokio::select! {
                    result = &mut operation => {
                        deadline.remaining().map_err(|_| StopReason::AuthorityExpired)?;
                        return Ok(result);
                    },
                    _ = tokio::time::sleep(remaining.min(OBSERVATION_PERIOD)) => {},
                }
            }
        })
        .await?
}
