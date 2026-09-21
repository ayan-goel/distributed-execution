//! Durable launch through confirmed start; running/finalization reporting follows separately.
use crate::{
    control::{ClientError, ControlClient, GrantedAssignment, PhaseStatus},
    execution::ExecutionSpec,
    journal::{AsyncJournal, JournalError},
    runtime::{ContainerState, PreparedWorkspace, Runtime, RuntimeError},
    supervisor::{terminate, StopReason, SupervisedAuthority},
};
use dispatch_protocol::v1::{
    Assignment, AttemptState, Decision, ReportPhaseRequest, WorkerSession,
};
use std::{fmt, future::Future, time::Duration};

#[derive(Debug)]
pub enum LaunchCause {
    Identity,
    Journal(JournalError),
    Control(ClientError),
    Runtime(RuntimeError),
    Authority(StopReason),
    UnexpectedPhase,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupEvidence {
    NotCreated,
    Stopped,
    Uncertain,
}
#[derive(Debug)]
pub struct LaunchError {
    pub cause: LaunchCause,
    pub cleanup: CleanupEvidence,
}
impl fmt::Display for LaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Keep transport errors and workload content out of launch diagnostics.
        let category = match self.cause {
            LaunchCause::Identity => "identity",
            LaunchCause::Journal(_) => "journal",
            LaunchCause::Control(_) => "control",
            LaunchCause::Runtime(_) => "runtime",
            LaunchCause::Authority(_) => "authority",
            LaunchCause::UnexpectedPhase => "phase",
        };
        write!(
            f,
            "worker launch failed: {category}; cleanup: {:?}",
            self.cleanup
        )
    }
}
impl std::error::Error for LaunchError {}
impl From<JournalError> for LaunchCause {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}
impl From<ClientError> for LaunchCause {
    fn from(e: ClientError) -> Self {
        Self::Control(e)
    }
}
impl From<RuntimeError> for LaunchCause {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
impl From<StopReason> for LaunchCause {
    fn from(e: StopReason) -> Self {
        Self::Authority(e)
    }
}

trait PhaseReporter {
    fn phase(
        &mut self,
        request: &ReportPhaseRequest,
    ) -> impl Future<Output = Result<PhaseStatus, ClientError>> + Send;
}
impl PhaseReporter for ControlClient {
    async fn phase(&mut self, request: &ReportPhaseRequest) -> Result<PhaseStatus, ClientError> {
        self.report_phase(request).await
    }
}
struct LaunchInput<'a> {
    assignment: &'a Assignment,
    execution: &'a ExecutionSpec,
}

pub async fn launch<R: Runtime>(
    runtime: &R,
    client: &mut ControlClient,
    journal: &AsyncJournal,
    grant: &GrantedAssignment,
    session: &WorkerSession,
    workspace: &PreparedWorkspace,
    authority: &mut SupervisedAuthority,
) -> Result<R::Handle, LaunchError> {
    launch_inner(
        runtime,
        client,
        journal,
        LaunchInput {
            assignment: grant.assignment(),
            execution: grant.execution(),
        },
        session,
        workspace,
        authority,
    )
    .await
}

async fn launch_inner<R: Runtime>(
    runtime: &R,
    client: &mut impl PhaseReporter,
    journal: &AsyncJournal,
    input: LaunchInput<'_>,
    session: &WorkerSession,
    workspace: &PreparedWorkspace,
    authority: &mut SupervisedAuthority,
) -> Result<R::Handle, LaunchError> {
    let mut handle = None;
    let mut create_attempted = false;
    let result: Result<(), LaunchCause> = async {
        let identity = input
            .assignment
            .authority
            .as_ref()
            .ok_or(LaunchCause::Identity)?;
        if identity != authority.identity()
            || identity.worker_id != session.worker_id
            || identity.session_id != session.session_id
            || workspace.root().file_name().and_then(|s| s.to_str())
                != Some(identity.attempt_id.as_str())
        {
            return Err(LaunchCause::Identity);
        }
        let starting = authority
            .while_live(journal.prepare_launch(input.assignment.clone(), session.clone()))
            .await??;
        loop {
            match authority.while_live(client.phase(&starting)).await? {
                Ok(status)
                    if status.decision == Decision::Accepted
                        && status.state == AttemptState::Starting =>
                {
                    break
                }
                Ok(status)
                    if matches!(
                        status.decision,
                        Decision::Fenced | Decision::StopRequested | Decision::AlreadyTerminal
                    ) =>
                {
                    return Err(StopReason::Rejected(status.decision).into())
                }
                Ok(_) => return Err(LaunchCause::UnexpectedPhase),
                Err(e) if e.retryable() => {
                    authority
                        .while_live(tokio::time::sleep(Duration::from_secs(1)))
                        .await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        let window = authority.window()?;
        create_attempted = true;
        handle = Some(
            authority
                .while_live(runtime.create(identity, input.execution, workspace, &window))
                .await??,
        );
        let container = handle.as_ref().unwrap();
        authority
            .while_live(journal.bind_container(
                identity.attempt_id.clone(),
                R::container_id(container).into(),
            ))
            .await??;
        // Durable container identity precedes the only start call. An uncertain
        // reply is resolved by inspection, never by restarting a fast-exit workload.
        let window = authority.window()?;
        let started = authority
            .while_live(runtime.start(container, &window))
            .await?;
        if let Err(error) = started {
            if !matches!(
                error,
                RuntimeError::Transport
                    | RuntimeError::Deadline
                    | RuntimeError::Terminal
                    | RuntimeError::Daemon(304 | 409 | 500..=599)
            ) {
                return Err(error.into());
            }
        }
        let status = authority.while_live(runtime.inspect(container)).await??;
        let running = status.running && status.state == ContainerState::Running;
        let exited = !status.running
            && matches!(status.state, ContainerState::Exited | ContainerState::Dead)
            && status
                .exit_code
                .is_some_and(|code| (0..=255).contains(&code));
        if !running && !exited {
            return Err(RuntimeError::Terminal.into());
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(handle.unwrap()),
        Err(cause) => {
            let cleanup = match &handle {
                Some(container) if terminate(runtime, container).await => CleanupEvidence::Stopped,
                Some(_) => CleanupEvidence::Uncertain,
                None if create_attempted => CleanupEvidence::Uncertain,
                None => CleanupEvidence::NotCreated,
            };
            Err(LaunchError { cause, cleanup })
        }
    }
}

#[cfg(test)]
mod tests;
