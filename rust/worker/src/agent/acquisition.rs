//! Sequential admission keeps local capacity owned until terminal cleanup.
use super::{workspace::prepare_attempt_workspace, *};
use crate::{
    control::{GrantedAssignment, WorkOutcome},
    finalization::prepare_completion,
    launch::execute,
    runtime::Runtime,
    supervisor::{authority_channel, SupervisedAuthority},
    transfer::TransferClient,
};
use dispatch_protocol::v1::{AcquireWorkRequest, WorkerSession};

pub(super) struct ExecutionContext<'a> {
    pub config: &'a AgentConfig,
    pub root: &'a Path,
    pub session_id: &'a str,
    pub runtime: &'a DockerRuntime,
    pub journal: &'a AsyncJournal,
}

pub(super) async fn acquire_and_run(
    context: ExecutionContext<'_>,
    client: &mut ControlClient,
    mut ready: watch::Receiver<bool>,
    active: watch::Sender<Option<AuthorityController>>,
) -> Result<(), AgentError> {
    let session = WorkerSession {
        worker_id: context.config.worker_id.clone(),
        session_id: context.session_id.to_owned(),
    };
    let transfers = TransferClient::new(true).map_err(|_| AgentError::Configuration)?;
    loop {
        while !*ready.borrow_and_update() {
            ready.changed().await.map_err(|_| AgentError::Task)?;
        }
        let request = AcquireWorkRequest {
            session: Some(session.clone()),
            request_id: new_uuid()?,
        };
        let grant = loop {
            match client.acquire(&request).await {
                Ok(result) => break result,
                Err(error) if error.retryable() => {
                    // An uncertain commit must replay the same request ID. A
                    // fresh ID could reserve a second attempt before lease expiry.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let WorkOutcome::Assignment(grant) = grant else {
            if matches!(grant, WorkOutcome::Rejected(Decision::Fenced)) {
                return Err(AgentError::Task);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let identity = grant
            .assignment()
            .authority
            .clone()
            .ok_or(AgentError::Task)?;
        let (controller, authority) = authority_channel(identity.clone(), *grant.authority())
            .map_err(|_| AgentError::Task)?;
        active.send_replace(Some(controller.clone()));
        let mut renewal_client = client.clone();
        let renewal =
            tokio::spawn(async move { renewal_client.maintain_leases(vec![controller]).await });
        // Do not admit another job until the container and workspace are gone.
        // On uncertain cleanup, exit and let a new fenced incarnation reconcile.
        let result =
            run_assignment(&context, client, &transfers, &session, &grant, authority).await;
        active.send_replace(None);
        let renewed = renewal.await.map_err(|_| AgentError::Task)?;
        result?;
        renewed?;
        println!(
            "{}",
            serde_json::json!({"event":"attempt_terminal",
            "worker_id":context.config.worker_id,"session_id":context.session_id,"attempt_id":identity.attempt_id})
        );
    }
}

async fn run_assignment(
    context: &ExecutionContext<'_>,
    client: &mut ControlClient,
    transfers: &TransferClient,
    session: &WorkerSession,
    grant: &GrantedAssignment,
    mut authority: SupervisedAuthority,
) -> Result<(), AgentError> {
    let identity = grant
        .assignment()
        .authority
        .as_ref()
        .ok_or(AgentError::Task)?;
    let path = context.root.join(&identity.attempt_id);
    let root = context.root.to_owned();
    let attempt_id = identity.attempt_id.clone();
    let scratch = grant
        .assignment()
        .resources
        .as_ref()
        .ok_or(AgentError::Task)?
        .scratch_bytes;
    let workspace = authority
        .while_live(async {
            tokio::task::spawn_blocking(move || {
                prepare_attempt_workspace(&root, &attempt_id, scratch)
            })
            .await
            .map_err(|_| AgentError::Task)?
        })
        .await
        .map_err(|_| AgentError::Task)??;
    let mut finalizing = execute(
        context.runtime,
        client,
        context.journal,
        grant,
        session,
        &workspace,
        authority,
    )
    .await?;
    prepare_completion(
        &mut finalizing,
        context.journal,
        client,
        transfers,
        workspace,
    )
    .await?;
    let reply = loop {
        match deliver_pending(context.journal, client, &identity.attempt_id).await {
            Ok(Some(reply)) => break reply,
            Ok(None) => return Err(AgentError::Task),
            Err(DeliveryError::Control(error)) if error.retryable() => {
                // A committed completion can already be terminal, so historical
                // replay does not require execution authority. Keep its ID exact.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    if !matches!(
        Decision::try_from(reply.decision),
        Ok(Decision::Accepted | Decision::AlreadyTerminal | Decision::Fenced)
    ) {
        return Err(AgentError::Task);
    }
    context.runtime.remove(finalizing.handle()).await?;
    drop(finalizing);
    // Removal follows a durable terminal decision. A failed unlink quarantines
    // this process rather than silently reusing local scratch for another job.
    tokio::task::spawn_blocking(move || {
        std::fs::remove_dir_all(path).map_err(|_| AgentError::File)
    })
    .await
    .map_err(|_| AgentError::Task)??;
    Ok(())
}
