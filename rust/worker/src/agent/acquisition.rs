//! Sequential admission keeps local capacity owned until terminal cleanup.
use super::{workspace::prepare_attempt_workspace, *};
use crate::{
    control::{ClientError, GrantedAssignment, WorkOutcome},
    dataset_assignment::{stage_validated_input, validate_replayed_input, InputStageError},
    dataset_cache_store::{CachePin, CacheStore, CacheStoreError},
    dataset_download::DatasetDownloader,
    finalization::{
        prepare_cancelled_completion, prepare_completion, prepare_removed_launch_failure,
        prepare_unlaunched_runtime_failure, prepare_unlaunched_transfer_failure,
        publish_finished_logs, FinalizationError,
    },
    launch::{execute_with_logs, CleanupEvidence, ExecutionError, LaunchCause},
    runtime::Runtime,
    supervisor::{authority_channel, StopReason, SupervisedAuthority},
    transfer::TransferClient,
};
use dispatch_protocol::v1::{AcquireWorkRequest, CompleteAttemptResponse, WorkerSession};

pub(super) struct ExecutionContext<'a> {
    pub config: &'a AgentConfig,
    pub root: &'a Path,
    pub session_id: &'a str,
    pub runtime: &'a DockerRuntime,
    pub journal: &'a AsyncJournal,
}

struct InputServices<'a> {
    request: &'a AcquireWorkRequest,
    downloader: &'a DatasetDownloader,
    cache: &'a CacheStore,
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
    let downloader = DatasetDownloader::new(true).map_err(|_| AgentError::Configuration)?;
    while !*ready.borrow_and_update() {
        ready.changed().await.map_err(|_| AgentError::Task)?;
    }
    // The health loop creates this empty root only after predecessor containers
    // are gone. No cache entry may be reused across an unreconciled incarnation.
    let cache = CacheStore::new(
        &context.root.join(".dataset-cache"),
        context.config.dataset_cache_high_mib << 20,
        context.config.dataset_cache_low_mib << 20,
    )?;
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
        let result = run_assignment(
            &context,
            client,
            &transfers,
            &InputServices {
                request: &request,
                downloader: &downloader,
                cache: &cache,
            },
            &session,
            &grant,
            authority,
        )
        .await;
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
    inputs: &InputServices<'_>,
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
    context
        .journal
        .claim_assignment(grant.assignment().clone(), session.clone())
        .await?;
    if let Err(reason) = authority.window() {
        if reason == StopReason::Rejected(Decision::StopRequested) {
            return acknowledge_unlaunched(context, client, identity, None).await;
        }
        return Err(AgentError::Task);
    }
    let mut preparation =
        tokio::task::spawn_blocking(move || prepare_attempt_workspace(&root, &attempt_id, scratch));
    let mut workspace = match authority.while_live(&mut preparation).await {
        Ok(result) => result.map_err(|_| AgentError::Task)??,
        Err(reason) => {
            // Dropping a blocking task does not stop its filesystem writes.
            // Wait for it before acknowledging that no execution was created.
            let prepared = preparation.await.map_err(|_| AgentError::Task)??;
            drop(prepared);
            if reason == StopReason::Rejected(Decision::StopRequested) {
                return acknowledge_unlaunched(context, client, identity, Some(path)).await;
            }
            remove_workspace(path).await?;
            return Err(AgentError::Task);
        }
    };
    // Image preparation owns no container. Live authority preserves the original
    // startup deadline across renewals and cancels pulls before stale execution.
    match authority
        .while_live(context.runtime.prepare_image(grant.execution()))
        .await
    {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            prepare_unlaunched_runtime_failure(context.journal, identity).await?;
            return deliver_unlaunched_failure(context, client, identity, path).await;
        }
        Err(StopReason::Rejected(Decision::StopRequested)) => {
            return acknowledge_unlaunched(context, client, identity, Some(path)).await;
        }
        Err(_) => {
            remove_workspace(path).await?;
            return Err(AgentError::Task);
        }
    }
    let _pins = if grant.assignment().inputs.is_empty() {
        Vec::new()
    } else {
        match authority
            .while_live(stage_refreshed_inputs(
                client,
                inputs.request,
                grant,
                inputs,
                &mut workspace,
            ))
            .await
        {
            Ok(Ok(pins)) => pins,
            Ok(Err(StageReplayError::Input(InputStageError::Cache(
                CacheStoreError::Preparation(_),
            )))) => {
                return complete_unlaunched_transfer_failure(context, client, identity, path).await;
            }
            Ok(Err(StageReplayError::Input(error))) => {
                remove_workspace(path).await?;
                return Err(error.into());
            }
            Ok(Err(StageReplayError::Control(error))) => {
                remove_workspace(path).await?;
                return Err(error.into());
            }
            Ok(Err(StageReplayError::StopRequested)) => {
                return acknowledge_unlaunched(context, client, identity, Some(path)).await;
            }
            Err(StopReason::Rejected(Decision::StopRequested)) => {
                return acknowledge_unlaunched(context, client, identity, Some(path)).await;
            }
            Err(_) => {
                remove_workspace(path).await?;
                return Err(AgentError::Task);
            }
        }
    };
    let outcome = execute_with_logs(
        context.runtime,
        client,
        context.journal,
        grant,
        session,
        &workspace,
        authority,
        transfers,
    )
    .await;
    let mut finalizing = match outcome {
        Ok(finalizing) => finalizing,
        Err(error) => {
            if matches!(&error, ExecutionError::Launch(failed) if matches!(&failed.cause, LaunchCause::Runtime(_)))
            {
                complete_removed_launch_failure(context, client, grant).await?;
                drop(workspace);
                remove_workspace(path).await?;
                return Ok(());
            }
            let Some(cleanup) = confirmed_cancellation(&error) else {
                return Err(error.into());
            };
            // Seal the exact stopped evidence before network delivery. A
            // failure to remove local state keeps this worker from admitting
            // another job, even if a later incarnation replays the completion.
            prepare_cancelled_completion(context.journal, identity, cleanup).await?;
            let saved = context
                .journal
                .load_attempt(identity.attempt_id.clone())
                .await?
                .ok_or(AgentError::Task)?;
            if let Some(container_id) = saved.container_id() {
                context
                    .runtime
                    .remove_stopped_current(identity, container_id, &grant.assignment().spec_sha256)
                    .await?;
            }
            drop(workspace);
            let reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
            if !terminal_decision(&reply) {
                return Err(AgentError::Task);
            }
            remove_workspace(path).await?;
            return Ok(());
        }
    };
    let prepared = match publish_finished_logs(
        &mut finalizing,
        context.runtime,
        context.journal,
        client,
        transfers,
        &workspace,
    )
    .await
    {
        Ok(logs) => {
            prepare_completion(
                &mut finalizing,
                context.journal,
                client,
                transfers,
                workspace,
                logs,
            )
            .await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = prepared {
        if !matches!(
            error,
            FinalizationError::Authority(StopReason::Rejected(Decision::StopRequested))
        ) {
            return Err(error.into());
        }
        let saved = context
            .journal
            .load_attempt(identity.attempt_id.clone())
            .await?
            .ok_or(AgentError::Task)?;
        if saved.completion().is_some() {
            // A result sealed just before cancellation may already have won the
            // server transaction. Resolve it before sealing different evidence.
            let reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
            if reply.decision != Decision::StopRequested as i32 {
                if !terminal_decision(&reply) {
                    return Err(AgentError::Task);
                }
                context.runtime.remove(finalizing.handle()).await?;
                drop(finalizing);
                return remove_workspace(path).await;
            }
        }
        prepare_cancelled_completion(context.journal, identity, CleanupEvidence::Stopped).await?;
        context.runtime.remove(finalizing.handle()).await?;
        drop(finalizing);
        let reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
        if !terminal_decision(&reply) {
            return Err(AgentError::Task);
        }
        return remove_workspace(path).await;
    }
    let reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
    if reply.decision == Decision::StopRequested as i32 {
        // Completion lost the server transaction race to cancellation. Preserve
        // that rejection, then acknowledge the already exited container.
        prepare_cancelled_completion(context.journal, identity, CleanupEvidence::Stopped).await?;
        context.runtime.remove(finalizing.handle()).await?;
        drop(finalizing);
        let cancelled = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
        if !terminal_decision(&cancelled) {
            return Err(AgentError::Task);
        }
        return remove_workspace(path).await;
    }
    if !terminal_decision(&reply) {
        return Err(AgentError::Task);
    }
    context.runtime.remove(finalizing.handle()).await?;
    drop(finalizing);
    remove_workspace(path).await
}

async fn complete_removed_launch_failure(
    context: &ExecutionContext<'_>,
    client: &mut ControlClient,
    grant: &GrantedAssignment,
) -> Result<(), AgentError> {
    let identity = grant
        .assignment()
        .authority
        .as_ref()
        .ok_or(AgentError::Task)?;
    let saved = context
        .journal
        .load_attempt(identity.attempt_id.clone())
        .await?
        .ok_or(AgentError::Task)?;
    if saved.exit().is_some()
        || saved.phase_reports().last().map(|r| r.phase)
            != Some(dispatch_protocol::v1::AttemptState::Starting as i32)
    {
        return Err(AgentError::Task);
    }
    let container = saved.container_id().ok_or(AgentError::Task)?;
    // A failed start can leave CREATED or race an ambiguous daemon operation.
    // Seal stopped evidence only after checked nonforced removal proves absence.
    context
        .runtime
        .remove_stopped_current(identity, container, &grant.assignment().spec_sha256)
        .await?;
    prepare_removed_launch_failure(context.journal, identity).await?;
    let mut reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
    if reply.decision == Decision::StopRequested as i32 {
        // Cancellation may win while cleanup runs. Retain the rejected runtime
        // request, then acknowledge the same already-removed container.
        prepare_cancelled_completion(context.journal, identity, CleanupEvidence::Stopped).await?;
        reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
    }
    if !terminal_decision(&reply) {
        return Err(AgentError::Task);
    }
    Ok(())
}

enum StageReplayError {
    Control(ClientError),
    Input(InputStageError),
    StopRequested,
}

async fn stage_refreshed_inputs(
    client: &mut ControlClient,
    request: &AcquireWorkRequest,
    grant: &GrantedAssignment,
    inputs: &InputServices<'_>,
    workspace: &mut crate::runtime::PreparedWorkspace,
) -> Result<Vec<CachePin>, StageReplayError> {
    let expected = &grant.execution().job().spec.inputs;
    let mut pins = Vec::with_capacity(expected.len());
    for index in 0..expected.len() {
        // Each input starts from a newly signed grant. A long earlier transfer
        // cannot consume the later input's capability lifetime.
        let replay = loop {
            match client.acquire(request).await {
                Ok(WorkOutcome::Assignment(replay)) => break replay,
                Ok(WorkOutcome::Rejected(Decision::StopRequested)) => {
                    return Err(StageReplayError::StopRequested);
                }
                Ok(_) => {
                    return Err(StageReplayError::Input(InputStageError::Assignment(
                        crate::dataset_assignment::InputError::SpecMismatch,
                    )))
                }
                Err(error) if error.retryable() => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(error) => return Err(StageReplayError::Control(error)),
            }
        };
        let input = validate_replayed_input(
            grant.assignment(),
            replay.assignment(),
            expected,
            index,
            inputs.downloader,
        )
        .map_err(|error| StageReplayError::Input(InputStageError::Assignment(error)))?;
        let pin = stage_validated_input(input, inputs.downloader, inputs.cache, workspace)
            .await
            .map_err(StageReplayError::Input)?;
        pins.push(pin);
    }
    Ok(pins)
}

async fn complete_unlaunched_transfer_failure(
    context: &ExecutionContext<'_>,
    client: &mut ControlClient,
    identity: &dispatch_protocol::v1::AttemptAuthority,
    workspace: std::path::PathBuf,
) -> Result<(), AgentError> {
    prepare_unlaunched_transfer_failure(context.journal, identity).await?;
    deliver_unlaunched_failure(context, client, identity, workspace).await
}

async fn deliver_unlaunched_failure(
    context: &ExecutionContext<'_>,
    client: &mut ControlClient,
    identity: &dispatch_protocol::v1::AttemptAuthority,
    workspace: std::path::PathBuf,
) -> Result<(), AgentError> {
    let reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
    if reply.decision == Decision::StopRequested as i32 {
        // Cancellation won after preparation failure was sealed; preserve the
        // rejected result before recording the confirmed unlaunched stop.
        prepare_cancelled_completion(context.journal, identity, CleanupEvidence::NotCreated)
            .await?;
        let cancelled = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
        if !terminal_decision(&cancelled) {
            return Err(AgentError::Task);
        }
    } else if !terminal_decision(&reply) {
        return Err(AgentError::Task);
    }
    remove_workspace(workspace).await
}

async fn acknowledge_unlaunched(
    context: &ExecutionContext<'_>,
    client: &mut ControlClient,
    identity: &dispatch_protocol::v1::AttemptAuthority,
    workspace: Option<std::path::PathBuf>,
) -> Result<(), AgentError> {
    prepare_cancelled_completion(context.journal, identity, CleanupEvidence::NotCreated).await?;
    if let Some(path) = workspace {
        remove_workspace(path).await?;
    }
    let reply = deliver_terminal(context.journal, client, &identity.attempt_id).await?;
    if !terminal_decision(&reply) {
        return Err(AgentError::Task);
    }
    Ok(())
}

fn confirmed_cancellation(error: &ExecutionError) -> Option<CleanupEvidence> {
    let (cause, cleanup) = match error {
        ExecutionError::Launch(error) => (&error.cause, error.cleanup),
        ExecutionError::AfterLaunch { cause, cleanup } => (cause, *cleanup),
    };
    if matches!(
        cause,
        LaunchCause::Authority(StopReason::Rejected(Decision::StopRequested))
    ) && matches!(
        cleanup,
        CleanupEvidence::Stopped | CleanupEvidence::NotCreated
    ) {
        Some(cleanup)
    } else {
        None
    }
}

async fn deliver_terminal(
    journal: &AsyncJournal,
    client: &mut ControlClient,
    attempt_id: &str,
) -> Result<CompleteAttemptResponse, AgentError> {
    loop {
        match deliver_pending(journal, client, attempt_id).await {
            Ok(Some(reply)) => return Ok(reply),
            Ok(None) => return Err(AgentError::Task),
            Err(DeliveryError::Control(error)) if error.retryable() => {
                // A committed completion can already be terminal, so historical
                // replay does not require execution authority. Keep its ID exact.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn terminal_decision(reply: &CompleteAttemptResponse) -> bool {
    matches!(
        Decision::try_from(reply.decision),
        Ok(Decision::Accepted | Decision::AlreadyTerminal | Decision::Fenced)
    )
}

async fn remove_workspace(path: std::path::PathBuf) -> Result<(), AgentError> {
    // Removal follows a durable terminal decision. A failed unlink quarantines
    // this process rather than silently reusing local scratch for another job.
    tokio::task::spawn_blocking(move || {
        std::fs::remove_dir_all(path).map_err(|_| AgentError::File)
    })
    .await
    .map_err(|_| AgentError::Task)??;
    Ok(())
}
