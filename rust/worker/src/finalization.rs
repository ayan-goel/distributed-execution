//! Collect and register outputs under live authority, then seal terminal evidence.
use crate::{
    control::{completion_digest, ClientError, ControlClient},
    execution::ExecutionSpec,
    journal::{new_uuid, AsyncJournal, JournalError},
    launch::FinalizingAttempt,
    outputs::{collect_outputs, CollectionError, CollectionLimits},
    runtime::PreparedWorkspace,
    supervisor::StopReason,
    transfer::TransferClient,
    upload::{deliver_output, UploadError},
};
use dispatch_protocol::v1::{CompleteAttemptRequest, FailureReason, OutputReference};
use std::{fmt, time::Duration};

#[derive(Debug)]
pub enum FinalizationError {
    Identity,
    Authority(StopReason),
    Journal(JournalError),
    Collection(CollectionError),
    Upload(UploadError),
    Payload(ClientError),
}
impl fmt::Display for FinalizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Neither workload paths nor signed storage capabilities belong in errors.
        let category = match self {
            Self::Identity => "identity",
            Self::Authority(_) => "authority",
            Self::Journal(_) => "journal",
            Self::Collection(_) => "collection",
            Self::Upload(_) => "upload",
            Self::Payload(_) => "payload",
        };
        write!(f, "worker finalization failed: {category}")
    }
}
impl std::error::Error for FinalizationError {}
impl From<JournalError> for FinalizationError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

/// Prepare durable completion for an observed exit. Renew leases independently.
/// Deliver the sealed request with `completion::deliver_pending`; that historical
/// replay remains safe when acceptance has already ended execution authority.
pub async fn prepare_completion<H>(
    attempt: &mut FinalizingAttempt<H>,
    journal: &AsyncJournal,
    client: &mut ControlClient,
    transfers: &TransferClient,
    workspace: PreparedWorkspace,
) -> Result<CompleteAttemptRequest, FinalizationError> {
    if !attempt.owns_workspace(&workspace) {
        return Err(FinalizationError::Identity);
    }
    let identity = attempt.identity().clone();
    let exit = attempt.exit().clone();
    attempt
        .while_finalizing(async {
            let saved = journal
                .load_attempt(identity.attempt_id.clone())
                .await?
                .ok_or(JournalError::Invalid)?;
            if saved.assignment().authority.as_ref() != Some(&identity)
                || saved.exit() != Some(&exit)
            {
                return Err(FinalizationError::Identity);
            }
            if let Some(request) = saved.completion() {
                return Ok(request.clone());
            }
            let execution = ExecutionSpec::from_assignment(saved.assignment())
                .map_err(|_| FinalizationError::Identity)?;
            // Hashing must not block lease timers. Cancellation can leave this
            // bounded read running, but it performs no uploads or journal mutations.
            let outputs = tokio::task::spawn_blocking(move || {
                collect_outputs(&workspace, &execution, CollectionLimits::default())
            })
            .await
            .map_err(|_| FinalizationError::Collection(CollectionError::Unsafe))?
            .map_err(FinalizationError::Collection)?;
            let mut references = Vec::with_capacity(outputs.len());
            for output in outputs {
                let name = output.name().to_owned();
                journal
                    .prepare_output(
                        identity.attempt_id.clone(),
                        name.clone(),
                        output.size_bytes(),
                        output.sha256().to_owned(),
                    )
                    .await?;
                let file = output.into_file();
                let reply = loop {
                    let source = file.try_clone().map_err(|error| {
                        FinalizationError::Collection(CollectionError::Io(error.kind()))
                    })?;
                    match deliver_output(
                        journal,
                        client,
                        transfers,
                        &identity.attempt_id,
                        &name,
                        Some(source),
                    )
                    .await
                    {
                        Ok(reply) => break reply,
                        // Stable journal identities resolve uncertain control-plane
                        // commits. The outer guard bounds both retries and backoff.
                        Err(UploadError::Control(error)) if error.retryable() => {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(error) => return Err(FinalizationError::Upload(error)),
                    }
                };
                references.push(OutputReference {
                    name,
                    artifact_id: reply.artifact_id,
                });
            }
            let mut request = CompleteAttemptRequest {
                authority: Some(identity),
                completion_id: new_uuid()?,
                exit_code: Some(exit.exit_code),
                reason: if exit.oom_killed {
                    FailureReason::Oom
                } else if exit.exit_code != 0 {
                    FailureReason::ApplicationExit
                } else {
                    FailureReason::Unspecified
                } as i32,
                stopped: true,
                outputs: references,
                // Log delivery is not connected yet. Never claim complete logs
                // from a stopped process or successful output upload alone.
                logs_complete: false,
                ..Default::default()
            };
            request.payload_sha256 =
                completion_digest(&request).map_err(FinalizationError::Payload)?;
            journal.persist_completion(request.clone()).await?;
            Ok(request)
        })
        .await
        .map_err(FinalizationError::Authority)?
}
