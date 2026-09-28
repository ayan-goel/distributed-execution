//! Collect and register outputs under live authority, then seal terminal evidence.
use crate::{
    control::{completion_digest, ClientError, ControlClient},
    execution::ExecutionSpec,
    journal::{new_uuid, AsyncJournal, JournalError},
    launch::{CleanupEvidence, FinalizingAttempt},
    log_assembler::LogAssembler,
    log_capture::CapturedChunk,
    log_delivery::{deliver_log, LogDeliveryError},
    log_spool::MAX_SPOOL_BYTES,
    log_summary::{self, LogSummary},
    outputs::{collect_outputs, CollectionError, CollectionLimits},
    runtime::{ContainerHandle, DockerRuntime, PreparedWorkspace, Runtime},
    supervisor::StopReason,
    transfer::{TransferClient, TransferError},
    upload::{deliver_output, UploadError},
};
use dispatch_protocol::v1::{
    AttemptAuthority, CompleteAttemptRequest, Decision, FailureReason, LogStream, OutputReference,
};
use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MAX_DELIVERY_ATTEMPTS: usize = 3;
const MAX_FINAL_LOG_BYTES: usize = 1 << 20;

#[derive(Debug)]
pub enum FinalizationError {
    Identity,
    StopUnconfirmed,
    Authority(StopReason),
    Journal(JournalError),
    Collection(CollectionError),
    Upload(UploadError),
    Payload(ClientError),
    Log,
}
impl fmt::Display for FinalizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Neither workload paths nor signed storage capabilities belong in errors.
        let category = match self {
            Self::Identity => "identity",
            Self::StopUnconfirmed => "stop unconfirmed",
            Self::Authority(_) => "authority",
            Self::Journal(_) => "journal",
            Self::Collection(_) => "collection",
            Self::Upload(_) => "upload",
            Self::Payload(_) => "payload",
            Self::Log => "log",
        };
        write!(f, "worker finalization failed: {category}")
    }
}

/// Publish a bounded Docker snapshot after exit. Truncation remains explicit
/// because this path cannot claim that bytes beyond its capture cap survived.
pub async fn publish_finished_logs(
    attempt: &mut FinalizingAttempt<ContainerHandle>,
    runtime: &DockerRuntime,
    journal: &AsyncJournal,
    client: &mut ControlClient,
    transfers: &TransferClient,
    workspace: &PreparedWorkspace,
) -> Result<LogSummary, FinalizationError> {
    if !attempt.owns_workspace(workspace) {
        return Err(FinalizationError::Identity);
    }
    let capture = attempt.take_capture();
    let handle = attempt.handle().clone();
    let id = attempt.identity().attempt_id.clone();
    attempt
        .while_finalizing(async {
            let (mut assembler, captured, capture_finished) = match capture {
                Some(Ok(capture)) => (capture.assembler, capture.counts, capture.complete),
                Some(Err(_)) => {
                    return Ok(LogSummary {
                        complete: false,
                        gaps: Vec::new(),
                    })
                }
                None => {
                    let logs = match runtime.logs(&handle, MAX_FINAL_LOG_BYTES).await {
                        Ok(logs) => logs,
                        Err(_) => {
                            return Ok(LogSummary {
                                complete: false,
                                gaps: Vec::new(),
                            })
                        }
                    };
                    let mut assembler = LogAssembler::new(workspace, MAX_SPOOL_BYTES)
                        .map_err(|_| FinalizationError::Log)?;
                    let mut captured = [0u64; 2];
                    for (index, (stream, bytes)) in [
                        (LogStream::Stdout, logs.stdout),
                        (LogStream::Stderr, logs.stderr),
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        for payload in bytes.chunks(crate::log_capture::MAX_CHUNK_BYTES) {
                            captured[index] += 1;
                            let nanos = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map_err(|_| FinalizationError::Log)?
                                .as_nanos();
                            let capture_unix_nanos =
                                i64::try_from(nanos).map_err(|_| FinalizationError::Log)?;
                            assembler
                                .ingest(CapturedChunk {
                                    stream,
                                    sequence: captured[index],
                                    capture_unix_nanos,
                                    payload: payload.to_vec(),
                                })
                                .map_err(|_| FinalizationError::Log)?;
                        }
                    }
                    assembler.flush_all().map_err(|_| FinalizationError::Log)?;
                    (assembler, captured, !logs.truncated)
                }
            };
            let mut delivered = true;
            while let Some(pending) = assembler.front() {
                let stream = pending.stream();
                let first = pending.first_sequence();
                let last = pending.last_sequence();
                let gaps = pending.gaps().to_vec();
                let size = pending.size();
                let sha256 = pending.sha256().to_owned();
                journal
                    .prepare_log(id.clone(), stream, first, last, gaps, size, sha256)
                    .await?;
                let mut accepted = false;
                for round in 0..MAX_DELIVERY_ATTEMPTS {
                    let source = assembler.read_front().map_err(|_| FinalizationError::Log)?;
                    match deliver_log(journal, client, transfers, &id, stream, first, source).await
                    {
                        Ok(()) => {
                            accepted = true;
                            break;
                        }
                        Err(LogDeliveryError::Control(error)) if error.upload_stop_requested() => {
                            return Err(FinalizationError::Authority(StopReason::Rejected(
                                Decision::StopRequested,
                            )));
                        }
                        Err(LogDeliveryError::Rejected(decision)) => {
                            return Err(FinalizationError::Authority(StopReason::Rejected(
                                decision,
                            )));
                        }
                        Err(LogDeliveryError::Journal(error)) => {
                            return Err(FinalizationError::Journal(error))
                        }
                        Err(_) if round + 1 < MAX_DELIVERY_ATTEMPTS => {
                            tokio::time::sleep(Duration::from_secs(1)).await
                        }
                        Err(_) => break,
                    }
                }
                if !accepted {
                    delivered = false;
                    break;
                }
                assembler
                    .acknowledge_front()
                    .map_err(|_| FinalizationError::Log)?;
            }
            let saved = journal
                .load_attempt(id)
                .await?
                .ok_or(JournalError::Invalid)?;
            log_summary::summarize(saved.logs(), captured, capture_finished && delivered)
                .map_err(|_| FinalizationError::Log)
        })
        .await
        .map_err(FinalizationError::Authority)?
}
impl std::error::Error for FinalizationError {}
impl From<JournalError> for FinalizationError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

/// Seal a cancellation acknowledgement only after the executor confirmed that
/// its container stopped or was never created. The journal binds this evidence
/// to one attempt so a lost reply cannot invent a second completion identity.
pub async fn prepare_cancelled_completion(
    journal: &AsyncJournal,
    identity: &AttemptAuthority,
    cleanup: CleanupEvidence,
) -> Result<CompleteAttemptRequest, FinalizationError> {
    let saved = journal
        .load_attempt(identity.attempt_id.clone())
        .await?
        .ok_or(FinalizationError::Identity)?;
    if saved.assignment().authority.as_ref() != Some(identity) {
        return Err(FinalizationError::Identity);
    }
    match (cleanup, saved.container_id()) {
        (CleanupEvidence::Stopped, Some(_)) | (CleanupEvidence::NotCreated, None) => {}
        _ => return Err(FinalizationError::StopUnconfirmed),
    }
    if let Some(request) = saved.completion() {
        return if request.reason == FailureReason::UserCancelled as i32 && request.stopped {
            Ok(request.clone())
        } else if saved.completion_response().is_some_and(|response| {
            response.decision == dispatch_protocol::v1::Decision::StopRequested as i32
        }) {
            let mut cancelled = CompleteAttemptRequest {
                authority: Some(identity.clone()),
                completion_id: new_uuid()?,
                exit_code: saved.exit().map(|exit| exit.exit_code),
                reason: FailureReason::UserCancelled as i32,
                stopped: true,
                logs_complete: false,
                ..Default::default()
            };
            cancelled.payload_sha256 =
                completion_digest(&cancelled).map_err(FinalizationError::Payload)?;
            journal
                .supersede_rejected_completion(cancelled.clone())
                .await?;
            Ok(cancelled)
        } else {
            Err(FinalizationError::Identity)
        };
    }
    let mut request = CompleteAttemptRequest {
        authority: Some(identity.clone()),
        completion_id: new_uuid()?,
        exit_code: saved.exit().map(|exit| exit.exit_code),
        reason: FailureReason::UserCancelled as i32,
        stopped: true,
        logs_complete: false,
        ..Default::default()
    };
    request.payload_sha256 = completion_digest(&request).map_err(FinalizationError::Payload)?;
    journal.persist_completion(request.clone()).await?;
    Ok(request)
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
    logs: LogSummary,
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
            .map_err(|_| FinalizationError::Collection(CollectionError::Unsafe))?;
            let mut failure = FailureReason::Unspecified;
            let outputs = match outputs {
                Ok(outputs) => outputs,
                Err(CollectionError::Configuration) => {
                    return Err(FinalizationError::Collection(
                        CollectionError::Configuration,
                    ));
                }
                Err(_) => {
                    // Missing, unreadable, unsafe, changing, or oversized declared
                    // files cannot yield success even when the process exited zero.
                    failure = FailureReason::OutputInvalid;
                    Vec::new()
                }
            };
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
                let mut round = 0;
                let reply = loop {
                    round += 1;
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
                        Ok(reply) => break Some(reply),
                        Err(error) => {
                            if matches!(&error, UploadError::Control(control) if control.upload_stop_requested()) {
                                // The upload gate can observe cancellation before the
                                // periodic lease renewal; it is equally authoritative.
                                return Err(FinalizationError::Authority(
                                    StopReason::Rejected(Decision::StopRequested),
                                ));
                            }
                            let Some((reason, retryable)) = upload_failure(&error) else {
                                return Err(FinalizationError::Upload(error));
                            };
                            // Count whole delivery rounds, including uncertain RPC
                            // commits. The live phase guard also bounds this backoff.
                            if retryable && round < MAX_DELIVERY_ATTEMPTS {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                            failure = reason;
                            break None;
                        }
                    }
                };
                let Some(reply) = reply else {
                    break;
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
                // Preserve the execution failure as the primary reason. A later
                // storage outage must not make an OOM eligible for transfer retries.
                reason: if exit.oom_killed {
                    FailureReason::Oom
                } else if exit.exit_code != 0 {
                    FailureReason::ApplicationExit
                } else {
                    failure
                } as i32,
                stopped: true,
                outputs: references,
                logs_complete: logs.complete,
                gaps: logs.gaps,
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

fn upload_failure(error: &UploadError) -> Option<(FailureReason, bool)> {
    match error {
        UploadError::Control(error) if error.retryable() => {
            Some((FailureReason::TransferFailed, true))
        }
        UploadError::Transfer(TransferError::Integrity) => {
            Some((FailureReason::OutputInvalid, false))
        }
        UploadError::Transfer(TransferError::Configuration | TransferError::Grant) => None,
        UploadError::Transfer(error) => Some((
            FailureReason::TransferFailed,
            matches!(
                error,
                TransferError::Transport
                    | TransferError::Deadline
                    | TransferError::Expired
                    | TransferError::Status(408 | 429 | 500..=599)
            ),
        )),
        // Journal/identity errors and control-plane rejection must remain errors.
        // They cannot authorize a new completion or be relabeled as job failures.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_policy_distinguishes_outages_from_rejection_and_corrupt_evidence() {
        assert_eq!(
            upload_failure(&UploadError::Transfer(TransferError::Status(503))),
            Some((FailureReason::TransferFailed, true))
        );
        assert_eq!(
            upload_failure(&UploadError::Transfer(TransferError::Status(403))),
            Some((FailureReason::TransferFailed, false))
        );
        assert_eq!(
            upload_failure(&UploadError::Transfer(TransferError::Integrity)),
            Some((FailureReason::OutputInvalid, false))
        );
        assert_eq!(
            upload_failure(&UploadError::Transfer(TransferError::Version)),
            Some((FailureReason::TransferFailed, false))
        );
        assert_eq!(
            upload_failure(&UploadError::Control(ClientError::Connection)),
            Some((FailureReason::TransferFailed, true))
        );
        for error in [
            UploadError::Journal(JournalError::Conflict),
            UploadError::Control(ClientError::Response),
            UploadError::Control(ClientError::Rpc(Box::new(
                tonic::Status::failed_precondition("FENCED"),
            ))),
            UploadError::Control(ClientError::Rpc(Box::new(tonic::Status::unauthenticated(
                "revoked",
            )))),
            UploadError::Transfer(TransferError::Grant),
            UploadError::MissingSource,
        ] {
            assert_eq!(upload_failure(&error), None);
        }
    }
}
