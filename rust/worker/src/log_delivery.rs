//! Deliver a sealed log segment using durable transfer and registration evidence.
use crate::{
    control::{ClientError, ControlClient},
    journal::{AsyncJournal, JournalError},
    transfer::{TransferClient, TransferError},
};
use dispatch_protocol::v1::{Decision, LogStream};
use std::{fmt, fs::File};

#[derive(Debug)]
pub enum LogDeliveryError {
    Journal(JournalError),
    Control(ClientError),
    Transfer(TransferError),
    MissingSource,
    Rejected(Decision),
}
impl fmt::Display for LogDeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Storage URLs and log content must not leak into worker diagnostics.
        match self {
            Self::Journal(_) => write!(f, "log delivery journal failure"),
            Self::Control(_) => write!(f, "log delivery control failure"),
            Self::Transfer(_) => write!(f, "log delivery transfer failure"),
            Self::MissingSource => write!(f, "log segment source unavailable"),
            Self::Rejected(_) => write!(f, "log registration rejected"),
        }
    }
}
impl std::error::Error for LogDeliveryError {}
impl From<JournalError> for LogDeliveryError {
    fn from(value: JournalError) -> Self {
        Self::Journal(value)
    }
}
impl From<ClientError> for LogDeliveryError {
    fn from(value: ClientError) -> Self {
        Self::Control(value)
    }
}
impl From<TransferError> for LogDeliveryError {
    fn from(value: TransferError) -> Self {
        Self::Transfer(value)
    }
}

/// Replay one journaled segment. The caller must retain current authority,
/// bound retries, and independently renew the lease during network waits.
pub async fn deliver_log(
    journal: &AsyncJournal,
    client: &mut ControlClient,
    transfers: &TransferClient,
    attempt_id: &str,
    stream: LogStream,
    first_sequence: u64,
    source: Option<File>,
) -> Result<(), LogDeliveryError> {
    let saved = journal
        .load_attempt(attempt_id.to_owned())
        .await?
        .ok_or(JournalError::Invalid)?;
    let log = saved
        .logs()
        .iter()
        .find(|s| s.stream() == stream && s.first_sequence() == first_sequence)
        .ok_or(JournalError::Invalid)?;
    if log.registered() {
        return Ok(());
    }
    let declaration = log.declaration().clone();
    let registration = if let Some(request) = log.registration() {
        request.clone()
    } else {
        if log.response().is_none() {
            let finalize = if let Some(request) = log.finalization() {
                // An observed version is immutable evidence. Resolve only that
                // version; another PUT could invalidate the saved operation.
                request.clone()
            } else {
                let source = source.ok_or(LogDeliveryError::MissingSource)?;
                let grant = client.create_upload(&declaration).await?;
                journal
                    .record_log_grant(declaration.clone(), grant.clone())
                    .await?;
                let object = transfers.put(&declaration, &grant, source).await?;
                journal
                    .prepare_log_finalization(
                        attempt_id.to_owned(),
                        declaration.request_id.clone(),
                        object,
                    )
                    .await?
            };
            let response = client.finalize_upload(&finalize).await?;
            journal.record_log_response(finalize, response).await?;
        }
        journal
            .prepare_log_registration(attempt_id.to_owned(), declaration.request_id)
            .await?
    };
    let status = client.register_log_segment(&registration).await?;
    if status.decision != Decision::Accepted {
        return Err(LogDeliveryError::Rejected(status.decision));
    }
    journal
        .record_log_registration(registration, status.decision, status.state)
        .await?;
    Ok(())
}
