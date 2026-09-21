//! Delivery of journaled output uploads; recovered evidence grants no authority.
use crate::{
    control::{ClientError, ControlClient},
    journal::{AsyncJournal, JournalError},
    transfer::{TransferClient, TransferError},
};
use dispatch_protocol::v1::FinalizeUploadResponse;
use std::{fmt, fs::File};

#[derive(Debug)]
pub enum UploadError {
    Journal(JournalError),
    Control(ClientError),
    Transfer(TransferError),
    MissingSource,
}
impl fmt::Display for UploadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(e) => e.fmt(f),
            Self::Control(e) => e.fmt(f),
            Self::Transfer(e) => e.fmt(f),
            Self::MissingSource => write!(f, "output source is unavailable"),
        }
    }
}
impl std::error::Error for UploadError {}
impl From<JournalError> for UploadError {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}
impl From<ClientError> for UploadError {
    fn from(e: ClientError) -> Self {
        Self::Control(e)
    }
}
impl From<TransferError> for UploadError {
    fn from(e: TransferError) -> Self {
        Self::Transfer(e)
    }
}

/// Deliver one already-journaled output. The caller owns live authority, cancellation,
/// and bounded retries, and must keep polling lease/phase deadlines independently.
pub async fn deliver_output(
    journal: &AsyncJournal,
    client: &mut ControlClient,
    transfers: &TransferClient,
    attempt_id: &str,
    name: &str,
    source: Option<File>,
) -> Result<FinalizeUploadResponse, UploadError> {
    let saved = journal
        .load_attempt(attempt_id.to_owned())
        .await?
        .ok_or(JournalError::Invalid)?;
    let upload = saved
        .outputs()
        .iter()
        .find(|u| u.declaration().name == name)
        .ok_or(JournalError::Invalid)?;
    if let Some(response) = upload.response() {
        // This is historical verified evidence, not permission to accept a job.
        // Returning it requires neither local bytes nor a new storage operation.
        return Ok(response.clone());
    }
    let finalize = if let Some(request) = upload.finalization() {
        // Once a version is durable, resolve only that version. Repeating PUT
        // would create a new object version and invalidate the saved retry identity.
        request.clone()
    } else {
        let source = source.ok_or(UploadError::MissingSource)?;
        let declaration = upload.declaration();
        let grant = client.create_upload(declaration).await?;
        journal
            .record_output_grant(declaration.clone(), grant.clone())
            .await?;
        let object = transfers.put(declaration, &grant, source).await?;
        journal
            .prepare_output_finalization(
                attempt_id.to_owned(),
                declaration.request_id.clone(),
                object,
            )
            .await?
    };
    let response = client.finalize_upload(&finalize).await?;
    // No network operation holds the journal lock. Persist the acknowledgement
    // before returning so completion can reference durable, exact-version evidence.
    journal
        .record_output_response(finalize, response.clone())
        .await?;
    Ok(response)
}
