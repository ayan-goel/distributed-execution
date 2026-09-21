//! Delivery of durable completion evidence; never creates execution authority.
use crate::{
    control::{ClientError, ControlClient},
    journal::{AsyncJournal, JournalError},
};
use dispatch_protocol::v1::{CompleteAttemptResponse, Decision};
use std::fmt;

#[derive(Debug)]
pub enum DeliveryError {
    Journal(JournalError),
    Control(ClientError),
}
impl fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(e) => e.fmt(f),
            Self::Control(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for DeliveryError {}
impl From<JournalError> for DeliveryError {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}
impl From<ClientError> for DeliveryError {
    fn from(e: ClientError) -> Self {
        Self::Control(e)
    }
}

/// Attempt one delivery and durably record its reply. A transport failure leaves
/// the original pending entry available for the caller's bounded retry policy.
pub async fn deliver_pending(
    journal: &AsyncJournal,
    client: &mut ControlClient,
    attempt_id: &str,
) -> Result<Option<CompleteAttemptResponse>, DeliveryError> {
    let saved = journal
        .load_attempt(attempt_id.to_owned())
        .await?
        .ok_or(JournalError::Invalid)?;
    let Some(request) = saved.completion() else {
        return Ok(None);
    };
    if let Some(reply) = saved.completion_response() {
        // Immutable outcomes need no network on restart. STOP_REQUESTED remains
        // unresolved until the server fences/terminalizes or cancellation is handled.
        if reply.decision != Decision::StopRequested as i32 {
            return Ok(Some(reply.clone()));
        }
    }
    let status = client.complete_attempt(request).await?;
    let reply = CompleteAttemptResponse {
        decision: status.decision as i32,
        state: status.state as i32,
        accepted_manifest_json: status.accepted_manifest_json,
    };
    // Returning success before this sync would let a caller discard evidence
    // whose only acknowledgement existed in the process that just crashed.
    journal
        .record_completion_response(request.clone(), reply.clone())
        .await?;
    Ok(Some(reply))
}
