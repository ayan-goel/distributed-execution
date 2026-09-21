//! Integration fixture: completion delivery and retries over authenticated RPC.
use dispatch_protocol::{
    v1::{AttemptState, CompleteAttemptRequest, CompleteAttemptResponse, Decision, FailureReason},
    MAX_MESSAGE_BYTES,
};
use dispatch_worker::control::{completion_digest, ClientError, ControlClient};
use prost::Message;
use std::io::{Read, Write};

fn bounded(reader: impl Read, limit: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    reader.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err("fixture input too large".into());
    }
    Ok(bytes)
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        return Err("expected endpoint, CA, certificate and key paths".into());
    }
    let ca = bounded(std::fs::File::open(&args[1])?, 1 << 20)?;
    let cert = bounded(std::fs::File::open(&args[2])?, 1 << 20)?;
    let key = bounded(std::fs::File::open(&args[3])?, 1 << 20)?;
    let input = bounded(std::io::stdin().lock(), MAX_MESSAGE_BYTES)?;
    let request = CompleteAttemptRequest::decode(input.as_slice())?;
    if completion_digest(&request)? != request.payload_sha256 {
        return Err("Go and Rust completion digests differ".into());
    }
    let mut client = ControlClient::connect(&args[0], &ca, &cert, &key).await?;
    let mut changed = request.clone();
    changed.logs_complete = !changed.logs_complete;
    if !matches!(
        client.complete_attempt(&changed).await,
        Err(ClientError::Configuration)
    ) {
        return Err("changed evidence reached the server with an old digest".into());
    }
    // The Go fixture commits the first request, then discards its acknowledgement.
    // Reusing this exact request must recover that result without another event.
    match client.complete_attempt(&request).await {
        Err(ClientError::Rpc(status)) if status.code() == tonic::Code::Unavailable => {}
        _ => return Err("fixture did not lose the first acknowledgement".into()),
    }
    let recovered = client.complete_attempt(&request).await?;
    let expected = match FailureReason::try_from(request.reason)? {
        FailureReason::Unspecified => AttemptState::Succeeded,
        FailureReason::UserCancelled => AttemptState::Cancelled,
        _ => AttemptState::Failed,
    };
    if recovered.decision != Decision::Accepted || recovered.state != expected {
        return Err("retry did not recover accepted completion".into());
    }
    let replay = client.complete_attempt(&request).await?;
    if replay.decision != recovered.decision
        || replay.state != recovered.state
        || replay.accepted_manifest_json != recovered.accepted_manifest_json
    {
        return Err("replay changed accepted result bytes".into());
    }
    changed.payload_sha256 = completion_digest(&changed)?;
    match client.complete_attempt(&changed).await {
        Err(ClientError::Rpc(status)) if status.code() == tonic::Code::AlreadyExists => {}
        _ => return Err("changed evidence replaced accepted completion".into()),
    }
    let mut spoofed = request.clone();
    spoofed
        .authority
        .as_mut()
        .ok_or("authority missing")?
        .worker_id = request.completion_id.clone();
    spoofed.payload_sha256 = completion_digest(&spoofed)?;
    match client.complete_attempt(&spoofed).await {
        Err(ClientError::Rpc(status)) if status.code() == tonic::Code::PermissionDenied => {}
        _ => return Err("completion bypassed authenticated worker binding".into()),
    }
    let mut unknown = request.clone();
    unknown
        .authority
        .as_mut()
        .ok_or("authority missing")?
        .attempt_id = request.completion_id.clone();
    unknown.payload_sha256 = completion_digest(&unknown)?;
    let fenced = client.complete_attempt(&unknown).await?;
    if fenced.decision != Decision::Fenced
        || fenced.state != AttemptState::Unspecified
        || !fenced.accepted_manifest_json.is_empty()
    {
        return Err("unknown attempt received completion authority".into());
    }
    std::io::stdout().lock().write_all(
        &CompleteAttemptResponse {
            decision: recovered.decision as i32,
            state: recovered.state as i32,
            accepted_manifest_json: recovered.accepted_manifest_json,
        }
        .encode_to_vec(),
    )?;
    Ok(())
}
