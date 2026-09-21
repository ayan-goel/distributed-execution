//! Integration fixture for Rust grant, HTTP transfer, and exact-version registration.
use dispatch_protocol::v1::{CreateUploadRequest, FinalizeUploadRequest};
use dispatch_worker::{control::ControlClient, transfer::TransferClient};
use prost::Message;
use std::io::{Read, Write};

fn bounded(path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((1 << 20) + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1 << 20 {
        return Err("fixture input too large".into());
    }
    Ok(bytes)
}
#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 7 {
        return Err("expected endpoint, CA, cert, key, declaration, file, finalize ID".into());
    }
    let mut client = ControlClient::connect(
        &args[0],
        &bounded(&args[1])?,
        &bounded(&args[2])?,
        &bounded(&args[3])?,
    )
    .await?;
    let request = CreateUploadRequest::decode(bounded(&args[4])?.as_slice())?;
    let grant = match client.create_upload(&request).await {
        Err(e) if e.retryable() => client.create_upload(&request).await?,
        result => result?,
    };
    let object = TransferClient::new(true)?
        .put(&request, &grant, std::fs::File::open(&args[5])?)
        .await?;
    let finalize = FinalizeUploadRequest {
        authority: request.authority,
        request_id: args[6].clone(),
        upload_id: grant.upload_id,
        object: Some(object),
        parts: vec![],
    };
    let result = match client.finalize_upload(&finalize).await {
        Err(e) if e.retryable() => client.finalize_upload(&finalize).await?,
        result => result?,
    };
    if client.finalize_upload(&finalize).await? != result {
        return Err("finalization replay changed evidence".into());
    }
    std::io::stdout()
        .lock()
        .write_all(&result.encode_to_vec())?;
    Ok(())
}
