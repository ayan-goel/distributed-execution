//! Cross-language integration fixture; this does not execute or reconcile containers.
use dispatch_protocol::{
    v1::{HeartbeatRequest, RegisterWorkerRequest},
    MAX_MESSAGE_BYTES,
};
use dispatch_worker::control::ControlClient;
use prost::Message;
use std::io::{Read, Write};

fn read_bounded(reader: impl Read, limit: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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
        return Err("expected endpoint, CA, client certificate, and key paths".into());
    }
    let ca = read_bounded(std::fs::File::open(&args[1])?, 1024 * 1024)?;
    let certificate = read_bounded(std::fs::File::open(&args[2])?, 1024 * 1024)?;
    let key = read_bounded(std::fs::File::open(&args[3])?, 1024 * 1024)?;
    let input = read_bounded(std::io::stdin().lock(), MAX_MESSAGE_BYTES)?;
    let request = RegisterWorkerRequest::decode(input.as_slice())?;
    let mut client = ControlClient::connect(&args[0], &ca, &certificate, &key).await?;
    let registered = client.register(&request).await?;
    let replay = client.register(&request).await?;
    if replay != registered {
        return Err("registration replay changed session".into());
    }
    // The fixture has no runtime inventory, so it must never authorize scheduling
    // or claim that orphaned containers have been stopped.
    let heartbeat = HeartbeatRequest {
        session: registered.session.clone(),
        request_id: request.request_id.clone(),
        report_sequence: 1,
        runtime_healthy: false,
        reconciliation_complete: false,
        ..Default::default()
    };
    let first = client.heartbeat(&heartbeat).await?;
    let replay = client.heartbeat(&heartbeat).await?;
    if first != replay || !first.reconcile {
        return Err("heartbeat replay lost reconciliation".into());
    }
    std::io::stdout()
        .lock()
        .write_all(&registered.encode_to_vec())?;
    Ok(())
}
