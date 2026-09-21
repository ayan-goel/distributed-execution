//! Protocol fixture: maintains real grants but does not launch workloads.
use dispatch_protocol::{v1::ListAssignmentsRequest, MAX_MESSAGE_BYTES};
use dispatch_worker::{control::ControlClient, supervisor::authority_channel};
use prost::Message;
use std::io::Read;

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
    let request = ListAssignmentsRequest::decode(input.as_slice())?;
    let mut client = ControlClient::connect(&args[0], &ca, &cert, &key).await?;
    let page = client.list_assignments(&request).await?;
    if page.assignments.len() != 2 || !page.expired.is_empty() || !page.next_after_job_id.is_empty()
    {
        return Err("expected two complete live assignments".into());
    }
    let mut controllers = Vec::new();
    let mut consumers = Vec::new();
    for grant in page.assignments {
        let identity = grant
            .assignment()
            .authority
            .as_ref()
            .ok_or("missing identity")?;
        let (controller, consumer) = authority_channel(identity.clone(), *grant.authority())?;
        controllers.push(controller);
        consumers.push(consumer);
    }
    tokio::spawn(async move { client.maintain_leases(controllers).await }).await??;
    // Keep consumers alive through renewal so closure cannot end the fixture early.
    drop(consumers);
    Ok(())
}
