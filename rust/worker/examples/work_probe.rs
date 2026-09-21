//! Integration fixture: fetches authority and recovery pages, never runs containers.
use dispatch_protocol::{
    v1::{
        acquire_work_response, AcquireWorkRequest, AcquireWorkResponse, ListAssignmentsRequest,
        RenewLeasesRequest,
    },
    MAX_MESSAGE_BYTES,
};
use dispatch_worker::control::{ControlClient, RenewalOutcome, WorkOutcome};
use prost::Message;
use std::{
    collections::HashSet,
    io::{Read, Write},
};

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
    let request = AcquireWorkRequest::decode(input.as_slice())?;
    let mut client = ControlClient::connect(&args[0], &ca, &cert, &key).await?;
    let outcome = match client.acquire(&request).await? {
        WorkOutcome::Assignment(grant) => {
            let replay = client.acquire(&request).await?;
            let WorkOutcome::Assignment(replay) = replay else {
                return Err("assignment replay lost authority".into());
            };
            if grant.assignment().authority != replay.assignment().authority
                || grant.assignment().spec_sha256 != replay.assignment().spec_sha256
            {
                return Err("assignment replay changed identity".into());
            }
            let identity = grant
                .assignment()
                .authority
                .as_ref()
                .ok_or("missing authority")?;
            // Request IDs are method-scoped. Reuse this one only for the same
            // renewal payload; two transport calls must persist one batch grant.
            let renewal = RenewLeasesRequest {
                session: request.session.clone(),
                request_id: request.request_id.clone(),
                attempts: vec![identity.clone()],
            };
            for _ in 0..2 {
                let results = client.renew(&renewal).await?;
                match &results[0] {
                    RenewalOutcome::Renewed(lease) if lease.identity() == identity => {
                        lease.authority().remaining()?;
                    }
                    RenewalOutcome::Expired(_) => {
                        return Err("local renewal authority expired".into())
                    }
                    _ => return Err("renewal lost expected authority".into()),
                }
            }
            // Recovery pauses acquisitions and processes one page at a time. Only
            // IDs are retained here to verify traversal, not all execution specs.
            let mut page_request = ListAssignmentsRequest {
                session: request.session.clone(),
                page_size: 1,
                after_job_id: String::new(),
            };
            let mut seen = HashSet::new();
            let mut completed = false;
            for _ in 0..1000 {
                let page = client.list_assignments(&page_request).await?;
                for assignment in page.assignments {
                    assignment.authority().remaining()?;
                    let authority = assignment
                        .assignment()
                        .authority
                        .as_ref()
                        .ok_or("missing authority")?;
                    if !seen.insert(authority.attempt_id.clone()) {
                        return Err("recovery duplicated an assignment".into());
                    }
                }
                if page.next_after_job_id.is_empty() {
                    completed = true;
                    break;
                }
                page_request.after_job_id = page.next_after_job_id;
            }
            let target = &grant
                .assignment()
                .authority
                .as_ref()
                .ok_or("missing authority")?
                .attempt_id;
            if !completed || !seen.contains(target) {
                return Err("recovery missed acquired assignment".into());
            }
            grant.authority().remaining()?;
            acquire_work_response::Outcome::Assignment(Box::new(grant.assignment().clone()))
        }
        WorkOutcome::NoWork(reason) => acquire_work_response::Outcome::NoWork(reason as i32),
        WorkOutcome::Rejected(decision) => {
            acquire_work_response::Outcome::Rejected(decision as i32)
        }
        WorkOutcome::Expired(_) => return Err("local authority expired".into()),
    };
    std::io::stdout().lock().write_all(
        &AcquireWorkResponse {
            outcome: Some(outcome),
        }
        .encode_to_vec(),
    )?;
    Ok(())
}
