//! Journaled transfer fixture; runtime observations are seeded, not live execution.
use dispatch_protocol::v1::{Assignment, AttemptState, CreateUploadRequest};
use dispatch_worker::{
    control::ControlClient,
    journal::{AsyncJournal, Journal, JournalLimits},
    transfer::TransferClient,
    upload::{deliver_output, UploadError},
};
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
    if args.len() != 8 {
        return Err(
            "expected endpoint, CA, cert, key, declaration, file, state, assignment".into(),
        );
    }
    let mut client = ControlClient::connect(
        &args[0],
        &bounded(&args[1])?,
        &bounded(&args[2])?,
        &bounded(&args[3])?,
    )
    .await?;
    let template = CreateUploadRequest::decode(bounded(&args[4])?.as_slice())?;
    let assignment = Assignment::decode(bounded(&args[7])?.as_slice())?;
    let authority = assignment
        .authority
        .clone()
        .ok_or("missing fixture authority")?;
    if assignment.authority != template.authority {
        return Err("fixture authority mismatch".into());
    }
    let state = args[6].clone();
    let attempt = authority.attempt_id.clone();
    let journal = tokio::task::spawn_blocking(move || {
        let state = std::path::Path::new(&state).canonicalize()?;
        let mut journal = Journal::open(state, &authority.worker_id, JournalLimits::default())?;
        if journal.load_attempt(&attempt)?.is_none() {
            journal.persist_assignment(&assignment)?;
            journal.prepare_phase(&attempt, AttemptState::Starting)?;
            journal.bind_container(&attempt, &"a".repeat(64))?;
            journal.record_exit(&attempt, 0, false)?;
            journal.prepare_phase(&attempt, AttemptState::Finalizing)?;
        }
        journal.prepare_output(
            &attempt,
            &template.name,
            template.size_bytes,
            &template.sha256,
        )?;
        Ok::<_, dispatch_worker::journal::JournalError>(journal)
    })
    .await??;
    let journal = AsyncJournal::new(journal);
    let transfer = TransferClient::new(true)?;
    for retry in 0..3 {
        let file = match std::fs::File::open(&args[5]) {
            Ok(file) => Some(file),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        match deliver_output(
            &journal,
            &mut client,
            &transfer,
            &authority.attempt_id,
            "result",
            file,
        )
        .await
        {
            Ok(result) => {
                std::io::stdout()
                    .lock()
                    .write_all(&result.encode_to_vec())?;
                return Ok(());
            }
            Err(UploadError::Control(e)) if e.retryable() && retry < 2 => {}
            Err(e) => return Err(e.into()),
        }
    }
    Err("fixture retry budget exhausted".into())
}
