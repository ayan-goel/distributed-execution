//! Fixture-only journal seeding and delivery; runtime observations are synthetic.
use dispatch_protocol::{
    v1::{Assignment, AttemptState, CompleteAttemptRequest},
    MAX_MESSAGE_BYTES,
};
use dispatch_worker::{
    completion::deliver_pending,
    control::ControlClient,
    journal::{AsyncJournal, Journal, JournalLimits},
};
use prost::Message;
use std::io::{Read, Write};

fn read(path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut raw = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_MESSAGE_BYTES as u64 + 1)
        .read_to_end(&mut raw)?;
    if raw.len() > MAX_MESSAGE_BYTES {
        return Err("fixture input too large".into());
    }
    Ok(raw)
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
    if args.len() != 8 {
        return Err(
            "expected mode, journal, assignment, request, endpoint, CA, certificate, key".into(),
        );
    }
    let assignment = Assignment::decode(read(&args[2])?.as_slice())?;
    let request = CompleteAttemptRequest::decode(read(&args[3])?.as_slice())?;
    let a = request.authority.as_ref().ok_or("authority missing")?;
    let mut journal = Journal::open(
        std::fs::canonicalize(&args[1])?,
        &a.worker_id,
        JournalLimits::default(),
    )?;
    if args[0] == "inspect" {
        let saved = journal
            .load_attempt(&a.attempt_id)?
            .ok_or("attempt missing")?;
        if saved.completion() != Some(&request) {
            return Err("stored completion changed".into());
        }
        let response = saved
            .completion_response()
            .ok_or("completion reply missing")?;
        std::io::stdout()
            .lock()
            .write_all(&response.encode_to_vec())?;
        return Ok(());
    }
    if args[0] != "deliver" && args[0] != "seed" {
        return Err("unknown fixture mode".into());
    }
    journal.persist_assignment(&assignment)?;
    journal.prepare_phase(&a.attempt_id, AttemptState::Starting)?;
    journal.bind_container(&a.attempt_id, &"a".repeat(64))?;
    journal.record_exit(
        &a.attempt_id,
        request.exit_code.ok_or("exit missing")?,
        false,
    )?;
    journal.prepare_phase(&a.attempt_id, AttemptState::Finalizing)?;
    journal.persist_completion(&request)?;
    if args[0] == "seed" {
        return Ok(());
    }
    let journal = AsyncJournal::new(journal);
    let mut client = ControlClient::connect(
        &args[4],
        &read(&args[5])?,
        &read(&args[6])?,
        &read(&args[7])?,
    )
    .await?;
    let reply = deliver_pending(&journal, &mut client, &a.attempt_id)
        .await?
        .ok_or("completion missing")?;
    std::io::stdout().lock().write_all(&reply.encode_to_vec())?;
    Ok(())
}
