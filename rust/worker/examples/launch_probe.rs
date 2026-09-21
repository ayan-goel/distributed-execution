//! Combined execution fixture; production acquisition and artifact publication remain separate.
use dispatch_protocol::{
    v1::{AcquireWorkRequest, AttemptState, HeartbeatRequest, RegisterWorkerRequest},
    MAX_MESSAGE_BYTES,
};
use dispatch_worker::{
    control::{ControlClient, WorkOutcome},
    execution::ExecutionSpec,
    journal::{AsyncJournal, Journal, JournalLimits},
    launch::{execute, launch},
    outputs::{collect_outputs, CollectionLimits},
    runtime::{DockerRuntime, PreparedWorkspace, RecoveryRuntime, Runtime},
    supervisor::{authority_channel, supervise_running, StopReason, SupervisionOutcome},
};
use prost::Message;
use std::{
    io::Read,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
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
    let claims = RegisterWorkerRequest::decode(input.as_slice())?;
    let state = PathBuf::from(std::env::var("DISPATCH_LAUNCH_STATE")?).canonicalize()?;
    let (journal, saved) = tokio::task::spawn_blocking(move || {
        let mut journal = Journal::open(state, &claims.worker_id, JournalLimits::default())?;
        let saved = journal.begin_incarnation(claims)?;
        Ok::<_, dispatch_worker::journal::JournalError>((journal, saved))
    })
    .await??;
    let journal = AsyncJournal::new(journal);
    let mut client = ControlClient::connect(&args[0], &ca, &cert, &key).await?;
    let reply = client.register(saved.registration()).await?;
    let session = reply
        .session
        .clone()
        .ok_or("missing registration session")?;
    journal.record_registration(reply).await?;
    let runtime = DockerRuntime::connect(&std::env::var("DISPATCH_LAUNCH_SOCKET")?).await?;
    runtime
        .check_capacity(
            saved.registration().allocatable.as_ref().unwrap(),
            &saved.registration().labels["architecture"],
        )
        .await?;
    if !runtime.inventory(&session.worker_id).await?.is_empty() {
        return Err("fixture worker has old containers".into());
    }
    client
        .heartbeat(&HeartbeatRequest {
            session: Some(session.clone()),
            request_id: saved.registration().request_id.clone(),
            report_sequence: 1,
            runtime_healthy: true,
            reconciliation_complete: true,
            ..Default::default()
        })
        .await?;
    let WorkOutcome::Assignment(grant) = client
        .acquire(&AcquireWorkRequest {
            session: Some(session.clone()),
            request_id: saved.registration().request_id.clone(),
        })
        .await?
    else {
        return Err("fixture did not acquire work".into());
    };
    let identity = grant
        .assignment()
        .authority
        .clone()
        .ok_or("missing assignment identity")?;
    let work = PathBuf::from(std::env::var("DISPATCH_LAUNCH_WORK")?)
        .canonicalize()?
        .join(&identity.attempt_id);
    let workspace = tokio::task::spawn_blocking(move || {
        std::fs::DirBuilder::new().mode(0o700).create(&work)?;
        for (name, mode) in [("inputs", 0o755), ("outputs", 0o777), ("scratch", 0o777)] {
            let path = work.join(name);
            std::fs::create_dir(&path)?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
        }
        PreparedWorkspace::soft_development(work).map_err(std::io::Error::other)
    })
    .await??;
    let (controller, mut authority) = authority_channel(identity.clone(), *grant.authority())?;
    let mut renewal_client = client.clone();
    let renewal =
        tokio::spawn(async move { renewal_client.maintain_leases(vec![controller]).await });
    if std::env::var("DISPATCH_LAUNCH_MODE").as_deref() == Ok("finalize") {
        let finalizing = execute(
            &runtime,
            &mut client,
            &journal,
            &grant,
            &session,
            &workspace,
            authority,
        )
        .await?;
        let saved = journal
            .load_attempt(identity.attempt_id.clone())
            .await?
            .ok_or("missing journal")?;
        if saved.exit() != Some(finalizing.exit())
            || saved.container_id() != Some(finalizing.handle().id())
            || runtime.inspect(finalizing.handle()).await?.running
            || saved.phase_reports().last().map(|r| r.phase)
                != Some(AttemptState::Finalizing as i32)
        {
            return Err("finalization did not preserve durable runtime evidence".into());
        }
        let execution = ExecutionSpec::from_assignment(grant.assignment())?;
        // Hashing can read large files; keep it off the independent lease task.
        let outputs = tokio::task::spawn_blocking(move || {
            collect_outputs(&workspace, &execution, CollectionLimits::default())
        })
        .await??;
        let outputs: Vec<_> = outputs.iter().map(|output| serde_json::json!({
            "name": output.name(), "size_bytes": output.size_bytes(), "sha256": output.sha256()
        })).collect();
        println!(
            "{}",
            serde_json::json!({
                "attempt_id":identity.attempt_id,"container_id":finalizing.handle().id(),
                "exit_code":finalizing.exit().exit_code,"oom_killed":finalizing.exit().oom_killed,
                "outputs": outputs
            })
        );
        // The fixture ends before artifact publication. Stop its renewal task
        // after dropping the finalization consumer; no live workload remains.
        drop(finalizing);
        renewal.abort();
        let _ = renewal.await;
        return Ok(());
    }
    let handle = launch(
        &runtime,
        &mut client,
        &journal,
        &grant,
        &session,
        &workspace,
        &mut authority,
    )
    .await?;
    let saved = journal
        .load_attempt(identity.attempt_id.clone())
        .await?
        .ok_or("missing journal evidence")?;
    if saved.container_id() != Some(handle.id()) || !runtime.inspect(&handle).await?.running {
        return Err("launch did not preserve a running container binding".into());
    }
    // This fixture explicitly observes and reports RUNNING. Production post-launch
    // phase orchestration is not implied by this direct test sequence.
    let running = journal
        .prepare_phase(identity.attempt_id.clone(), AttemptState::Running)
        .await?;
    if client.report_phase(&running).await?.decision != dispatch_protocol::v1::Decision::Accepted {
        return Err("running phase rejected".into());
    }
    // Phase acknowledgement supplies no lease. This fixture fences the next
    // renewal, so the post-phase refresh must fail before supervision cleans up.
    if !matches!(
        authority.refresh().await,
        Err(StopReason::Rejected(
            dispatch_protocol::v1::Decision::Fenced
        ))
    ) {
        return Err("phase refresh ignored server fencing".into());
    }
    if !matches!(
        supervise_running(&runtime, &handle, authority).await,
        SupervisionOutcome::Stopped {
            reason: StopReason::Rejected(dispatch_protocol::v1::Decision::Fenced),
            confirmed: true
        }
    ) {
        return Err("watchdog failed to stop the fenced container".into());
    }
    renewal.await??;
    if runtime.inspect(&handle).await?.running {
        return Err("container survived fenced cleanup".into());
    }
    println!(
        "{}",
        serde_json::json!({"attempt_id":identity.attempt_id,"container_id":handle.id()})
    );
    Ok(())
}
