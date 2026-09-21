//! Worker registration, startup cleanup, and health reporting. Acquisition follows separately.
use crate::{
    completion::{deliver_pending, DeliveryError},
    control::{canonical_uuid, ClientError, ControlClient},
    journal::{new_uuid, AsyncJournal, Journal, JournalError, JournalLimits},
    runtime::{DockerRuntime, RecoveryRuntime, RuntimeError},
};
use dispatch_protocol::{
    v1::{Decision, ExecutionInventory, HeartbeatRequest, RegisterWorkerRequest, Resources},
    VERSION,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug)]
pub enum AgentError {
    Configuration,
    File,
    Task,
    Journal(JournalError),
    Control(ClientError),
    Runtime(RuntimeError),
}
impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration => f.write_str("invalid worker configuration"),
            Self::File => f.write_str("worker local state or credential file unavailable"),
            Self::Task => f.write_str("worker blocking task failed"),
            Self::Journal(e) => e.fmt(f),
            Self::Control(e) => e.fmt(f),
            Self::Runtime(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for AgentError {}
impl From<JournalError> for AgentError {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}
impl From<ClientError> for AgentError {
    fn from(e: ClientError) -> Self {
        Self::Control(e)
    }
}
impl From<RuntimeError> for AgentError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
impl From<DeliveryError> for AgentError {
    fn from(e: DeliveryError) -> Self {
        match e {
            DeliveryError::Journal(e) => Self::Journal(e),
            DeliveryError::Control(e) => Self::Control(e),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    worker_id: String,
    server_url: String,
    ca_cert: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
    journal_dir: PathBuf,
    workspace_root: PathBuf,
    docker_socket: String,
    cpu_millis: u32,
    memory_mib: u64,
    scratch_mib: u64,
    execution_slots: u32,
    #[serde(deserialize_with = "crate::execution::unique_map")]
    labels: BTreeMap<String, String>,
}
impl AgentConfig {
    pub fn load(path: &Path) -> Result<Self, AgentError> {
        Self::decode(&read_file(path, 65536, false)?)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, AgentError> {
        if bytes.len() > 65536 {
            return Err(AgentError::Configuration);
        }
        let config: Self = serde_json::from_slice(bytes).map_err(|_| AgentError::Configuration)?;
        if !canonical_uuid(&config.worker_id)
            || crate::control::endpoint(&config.server_url).is_err()
            || !(1..=1_024_000).contains(&config.cpu_millis)
            || !(1..=16_777_216).contains(&config.memory_mib)
            || !(1..=1_073_741_824).contains(&config.scratch_mib)
            || !(1..=1000).contains(&config.execution_slots)
            || [
                &config.ca_cert,
                &config.client_cert,
                &config.client_key,
                &config.journal_dir,
                &config.workspace_root,
            ]
            .iter()
            .any(|p| !p.is_absolute())
            || !Path::new(&config.docker_socket).is_absolute()
        {
            return Err(AgentError::Configuration);
        }
        Ok(config)
    }
    fn claims(&self) -> RegisterWorkerRequest {
        RegisterWorkerRequest {
            worker_id: self.worker_id.clone(),
            protocol_version: VERSION,
            allocatable: Some(Resources {
                cpu_millis: self.cpu_millis,
                memory_bytes: self.memory_mib << 20,
                scratch_bytes: self.scratch_mib << 20,
            }),
            execution_slots: self.execution_slots,
            labels: self.labels.clone().into_iter().collect(),
            capabilities: [
                "docker.v1",
                "cpu.hard",
                "memory.hard",
                "pids.hard",
                "scratch.soft",
            ]
            .map(str::to_string)
            .to_vec(),
            ..Default::default()
        }
    }
}

fn read_file(path: &Path, limit: usize, private: bool) -> Result<Vec<u8>, AgentError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| AgentError::File)?;
    let metadata = file.metadata().map_err(|_| AgentError::File)?;
    // Private keys must not be readable by another user or aliased by hard links.
    // SAFETY: geteuid reads process credentials and takes no pointers.
    if !metadata.is_file()
        || (private
            && (metadata.mode() & 0o077 != 0
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.nlink() != 1))
    {
        return Err(AgentError::File);
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| AgentError::File)?;
    if bytes.len() > limit {
        return Err(AgentError::File);
    }
    Ok(bytes)
}

fn workspace(path: &Path) -> Result<PathBuf, AgentError> {
    let path = path.canonicalize().map_err(|_| AgentError::File)?;
    let metadata = std::fs::metadata(&path).map_err(|_| AgentError::File)?;
    // This parent is never mounted into a job. Only later per-attempt children
    // may be writable to workloads; sibling attempts and agent state stay private.
    // SAFETY: geteuid reads process credentials and takes no pointers.
    if !metadata.is_dir()
        || metadata.mode() & 0o777 != 0o700
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(AgentError::File);
    }
    Ok(path)
}

fn disk_pressure(path: &Path, reservation: u64) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return true;
    };
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: both pointers are live; statvfs initializes stats on success.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return true;
    }
    let stats = unsafe { stats.assume_init() };
    let available = u128::from(stats.f_bavail) * u128::from(stats.f_frsize);
    available < u128::from(reservation) + (64 << 20)
}

fn event(name: &str, worker: &str, session: &str) {
    println!(
        "{}",
        serde_json::json!({"event":name,"worker_id":worker,"session_id":session})
    );
}

pub async fn run(config: AgentConfig) -> Result<(), AgentError> {
    let prepared = config.clone();
    let (journal, saved, root, ca, cert, key) = tokio::task::spawn_blocking(move || {
        let root = workspace(&prepared.workspace_root)?;
        let journal_root = prepared
            .journal_dir
            .canonicalize()
            .map_err(|_| AgentError::File)?;
        if root == journal_root
            || root.starts_with(&journal_root)
            || journal_root.starts_with(&root)
        {
            return Err(AgentError::Configuration);
        }
        let ca = read_file(&prepared.ca_cert, 1 << 20, false)?;
        let cert = read_file(&prepared.client_cert, 1 << 20, false)?;
        let key = read_file(&prepared.client_key, 1 << 20, true)?;
        let mut journal =
            Journal::open(journal_root, &prepared.worker_id, JournalLimits::default())?;
        let saved = journal.begin_incarnation(prepared.claims())?;
        Ok::<_, AgentError>((journal, saved, root, ca, cert, key))
    })
    .await
    .map_err(|_| AgentError::Task)??;
    let session_id = saved.registration().requested_session_id.clone();
    event("session_pending", &config.worker_id, &session_id);
    let runtime = DockerRuntime::connect(&config.docker_socket).await?;
    runtime
        .check_capacity(
            saved.registration().allocatable.as_ref().unwrap(),
            &saved.registration().labels["architecture"],
        )
        .await?;
    let mut client = loop {
        match ControlClient::connect(&config.server_url, &ca, &cert, &key).await {
            Ok(client) => break client,
            Err(e) if e.retryable() => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(e) => return Err(e.into()),
        }
    };
    let registration = loop {
        match client.register(saved.registration()).await {
            Ok(reply) => break reply,
            Err(e) if registration_retryable(&e) => {
                tokio::time::sleep(Duration::from_secs(1)).await
            }
            Err(e) => return Err(e.into()),
        }
    };
    // Keep the exclusive journal handle alive for the entire process. The sync
    // runs off Tokio's I/O threads and finishes before any readiness report.
    let journal = AsyncJournal::new(journal);
    journal.record_registration(registration).await?;
    event("registered", &config.worker_id, &session_id);
    let mut completion_client = client.clone();
    // Recovery starts only after registration fences the predecessor. A separate
    // RPC handle lets old completion delivery wait without delaying physical cleanup.
    tokio::try_join!(
        health_loop(&config, &root, &session_id, &runtime, &mut client),
        recover_completions(
            &journal,
            &mut completion_client,
            &config.worker_id,
            &session_id
        ),
    )?;
    Ok(())
}

async fn recover_completions(
    journal: &AsyncJournal,
    client: &mut ControlClient,
    worker: &str,
    session: &str,
) -> Result<(), AgentError> {
    let mut pending: VecDeque<_> = journal.attempt_ids().await?.into();
    while let Some(attempt) = pending.pop_front() {
        let retry = match deliver_pending(journal, client, &attempt).await {
            Ok(Some(reply)) if reply.decision == Decision::StopRequested as i32 => true,
            Ok(Some(reply)) => {
                println!(
                    "{}",
                    serde_json::json!({"event":"completion_recovered","worker_id":worker,
                    "session_id":session,"attempt_id":attempt,"decision":reply.decision,"state":reply.state})
                );
                false
            }
            Ok(None) => false,
            Err(DeliveryError::Control(error)) if error.retryable() => true,
            Err(error) => return Err(error.into()),
        };
        if retry {
            // One outstanding request and a bounded ID queue prevent recovery
            // from flooding the server. Requeue failures so other attempts progress.
            pending.push_back(attempt);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    Ok(())
}

fn registration_retryable(error: &ClientError) -> bool {
    error.retryable()
        || matches!(error, ClientError::Rpc(status) if status.code() == tonic::Code::FailedPrecondition && status.message() == "SESSION_ACTIVE")
}

async fn health_loop(
    config: &AgentConfig,
    root: &Path,
    session: &str,
    runtime: &DockerRuntime,
    client: &mut ControlClient,
) -> Result<(), AgentError> {
    let mut sequence = 0u64;
    let mut pending: Option<HeartbeatRequest> = None;
    let mut announced = "registered";
    loop {
        let inventory = runtime.inventory(&config.worker_id).await;
        let healthy = inventory.is_ok();
        let inventory = inventory.unwrap_or_default();
        let disk_root = root.to_owned();
        let reserved = config.scratch_mib << 20;
        let pressure = tokio::task::spawn_blocking(move || disk_pressure(&disk_root, reserved))
            .await
            .map_err(|_| AgentError::Task)?;
        if pending.is_none() {
            sequence = sequence
                .checked_add(1)
                .filter(|s| *s <= i64::MAX as u64)
                .ok_or(AgentError::Configuration)?;
            pending = Some(HeartbeatRequest {
                session: Some(dispatch_protocol::v1::WorkerSession {
                    worker_id: config.worker_id.clone(),
                    session_id: session.into(),
                }),
                request_id: new_uuid()?,
                report_sequence: sequence,
                runtime_healthy: healthy,
                disk_pressure: pressure,
                reconciliation_complete: healthy && inventory.is_empty(),
                inventory: inventory
                    .iter()
                    .map(|c| ExecutionInventory {
                        authority: Some(c.authority().clone()),
                        container_id: c.id().into(),
                        running: c.status().running,
                    })
                    .collect(),
            });
        }
        let request = pending.as_ref().unwrap();
        let state = match client.heartbeat(request).await {
            Ok(reply) => {
                let ready = request.reconciliation_complete
                    && request.runtime_healthy
                    && !request.disk_pressure
                    && healthy
                    && !pressure
                    && inventory.is_empty()
                    && !reply.reconcile
                    && reply.stop.is_empty();
                pending = None;
                if ready {
                    if reply.drain {
                        "draining"
                    } else {
                        "ready"
                    }
                } else {
                    "reconciling"
                }
            }
            Err(e) if e.retryable() => "control_unavailable",
            Err(e) => return Err(e.into()),
        };
        if state != announced {
            event(state, &config.worker_id, session);
            announced = state;
        }
        // Fencing was established by registration. Cleanup does not wait forever
        // for heartbeat connectivity; retry any pending report with unchanged bytes.
        // Removing at most one container per iteration keeps heartbeats interleaved.
        if let Some(container) = inventory
            .iter()
            .find(|c| c.authority().session_id != session)
        {
            if runtime.remove_previous(container, session).await.is_ok() {
                continue;
            }
        }
        tokio::time::sleep(Duration::from_secs(if pending.is_some() { 1 } else { 5 })).await;
    }
}
