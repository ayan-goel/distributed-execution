//! Docker operations bound to an immutable attempt; the supervisor owns journaling and leases.
use crate::{
    execution::ExecutionSpec,
    lease::AuthorityWindow,
    log_capture::{LogQueue, QUEUE_CHUNKS},
    log_live::{collect_capture_shared, new_shared, LiveLogError, RunningCapture},
    log_spool::MAX_SPOOL_BYTES,
};
use bollard::{
    container::LogOutput, errors::Error as DockerError, models::*, query_parameters::*, Docker,
    API_DEFAULT_VERSION,
};
use dispatch_protocol::v1::AttemptAuthority;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(5);
mod config;
mod recovery;
pub use recovery::{RecoveredContainer, RecoveryRuntime};

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Configuration,
    Unsupported,
    Identity,
    Authority,
    Deadline,
    Transport,
    Daemon(u16),
    Terminal,
    InventoryLimit,
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Docker diagnostics may echo commands, environment, and host paths.
        // Preserve a useful category/status without forwarding those contents.
        write!(f, "container runtime error: {self:?}")
    }
}
impl std::error::Error for RuntimeError {}
impl From<DockerError> for RuntimeError {
    fn from(error: DockerError) -> Self {
        match error {
            DockerError::DockerResponseServerError { status_code, .. } => Self::Daemon(status_code),
            DockerError::RequestTimeoutError => Self::Deadline,
            _ => Self::Transport,
        }
    }
}

#[derive(Debug)]
pub struct PreparedWorkspace {
    root: PathBuf,
    inputs: Vec<InputMount>,
}

#[derive(Debug)]
struct InputMount {
    target: String,
    source: PathBuf,
}
impl PreparedWorkspace {
    /// Explicit development profile: these bind mounts do not enforce a scratch quota.
    pub fn soft_development(root: impl AsRef<Path>) -> Result<Self, RuntimeError> {
        let workspace = Self {
            root: root.as_ref().to_owned(),
            inputs: Vec::new(),
        };
        workspace.validate()?;
        Ok(workspace)
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn bind_input(&mut self, target: &str, source: &Path) -> Result<(), RuntimeError> {
        if self.inputs.len() >= 64
            || target.len() > 4096
            || !target.starts_with("/inputs/")
            || target.contains(['\\', '\0'])
            || target[1..]
                .split('/')
                .any(|part| matches!(part, "" | "." | ".."))
            || self.inputs.iter().any(|previous| {
                previous.target == target
                    || previous
                        .target
                        .strip_prefix(target)
                        .is_some_and(|tail| tail.starts_with('/'))
                    || target
                        .strip_prefix(&previous.target)
                        .is_some_and(|tail| tail.starts_with('/'))
            })
            || !source.is_absolute()
            || source.starts_with(&self.root)
            || std::fs::canonicalize(source).ok().as_deref() != Some(source)
        {
            return Err(RuntimeError::Configuration);
        }
        let metadata =
            std::fs::symlink_metadata(source).map_err(|_| RuntimeError::Configuration)?;
        // INVARIANT: only a sealed worker-owned cache tree can enter Docker's
        // host mount list; job documents supply targets, never host paths.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.mode() & 0o777 != 0o555
            {
                return Err(RuntimeError::Configuration);
            }
        }
        // Docker cannot create a nested mountpoint after /inputs is mounted
        // read-only. Precreate only validated components in this private tree.
        let mut mountpoint = self.root.join("inputs");
        for part in target.trim_start_matches("/inputs/").split('/') {
            mountpoint.push(part);
            match std::fs::create_dir(&mountpoint) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let existing = std::fs::symlink_metadata(&mountpoint)
                        .map_err(|_| RuntimeError::Configuration)?;
                    if !existing.is_dir() || existing.file_type().is_symlink() {
                        return Err(RuntimeError::Configuration);
                    }
                }
                Err(_) => return Err(RuntimeError::Configuration),
            }
        }
        self.inputs.push(InputMount {
            target: target.to_owned(),
            source: source.to_owned(),
        });
        Ok(())
    }
    fn validate(&self) -> Result<(), RuntimeError> {
        // The agent, not the job document, supplies an already prepared private
        // parent. Workloads mount its children and cannot rename their host parent.
        if !self.root.is_absolute()
            || std::fs::canonicalize(&self.root).ok().as_ref() != Some(&self.root)
        {
            return Err(RuntimeError::Configuration);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let parent = std::fs::metadata(&self.root).map_err(|_| RuntimeError::Configuration)?;
            // SAFETY: geteuid reads process credentials and takes no pointers.
            let owner = unsafe { libc::geteuid() };
            if !parent.is_dir() || parent.mode() & 0o022 != 0 || parent.uid() != owner {
                return Err(RuntimeError::Configuration);
            }
        }
        for name in ["inputs", "outputs", "scratch"] {
            let path = self.root.join(name);
            let metadata =
                std::fs::symlink_metadata(&path).map_err(|_| RuntimeError::Configuration)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() || path.to_str().is_none() {
                return Err(RuntimeError::Configuration);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ContainerHandle {
    id: String,
    expected: ContainerCreateBody,
    image_id: String,
    capture: Option<Arc<CaptureSlot>>,
}

struct CaptureSlot(tokio::sync::Mutex<Option<Result<RunningCapture, LiveLogError>>>);
impl fmt::Debug for CaptureSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureSlot").finish_non_exhaustive()
    }
}
impl Drop for CaptureSlot {
    fn drop(&mut self) {
        // A failed launch must not leave a detached Docker reader or private
        // spool writer running after the container cleanup path has started.
        if let Some(Ok(capture)) = self.0.get_mut().take() {
            capture.task.abort();
        }
    }
}
impl ContainerHandle {
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Debug)]
pub struct ContainerStatus {
    pub running: bool,
    pub exit_code: Option<i64>,
    pub oom_killed: bool,
    pub state: ContainerState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerState {
    Created,
    Running,
    Paused,
    Restarting,
    Removing,
    Stopping,
    Exited,
    Dead,
}

#[derive(Debug, Default)]
pub struct ContainerLogs {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub truncated: bool,
}

// Static dispatch allows the deterministic fake runtime to share this contract
// without exposing Docker-specific response types to the future supervisor.
pub trait Runtime {
    type Handle: Send + Sync;
    fn container_id(handle: &Self::Handle) -> &str;
    fn create(
        &self,
        identity: &AttemptAuthority,
        spec: &ExecutionSpec,
        workspace: &PreparedWorkspace,
        authority: &AuthorityWindow,
    ) -> impl Future<Output = Result<Self::Handle, RuntimeError>> + Send;
    fn start(
        &self,
        handle: &Self::Handle,
        authority: &AuthorityWindow,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;
    fn inspect(
        &self,
        handle: &Self::Handle,
    ) -> impl Future<Output = Result<ContainerStatus, RuntimeError>> + Send;
    fn wait(
        &self,
        handle: &Self::Handle,
    ) -> impl Future<Output = Result<ContainerStatus, RuntimeError>> + Send;
    // INVARIANT: Ok confirms the container is no longer running; cancellation
    // completion can release its reservation only after that evidence.
    fn stop(
        &self,
        handle: &Self::Handle,
        grace_seconds: u32,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;
    fn kill(&self, handle: &Self::Handle) -> impl Future<Output = Result<(), RuntimeError>> + Send;
    fn logs(
        &self,
        handle: &Self::Handle,
        max_bytes: usize,
    ) -> impl Future<Output = Result<ContainerLogs, RuntimeError>> + Send;
    fn follow_logs(
        &self,
        _handle: &Self::Handle,
        _queue: LogQueue,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send {
        async { Err(RuntimeError::Unsupported) }
    }
    fn take_capture(
        &self,
        _handle: &Self::Handle,
    ) -> impl Future<Output = Option<Result<RunningCapture, LiveLogError>>> + Send {
        async { None }
    }
    fn remove(
        &self,
        handle: &Self::Handle,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;
}

pub struct DockerRuntime {
    docker: Docker,
    start_gate: tokio::sync::Mutex<()>,
    capture_logs: bool,
}
impl DockerRuntime {
    pub async fn connect_with_logs(socket: &str) -> Result<Self, RuntimeError> {
        let mut runtime = Self::connect(socket).await?;
        runtime.capture_logs = true;
        Ok(runtime)
    }
    pub async fn check_capacity(
        &self,
        resources: &dispatch_protocol::v1::Resources,
        architecture: &str,
    ) -> Result<(), RuntimeError> {
        let info = bounded(RPC_TIMEOUT, self.docker.info()).await?;
        let arch = match info.architecture.as_deref() {
            Some("aarch64" | "arm64") => "arm64",
            Some("x86_64" | "amd64") => "amd64",
            _ => return Err(RuntimeError::Unsupported),
        };
        // Operator ceilings still apply on the server. This local check prevents
        // claims exceeding the actual daemon host or advertising the wrong ISA.
        if resources.cpu_millis == 0
            || resources.memory_bytes == 0
            || arch != architecture
            || info.ncpu.unwrap_or(0) <= 0
            || info.mem_total.unwrap_or(0) <= 0
            || u128::from(resources.cpu_millis) > info.ncpu.unwrap() as u128 * 1000
            || u128::from(resources.memory_bytes) > info.mem_total.unwrap() as u128
        {
            return Err(RuntimeError::Unsupported);
        }
        Ok(())
    }

    pub async fn connect(socket: &str) -> Result<Self, RuntimeError> {
        // Docker grants host-level power. Only an explicit local absolute socket
        // is accepted; environment-based TCP/TLS discovery is never used.
        if !Path::new(socket).is_absolute() || socket.contains('\0') {
            return Err(RuntimeError::Configuration);
        }
        let docker = Docker::connect_with_socket(socket, 305, API_DEFAULT_VERSION)?;
        let docker = bounded(RPC_TIMEOUT, docker.negotiate_version()).await?;
        let info = bounded(RPC_TIMEOUT, docker.info()).await?;
        if info.os_type.as_deref() != Some("linux")
            || [
                info.memory_limit,
                info.swap_limit,
                info.cpu_cfs_period,
                info.cpu_cfs_quota,
                info.pids_limit,
            ]
            .iter()
            .any(|v| *v != Some(true))
            || !info.security_options.as_ref().is_some_and(|options| {
                options
                    .iter()
                    .any(|option| option.starts_with("name=seccomp,"))
            })
        {
            return Err(RuntimeError::Unsupported);
        }
        Ok(Self {
            docker,
            start_gate: tokio::sync::Mutex::new(()),
            capture_logs: false,
        })
    }

    async fn checked(
        &self,
        handle: &ContainerHandle,
        launching: bool,
    ) -> Result<ContainerInspectResponse, RuntimeError> {
        let actual = bounded(RPC_TIMEOUT, self.docker.inspect_container(&handle.id, None)).await?;
        if launching {
            config::verify(&actual, &handle.expected, &handle.image_id)?;
        } else {
            config::verify_identity(&actual, &handle.expected, &handle.image_id)?;
        }
        if actual.id.as_deref() != Some(&handle.id) {
            return Err(RuntimeError::Identity);
        }
        Ok(actual)
    }

    async fn attach_capture(
        &self,
        id: &str,
        workspace: &PreparedWorkspace,
        before_start: bool,
    ) -> Arc<CaptureSlot> {
        let options = AttachContainerOptionsBuilder::default()
            .logs(true)
            .stream(true)
            .stdout(true)
            .stderr(true)
            .build();
        let attached = bounded(RPC_TIMEOUT, self.docker.attach_container(id, Some(options))).await;
        let task = match attached {
            Ok(attached) => match new_shared(workspace, MAX_SPOOL_BYTES) {
                Ok(assembler) => {
                    let mut output = attached.output;
                    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                    let collecting = assembler.clone();
                    let task = tokio::spawn(async move {
                        let (queue, receiver, counts) =
                            LogQueue::bounded(QUEUE_CHUNKS).map_err(|_| LiveLogError::Capture)?;
                        let producer = async move {
                            let _ = ready_tx.send(());
                            while let Some(frame) = output.next().await {
                                let (stream, bytes) = match frame.map_err(RuntimeError::from)? {
                                    LogOutput::StdOut { message } => {
                                        (dispatch_protocol::v1::LogStream::Stdout, message)
                                    }
                                    LogOutput::StdErr { message } => {
                                        (dispatch_protocol::v1::LogStream::Stderr, message)
                                    }
                                    _ => return Err(RuntimeError::Transport),
                                };
                                queue
                                    .push_frame(stream, &bytes)
                                    .map_err(|_| RuntimeError::Transport)?;
                            }
                            Ok(())
                        };
                        let mut captured =
                            collect_capture_shared(collecting, receiver, counts, producer).await?;
                        // Recovered containers that already ran may have rotated
                        // Docker files before this attachment began.
                        captured.complete &= before_start;
                        Ok(captured)
                    });
                    match tokio::time::timeout(RPC_TIMEOUT, ready_rx).await {
                        Ok(Ok(())) => Ok(RunningCapture { assembler, task }),
                        _ => {
                            task.abort();
                            Err(LiveLogError::Capture)
                        }
                    }
                }
                Err(error) => Err(error),
            },
            Err(_) => Err(LiveLogError::Capture),
        };
        Arc::new(CaptureSlot(tokio::sync::Mutex::new(Some(task))))
    }
}

impl Runtime for DockerRuntime {
    type Handle = ContainerHandle;
    fn container_id(handle: &ContainerHandle) -> &str {
        handle.id()
    }
    async fn create(
        &self,
        identity: &AttemptAuthority,
        spec: &ExecutionSpec,
        workspace: &PreparedWorkspace,
        authority: &AuthorityWindow,
    ) -> Result<ContainerHandle, RuntimeError> {
        let _ = remaining(authority)?;
        workspace.validate()?;
        let expected = config::build(identity, spec, workspace)?;
        let name = format!("dispatch-{}", identity.attempt_id);
        let image = bounded(
            remaining(authority)?,
            self.docker.inspect_image(&spec.job().spec.image),
        )
        .await?;
        // Image-declared volumes create additional writable host storage. Reject
        // them until an explicit bounded-volume policy exists.
        if image
            .config
            .as_ref()
            .and_then(|c| c.volumes.as_ref())
            .is_some_and(|v| !v.is_empty())
        {
            return Err(RuntimeError::Unsupported);
        }
        let image_id = image.id.ok_or(RuntimeError::Identity)?;
        let existing = bounded(
            remaining(authority)?,
            self.docker.inspect_container(&name, None),
        )
        .await;
        let actual = match existing {
            Ok(actual) => actual,
            Err(RuntimeError::Daemon(404)) => {
                let options = CreateContainerOptionsBuilder::default().name(&name).build();
                let created = bounded(
                    remaining(authority)?,
                    self.docker
                        .create_container(Some(options), expected.clone()),
                )
                .await;
                // A lost create response may still have committed. Resolve only
                // the deterministic name and validate it before adopting an ID.
                let failure = created.err();
                let may_commit = matches!(
                    failure,
                    None | Some(
                        RuntimeError::Transport
                            | RuntimeError::Deadline
                            | RuntimeError::Daemon(409)
                            | RuntimeError::Daemon(500..=599)
                    )
                );
                let recovered = tokio::time::timeout(RPC_TIMEOUT, async {
                    loop {
                        let found = self
                            .docker
                            .inspect_container(&name, None)
                            .await
                            .map_err(RuntimeError::from);
                        match found {
                            // Docker reserves the name before a concurrent create
                            // becomes inspectable. A 409 plus an immediate 404 is
                            // still ambiguous; wait within one bounded lookup budget.
                            Err(RuntimeError::Daemon(404)) if may_commit => {
                                tokio::time::sleep(Duration::from_millis(20)).await
                            }
                            result => return result,
                        }
                    }
                })
                .await
                .map_err(|_| RuntimeError::Deadline)
                .and_then(|result| result);
                match recovered {
                    Ok(actual) => actual,
                    Err(error) => return Err(failure.unwrap_or(error)),
                }
            }
            Err(error) => return Err(error),
        };
        config::verify(&actual, &expected, &image_id)?;
        let before_start = actual
            .state
            .as_ref()
            .and_then(|state| state.status.as_ref())
            == Some(&ContainerStateStatusEnum::CREATED);
        let id = actual.id.ok_or(RuntimeError::Identity)?;
        if id.len() != 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(RuntimeError::Identity);
        }
        remaining(authority)?;
        let capture = if self.capture_logs {
            // The attach handshake completes while Docker still reports a
            // created container. This prevents daemon log rotation before
            // the worker has a reader for the first workload byte.
            Some(self.attach_capture(&id, workspace, before_start).await)
        } else {
            None
        };
        Ok(ContainerHandle {
            id,
            expected,
            image_id,
            capture,
        })
    }

    async fn start(
        &self,
        handle: &ContainerHandle,
        authority: &AuthorityWindow,
    ) -> Result<(), RuntimeError> {
        // Serialize inspect-and-start within the single agent runtime. Otherwise
        // two callers can both observe CREATED and the second can restart a fast
        // workload that already exited after the first caller started it.
        let _guard = tokio::time::timeout(remaining(authority)?, self.start_gate.lock())
            .await
            .map_err(|_| RuntimeError::Deadline)?;
        remaining(authority)?;
        let status = container_status(self.checked(handle, true).await?)?;
        if status.running {
            remaining(authority)?;
            return Ok(());
        }
        // INVARIANT: start replay must never restart an exited attempt. After an
        // uncertain start, inspection decides whether to supervise or finalize it.
        if status.state != ContainerState::Created {
            return Err(RuntimeError::Terminal);
        }
        bounded(
            remaining(authority)?,
            self.docker.start_container(&handle.id, None),
        )
        .await?;
        remaining(authority)?;
        Ok(())
    }

    async fn inspect(&self, handle: &ContainerHandle) -> Result<ContainerStatus, RuntimeError> {
        container_status(self.checked(handle, false).await?)
    }

    async fn wait(&self, handle: &ContainerHandle) -> Result<ContainerStatus, RuntimeError> {
        self.checked(handle, false).await?;
        let options = WaitContainerOptionsBuilder::default()
            .condition("not-running")
            .build();
        let mut stream = self.docker.wait_container(&handle.id, Some(options));
        // Wait is one bounded observation, not an unbounded supervision task.
        // Nonzero exit codes also arrive as Bollard errors; inspect is authoritative.
        let result = tokio::time::timeout(RPC_TIMEOUT, stream.next())
            .await
            .map_err(|_| RuntimeError::Deadline)?;
        if result.is_none() {
            return Err(RuntimeError::Transport);
        }
        let status = self.inspect(handle).await?;
        if status.exit_code.is_none() {
            return Err(RuntimeError::Transport);
        }
        Ok(status)
    }

    async fn stop(&self, handle: &ContainerHandle, grace_seconds: u32) -> Result<(), RuntimeError> {
        if grace_seconds > 300 {
            return Err(RuntimeError::Configuration);
        }
        if !self.inspect(handle).await?.running {
            return Ok(());
        }
        let options = StopContainerOptionsBuilder::default()
            .signal("SIGTERM")
            .t(grace_seconds as i32)
            .build();
        // Cleanup remains permitted after authority expires. The supervisor must
        // initiate this early enough to include the configured termination grace.
        bounded(
            RPC_TIMEOUT + Duration::from_secs(grace_seconds as u64),
            self.docker.stop_container(&handle.id, Some(options)),
        )
        .await?;
        if self.inspect(handle).await?.running {
            return Err(RuntimeError::Transport);
        }
        Ok(())
    }

    async fn kill(&self, handle: &ContainerHandle) -> Result<(), RuntimeError> {
        if !self.inspect(handle).await?.running {
            return Ok(());
        }
        let options = KillContainerOptionsBuilder::default()
            .signal("SIGKILL")
            .build();
        bounded(
            RPC_TIMEOUT,
            self.docker.kill_container(&handle.id, Some(options)),
        )
        .await
    }

    async fn logs(
        &self,
        handle: &ContainerHandle,
        max_bytes: usize,
    ) -> Result<ContainerLogs, RuntimeError> {
        if max_bytes == 0 || max_bytes > 1024 * 1024 {
            return Err(RuntimeError::Configuration);
        }
        self.checked(handle, false).await?;
        let options = LogsOptionsBuilder::default()
            .follow(false)
            .stdout(true)
            .stderr(true)
            .build();
        let mut stream = self.docker.logs(&handle.id, Some(options));
        tokio::time::timeout(RPC_TIMEOUT, async {
            let mut result = ContainerLogs::default();
            while let Some(frame) = stream.next().await {
                let frame = frame.map_err(RuntimeError::from)?;
                let (stderr, bytes) = match frame {
                    LogOutput::StdOut { message } => (false, message),
                    LogOutput::StdErr { message } => (true, message),
                    _ => return Err(RuntimeError::Transport),
                };
                let available = max_bytes - result.stdout.len() - result.stderr.len();
                let target = if stderr {
                    &mut result.stderr
                } else {
                    &mut result.stdout
                };
                target.extend_from_slice(&bytes[..bytes.len().min(available)]);
                if bytes.len() > available {
                    result.truncated = true;
                    break;
                }
            }
            Ok(result)
        })
        .await
        .map_err(|_| RuntimeError::Deadline)?
    }

    async fn follow_logs(
        &self,
        handle: &ContainerHandle,
        queue: LogQueue,
    ) -> Result<(), RuntimeError> {
        self.checked(handle, false).await?;
        let options = LogsOptionsBuilder::default()
            .follow(true)
            .stdout(true)
            .stderr(true)
            .tail("all")
            .build();
        let mut stream = self.docker.logs(&handle.id, Some(options));
        while let Some(frame) = stream.next().await {
            let (stream, bytes) = match frame.map_err(RuntimeError::from)? {
                LogOutput::StdOut { message } => {
                    (dispatch_protocol::v1::LogStream::Stdout, message)
                }
                LogOutput::StdErr { message } => {
                    (dispatch_protocol::v1::LogStream::Stderr, message)
                }
                _ => return Err(RuntimeError::Transport),
            };
            // Docker's log stream is always drained, including when the bounded
            // handoff queue is saturated; sequence gaps expose dropped chunks.
            queue
                .push_frame(stream, &bytes)
                .map_err(|_| RuntimeError::Transport)?;
        }
        Ok(())
    }

    async fn take_capture(
        &self,
        handle: &ContainerHandle,
    ) -> Option<Result<RunningCapture, LiveLogError>> {
        let slot = handle.capture.as_ref()?;
        slot.0.lock().await.take()
    }

    async fn remove(&self, handle: &ContainerHandle) -> Result<(), RuntimeError> {
        match self.inspect(handle).await {
            Err(RuntimeError::Daemon(404)) => return Ok(()),
            Ok(status) if !status.running => {}
            Ok(_) => return Err(RuntimeError::Configuration),
            Err(error) => return Err(error),
        }
        let options = RemoveContainerOptionsBuilder::default()
            .force(false)
            .v(true)
            .build();
        match bounded(
            RPC_TIMEOUT,
            self.docker.remove_container(&handle.id, Some(options)),
        )
        .await
        {
            Err(RuntimeError::Daemon(404)) => Ok(()),
            result => result,
        }
    }
}

fn container_status(actual: ContainerInspectResponse) -> Result<ContainerStatus, RuntimeError> {
    let state = actual.state.ok_or(RuntimeError::Identity)?;
    let status = match state.status.ok_or(RuntimeError::Identity)? {
        ContainerStateStatusEnum::CREATED => ContainerState::Created,
        ContainerStateStatusEnum::RUNNING => ContainerState::Running,
        ContainerStateStatusEnum::PAUSED => ContainerState::Paused,
        ContainerStateStatusEnum::RESTARTING => ContainerState::Restarting,
        ContainerStateStatusEnum::REMOVING => ContainerState::Removing,
        ContainerStateStatusEnum::STOPPING => ContainerState::Stopping,
        ContainerStateStatusEnum::EXITED => ContainerState::Exited,
        ContainerStateStatusEnum::DEAD => ContainerState::Dead,
        _ => return Err(RuntimeError::Identity),
    };
    Ok(ContainerStatus {
        running: state.running.ok_or(RuntimeError::Identity)?,
        exit_code: if matches!(status, ContainerState::Exited | ContainerState::Dead) {
            state.exit_code
        } else {
            None
        },
        oom_killed: state.oom_killed.ok_or(RuntimeError::Identity)?,
        state: status,
    })
}

fn remaining(authority: &AuthorityWindow) -> Result<Duration, RuntimeError> {
    authority
        .remaining()
        .map(|duration| duration.min(RPC_TIMEOUT))
        .map_err(|_| RuntimeError::Authority)
}
async fn bounded<T>(
    duration: Duration,
    operation: impl Future<Output = Result<T, DockerError>>,
) -> Result<T, RuntimeError> {
    tokio::time::timeout(duration, operation)
        .await
        .map_err(|_| RuntimeError::Deadline)?
        .map_err(RuntimeError::from)
}
