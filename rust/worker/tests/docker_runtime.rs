use dispatch_protocol::v1::{Assignment, AttemptAuthority, Resources};
use dispatch_worker::{
    execution::ExecutionSpec,
    lease::{AuthorityWindow, MonoTime},
    runtime::{DockerRuntime, PreparedWorkspace, RecoveryRuntime, Runtime, RuntimeError},
};
use ring::digest::{digest, SHA256};

fn execution(image: &str, command: &[&str]) -> ExecutionSpec {
    let raw = serde_json::to_vec(&serde_json::json!({
        "apiVersion":"dispatch.dev/v1alpha1", "kind":"Job",
        "metadata":{"name":"runtime-test", "project":"research"},
        "spec": {
            "image":image,"command":command,
            "resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},
            "placement":{},"network":"disabled",
            "timeouts":{"startupSeconds":30,"executionSeconds":30,"finalizationSeconds":30},
            "retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},
            "terminationGraceSeconds":1
        }
    }))
    .unwrap();
    let hash = digest(&SHA256, &raw)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    ExecutionSpec::from_assignment(&Assignment {
        image_digest: image.into(),
        argv: command.iter().map(|s| (*s).into()).collect(),
        resources: Some(Resources {
            cpu_millis: 1000,
            memory_bytes: 128 << 20,
            scratch_bytes: 64 << 20,
        }),
        spec_sha256: hash,
        canonical_job_spec_json: raw,
        ..Default::default()
    })
    .unwrap()
}

fn authority() -> AttemptAuthority {
    AttemptAuthority {
        worker_id: std::env::var("DISPATCH_TEST_WORKER_ID").unwrap(),
        session_id: "00000000-0000-0000-0000-000000000002".into(),
        job_id: std::env::var("DISPATCH_TEST_JOB_ID").unwrap(),
        attempt_id: std::env::var("DISPATCH_TEST_ATTEMPT_ID").unwrap(),
        generation: 1,
    }
}
fn lease() -> AuthorityWindow {
    let now = MonoTime::now().unwrap();
    AuthorityWindow::from_grant(now, now, 30_000, 30_000).unwrap()
}

#[tokio::test]
#[ignore = "requires scripts/test-runtime.sh and its isolated Docker fixture"]
async fn real_container_lifecycle_is_bounded_owned_and_constrained() {
    let runtime = DockerRuntime::connect(&std::env::var("DISPATCH_TEST_DOCKER_SOCKET").unwrap())
        .await
        .unwrap();
    let workspace =
        PreparedWorkspace::soft_development(std::env::var("DISPATCH_TEST_WORKSPACE").unwrap())
            .unwrap();
    let constraints = r#"set -eu
test "$(awk '$1=="CapEff:" {print $2}' /proc/self/status)" = 0000000000000000
test "$(awk '$1=="NoNewPrivs:" {print $2}' /proc/self/status)" = 1
test "$(awk '$1=="Seccomp:" {print $2}' /proc/self/status)" = 2
case "$(awk '$2=="/" {print $4}' /proc/mounts)" in ro,*) ;; *) exit 21;; esac
case "$(awk '$2=="/inputs" {print $4}' /proc/mounts)" in ro,*) ;; *) exit 22;; esac
test ! -e /sys/class/net/eth0
if test -r /sys/fs/cgroup/memory.max; then
    test "$(cat /sys/fs/cgroup/memory.max)" = 134217728
    test "$(cat /sys/fs/cgroup/memory.swap.max)" = 0
    test "$(cat /sys/fs/cgroup/pids.max)" = 256
    test "$(cat /sys/fs/cgroup/cpu.max)" = '100000 100000'
else
    test "$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)" = 134217728
    test "$(cat /sys/fs/cgroup/memory/memory.memsw.limit_in_bytes)" = 134217728
    test "$(cat /sys/fs/cgroup/pids/pids.max)" = 256
    test "$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)" = 100000
    test "$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)" = 100000
fi
id -u; printf out; printf err >&2; printf result > /outputs/result; sleep 1
"#;
    let spec = execution(
        &std::env::var("DISPATCH_TEST_IMAGE").unwrap(),
        &["/bin/sh", "-c", constraints],
    );
    let identity = authority();
    let initial_window = lease();
    let (first, concurrent) = tokio::join!(
        runtime.create(&identity, &spec, &workspace, &initial_window),
        runtime.create(&identity, &spec, &workspace, &initial_window)
    );
    let handle = first.unwrap();
    assert_eq!(handle.id(), concurrent.unwrap().id());
    let replay = runtime
        .create(&identity, &spec, &workspace, &lease())
        .await
        .unwrap();
    assert_eq!(handle.id(), replay.id());
    let mut foreign = identity.clone();
    foreign.worker_id = "00000000-0000-0000-0000-000000000099".into();
    assert!(matches!(
        runtime.create(&foreign, &spec, &workspace, &lease()).await,
        Err(RuntimeError::Identity)
    ));
    let changed = execution(&std::env::var("DISPATCH_TEST_IMAGE").unwrap(), &["false"]);
    assert!(matches!(
        runtime
            .create(&identity, &changed, &workspace, &lease())
            .await,
        Err(RuntimeError::Identity)
    ));
    runtime.start(&handle, &lease()).await.unwrap();
    let exit = runtime.wait(&handle).await.unwrap();
    assert_eq!(exit.exit_code, Some(0));
    assert!(!exit.oom_killed);
    assert!(matches!(
        runtime.start(&handle, &lease()).await,
        Err(RuntimeError::Terminal)
    ));
    let logs = runtime.logs(&handle, 65536).await.unwrap();
    assert_eq!(logs.stdout, b"65532\nout");
    assert_eq!(logs.stderr, b"err");
    assert!(!logs.truncated);
    let short = runtime.logs(&handle, 3).await.unwrap();
    assert!(short.truncated);
    assert_eq!(short.stdout.len() + short.stderr.len(), 3);
    assert_eq!(
        std::fs::read(workspace.root().join("outputs/result")).unwrap(),
        b"result"
    );
    runtime.remove(&handle).await.unwrap();
    runtime.remove(&handle).await.unwrap();

    let mut identity = identity;
    identity.attempt_id.replace_range(24..36, "00000000000a");
    let sleeper = execution(
        &std::env::var("DISPATCH_TEST_IMAGE").unwrap(),
        &["/bin/sh", "-c", "sleep 30"],
    );
    let handle = runtime
        .create(&identity, &sleeper, &workspace, &lease())
        .await
        .unwrap();
    runtime.start(&handle, &lease()).await.unwrap();
    assert!(runtime.remove(&handle).await.is_err());
    // An operator can change live resource settings. Launch must reject drift,
    // but cleanup must still be able to stop our correctly labelled container.
    let admin = bollard::Docker::connect_with_socket(
        &std::env::var("DISPATCH_TEST_DOCKER_SOCKET").unwrap(),
        5,
        bollard::API_DEFAULT_VERSION,
    )
    .unwrap()
    .negotiate_version()
    .await
    .unwrap();
    admin
        .update_container(
            handle.id(),
            bollard::models::ContainerUpdateBody {
                cpu_quota: Some(200_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        runtime.start(&handle, &lease()).await,
        Err(RuntimeError::Identity)
    ));
    runtime.kill(&handle).await.unwrap();
    let killed = runtime.wait(&handle).await.unwrap();
    assert_eq!(killed.exit_code, Some(137));
    assert!(!killed.oom_killed);
    runtime.remove(&handle).await.unwrap();

    identity.attempt_id.replace_range(24..36, "00000000000b");
    let handle = runtime
        .create(&identity, &sleeper, &workspace, &lease())
        .await
        .unwrap();
    runtime.start(&handle, &lease()).await.unwrap();
    runtime.stop(&handle, 0).await.unwrap();
    assert!(!runtime.inspect(&handle).await.unwrap().running);
    runtime.remove(&handle).await.unwrap();

    identity.attempt_id.replace_range(24..36, "00000000000c");
    let memory_hog = execution(
        &std::env::var("DISPATCH_TEST_IMAGE").unwrap(),
        &["awk", "BEGIN { for(i=0;;i++) a[i]=i }"],
    );
    let handle = runtime
        .create(&identity, &memory_hog, &workspace, &lease())
        .await
        .unwrap();
    runtime.start(&handle, &lease()).await.unwrap();
    let exhausted = runtime.wait(&handle).await.unwrap();
    assert_eq!(exhausted.exit_code, Some(137));
    assert!(exhausted.oom_killed);
    runtime.remove(&handle).await.unwrap();

    identity.attempt_id.replace_range(24..36, "00000000000d");
    let handle = runtime
        .create(&identity, &sleeper, &workspace, &lease())
        .await
        .unwrap();
    let now = MonoTime::now().unwrap();
    let expired = AuthorityWindow::from_grant(now, now, 30_000, 1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(matches!(
        runtime.start(&handle, &expired).await,
        Err(RuntimeError::Authority)
    ));
    assert_eq!(
        runtime.inspect(&handle).await.unwrap().state,
        dispatch_worker::runtime::ContainerState::Created
    );
    runtime.remove(&handle).await.unwrap();
}

#[tokio::test]
async fn refuses_remote_or_relative_docker_endpoints() {
    for endpoint in [
        "tcp://localhost:2375",
        "relative.sock",
        "https://docker.example",
        "",
    ] {
        assert!(matches!(
            DockerRuntime::connect(endpoint).await,
            Err(RuntimeError::Configuration)
        ));
    }
}

#[tokio::test]
#[ignore = "requires scripts/test-runtime.sh and its isolated Docker fixture"]
async fn fresh_runtime_discovers_and_removes_only_previous_session_containers() {
    let socket = std::env::var("DISPATCH_TEST_DOCKER_SOCKET").unwrap();
    let runtime = DockerRuntime::connect(&socket).await.unwrap();
    let workspace =
        PreparedWorkspace::soft_development(std::env::var("DISPATCH_TEST_WORKSPACE").unwrap())
            .unwrap();
    let spec = execution(
        &std::env::var("DISPATCH_TEST_IMAGE").unwrap(),
        &["sleep", "60"],
    );
    let mut old = authority();
    old.worker_id = old.job_id.clone();
    old.attempt_id.replace_range(24..36, "00000000000d");
    let mut current = old.clone();
    current.session_id = "00000000-0000-0000-0000-000000000003".into();
    current.attempt_id.replace_range(24..36, "00000000000e");
    let mut foreign = old.clone();
    foreign.worker_id = "00000000-0000-0000-0000-000000000099".into();
    foreign.attempt_id.replace_range(24..36, "00000000000f");
    let old_handle = runtime
        .create(&old, &spec, &workspace, &lease())
        .await
        .unwrap();
    let current_handle = runtime
        .create(&current, &spec, &workspace, &lease())
        .await
        .unwrap();
    let foreign_handle = runtime
        .create(&foreign, &spec, &workspace, &lease())
        .await
        .unwrap();
    runtime.start(&old_handle, &lease()).await.unwrap();
    runtime.start(&current_handle, &lease()).await.unwrap();
    runtime.start(&foreign_handle, &lease()).await.unwrap();
    let admin = bollard::Docker::connect_with_socket(&socket, 5, bollard::API_DEFAULT_VERSION)
        .unwrap()
        .negotiate_version()
        .await
        .unwrap();
    admin.pause_container(old_handle.id()).await.unwrap();
    let mut created = old.clone();
    created.attempt_id.replace_range(24..36, "000000000010");
    let created_handle = runtime
        .create(&created, &spec, &workspace, &lease())
        .await
        .unwrap();
    let mut exited = old.clone();
    exited.attempt_id.replace_range(24..36, "000000000011");
    let fast = execution(&std::env::var("DISPATCH_TEST_IMAGE").unwrap(), &["true"]);
    let exited_handle = runtime
        .create(&exited, &fast, &workspace, &lease())
        .await
        .unwrap();
    runtime.start(&exited_handle, &lease()).await.unwrap();
    assert_eq!(
        runtime.wait(&exited_handle).await.unwrap().exit_code,
        Some(0)
    );
    drop(runtime);

    // A new client has no creation handles or journal binding. Recovery must
    // discover labels on the daemon, then restrict cleanup to fenced sessions.
    let replacement = DockerRuntime::connect(&socket).await.unwrap();
    let inventory = replacement.inventory(&old.worker_id).await.unwrap();
    assert_eq!(inventory.len(), 4);
    let previous = inventory
        .iter()
        .find(|c| c.id() == old_handle.id())
        .unwrap();
    assert_eq!(previous.authority(), &old);
    assert_eq!(
        previous.status().state,
        dispatch_worker::runtime::ContainerState::Paused
    );
    assert_eq!(
        inventory
            .iter()
            .find(|c| c.id() == created_handle.id())
            .unwrap()
            .status()
            .state,
        dispatch_worker::runtime::ContainerState::Created
    );
    assert_eq!(
        inventory
            .iter()
            .find(|c| c.id() == exited_handle.id())
            .unwrap()
            .status()
            .exit_code,
        Some(0)
    );
    let retained = inventory
        .iter()
        .find(|c| c.id() == current_handle.id())
        .unwrap();
    assert_eq!(
        replacement
            .remove_previous(retained, &current.session_id)
            .await,
        Err(RuntimeError::Identity)
    );
    replacement
        .remove_previous(previous, &current.session_id)
        .await
        .unwrap();
    for container in &inventory {
        if container.authority().session_id != current.session_id {
            replacement
                .remove_previous(container, &current.session_id)
                .await
                .unwrap();
        }
    }
    replacement
        .remove_previous(previous, &current.session_id)
        .await
        .unwrap();
    let after = replacement.inventory(&old.worker_id).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id(), current_handle.id());
    assert!(replacement.inspect(&current_handle).await.unwrap().running);
    assert!(replacement.inspect(&foreign_handle).await.unwrap().running);
    replacement.stop(&current_handle, 0).await.unwrap();
    replacement.remove(&current_handle).await.unwrap();
    replacement.stop(&foreign_handle, 0).await.unwrap();
    replacement.remove(&foreign_handle).await.unwrap();
}

#[cfg(unix)]
mod faults {
    use super::*;
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixListener,
    };

    struct Fixture {
        root: PathBuf,
        task: tokio::task::JoinHandle<()>,
        creates: Arc<AtomicUsize>,
        starts: Arc<AtomicUsize>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
    impl Fixture {
        fn new(stall_image: bool) -> Self {
            let root = std::env::temp_dir().join(format!(
                "dr-{:x}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&root).unwrap();
            let root = std::fs::canonicalize(root).unwrap();
            for child in ["inputs", "outputs", "scratch"] {
                std::fs::create_dir(root.join(child)).unwrap();
            }
            let listener = UnixListener::bind(root.join("d.sock")).unwrap();
            let creates = Arc::new(AtomicUsize::new(0));
            let starts = Arc::new(AtomicUsize::new(0));
            let create_count = creates.clone();
            let start_count = starts.clone();
            let task = tokio::spawn(async move {
                let mut config = None;
                let mut hidden_lookups = 0;
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        let byte = socket.read_u8().await.unwrap();
                        header.push(byte);
                        assert!(header.len() < 65536);
                    }
                    let header = String::from_utf8(header).unwrap();
                    let first = header.lines().next().unwrap();
                    let path = first.split_whitespace().nth(1).unwrap();
                    let length = header
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    assert!(length < 2 * 1024 * 1024);
                    let mut body = vec![0; length];
                    socket.read_exact(&mut body).await.unwrap();
                    let mut status = 200;
                    let reply = if path.ends_with("/version") {
                        serde_json::json!({"ApiVersion":"1.51"})
                    } else if path.ends_with("/info") {
                        serde_json::json!({"OSType":"linux","MemoryLimit":true,"SwapLimit":true,"CpuCfsPeriod":true,"CpuCfsQuota":true,"PidsLimit":true,"SecurityOptions":["name=seccomp,profile=builtin"]})
                    } else if path.contains("/images/") {
                        if stall_image {
                            std::future::pending::<()>().await;
                        }
                        serde_json::json!({"Id":format!("sha256:{}","c".repeat(64)),"Config":{}})
                    } else if path.contains("/containers/create?") {
                        config = Some(serde_json::from_slice::<serde_json::Value>(&body).unwrap());
                        hidden_lookups = 2;
                        create_count.fetch_add(1, Ordering::SeqCst);
                        // Commit the fake daemon state, then lose the HTTP reply.
                        // Recovery must inspect the same name instead of POSTing again.
                        continue;
                    } else if path.ends_with("/start") {
                        start_count.fetch_add(1, Ordering::SeqCst);
                        status = 204;
                        serde_json::Value::Null
                    } else if hidden_lookups > 0 {
                        hidden_lookups -= 1;
                        status = 404;
                        serde_json::json!({"message":"creation is not visible yet"})
                    } else if let Some(c) = &config {
                        serde_json::json!({"Id":"f".repeat(64),"Image":format!("sha256:{}","c".repeat(64)),"Config":c,"HostConfig":c["HostConfig"],"State":{"Status":if start_count.load(Ordering::SeqCst)==0 {"created"} else {"exited"},"Running":false,"OOMKilled":false,"ExitCode":0}})
                    } else {
                        status = 404;
                        serde_json::json!({"message":"not found"})
                    };
                    let raw = if status == 204 {
                        Vec::new()
                    } else {
                        serde_json::to_vec(&reply).unwrap()
                    };
                    let response=format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",raw.len());
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.write_all(&raw).await.unwrap();
                }
            });
            Self {
                root,
                task,
                creates,
                starts,
            }
        }
        async fn runtime(&self) -> DockerRuntime {
            DockerRuntime::connect(self.root.join("d.sock").to_str().unwrap())
                .await
                .unwrap()
        }
        fn workspace(&self) -> PreparedWorkspace {
            PreparedWorkspace::soft_development(&self.root).unwrap()
        }
    }
    fn identity() -> AttemptAuthority {
        AttemptAuthority {
            worker_id: "00000000-0000-0000-0000-000000000001".into(),
            session_id: "00000000-0000-0000-0000-000000000002".into(),
            job_id: "00000000-0000-0000-0000-000000000003".into(),
            attempt_id: "00000000-0000-0000-0000-000000000004".into(),
            generation: 1,
        }
    }
    #[tokio::test]
    async fn lost_create_reply_recovers_one_container_and_concurrent_starts_do_not_restart_it() {
        let fixture = Fixture::new(false);
        let runtime = fixture.runtime().await;
        let spec = execution(
            &format!("example.org/test@sha256:{}", "a".repeat(64)),
            &["true"],
        );
        let handle = runtime
            .create(&identity(), &spec, &fixture.workspace(), &lease())
            .await
            .unwrap();
        let replay = runtime
            .create(&identity(), &spec, &fixture.workspace(), &lease())
            .await
            .unwrap();
        assert_eq!(handle.id(), replay.id());
        assert_eq!(fixture.creates.load(Ordering::SeqCst), 1);
        let window = lease();
        let (first, second) = tokio::join!(
            runtime.start(&handle, &window),
            runtime.start(&handle, &window)
        );
        assert!(first.is_ok() || second.is_ok());
        assert_eq!(fixture.starts.load(Ordering::SeqCst), 1);
        assert!(first == Err(RuntimeError::Terminal) || second == Err(RuntimeError::Terminal));
    }
    #[tokio::test]
    async fn stalled_daemon_request_is_bounded_by_local_authority() {
        let fixture = Fixture::new(true);
        let runtime = fixture.runtime().await;
        let spec = execution(
            &format!("example.org/test@sha256:{}", "a".repeat(64)),
            &["true"],
        );
        let now = MonoTime::now().unwrap();
        let window = AuthorityWindow::from_grant(now, now, 30_000, 200).unwrap();
        let started = std::time::Instant::now();
        assert!(matches!(
            runtime
                .create(&identity(), &spec, &fixture.workspace(), &window)
                .await,
            Err(RuntimeError::Deadline)
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(fixture.creates.load(Ordering::SeqCst), 0);
    }
}
