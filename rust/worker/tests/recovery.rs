#![cfg(unix)]

use dispatch_worker::runtime::{DockerRuntime, RecoveryRuntime, RuntimeError};
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

const WORKER: &str = "00000000-0000-0000-0000-000000000001";
const CURRENT: &str = "00000000-0000-0000-0000-000000000009";

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Normal,
    Malformed(u8),
    Overflow,
    Duplicate,
    Foreign,
    Changed,
    Gone,
    LostDelete,
    PhantomDelete,
    Stall,
}

struct Fixture {
    root: PathBuf,
    task: tokio::task::JoinHandle<()>,
    deletes: Arc<AtomicUsize>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    fn new(mode: Mode) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        // Wall-clock resolution can be coarser than concurrent fixture creation.
        // The counter separates same-tick tests; the timestamp separates runs.
        let root = std::env::temp_dir().join(format!(
            "dr-recovery-{:x}-{:x}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let listener = UnixListener::bind(root.join("d.sock")).unwrap();
        let deletes = Arc::new(AtomicUsize::new(0));
        let count = deletes.clone();
        let task = tokio::spawn(async move {
            let mut inspections = 0;
            let mut gone = mode == Mode::Gone;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    header.push(socket.read_u8().await.unwrap());
                    assert!(header.len() < 8192);
                }
                let header = String::from_utf8(header).unwrap();
                let line = header.lines().next().unwrap();
                let path = line.split_whitespace().nth(1).unwrap();
                let mut status = 200;
                let body = if path.ends_with("/version") {
                    serde_json::json!({"ApiVersion":"1.51"})
                } else if path.ends_with("/info") {
                    serde_json::json!({"OSType":"linux","MemoryLimit":true,"SwapLimit":true,"CpuCfsPeriod":true,"CpuCfsQuota":true,"PidsLimit":true,"SecurityOptions":["name=seccomp,profile=builtin"]})
                } else if path.contains("/containers/json?") {
                    assert!(
                        path.contains("all=true")
                            && path.contains("limit=1025")
                            && path.contains(WORKER)
                    );
                    if mode == Mode::Stall {
                        std::future::pending::<()>().await;
                    }
                    let n = match mode {
                        Mode::Overflow => 1025,
                        Mode::Duplicate => 2,
                        _ => 1,
                    };
                    serde_json::json!(vec![serde_json::json!({"Id":"a".repeat(64)}); n])
                } else if line.starts_with("DELETE ") {
                    assert!(path.contains("force=true") && path.contains("v=false"));
                    count.fetch_add(1, Ordering::SeqCst);
                    gone = mode != Mode::PhantomDelete;
                    if mode == Mode::LostDelete {
                        continue;
                    }
                    status = 204;
                    serde_json::Value::Null
                } else if gone {
                    status = 404;
                    serde_json::json!({"message":"not found"})
                } else {
                    inspections += 1;
                    let mut actual = inspection();
                    if let Mode::Malformed(case) = mode {
                        match case {
                            0 => actual["Id"] = "short".into(),
                            1 => actual["Image"] = "not-a-digest".into(),
                            2 => actual["Name"] = "/unrelated".into(),
                            3 => actual["Config"]["Labels"]["dev.dispatch.generation"] = "0".into(),
                            4 => {
                                actual["Config"]["Labels"]["dev.dispatch.generation"] =
                                    "9223372036854775808".into()
                            }
                            5 => {
                                actual["Config"]["Labels"]["dev.dispatch.generation"] = "01".into()
                            }
                            6 => {
                                actual["Config"]["Labels"]["dev.dispatch.session"] =
                                    "../escape".into()
                            }
                            7 => actual["Config"]["Labels"]["dev.dispatch.spec-sha256"] = "".into(),
                            8 => {
                                actual["Config"]["Labels"]["dev.dispatch.scratch-policy"] =
                                    "unknown".into()
                            }
                            _ => actual["Config"]["Labels"].as_object_mut().unwrap().clear(),
                        }
                    }
                    if mode == Mode::Foreign {
                        actual["Config"]["Labels"]["dev.dispatch.worker"] = CURRENT.into();
                    }
                    if mode == Mode::Changed && inspections > 1 {
                        actual["Config"]["Labels"]["dev.dispatch.generation"] = "2".into();
                    }
                    actual
                };
                let body = if status == 204 {
                    Vec::new()
                } else {
                    serde_json::to_vec(&body).unwrap()
                };
                let head = format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        Self {
            root,
            task,
            deletes,
        }
    }
    async fn runtime(&self) -> DockerRuntime {
        DockerRuntime::connect(self.root.join("d.sock").to_str().unwrap())
            .await
            .unwrap()
    }
}

fn inspection() -> serde_json::Value {
    serde_json::json!({"Id":"a".repeat(64),"Image":format!("sha256:{}","b".repeat(64)),
        "Name":"/dispatch-00000000-0000-0000-0000-000000000004",
        "Config":{"Labels":{
            "dev.dispatch.worker":WORKER,"dev.dispatch.session":"00000000-0000-0000-0000-000000000002",
            "dev.dispatch.job":"00000000-0000-0000-0000-000000000003",
            "dev.dispatch.attempt":"00000000-0000-0000-0000-000000000004",
            "dev.dispatch.generation":"1","dev.dispatch.spec-sha256":"c".repeat(64),"dev.dispatch.scratch-policy":"soft-development"}},
        "State":{"Status":"running","Running":true,"OOMKilled":false,"ExitCode":0}})
}

#[tokio::test]
async fn incomplete_or_foreign_inventory_never_becomes_a_cleanup_snapshot() {
    for (mode, expected) in [
        (Mode::Overflow, RuntimeError::InventoryLimit),
        (Mode::Duplicate, RuntimeError::Identity),
        (Mode::Foreign, RuntimeError::Identity),
    ] {
        let fixture = Fixture::new(mode);
        let runtime = fixture.runtime().await;
        assert_eq!(runtime.inventory(WORKER).await.unwrap_err(), expected);
        assert_eq!(fixture.deletes.load(Ordering::SeqCst), 0);
    }
    let fixture = Fixture::new(Mode::Gone);
    assert!(fixture
        .runtime()
        .await
        .inventory(WORKER)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn malformed_identity_never_authorizes_cleanup() {
    for case in 0..10 {
        let fixture = Fixture::new(Mode::Malformed(case));
        let runtime = fixture.runtime().await;
        assert_eq!(
            runtime.inventory(WORKER).await.unwrap_err(),
            RuntimeError::Identity,
            "case {case}"
        );
        assert_eq!(fixture.deletes.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn cleanup_rechecks_ownership_and_requires_observed_absence() {
    for (mode, expected, deletes) in [
        (Mode::Normal, None, 1),
        (Mode::Changed, Some(RuntimeError::Identity), 0),
        (Mode::PhantomDelete, Some(RuntimeError::Transport), 1),
        (Mode::LostDelete, Some(RuntimeError::Transport), 1),
    ] {
        let fixture = Fixture::new(mode);
        let runtime = fixture.runtime().await;
        let inventory = runtime.inventory(WORKER).await.unwrap();
        let previous = &inventory[0];
        assert_eq!(
            runtime
                .remove_previous(previous, &previous.authority().session_id)
                .await,
            Err(RuntimeError::Identity)
        );
        let result = runtime.remove_previous(previous, CURRENT).await;
        assert_eq!(result.err(), expected);
        if mode == Mode::LostDelete || mode == Mode::Normal {
            runtime.remove_previous(previous, CURRENT).await.unwrap();
        }
        assert_eq!(fixture.deletes.load(Ordering::SeqCst), deletes);
    }
}

#[tokio::test]
async fn stalled_inventory_returns_deadline_instead_of_empty_success() {
    let fixture = Fixture::new(Mode::Stall);
    let runtime = fixture.runtime().await;
    let started = std::time::Instant::now();
    assert_eq!(
        runtime.inventory(WORKER).await.unwrap_err(),
        RuntimeError::Deadline
    );
    assert!(started.elapsed() < Duration::from_secs(7));
    assert_eq!(fixture.deletes.load(Ordering::SeqCst), 0);
}
