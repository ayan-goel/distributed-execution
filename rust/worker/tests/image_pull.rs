#![cfg(unix)]

use dispatch_protocol::v1::{Assignment, Resources};
use dispatch_worker::{
    execution::ExecutionSpec,
    runtime::{DockerRuntime, RuntimeError},
};
use ring::digest::{digest, SHA256};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

const IMAGE: &str = "example.org/approved/job@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn execution() -> ExecutionSpec {
    execution_with_startup(1)
}

fn execution_with_startup(startup: u64) -> ExecutionSpec {
    let raw=serde_json::to_vec(&serde_json::json!({
		"apiVersion":"dispatch.dev/v1alpha1","kind":"Job","metadata":{"name":"pull-test","project":"research"},
		"spec":{"image":IMAGE,"command":["true"],"resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},"placement":{},"network":"disabled",
		"timeouts":{"startupSeconds":startup,"executionSeconds":30,"finalizationSeconds":30},"retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},"terminationGraceSeconds":1}
	})).unwrap();
    ExecutionSpec::from_assignment(&Assignment {
        image_digest: IMAGE.into(),
        argv: vec!["true".into()],
        resources: Some(Resources {
            cpu_millis: 1000,
            memory_bytes: 128 << 20,
            scratch_bytes: 64 << 20,
        }),
        spec_sha256: digest(&SHA256, &raw)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        canonical_job_spec_json: raw,
        ..Default::default()
    })
    .unwrap()
}

#[derive(Clone, Copy)]
enum Mode {
    Cached,
    Missing,
    Denied,
    StreamError,
    AbsentAfterPull,
    Stalled,
    Chatty,
    Overflow,
}

struct Fixture {
    root: PathBuf,
    task: tokio::task::JoinHandle<()>,
    pulls: Arc<AtomicUsize>,
    inspects: Arc<AtomicUsize>,
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
        let root = std::env::temp_dir().join(format!(
            "dispatch-pull-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let listener = UnixListener::bind(root.join("docker.sock")).unwrap();
        let pulls = Arc::new(AtomicUsize::new(0));
        let inspects = Arc::new(AtomicUsize::new(0));
        let (p, i) = (pulls.clone(), inspects.clone());
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    header.push(socket.read_u8().await.unwrap());
                    assert!(header.len() < 65536);
                }
                let header = String::from_utf8(header).unwrap();
                let first = header.lines().next().unwrap();
                let mut status = 200;
                let raw = if first.contains("/version ") {
                    serde_json::to_vec(&serde_json::json!({"ApiVersion":"1.51"})).unwrap()
                } else if first.contains("/info ") {
                    serde_json::to_vec(&serde_json::json!({"OSType":"linux","MemoryLimit":true,"SwapLimit":true,"CpuCfsPeriod":true,"CpuCfsQuota":true,"PidsLimit":true,"SecurityOptions":["name=seccomp,profile=builtin"]})).unwrap()
                } else if first.contains("/images/create?") {
                    assert!(first.starts_with("POST "));
                    let query = first
                        .split_once('?')
                        .unwrap()
                        .1
                        .split_whitespace()
                        .next()
                        .unwrap();
                    let expected = format!(
                        "fromImage=example.org%2Fapproved%2Fjob%40sha256%3A{}",
                        "a".repeat(64)
                    );
                    assert!(query.split('&').any(|field| field == expected));
                    assert!(!first.contains("fromSrc="));
                    assert!(!first.contains("tag="));
                    p.fetch_add(1, Ordering::SeqCst);
                    if matches!(mode, Mode::Stalled) {
                        std::future::pending::<()>().await;
                    }
                    if matches!(mode, Mode::Chatty) {
                        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
                        let raw = b"{\"status\":\"Downloading\"}\n";
                        let chunk = format!(
                            "{:x}\r\n{}\r\n",
                            raw.len(),
                            std::str::from_utf8(raw).unwrap()
                        );
                        loop {
                            if socket.write_all(chunk.as_bytes()).await.is_err() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        }
                        continue;
                    }
                    if matches!(mode, Mode::Overflow) {
                        b"{\"status\":\"Downloading\"}\n".repeat(65_537)
                    } else if matches!(mode, Mode::StreamError) {
                        b"{\"error\":\"private registry credential detail\",\"errorDetail\":{\"message\":\"private registry credential detail\"}}\n".to_vec()
                    } else {
                        b"{\"status\":\"Pull complete\"}\n".to_vec()
                    }
                } else if first.contains("/images/") {
                    assert!(first.contains(&format!("/images/{IMAGE}/json ")));
                    i.fetch_add(1, Ordering::SeqCst);
                    if matches!(mode, Mode::Denied) {
                        status = 500;
                        b"{\"message\":\"daemon unavailable\"}".to_vec()
                    } else if !matches!(mode, Mode::Cached)
                        && (p.load(Ordering::SeqCst) == 0 || matches!(mode, Mode::AbsentAfterPull))
                    {
                        status = 404;
                        b"{\"message\":\"not found\"}".to_vec()
                    } else {
                        serde_json::to_vec(&serde_json::json!({"Id":format!("sha256:{}","c".repeat(64)),"Config":{}})).unwrap()
                    }
                } else {
                    panic!("unexpected Docker operation: {first}")
                };
                let response=format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",raw.len());
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.write_all(&raw).await;
            }
        });
        Self {
            root,
            task,
            pulls,
            inspects,
        }
    }
    async fn runtime(&self) -> DockerRuntime {
        DockerRuntime::connect(self.root.join("docker.sock").to_str().unwrap())
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn missing_image_pulls_exact_digest_and_reinspects_before_success() {
    let fixture = Fixture::new(Mode::Missing);
    let runtime = fixture.runtime().await;
    runtime.prepare_image(&execution()).await.unwrap();
    assert_eq!(fixture.pulls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.inspects.load(Ordering::SeqCst), 2);
    runtime.prepare_image(&execution()).await.unwrap();
    assert_eq!(fixture.pulls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cached_images_skip_pull_and_non_missing_inspection_errors_fail() {
    for mode in [Mode::Cached, Mode::Denied] {
        let fixture = Fixture::new(mode);
        let runtime = fixture.runtime().await;
        let result = runtime.prepare_image(&execution()).await;
        assert_eq!(result.is_ok(), matches!(mode, Mode::Cached));
        assert_eq!(fixture.pulls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn stream_errors_and_missing_post_pull_images_cannot_authorize_launch() {
    for mode in [Mode::StreamError, Mode::AbsentAfterPull] {
        let fixture = Fixture::new(mode);
        let runtime = fixture.runtime().await;
        let error = runtime.prepare_image(&execution()).await.unwrap_err();
        assert!(!error.to_string().contains("private registry"));
        assert_eq!(fixture.pulls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.inspects.load(Ordering::SeqCst),
            if matches!(mode, Mode::StreamError) {
                1
            } else {
                2
            }
        );
    }
}

#[tokio::test]
async fn stalled_pull_obeys_startup_budget_without_creating_a_container() {
    for mode in [Mode::Stalled, Mode::Chatty] {
        let fixture = Fixture::new(mode);
        let runtime = fixture.runtime().await;
        let started = std::time::Instant::now();
        assert_eq!(
            runtime.prepare_image(&execution()).await,
            Err(RuntimeError::Deadline)
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
        assert_eq!(fixture.pulls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn excessive_progress_records_fail_without_claiming_a_prepared_image() {
    let fixture = Fixture::new(Mode::Overflow);
    let runtime = fixture.runtime().await;
    assert_eq!(
        // Keep this record-count assertion independent of parser speed; the
        // separate one-second cases establish the absolute time bound.
        runtime.prepare_image(&execution_with_startup(30)).await,
        Err(RuntimeError::Transport)
    );
    assert_eq!(fixture.inspects.load(Ordering::SeqCst), 1);
}
