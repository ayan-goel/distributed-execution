#![cfg(unix)]

use dispatch_protocol::v1::LogStream;
use dispatch_worker::{
    log_capture::LogQueue,
    log_live::{collect_capture, collect_capture_shared, new_shared},
    log_spool::MAX_SPOOL_BYTES,
    runtime::PreparedWorkspace,
};
use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn workspace() -> (PathBuf, PreparedWorkspace) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir().join(format!(
        "dispatch-live-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::DirBuilder::new().mode(0o700).create(&base).unwrap();
    let root = base.join("123e4567-e89b-12d3-a456-426614174000");
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    for name in ["inputs", "outputs", "scratch"] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join(name))
            .unwrap();
    }
    (
        base,
        PreparedWorkspace::soft_development(root.canonicalize().unwrap()).unwrap(),
    )
}

#[tokio::test]
async fn collector_drains_both_streams_and_seals_segments() {
    let (base, prepared) = workspace();
    let (queue, receiver, counts) = LogQueue::bounded(256).unwrap();
    let producer = async move {
        queue.push_frame(LogStream::Stdout, b"out").unwrap();
        queue.push_frame(LogStream::Stderr, b"err").unwrap();
        Ok(())
    };
    let captured = collect_capture(&prepared, MAX_SPOOL_BYTES, receiver, counts, producer)
        .await
        .unwrap();
    assert!(captured.complete);
    assert_eq!(captured.counts, [1, 1]);
    let mut assembler = captured.assembler.lock().await;
    assert_eq!(assembler.front().unwrap().stream(), LogStream::Stdout);
    assembler.acknowledge_front().unwrap();
    assert_eq!(assembler.front().unwrap().stream(), LogStream::Stderr);
    drop(assembler);
    fs::remove_dir_all(base).unwrap();
}

#[tokio::test]
async fn collector_keeps_drop_counts_and_marks_follower_failure() {
    let (base, prepared) = workspace();
    let (queue, receiver, counts) = LogQueue::bounded(1).unwrap();
    let producer = async move {
        queue.push_frame(LogStream::Stdout, b"first").unwrap();
        queue.push_frame(LogStream::Stdout, b"lost").unwrap();
        queue.push_frame(LogStream::Stdout, b"lost too").unwrap();
        Err(dispatch_worker::runtime::RuntimeError::Transport)
    };
    let captured = collect_capture(&prepared, MAX_SPOOL_BYTES, receiver, counts, producer)
        .await
        .unwrap();
    assert!(!captured.complete);
    assert_eq!(captured.counts, [3, 0]);
    assert_eq!(
        captured
            .assembler
            .lock()
            .await
            .front()
            .unwrap()
            .last_sequence(),
        1
    );
    fs::remove_dir_all(base).unwrap();
}

#[tokio::test]
async fn sealed_segment_is_visible_before_producer_exits() {
    let (base, prepared) = workspace();
    let assembler = new_shared(&prepared, MAX_SPOOL_BYTES).unwrap();
    let (queue, receiver, counts) = LogQueue::bounded(256).unwrap();
    let (release, paused) = tokio::sync::oneshot::channel::<()>();
    let producer = async move {
        queue.push_frame(LogStream::Stdout, b"running").unwrap();
        let _ = paused.await;
        Ok(())
    };
    let observed = assembler.clone();
    let collecting = tokio::spawn(collect_capture_shared(
        assembler, receiver, counts, producer,
    ));
    tokio::time::sleep(std::time::Duration::from_millis(2150)).await;
    assert_eq!(observed.lock().await.front().unwrap().first_sequence(), 1);
    release.send(()).unwrap();
    assert!(collecting.await.unwrap().unwrap().complete);
    fs::remove_dir_all(base).unwrap();
}
