#![cfg(unix)]

use dispatch_protocol::v1::LogStream;
use dispatch_worker::{
    log_assembler::LogAssembler, log_capture::CapturedChunk, runtime::PreparedWorkspace,
};
use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const ATTEMPT: &str = "123e4567-e89b-12d3-a456-426614174000";

fn workspace() -> (PathBuf, PreparedWorkspace) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir().join(format!(
        "dispatch-assemble-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::DirBuilder::new().mode(0o700).create(&base).unwrap();
    let root = base.join(ATTEMPT);
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
fn chunk(stream: LogStream, sequence: u64, byte: u8) -> CapturedChunk {
    CapturedChunk {
        stream,
        sequence,
        capture_unix_nanos: 1,
        payload: vec![byte],
    }
}

#[test]
fn queue_and_spool_loss_become_registered_segment_gaps() {
    let (base, prepared) = workspace();
    let mut assembler = LogAssembler::new(&prepared, 69).unwrap();
    assembler.ingest(chunk(LogStream::Stdout, 1, b'a')).unwrap();
    assembler.flush_all().unwrap();
    assert_eq!(assembler.front().unwrap().size(), 69);
    assembler.ingest(chunk(LogStream::Stdout, 2, b'b')).unwrap();
    assembler.flush_all().unwrap();
    assert_eq!(assembler.last_stored(LogStream::Stdout).unwrap(), 1);
    assembler.acknowledge_front().unwrap();
    assembler.ingest(chunk(LogStream::Stdout, 4, b'd')).unwrap();
    assembler.flush_all().unwrap();
    let second = assembler.front().unwrap();
    assert_eq!((second.first_sequence(), second.last_sequence()), (2, 4));
    assert_eq!(second.gaps()[0].first_sequence, 2);
    assert_eq!(second.gaps()[0].last_sequence, 3);
    assert_eq!(
        fs::read(second_path(&prepared)).unwrap().len(),
        second.size() as usize
    );
    fs::remove_dir_all(base).unwrap();
}

fn second_path(prepared: &PreparedWorkspace) -> PathBuf {
    prepared
        .root()
        .join("logs/stdout-0000000000000000002-0000000000000000004.seg")
}

#[test]
fn streams_are_independent_and_acknowledgement_frees_only_front() {
    let (base, prepared) = workspace();
    let mut assembler = LogAssembler::new(&prepared, 1024).unwrap();
    assembler.ingest(chunk(LogStream::Stdout, 1, 0xff)).unwrap();
    assembler.ingest(chunk(LogStream::Stderr, 1, 0)).unwrap();
    assembler.flush_all().unwrap();
    assert_eq!(assembler.front().unwrap().stream(), LogStream::Stdout);
    assert!(assembler.read_front().unwrap().is_some());
    assembler.acknowledge_front().unwrap();
    assert_eq!(assembler.front().unwrap().stream(), LogStream::Stderr);
    assert!(assembler.ingest(chunk(LogStream::Stderr, 1, b'x')).is_err());
    fs::remove_dir_all(base).unwrap();
}
