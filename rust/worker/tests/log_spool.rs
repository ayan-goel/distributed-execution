#![cfg(unix)]

use dispatch_protocol::v1::LogStream;
use dispatch_worker::{
    log_format::LogSegmentWriter,
    log_spool::{LogSpool, SpoolError, SpoolOutcome, MAX_SPOOL_SEGMENTS},
    runtime::PreparedWorkspace,
};
use std::{
    fs,
    io::Read,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
};

const ATTEMPT: &str = "123e4567-e89b-12d3-a456-426614174000";
const OTHER: &str = "123e4567-e89b-12d3-a456-426614174001";

fn workspace(name: &str) -> (PathBuf, PreparedWorkspace) {
    let base = std::env::temp_dir().join(format!(
        "dispatch-spool-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
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
    let prepared = PreparedWorkspace::soft_development(root.canonicalize().unwrap()).unwrap();
    (base, prepared)
}

fn segment(
    attempt: &str,
    sequence: u64,
    payload: &[u8],
) -> dispatch_worker::log_format::LogSegment {
    let mut writer = LogSegmentWriter::new(attempt, LogStream::Stdout, sequence).unwrap();
    writer.append(sequence, 1, payload).unwrap();
    writer.finish().unwrap()
}

#[test]
fn private_spool_enforces_cap_and_reclaims_only_delivered_objects() {
    let (base, prepared) = workspace("cap");
    let first = segment(ATTEMPT, 1, b"a");
    let mut spool = LogSpool::create(&prepared, first.bytes().len() as u64).unwrap();
    let entry = match spool.store(LogStream::Stdout, first).unwrap() {
        SpoolOutcome::Stored(entry) => entry,
        SpoolOutcome::Saturated => panic!("first segment did not fit"),
    };
    assert_eq!(spool.bytes_used(), 69);
    assert_eq!(
        fs::metadata(prepared.root().join("logs"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(entry.path()).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let mut read = Vec::new();
    spool.read(&entry).unwrap().read_to_end(&mut read).unwrap();
    assert_eq!(read.len(), 69);
    assert!(matches!(
        spool
            .store(LogStream::Stdout, segment(ATTEMPT, 2, b"b"))
            .unwrap(),
        SpoolOutcome::Saturated
    ));
    assert_eq!(spool.bytes_used(), 69);
    spool.remove(entry).unwrap();
    assert_eq!(spool.bytes_used(), 0);
    assert!(matches!(
        spool
            .store(LogStream::Stdout, segment(ATTEMPT, 2, b"b"))
            .unwrap(),
        SpoolOutcome::Stored(_)
    ));
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn spool_rejects_foreign_segments_aliases_and_range_reuse() {
    let (base, prepared) = workspace("identity");
    let mut spool = LogSpool::create(&prepared, 1024).unwrap();
    assert_eq!(
        spool
            .store(LogStream::Stdout, segment(OTHER, 1, b"x"))
            .unwrap_err(),
        SpoolError::Identity
    );
    let first = match spool
        .store(LogStream::Stdout, segment(ATTEMPT, 1, b"x"))
        .unwrap()
    {
        SpoolOutcome::Stored(entry) => entry,
        _ => panic!("first segment did not fit"),
    };
    assert_eq!(
        spool
            .store(LogStream::Stdout, segment(ATTEMPT, 1, b"y"))
            .unwrap_err(),
        SpoolError::Conflict
    );
    let mut read = Vec::new();
    spool.read(&first).unwrap().read_to_end(&mut read).unwrap();
    assert_eq!(*read.last().unwrap(), b'x');
    fs::remove_dir_all(base).unwrap();

    let (base, prepared) = workspace("alias");
    std::os::unix::fs::symlink(prepared.root(), prepared.root().join("logs")).unwrap();
    assert_eq!(
        LogSpool::create(&prepared, 1024).unwrap_err(),
        SpoolError::UnsafePath
    );
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn tiny_segments_cannot_exhaust_the_file_identity_budget() {
    let (base, prepared) = workspace("count");
    let mut spool = LogSpool::create(&prepared, 1 << 20).unwrap();
    for sequence in 1..=MAX_SPOOL_SEGMENTS as u64 {
        assert!(matches!(
            spool
                .store(LogStream::Stdout, segment(ATTEMPT, sequence, b"x"))
                .unwrap(),
            SpoolOutcome::Stored(_)
        ));
    }
    assert!(matches!(
        spool
            .store(
                LogStream::Stdout,
                segment(ATTEMPT, MAX_SPOOL_SEGMENTS as u64 + 1, b"x")
            )
            .unwrap(),
        SpoolOutcome::Saturated
    ));
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn failed_publication_never_deletes_an_existing_object_or_pending_alias() {
    let (base, prepared) = workspace("collision");
    let mut spool = LogSpool::create(&prepared, 1024).unwrap();
    let existing = prepared
        .root()
        .join("logs/stdout-0000000000000000001-0000000000000000001.seg");
    fs::write(&existing, b"sentinel").unwrap();
    assert_eq!(
        spool
            .store(LogStream::Stdout, segment(ATTEMPT, 1, b"x"))
            .unwrap_err(),
        SpoolError::Io(std::io::ErrorKind::AlreadyExists)
    );
    assert_eq!(fs::read(&existing).unwrap(), b"sentinel");
    fs::remove_dir_all(base).unwrap();

    let (base, prepared) = workspace("pending");
    let mut spool = LogSpool::create(&prepared, 1024).unwrap();
    let pending = prepared.root().join("logs/.pending");
    std::os::unix::fs::symlink(prepared.root().join("outputs"), &pending).unwrap();
    assert_eq!(
        spool
            .store(LogStream::Stdout, segment(ATTEMPT, 1, b"x"))
            .unwrap_err(),
        SpoolError::Io(std::io::ErrorKind::AlreadyExists)
    );
    assert!(fs::symlink_metadata(&pending)
        .unwrap()
        .file_type()
        .is_symlink());
    fs::remove_dir_all(base).unwrap();
}
