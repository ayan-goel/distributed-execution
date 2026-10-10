#![cfg(unix)]

use dispatch_worker::journal::{JournalError, ProjectIds};
use std::{fs, os::unix::fs::DirBuilderExt, path::PathBuf};

const WORKER: &str = "11111111-1111-4111-8111-111111111111";
const OTHER: &str = "22222222-2222-4222-8222-222222222222";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "dispatch-project-ids-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn project_ids_are_exclusive_worker_bound_and_never_reused_after_reopen() {
    let fixture = Fixture::new();
    let mut ids = ProjectIds::initialize(&fixture.0, WORKER).unwrap();
    assert!(matches!(
        ProjectIds::open(&fixture.0, WORKER),
        Err(JournalError::Busy)
    ));
    assert_eq!(ids.reserve().unwrap(), 1);
    assert_eq!(ids.reserve().unwrap(), 2);
    drop(ids);
    assert!(matches!(
        ProjectIds::open(&fixture.0, OTHER),
        Err(JournalError::Identity)
    ));
    assert!(ProjectIds::initialize(&fixture.0, WORKER).is_err());
    let mut ids = ProjectIds::open(&fixture.0, WORKER).unwrap();
    assert_eq!(ids.reserve().unwrap(), 3);
}

#[test]
fn missing_or_corrupt_counter_cannot_silently_reset_ids() {
    let fixture = Fixture::new();
    assert!(ProjectIds::open(&fixture.0, WORKER).is_err());
    drop(ProjectIds::initialize(&fixture.0, WORKER).unwrap());
    let counter = fixture.0.join(".project-ids");
    let saved = fs::read(&counter).unwrap();
    fs::remove_file(&counter).unwrap();
    assert!(ProjectIds::open(&fixture.0, WORKER).is_err());
    assert!(ProjectIds::initialize(&fixture.0, WORKER).is_err());
    fs::write(&counter, saved).unwrap();
    fs::write(&counter, b"corrupt").unwrap();
    assert!(ProjectIds::open(&fixture.0, WORKER).is_err());
}

#[test]
fn allocator_refuses_nonempty_initialization_and_aliased_roots() {
    let fixture = Fixture::new();
    fs::write(fixture.0.join("unrelated"), b"keep").unwrap();
    assert!(ProjectIds::initialize(&fixture.0, WORKER).is_err());
    assert_eq!(fs::read(fixture.0.join("unrelated")).unwrap(), b"keep");
    fs::remove_file(fixture.0.join("unrelated")).unwrap();
    let alias = fixture.0.with_extension("alias");
    std::os::unix::fs::symlink(&fixture.0, &alias).unwrap();
    assert!(ProjectIds::initialize(&alias, WORKER).is_err());
    fs::remove_file(alias).unwrap();
}
