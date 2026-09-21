#![cfg(unix)]

use dispatch_protocol::v1::{Assignment, Resources};
use dispatch_worker::{
    execution::ExecutionSpec,
    outputs::{collect_outputs, CollectionError, CollectionLimits},
    runtime::PreparedWorkspace,
};
use ring::digest::{digest, SHA256};
use std::{
    fs,
    io::Read,
    os::unix::fs::{symlink, DirBuilderExt},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "dispatch-outputs-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        for name in ["inputs", "outputs", "scratch"] {
            fs::create_dir(root.join(name)).unwrap();
        }
        Self(root.canonicalize().unwrap())
    }
    fn workspace(&self) -> PreparedWorkspace {
        PreparedWorkspace::soft_development(&self.0).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn spec(outputs: serde_json::Value) -> ExecutionSpec {
    let image = format!("example.org/eval@sha256:{}", "a".repeat(64));
    let raw = serde_json::to_vec(&serde_json::json!({
        "apiVersion":"dispatch.dev/v1alpha1", "kind":"Job", "metadata":{"name":"outputs","project":"research"},
        "spec":{"image":image,"command":["true"],"resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},
            "placement":{},"network":"disabled","outputs":outputs,"timeouts":{"startupSeconds":30,"executionSeconds":30,"finalizationSeconds":30},
            "retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},"terminationGraceSeconds":1}
    })).unwrap();
    ExecutionSpec::from_assignment(&Assignment {
        spec_sha256: digest(&SHA256, &raw)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        canonical_job_spec_json: raw,
        image_digest: image,
        argv: vec!["true".into()],
        resources: Some(Resources {
            cpu_millis: 1000,
            memory_bytes: 128 << 20,
            scratch_bytes: 64 << 20,
        }),
        ..Default::default()
    })
    .unwrap()
}
fn output(path: &str, required: bool, max: u64) -> serde_json::Value {
    serde_json::json!({"name":"result", "path":path, "required":required, "maxBytes":max})
}

#[test]
fn hashes_declared_files_and_preserves_open_inode_after_path_replacement() {
    let f = Fixture::new();
    fs::create_dir(f.0.join("outputs/nested")).unwrap();
    fs::write(f.0.join("outputs/nested/result"), b"abc").unwrap();
    fs::write(f.0.join("outputs/undeclared"), b"ignored").unwrap();
    let s = spec(serde_json::json!([output(
        "/outputs/nested/result",
        true,
        3
    )]));
    let mut files = collect_outputs(&f.workspace(), &s, CollectionLimits::default()).unwrap();
    assert_eq!(files.len(), 1);
    let file = files.pop().unwrap();
    assert_eq!(file.name(), "result");
    assert_eq!(file.size_bytes(), 3);
    assert_eq!(
        file.sha256(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    fs::rename(f.0.join("outputs/nested"), f.0.join("outputs/old")).unwrap();
    symlink(f.0.join("scratch"), f.0.join("outputs/nested")).unwrap();
    fs::write(f.0.join("scratch/result"), b"secret").unwrap();
    let mut body = Vec::new();
    file.into_file().read_to_end(&mut body).unwrap();
    assert_eq!(body, b"abc");
}

#[test]
fn optional_absence_is_allowed_but_required_absence_and_size_limits_fail() {
    let f = Fixture::new();
    let workspace = f.workspace();
    assert!(collect_outputs(
        &workspace,
        &spec(serde_json::json!([output(
            "/outputs/missing/file",
            false,
            5
        )])),
        CollectionLimits::default()
    )
    .unwrap()
    .is_empty());
    assert!(matches!(
        collect_outputs(
            &workspace,
            &spec(serde_json::json!([output(
                "/outputs/missing/file",
                true,
                5
            )])),
            CollectionLimits::default()
        ),
        Err(CollectionError::Missing)
    ));
    fs::write(f.0.join("outputs/result"), b"12345").unwrap();
    let s = spec(serde_json::json!([output("/outputs/result", true, 4)]));
    assert!(matches!(
        collect_outputs(&workspace, &s, CollectionLimits::default()),
        Err(CollectionError::TooLarge)
    ));
    let s = spec(serde_json::json!([output("/outputs/result", true, 5)]));
    assert!(matches!(
        collect_outputs(
            &workspace,
            &s,
            CollectionLimits {
                max_file_bytes: 4,
                max_total_bytes: 8
            }
        ),
        Err(CollectionError::TooLarge)
    ));
    assert!(matches!(
        collect_outputs(
            &workspace,
            &s,
            CollectionLimits {
                max_file_bytes: 8,
                max_total_bytes: 4
            }
        ),
        Err(CollectionError::Limit)
    ));
    fs::write(f.0.join("outputs/result"), b"").unwrap();
    let files = collect_outputs(&workspace, &s, CollectionLimits::default()).unwrap();
    assert_eq!(files[0].size_bytes(), 0);
    assert_eq!(
        files[0].sha256(),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn rejects_symlinks_hardlinks_directories_and_fifos_without_reading_them() {
    for kind in ["root", "leaf", "parent", "hardlink", "directory", "fifo"] {
        let f = Fixture::new();
        let workspace = f.workspace();
        fs::write(f.0.join("scratch/secret"), b"private").unwrap();
        let mut path = "/outputs/result";
        match kind {
            "root" => {
                fs::remove_dir(f.0.join("outputs")).unwrap();
                symlink(f.0.join("scratch"), f.0.join("outputs")).unwrap();
            }
            "leaf" => symlink(f.0.join("scratch/secret"), f.0.join("outputs/result")).unwrap(),
            "parent" => {
                symlink(f.0.join("scratch"), f.0.join("outputs/nested")).unwrap();
                path = "/outputs/nested/secret";
            }
            "hardlink" => {
                fs::hard_link(f.0.join("scratch/secret"), f.0.join("outputs/result")).unwrap()
            }
            "directory" => fs::create_dir(f.0.join("outputs/result")).unwrap(),
            "fifo" => {
                use std::os::unix::ffi::OsStrExt;
                let path =
                    std::ffi::CString::new(f.0.join("outputs/result").as_os_str().as_bytes())
                        .unwrap();
                // SAFETY: path is a live NUL-terminated fixture path; mkfifo retains no pointers.
                assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
            }
            _ => unreachable!(),
        }
        let s = spec(serde_json::json!([output(path, false, 64)]));
        assert!(
            matches!(
                collect_outputs(&workspace, &s, CollectionLimits::default()),
                Err(CollectionError::Unsafe)
            ),
            "{kind}"
        );
    }
}

#[test]
fn aggregate_limit_counts_all_declared_files() {
    let f = Fixture::new();
    fs::write(f.0.join("outputs/result"), b"abc").unwrap();
    fs::write(f.0.join("outputs/second"), b"def").unwrap();
    let mut second = output("/outputs/second", true, 3);
    second["name"] = "second".into();
    let s = spec(serde_json::json!([
        output("/outputs/result", true, 3),
        second
    ]));
    let workspace = f.workspace();
    assert!(matches!(
        collect_outputs(
            &workspace,
            &s,
            CollectionLimits {
                max_file_bytes: 3,
                max_total_bytes: 5
            }
        ),
        Err(CollectionError::Limit)
    ));
    assert_eq!(
        collect_outputs(
            &workspace,
            &s,
            CollectionLimits {
                max_file_bytes: 3,
                max_total_bytes: 6
            }
        )
        .unwrap()
        .len(),
        2
    );
}
