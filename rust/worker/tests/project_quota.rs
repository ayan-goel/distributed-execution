#![cfg(target_os = "linux")]

use dispatch_worker::project_quota::ProjectQuota;
use std::{
    fs,
    io::Write,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::Path,
    process::Command,
};

#[test]
fn invalid_limits_do_not_touch_the_filesystem() {
    for (id, bytes, inodes) in [(0, 8192, 8), (1, 0, 8), (1, 1025, 8), (1, 8192, 0)] {
        let error = ProjectQuota::attach_empty(Path::new("/does-not-exist"), id, bytes, inodes)
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}

#[test]
#[ignore = "requires the fixture's accounting-only ext4 remount"]
fn accounting_without_enforcement_cannot_prepare_strict_scratch() {
    let root = std::env::var_os("DISPATCH_QUOTA_TEST_ROOT").expect("explicit quota fixture root");
    let path = Path::new(&root).join("accounting-only");
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(ProjectQuota::attach_empty(&path, 1004, 8 << 20, 128).is_err());
    fs::remove_dir(path).unwrap();
}

#[test]
#[ignore = "requires a private ext4 project-quota filesystem; see test-project-quota.sh"]
fn ext4_limits_cover_nested_outputs_and_scratch_without_affecting_siblings() {
    let root = std::env::var_os("DISPATCH_QUOTA_TEST_ROOT").expect("explicit quota fixture root");
    let root = Path::new(&root);
    let first = root.join("first");
    let second = root.join("second");
    for path in [&first, &second] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::os::unix::fs::symlink(&first, root.join("alias")).unwrap();
    assert!(ProjectQuota::attach_empty(&root.join("alias"), 1003, 8 << 20, 128).is_err());
    fs::remove_file(root.join("alias")).unwrap();
    let quota = ProjectQuota::attach_empty(&first, 1001, 8 << 20, 128).unwrap();
    let sibling = ProjectQuota::attach_empty(&second, 1002, 8 << 20, 128).unwrap();
    assert!(ProjectQuota::attach_empty(&first, 1001, 16 << 20, 128).is_err());
    let collision = root.join("collision");
    fs::create_dir(&collision).unwrap();
    fs::set_permissions(&collision, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(ProjectQuota::attach_empty(&collision, 1001, 16 << 20, 128).is_err());
    for base in [&first, &second] {
        for name in ["scratch", "outputs"] {
            fs::create_dir(base.join(name)).unwrap();
            fs::set_permissions(base.join(name), fs::Permissions::from_mode(0o777)).unwrap();
        }
        fs::set_permissions(base, fs::Permissions::from_mode(0o711)).unwrap();
    }
    // A root writer can bypass ext4 quotas. Exercise the same unprivileged UID
    // used by Docker jobs, and require EDQUOT rather than host ENOSPC.
    run_writer(&first.join("scratch"), "bytes");
    let usage = quota.usage().unwrap();
    assert!(usage.bytes > 4 << 20 && usage.bytes <= 8 << 20, "{usage:?}");
    run_writer(&first.join("outputs"), "full");
    run_writer(&second.join("outputs"), "small");
    assert!(sibling.usage().unwrap().bytes < 1 << 20);
    quota.verify().unwrap();
    sibling.verify().unwrap();
    let inode_path = root.join("inodes");
    fs::create_dir(&inode_path).unwrap();
    fs::set_permissions(&inode_path, fs::Permissions::from_mode(0o700)).unwrap();
    let inode_quota = ProjectQuota::attach_empty(&inode_path, 1003, 8 << 20, 16).unwrap();
    fs::create_dir(inode_path.join("scratch")).unwrap();
    fs::set_permissions(
        inode_path.join("scratch"),
        fs::Permissions::from_mode(0o777),
    )
    .unwrap();
    fs::set_permissions(&inode_path, fs::Permissions::from_mode(0o711)).unwrap();
    run_writer(&inode_path.join("scratch"), "inodes");
    let usage = inode_quota.usage().unwrap();
    assert_eq!(usage.inodes, 16);
    assert!(usage.bytes < 1 << 20);
    eprintln!("Byte and inode limits returned EDQUOT; sibling writes succeeded.");
    fs::remove_dir_all(first).unwrap();
    fs::remove_dir_all(second).unwrap();
    fs::remove_dir(collision).unwrap();
    fs::remove_dir_all(inode_path).unwrap();
}

fn run_writer(path: &Path, mode: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "quota_writer", "--nocapture"])
        .env("DISPATCH_QUOTA_WRITE_PATH", path)
        .env("DISPATCH_QUOTA_WRITE_MODE", mode)
        .uid(65532)
        .gid(65532)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
}

#[test]
#[ignore = "unprivileged subprocess helper; invoked only by the quota fixture"]
fn quota_writer() {
    let path = std::env::var_os("DISPATCH_QUOTA_WRITE_PATH").expect("writer path");
    let mode = std::env::var("DISPATCH_QUOTA_WRITE_MODE").unwrap();
    if mode == "inodes" {
        for index in 0..32 {
            if let Err(error) = fs::File::create(Path::new(&path).join(format!("file-{index}"))) {
                assert_eq!(error.raw_os_error(), Some(libc::EDQUOT));
                return;
            }
        }
        panic!("project inode hard limit did not reject creation");
    }
    let mut file = fs::File::create(Path::new(&path).join("data")).unwrap();
    let chunk = [0x5a; 64 * 1024];
    if mode == "small" {
        file.write_all(&chunk).unwrap();
        file.sync_all().unwrap();
        return;
    }
    let count = if mode == "full" { 1 } else { 256 };
    for _ in 0..count {
        match file.write_all(&chunk).and_then(|_| file.sync_all()) {
            Ok(()) => {}
            Err(error) => {
                assert_eq!(error.raw_os_error(), Some(libc::EDQUOT));
                return;
            }
        }
    }
    panic!("project hard limit did not reject the write");
}
