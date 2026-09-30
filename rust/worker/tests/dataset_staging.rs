#![cfg(unix)]

use dispatch_worker::dataset_staging::{stage_archive, DatasetFile, DatasetManifest};
use ring::digest::{digest, SHA256};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

fn sha(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn fixture() -> (PathBuf, PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "dispatch-stage-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let archive = root.join("dataset.tar");
    (root, archive)
}

fn write_archive(path: &std::path::Path, entries: &[(&str, &[u8], tar::EntryType)]) {
    let file = fs::File::create(path).unwrap();
    let mut builder = tar::Builder::new(file);
    for (name, bytes, kind) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(*kind);
        header.set_size(if kind.is_file() {
            bytes.len() as u64
        } else {
            0
        });
        header.set_mode(0o644);
        header.set_cksum();
        builder.append_data(&mut header, name, *bytes).unwrap();
    }
    builder.finish().unwrap();
}

#[test]
fn verifies_every_entry_before_publishing_a_dataset() {
    let (root, archive) = fixture();
    let bytes = b"verified input";
    write_archive(
        &archive,
        &[("nested/input.txt", bytes, tar::EntryType::Regular)],
    );
    let manifest = DatasetManifest {
        format: "tar.v1".into(),
        files: vec![DatasetFile {
            path: "nested/input.txt".into(),
            size_bytes: bytes.len() as u64,
            sha256: sha(bytes),
        }],
    };
    let destination = root.join("published");
    stage_archive(&archive, &manifest, &destination).unwrap();
    assert_eq!(
        fs::read(destination.join("nested/input.txt")).unwrap(),
        bytes
    );
    assert_eq!(
        fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
        0o555
    );
    assert!(stage_archive(&archive, &manifest, &destination).is_err());
    fs::set_permissions(
        destination.join("nested"),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    fs::set_permissions(&destination, fs::Permissions::from_mode(0o700)).unwrap();
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn corrupt_or_unlisted_archive_never_publishes_partial_inputs() {
    let (root, archive) = fixture();
    let bytes = b"verified input";
    let manifest = DatasetManifest {
        format: "tar.v1".into(),
        files: vec![DatasetFile {
            path: "input.txt".into(),
            size_bytes: bytes.len() as u64,
            sha256: sha(bytes),
        }],
    };
    let destination = root.join("published");
    for entries in [
        vec![(
            "input.txt",
            b"changed input".as_slice(),
            tar::EntryType::Regular,
        )],
        vec![
            ("input.txt", bytes.as_slice(), tar::EntryType::Regular),
            ("extra.txt", b"extra".as_slice(), tar::EntryType::Regular),
        ],
        vec![("input.txt", b"".as_slice(), tar::EntryType::Symlink)],
    ] {
        write_archive(&archive, &entries);
        assert!(stage_archive(&archive, &manifest, &destination).is_err());
        assert!(!destination.exists());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    }
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn rejects_unsafe_manifest_paths_and_missing_archive_entries() {
    let (root, archive) = fixture();
    write_archive(&archive, &[("ok.txt", b"ok", tar::EntryType::Regular)]);
    let destination = root.join("published");
    for path in ["../escape", "/absolute", "a//b", "a/./b", "a/../b", "a\\b"] {
        let manifest = DatasetManifest {
            format: "tar.v1".into(),
            files: vec![DatasetFile {
                path: path.into(),
                size_bytes: 2,
                sha256: sha(b"ok"),
            }],
        };
        assert!(
            stage_archive(&archive, &manifest, &destination).is_err(),
            "{path}"
        );
        assert!(!destination.exists());
    }
    let manifest = DatasetManifest {
        format: "tar.v1".into(),
        files: vec![
            DatasetFile {
                path: "ok.txt".into(),
                size_bytes: 2,
                sha256: sha(b"ok"),
            },
            DatasetFile {
                path: "other.txt".into(),
                size_bytes: 0,
                sha256: sha(b""),
            },
        ],
    };
    assert!(stage_archive(&archive, &manifest, &destination).is_err());
    assert!(!destination.exists());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    fs::remove_dir_all(&root).unwrap();
}
