#![cfg(unix)]

use dispatch_protocol::v1::{Assignment, InputManifest, ObjectVersion, Resources};
use dispatch_worker::{
    dataset_assignment::stage_inputs,
    dataset_cache::CacheError,
    dataset_cache_store::{CacheStore, CacheStoreError},
    dataset_download::DatasetDownloader,
    dataset_staging::{DatasetFile, DatasetManifest},
    execution::ExecutionSpec,
    runtime::PreparedWorkspace,
};
use ring::digest::{digest, SHA256};
use std::{
    fs,
    io::Cursor,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn sha(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn fixture() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "dispatch-cache-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    root
}

fn dataset(name: &str) -> (ObjectVersion, DatasetManifest, Vec<u8>) {
    let bytes = name.as_bytes();
    let mut tar = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "input.txt", Cursor::new(bytes))
        .unwrap();
    let archive = tar.into_inner().unwrap();
    let object = ObjectVersion {
        key: format!("projects/p/datasets/{name}"),
        version_id: "v1".into(),
        size_bytes: archive.len() as u64,
        sha256: sha(&archive),
    };
    let manifest = DatasetManifest {
        format: "tar.v1".into(),
        files: vec![DatasetFile {
            path: "input.txt".into(),
            size_bytes: bytes.len() as u64,
            sha256: sha(bytes),
        }],
    };
    (object, manifest, archive)
}

async fn serve(name: &str, body: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(&body).await.unwrap();
    });
    format!("http://{address}/projects/p/datasets/{name}?versionId=v1")
}

fn expiry() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000
}

#[tokio::test]
async fn reuses_verified_inputs_and_evicts_only_unpinned_lru_entry() {
    let root = fixture();
    let (a, a_manifest, a_bytes) = dataset("a");
    let archive_size = a.size_bytes;
    let cache = CacheStore::new(&root, archive_size * 2, archive_size).unwrap();
    let downloader = DatasetDownloader::new(true).unwrap();
    let a_url = serve("a", a_bytes).await;
    let a_pin = cache
        .prepare(&downloader, &a, &a_url, expiry(), &a_manifest)
        .await
        .unwrap();
    assert_eq!(fs::read(a_pin.path().join("input.txt")).unwrap(), b"a");
    let a_again = cache
        .prepare(&downloader, &a, "", 0, &a_manifest)
        .await
        .unwrap();
    assert_eq!(a_again.path(), a_pin.path());
    drop(a_again);
    let (b, b_manifest, b_bytes) = dataset("b");
    let b_url = serve("b", b_bytes).await;
    let b_pin = cache
        .prepare(&downloader, &b, &b_url, expiry(), &b_manifest)
        .await
        .unwrap();
    let b_path = b_pin.path().to_path_buf();
    let (c, c_manifest, c_bytes) = dataset("c");
    let c_url = serve("c", c_bytes).await;
    assert!(matches!(
        cache
            .prepare(&downloader, &c, &c_url, expiry(), &c_manifest)
            .await,
        Err(CacheStoreError::Policy(CacheError::Capacity))
    ));
    assert!(b_path.exists());
    drop(b_pin);
    let c_pin = cache
        .prepare(&downloader, &c, &c_url, expiry(), &c_manifest)
        .await
        .unwrap();
    assert!(!b_path.exists());
    assert!(a_pin.path().exists());
    assert_eq!(fs::read(c_pin.path().join("input.txt")).unwrap(), b"c");
    drop(c_pin);
    drop(a_pin);
    assert_eq!(cache.used_bytes().unwrap(), archive_size * 2);
    fs::remove_dir_all(&root).unwrap_or_else(|_| {
        for entry in fs::read_dir(&root).unwrap() {
            let path = entry.unwrap().path();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::remove_dir_all(&root).unwrap();
    });
}

#[tokio::test]
async fn corrupt_download_does_not_create_a_cache_entry() {
    let root = fixture();
    let (object, manifest, bytes) = dataset("clean");
    let cache = CacheStore::new(&root, object.size_bytes * 2, object.size_bytes).unwrap();
    let downloader = DatasetDownloader::new(true).unwrap();
    let mut corrupted = bytes.clone();
    corrupted[512] ^= 1;
    let bad_url = serve("clean", corrupted).await;
    assert!(matches!(
        cache
            .prepare(&downloader, &object, &bad_url, expiry(), &manifest)
            .await,
        Err(CacheStoreError::Preparation(_))
    ));
    assert_eq!(cache.used_bytes().unwrap(), 0);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    let url = serve("clean", bytes).await;
    let pin = cache
        .prepare(&downloader, &object, &url, expiry(), &manifest)
        .await
        .unwrap();
    assert_eq!(fs::read(pin.path().join("input.txt")).unwrap(), b"clean");
    drop(pin);
    for entry in fs::read_dir(&root).unwrap() {
        fs::set_permissions(entry.unwrap().path(), fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn assignment_preparation_binds_only_verified_cached_bytes() {
    let root = fixture();
    let cache_root = root.join("cache");
    fs::create_dir(&cache_root).unwrap();
    fs::set_permissions(&cache_root, fs::Permissions::from_mode(0o700)).unwrap();
    let work = root.join("work");
    fs::create_dir(&work).unwrap();
    let work = work.canonicalize().unwrap();
    for child in ["inputs", "outputs", "scratch"] {
        fs::create_dir(work.join(child)).unwrap();
    }
    let (archive, manifest, bytes) = dataset("bound");
    let cache = CacheStore::new(&cache_root, archive.size_bytes * 2, archive.size_bytes).unwrap();
    let downloader = DatasetDownloader::new(true).unwrap();
    let url = serve("bound", bytes).await;
    let image = format!("example.org/test@sha256:{}", "a".repeat(64));
    let raw = serde_json::to_vec(&serde_json::json!({
        "apiVersion":"dispatch.dev/v1alpha1", "kind":"Job", "metadata":{"name":"test", "project":"research"},
        "spec":{"image":image,"command":["true"],"resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},
        "placement":{},"inputs":[{"dataset":"bound","mountPath":"/inputs/bound"}],"network":"disabled",
        "timeouts":{"startupSeconds":30,"executionSeconds":30,"finalizationSeconds":30},
        "retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},"terminationGraceSeconds":1}
    })).unwrap();
    let assignment = Assignment {
        image_digest: image,
        argv: vec!["true".into()],
        resources: Some(Resources {
            cpu_millis: 1000,
            memory_bytes: 128 << 20,
            scratch_bytes: 64 << 20,
        }),
        spec_sha256: sha(&raw),
        canonical_job_spec_json: raw,
        inputs: vec![InputManifest {
            dataset_id: "00000000-0000-0000-0000-000000000005".into(),
            dataset_name: "bound".into(),
            mount_path: "/inputs/bound".into(),
            archive: Some(archive),
            file_manifest_json: serde_json::to_vec(&manifest).unwrap(),
            download_url: url,
            expires_unix_ms: expiry(),
        }],
        ..Default::default()
    };
    let execution = ExecutionSpec::from_assignment(&assignment).unwrap();
    let mut workspace = PreparedWorkspace::soft_development(&work).unwrap();
    let pins = stage_inputs(&assignment, &execution, &downloader, &cache, &mut workspace)
        .await
        .unwrap();
    assert_eq!(pins.len(), 1);
    assert_eq!(
        fs::read(pins[0].path().join("input.txt")).unwrap(),
        b"bound"
    );
    assert!(work.join("inputs/bound").is_dir());
    let rejected_work = root.join("rejected");
    fs::create_dir(&rejected_work).unwrap();
    let rejected_work = rejected_work.canonicalize().unwrap();
    for child in ["inputs", "outputs", "scratch"] {
        fs::create_dir(rejected_work.join(child)).unwrap();
    }
    let mut rejected = assignment.clone();
    rejected.inputs[0].download_url = "http://example.org/other".into();
    let mut rejected_workspace = PreparedWorkspace::soft_development(&rejected_work).unwrap();
    assert!(stage_inputs(
        &rejected,
        &execution,
        &downloader,
        &cache,
        &mut rejected_workspace
    )
    .await
    .is_err());
    assert!(!rejected_work.join("inputs/bound").exists());
    drop(pins);
    for entry in fs::read_dir(&cache_root).unwrap() {
        fs::set_permissions(entry.unwrap().path(), fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}
