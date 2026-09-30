#![cfg(unix)]

use dispatch_protocol::v1::ObjectVersion;
use dispatch_worker::dataset_download::DatasetDownloader;
use ring::digest::{digest, SHA256};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn sha(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn root() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "dispatch-download-{}-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}

async fn serve(status: &str, body: &'static [u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let status = status.to_owned();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 4096];
        let _ = socket.read(&mut buffer).await.unwrap();
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(body).await.unwrap();
    });
    format!("http://{address}/projects/p/datasets/data?versionId=v1")
}

fn object(bytes: &[u8]) -> ObjectVersion {
    ObjectVersion {
        key: "projects/p/datasets/data".into(),
        version_id: "v1".into(),
        size_bytes: bytes.len() as u64,
        sha256: sha(bytes),
    }
}

fn expiry() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000
}

#[tokio::test]
async fn downloads_only_the_verified_version_into_a_private_file() {
    let root = root();
    let body = b"dataset archive bytes";
    let url = serve("200 OK", body).await;
    let downloader = DatasetDownloader::new(true).unwrap();
    let destination = root.join("archive.tar");
    downloader
        .download(&object(body), &url, expiry(), &destination)
        .await
        .unwrap();
    assert_eq!(fs::read(&destination).unwrap(), body);
    assert_eq!(
        fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
        0o400
    );
    assert!(downloader
        .download(&object(body), &url, expiry(), &destination)
        .await
        .is_err());
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn corruption_and_redirects_leave_no_published_file() {
    let root = root();
    let downloader = DatasetDownloader::new(true).unwrap();
    let destination = root.join("archive.tar");
    let url = serve("200 OK", b"changed!").await;
    assert!(downloader
        .download(&object(b"expected"), &url, expiry(), &destination)
        .await
        .is_err());
    assert!(!destination.exists());
    let url = serve("302 Found", b"").await;
    assert!(downloader
        .download(&object(b""), &url, expiry(), &destination)
        .await
        .is_err());
    assert!(!destination.exists());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn rejects_wrong_version_before_network_io() {
    let root = root();
    let downloader = DatasetDownloader::new(true).unwrap();
    let destination = root.join("archive.tar");
    assert!(downloader
        .download(
            &object(b"x"),
            "http://127.0.0.1:1/projects/p/datasets/data?versionId=v2",
            expiry(),
            &destination
        )
        .await
        .is_err());
    assert!(downloader
        .download(
            &object(b"x"),
            "http://127.0.0.1:1/projects/p/datasets/data?versionId=v1",
            1,
            &destination,
        )
        .await
        .is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    fs::remove_dir_all(root).unwrap();
}
