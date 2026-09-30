#![cfg(unix)]

use dispatch_protocol::v1::ObjectVersion;
use dispatch_worker::{
    dataset_download::DatasetDownloader,
    dataset_prepare::prepare_dataset,
    dataset_staging::{DatasetFile, DatasetManifest},
};
use ring::digest::{digest, SHA256};
use std::{fs, io::Cursor, os::unix::fs::PermissionsExt};
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

#[tokio::test]
async fn only_exact_version_and_manifest_bytes_reach_published_inputs() {
    let root = std::env::temp_dir().join(format!(
        "dispatch-prepare-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let payload = b"actual dataset";
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(payload.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, "input.txt", Cursor::new(payload))
        .unwrap();
    let bytes = builder.into_inner().unwrap();
    let object = ObjectVersion {
        key: "projects/p/datasets/data".into(),
        version_id: "v1".into(),
        size_bytes: bytes.len() as u64,
        sha256: sha(&bytes),
    };
    let manifest = DatasetManifest {
        format: "tar.v1".into(),
        files: vec![DatasetFile {
            path: "input.txt".into(),
            size_bytes: payload.len() as u64,
            sha256: sha(payload),
        }],
    };
    let downloader = DatasetDownloader::new(true).unwrap();
    let expiry = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000
    };
    let destination = root.join("published");
    let bad = DatasetManifest {
        format: "tar.v1".into(),
        files: vec![DatasetFile {
            path: "input.txt".into(),
            size_bytes: payload.len() as u64,
            sha256: sha(b"wrong dataset"),
        }],
    };
    let url = serve(bytes.clone()).await;
    assert!(
        prepare_dataset(&downloader, &object, &url, expiry(), &bad, &destination)
            .await
            .is_err()
    );
    assert!(!destination.exists());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    let url = serve(bytes).await;
    prepare_dataset(
        &downloader,
        &object,
        &url,
        expiry(),
        &manifest,
        &destination,
    )
    .await
    .unwrap();
    assert_eq!(fs::read(destination.join("input.txt")).unwrap(), payload);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    fs::set_permissions(&destination, fs::Permissions::from_mode(0o700)).unwrap();
    fs::remove_dir_all(root).unwrap();
}

async fn serve(body: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 4096];
        let _ = socket.read(&mut buffer).await.unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    });
    format!("http://{address}/projects/p/datasets/data?versionId=v1")
}
