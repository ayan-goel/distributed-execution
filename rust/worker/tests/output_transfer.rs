#![cfg(unix)]

use dispatch_protocol::v1::{AttemptAuthority, CreateUploadRequest, CreateUploadResponse};
use dispatch_worker::transfer::{TransferClient, TransferError};
use std::{
    fs,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn request() -> CreateUploadRequest {
    CreateUploadRequest {
        authority: Some(AttemptAuthority {
            worker_id: "00000000-0000-0000-0000-000000000001".into(),
            session_id: "00000000-0000-0000-0000-000000000002".into(),
            job_id: "00000000-0000-0000-0000-000000000003".into(),
            attempt_id: "00000000-0000-0000-0000-000000000004".into(),
            generation: 1,
        }),
        request_id: "00000000-0000-0000-0000-000000000005".into(),
        name: "result".into(),
        kind: 1,
        size_bytes: 3,
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
        part_count: 1,
    }
}
fn grant(endpoint: &str) -> CreateUploadResponse {
    let key = "projects/00000000-0000-0000-0000-000000000006/jobs/00000000-0000-0000-0000-000000000003/attempts/00000000-0000-0000-0000-000000000004/uploads/00000000-0000-0000-0000-000000000007";
    CreateUploadResponse {
        upload_id: "00000000-0000-0000-0000-000000000007".into(),
        object_key: key.into(),
        upload_url: format!("{endpoint}/bucket/{key}?secret=redacted"),
        required_headers: [("Content-Length".into(), "3".into())].into(),
        expires_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 30_000,
        parts: vec![],
    }
}
fn file(bytes: &[u8]) -> fs::File {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "dispatch-transfer-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    fs::remove_file(path).unwrap();
    file.write_all(bytes).unwrap();
    file
}

#[tokio::test]
async fn uploads_exact_bytes_and_requires_immutable_version() {
    for (header, valid) in [
        ("x-amz-version-id: version-1\r\n", true),
        ("", false),
        ("x-amz-version-id: null\r\n", false),
        ("x-amz-version-id: a\r\nx-amz-version-id: b\r\n", false),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let g = grant(&format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                received.extend_from_slice(&buffer[..n]);
                if let Some(end) = received.windows(4).position(|w| w == b"\r\n\r\n") {
                    if received.len() >= end + 7 {
                        assert_eq!(&received[end + 4..], b"abc");
                        let headers = String::from_utf8_lossy(&received[..end]).to_lowercase();
                        assert!(headers.contains("content-length: 3"));
                        assert!(!headers.contains("authorization:"));
                        break;
                    }
                }
                assert!(received.len() < 8192);
            }
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\n{header}Content-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let result = TransferClient::new(true)
            .unwrap()
            .put(&request(), &g, file(b"abc"))
            .await;
        if valid {
            let object = result.unwrap();
            assert_eq!(object.key, g.object_key);
            assert_eq!(object.version_id, "version-1");
            assert_eq!(object.sha256, request().sha256);
            assert_eq!(object.size_bytes, 3);
        } else {
            assert!(matches!(result, Err(TransferError::Version)));
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn rejects_unsafe_grants_before_network_io() {
    let client = TransferClient::new(false).unwrap();
    let g = grant("http://127.0.0.1:1");
    assert!(matches!(
        client.put(&request(), &g, file(b"abc")).await,
        Err(TransferError::Grant)
    ));
    let client = TransferClient::new(true).unwrap();
    for kind in [
        "expired",
        "foreign",
        "length",
        "authorization",
        "credentials",
        "remote_http",
        "redirect_path",
    ] {
        let mut g = grant("http://127.0.0.1:1");
        match kind {
            "expired" => g.expires_unix_ms = 1,
            "foreign" => g.object_key = g.object_key.replace("000000000004", "000000000009"),
            "length" => {
                g.required_headers
                    .insert("Content-Length".into(), "4".into());
            }
            "authorization" => {
                g.required_headers
                    .insert("Authorization".into(), "private".into());
            }
            "credentials" => {
                g.upload_url = g.upload_url.replace("http://", "http://user:password@")
            }
            "remote_http" => g.upload_url = g.upload_url.replace("127.0.0.1", "example.org"),
            "redirect_path" => g.upload_url = "http://127.0.0.1:1/wrong".into(),
            _ => unreachable!(),
        }
        let error = client.put(&request(), &g, file(b"abc")).await.unwrap_err();
        assert!(
            matches!(error, TransferError::Grant | TransferError::Expired),
            "{kind}: {error}"
        );
        assert!(!format!("{error:?}").contains("secret"));
    }
}

#[tokio::test]
async fn empty_files_are_verified_even_when_http_does_not_poll_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut g = grant(&format!("http://{}", listener.local_addr().unwrap()));
    let mut r = request();
    r.size_bytes = 0;
    r.sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into();
    g.required_headers
        .insert("Content-Length".into(), "0".into());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        let n = stream.read(&mut bytes).await.unwrap();
        assert!(String::from_utf8_lossy(&bytes[..n])
            .to_lowercase()
            .contains("content-length: 0"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nx-amz-version-id: empty-1\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    assert_eq!(
        TransferClient::new(true)
            .unwrap()
            .put(&r, &g, file(b""))
            .await
            .unwrap()
            .size_bytes,
        0
    );
    server.await.unwrap();
}

#[tokio::test]
async fn redirects_are_not_followed_and_stalled_replies_expire() {
    for redirect in [true, false] {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut g = grant(&format!("http://{}", listener.local_addr().unwrap()));
        if !redirect {
            g.expires_unix_ms -= 29_750;
        }
        let location = format!("http://{}", target.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            let _ = stream.read(&mut bytes).await;
            if redirect {
                stream.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
            } else {
                std::future::pending::<()>().await;
            }
        });
        let result = TransferClient::new(true)
            .unwrap()
            .put(&request(), &g, file(b"abc"))
            .await;
        if redirect {
            assert!(matches!(result, Err(TransferError::Status(307))));
        } else {
            assert!(matches!(result, Err(TransferError::Deadline)));
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), target.accept())
                .await
                .is_err()
        );
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn changed_truncated_and_grown_files_never_return_object_evidence() {
    for bytes in [b"xyz".as_slice(), b"ab", b"abcd"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let g = grant(&format!("http://{}", listener.local_addr().unwrap()));
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut data = [0; 4096];
            let _ = stream.read(&mut data).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nx-amz-version-id: forged\r\nContent-Length: 0\r\n\r\n",
                )
                .await;
        });
        let result = TransferClient::new(true)
            .unwrap()
            .put(&request(), &g, file(bytes))
            .await;
        assert!(matches!(
            result,
            Err(TransferError::Integrity | TransferError::Transport)
        ));
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn streams_multiple_chunks_with_exact_content_length() {
    let body = vec![b'z'; (128 << 10) + 17];
    let mut r = request();
    r.size_bytes = body.len() as u64;
    r.sha256 = ring::digest::digest(&ring::digest::SHA256, &body)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut g = grant(&format!("http://{}", listener.local_addr().unwrap()));
    g.required_headers
        .insert("Content-Length".into(), body.len().to_string());
    let size = body.len();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(stream.read_u8().await.unwrap());
            assert!(headers.len() < 8192);
        }
        let mut bytes = [0; 8192];
        let mut received = 0;
        while received < size {
            let count = stream.read(&mut bytes).await.unwrap();
            assert!(count > 0);
            assert!(bytes[..count].iter().all(|b| *b == b'z'));
            received += count;
        }
        assert_eq!(received, size);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nx-amz-version-id: chunks-1\r\nContent-Length: 0\r\n\r\n",
            )
            .await
            .unwrap();
    });
    assert_eq!(
        TransferClient::new(true)
            .unwrap()
            .put(&r, &g, file(&body))
            .await
            .unwrap()
            .size_bytes,
        size as u64
    );
    server.await.unwrap();
}
