//! Bounded, version-pinned dataset archive download.

use crate::dataset_staging::{rename_noreplace, StageError};
use dispatch_protocol::v1::ObjectVersion;
use reqwest::Url;
use ring::digest::{Context, SHA256};
use std::{
    fmt,
    fs::{self, OpenOptions},
    net::IpAddr,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;

const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum DownloadError {
    Grant,
    Integrity,
    Transport,
    File,
    AlreadyPublished,
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Signed URLs and transport failures may contain bearer capabilities.
        f.write_str("dataset download failed")
    }
}
impl std::error::Error for DownloadError {}

struct PendingFile(PathBuf);
impl Drop for PendingFile {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.0);
        }
    }
}

pub struct DatasetDownloader {
    client: reqwest::Client,
    allow_loopback_http: bool,
}

impl DatasetDownloader {
    pub fn new(allow_loopback_http: bool) -> Result<Self, DownloadError> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        // Storage receives a short-lived signed capability, never worker mTLS
        // identity. Redirects, proxies, and implicit retries could leak it.
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .http1_only()
            .http1_max_headers(64)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .build()
            .map_err(|_| DownloadError::Grant)?;
        Ok(Self {
            client,
            allow_loopback_http,
        })
    }

    pub async fn download(
        &self,
        object: &ObjectVersion,
        download_url: &str,
        expires_unix_ms: i64,
        destination: &Path,
    ) -> Result<(), DownloadError> {
        let url = self.validate_grant(object, download_url, expires_unix_ms)?;
        let parent = destination.parent().ok_or(DownloadError::File)?;
        let meta = fs::symlink_metadata(parent).map_err(|_| DownloadError::File)?;
        // INVARIANT: temporary bytes stay beneath a worker-owned private root
        // until the exact object version passes length and digest checks.
        if !meta.is_dir()
            || meta.file_type().is_symlink()
            || meta.permissions().mode() & 0o077 != 0
            || meta.uid() != unsafe { libc::geteuid() }
        {
            return Err(DownloadError::File);
        }
        if fs::symlink_metadata(destination).is_ok() {
            return Err(DownloadError::AlreadyPublished);
        }
        let nonce = crate::journal::new_uuid().map_err(|_| DownloadError::File)?;
        let pending = PendingFile(parent.join(format!(".download-{nonce}")));
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&pending.0)
            .map_err(|_| DownloadError::File)?;
        let mut output = tokio::fs::File::from_std(output);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DownloadError::Grant)?
            .as_millis();
        let remaining = (expires_unix_ms as u128)
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .ok_or(DownloadError::Grant)?;
        let transfer = async {
            let mut response = self
                .client
                .get(url)
                .send()
                .await
                .map_err(|_| DownloadError::Transport)?;
            if response.status() != reqwest::StatusCode::OK {
                return Err(DownloadError::Transport);
            }
            if response
                .content_length()
                .is_some_and(|length| length != object.size_bytes)
            {
                return Err(DownloadError::Integrity);
            }
            let mut total = 0u64;
            let mut digest = Context::new(&SHA256);
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| DownloadError::Transport)?
            {
                total = total
                    .checked_add(chunk.len() as u64)
                    .filter(|count| *count <= object.size_bytes)
                    .ok_or(DownloadError::Integrity)?;
                digest.update(&chunk);
                output
                    .write_all(&chunk)
                    .await
                    .map_err(|_| DownloadError::File)?;
            }
            if total != object.size_bytes || hex_sha(digest.finish().as_ref()) != object.sha256 {
                return Err(DownloadError::Integrity);
            }
            output.sync_all().await.map_err(|_| DownloadError::File)?;
            Ok(())
        };
        tokio::time::timeout(
            Duration::from_millis(remaining.min(120_000) as u64),
            transfer,
        )
        .await
        .map_err(|_| DownloadError::Transport)??;
        drop(output);
        fs::set_permissions(&pending.0, fs::Permissions::from_mode(0o400))
            .map_err(|_| DownloadError::File)?;
        rename_noreplace(&pending.0, destination).map_err(|error| match error {
            StageError::AlreadyPublished => DownloadError::AlreadyPublished,
            _ => DownloadError::File,
        })?;
        // The guard sees the old temporary path, which no longer exists.
        fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| DownloadError::File)?;
        Ok(())
    }

    pub(crate) fn validate_grant(
        &self,
        object: &ObjectVersion,
        raw_url: &str,
        expires_unix_ms: i64,
    ) -> Result<Url, DownloadError> {
        let invalid = DownloadError::Grant;
        if object.key.is_empty()
            || object.key.len() > 1024
            || !object.key.split('/').all(|part| {
                !part.is_empty()
                    && part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            })
            || !crate::control::valid_version(&object.version_id)
            || object.size_bytes > MAX_ARCHIVE_BYTES
            || object.sha256.len() != 64
            || !object
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || raw_url.len() > 64 << 10
            || raw_url.contains('\\')
            || raw_url.bytes().any(|b| b.is_ascii_control())
        {
            return Err(invalid);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DownloadError::Grant)?
            .as_millis();
        if expires_unix_ms <= 0
            || expires_unix_ms as u128 <= now
            || expires_unix_ms as u128 > now + 5 * 60_000
        {
            return Err(DownloadError::Grant);
        }
        let url = Url::parse(raw_url).map_err(|_| DownloadError::Grant)?;
        let loopback = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|address| address.is_loopback());
        let versions: Vec<_> = url
            .query_pairs()
            .filter(|(name, _)| name == "versionId")
            .map(|(_, value)| value.into_owned())
            .collect();
        if !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https"
                || self.allow_loopback_http && url.scheme() == "http" && loopback)
            || !url.path().ends_with(&format!("/{}", object.key))
            || versions != [object.version_id.as_str()]
        {
            return Err(DownloadError::Grant);
        }
        Ok(url)
    }
}

fn hex_sha(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(result, "{byte:02x}");
    }
    result
}
