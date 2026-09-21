//! Bounded single-part uploads using control-plane-issued capabilities.
use crate::control::{valid_version, validate_create, validate_grant};
use dispatch_protocol::v1::{CreateUploadRequest, CreateUploadResponse, ObjectVersion};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH, HOST},
    Url,
};
use ring::digest::{Context, SHA256};
use std::{
    fmt,
    fs::File,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Semaphore,
};

#[derive(Debug, PartialEq, Eq)]
pub enum TransferError {
    Configuration,
    Grant,
    Expired,
    Integrity,
    Transport,
    Deadline,
    Status(u16),
    Version,
}
impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // URLs, storage errors, and headers contain bearer capabilities. Keep
        // both display and Debug limited to categories, never transport sources.
        write!(f, "artifact transfer error: {self:?}")
    }
}
impl std::error::Error for TransferError {}

#[derive(Clone)]
pub struct TransferClient {
    client: reqwest::Client,
    permits: Arc<Semaphore>,
    allow_loopback_http: bool,
}
impl TransferClient {
    pub fn new(allow_loopback_http: bool) -> Result<Self, TransferError> {
        // Reuse the worker's ring backend. Concurrent initialization or a provider
        // installed by the control channel may already have set the process default.
        let _ = rustls::crypto::ring::default_provider().install_default();
        // The storage channel never receives worker mTLS identity. Capabilities
        // must not leak through redirects, environment proxies, or implicit retries.
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .http1_only()
            .http1_max_headers(64)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .build()
            .map_err(|_| TransferError::Configuration)?;
        Ok(Self {
            client,
            permits: Arc::new(Semaphore::new(4)),
            allow_loopback_http,
        })
    }

    pub async fn put(
        &self,
        declaration: &CreateUploadRequest,
        grant: &CreateUploadResponse,
        file: File,
    ) -> Result<ObjectVersion, TransferError> {
        validate_create(declaration).map_err(|_| TransferError::Configuration)?;
        validate_grant(declaration, grant).map_err(|_| TransferError::Grant)?;
        let (url, headers) = self.destination(declaration, grant)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TransferError::Expired)?
            .as_millis();
        let remaining = (grant.expires_unix_ms as u128)
            .checked_sub(now)
            .filter(|n| *n > 0)
            .ok_or(TransferError::Expired)?;
        let budget = Duration::from_millis(remaining.min(30_000) as u64);
        // Bound queue wait and transfer together. Dropping this future cancels
        // local network work; it cannot prove that the remote PUT did not commit.
        tokio::time::timeout(budget, async {
            let _permit = self
                .permits
                .acquire()
                .await
                .map_err(|_| TransferError::Configuration)?;
            let mut file = tokio::fs::File::from_std(file);
            file.rewind().await.map_err(|_| TransferError::Integrity)?;
            let metadata = file
                .metadata()
                .await
                .map_err(|_| TransferError::Integrity)?;
            if !metadata.is_file() || metadata.len() != declaration.size_bytes {
                return Err(TransferError::Integrity);
            }
            let verified = Arc::new(AtomicBool::new(false));
            let state = BodyState {
                file,
                remaining: declaration.size_bytes,
                hash: Context::new(&SHA256),
                expected: declaration.sha256.clone(),
                verified: verified.clone(),
                done: false,
            };
            let body = if declaration.size_bytes == 0 {
                // HTTP may omit polling an empty body. Verify empty-file evidence
                // explicitly so a zero Content-Length cannot skip integrity checks.
                next_chunk(state)
                    .await
                    .map_err(|_| TransferError::Integrity)?;
                reqwest::Body::from(Vec::new())
            } else {
                reqwest::Body::wrap_stream(futures_util::stream::try_unfold(state, next_chunk))
            };
            let response = self
                .client
                .put(url)
                .headers(headers)
                .body(body)
                .send()
                .await
                .map_err(|e| {
                    if e.is_timeout() {
                        TransferError::Deadline
                    } else {
                        TransferError::Transport
                    }
                })?;
            if !response.status().is_success() {
                return Err(TransferError::Status(response.status().as_u16()));
            }
            if !verified.load(Ordering::Acquire) {
                return Err(TransferError::Integrity);
            }
            let versions: Vec<_> = response
                .headers()
                .get_all("x-amz-version-id")
                .iter()
                .collect();
            let version = if versions.len() == 1 {
                versions[0].to_str().ok()
            } else {
                None
            }
            .filter(|s| valid_version(s))
            .ok_or(TransferError::Version)?;
            // Do not read untrusted response bodies or infer versions from ETags.
            // The server subsequently verifies this exact version's size and hash.
            Ok(ObjectVersion {
                key: grant.object_key.clone(),
                version_id: version.into(),
                size_bytes: declaration.size_bytes,
                sha256: declaration.sha256.clone(),
            })
        })
        .await
        .map_err(|_| TransferError::Deadline)?
    }

    fn destination(
        &self,
        declaration: &CreateUploadRequest,
        grant: &CreateUploadResponse,
    ) -> Result<(Url, HeaderMap), TransferError> {
        let url = Url::parse(&grant.upload_url).map_err(|_| TransferError::Grant)?;
        let loopback = url
            .host_str()
            .and_then(|s| s.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || grant.upload_url.contains('\\')
            || grant.upload_url.bytes().any(|b| b.is_ascii_control())
            || !(url.scheme() == "https"
                || self.allow_loopback_http && url.scheme() == "http" && loopback)
            || !url.path().ends_with(&format!("/{}", grant.object_key))
        {
            return Err(TransferError::Grant);
        }
        let mut headers = HeaderMap::new();
        for (name, value) in &grant.required_headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| TransferError::Grant)?;
            let mut value = HeaderValue::from_str(value).map_err(|_| TransferError::Grant)?;
            if headers.contains_key(&name) {
                return Err(TransferError::Grant);
            }
            match name.as_str() {
                "content-length" if value != declaration.size_bytes.to_string() => {
                    return Err(TransferError::Grant)
                }
                "content-length"
                | "x-amz-checksum-sha256"
                | "x-amz-content-sha256"
                | "content-type" => {}
                "host" => {
                    let host = url.host().ok_or(TransferError::Grant)?.to_string();
                    let authority = url
                        .port()
                        .map_or_else(|| host.clone(), |port| format!("{host}:{port}"));
                    if value != authority {
                        return Err(TransferError::Grant);
                    }
                }
                _ => return Err(TransferError::Grant),
            }
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        headers.remove(HOST);
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&declaration.size_bytes.to_string())
                .map_err(|_| TransferError::Configuration)?,
        );
        Ok((url, headers))
    }
}

struct BodyState {
    file: tokio::fs::File,
    remaining: u64,
    hash: Context,
    expected: String,
    verified: Arc<AtomicBool>,
    done: bool,
}
async fn next_chunk(mut state: BodyState) -> Result<Option<(Vec<u8>, BodyState)>, std::io::Error> {
    if state.done {
        return Ok(None);
    }
    let mut bytes = vec![0; state.remaining.min(64 << 10) as usize];
    state.file.read_exact(&mut bytes).await?;
    state.hash.update(&bytes);
    state.remaining -= bytes.len() as u64;
    if state.remaining == 0 {
        let mut extra = [0];
        let hash = state
            .hash
            .clone()
            .finish()
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        // Verify the last chunk before releasing it; detect both same-size edits
        // and length changes since collection without buffering the whole file.
        if state.file.read(&mut extra).await? != 0 || hash != state.expected {
            return Err(std::io::Error::other("output integrity mismatch"));
        }
        state.done = true;
        state.verified.store(true, Ordering::Release);
    }
    Ok(Some((bytes, state)))
}
