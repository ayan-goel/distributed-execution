//! Compose exact-version transfer with archive verification before publication.

use crate::{
    dataset_download::{DatasetDownloader, DownloadError},
    dataset_staging::{stage_archive, DatasetManifest, StageError},
};
use dispatch_protocol::v1::ObjectVersion;
use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum PrepareError {
    Download(DownloadError),
    Stage(StageError),
    Workspace,
}

impl fmt::Display for PrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("dataset preparation failed")
    }
}
impl std::error::Error for PrepareError {}

struct PendingArchive(PathBuf);
impl Drop for PendingArchive {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub async fn prepare_dataset(
    downloader: &DatasetDownloader,
    object: &ObjectVersion,
    url: &str,
    expires_unix_ms: i64,
    manifest: &DatasetManifest,
    destination: &Path,
) -> Result<(), PrepareError> {
    let parent = destination.parent().ok_or(PrepareError::Workspace)?;
    let nonce = crate::journal::new_uuid().map_err(|_| PrepareError::Workspace)?;
    let archive = PendingArchive(parent.join(format!(".archive-{nonce}")));
    // The archive is never mounted into a workload. Only the verified, sealed
    // tree becomes visible at destination; failures remove transfer bytes.
    downloader
        .download(object, url, expires_unix_ms, &archive.0)
        .await
        .map_err(PrepareError::Download)?;
    stage_archive(&archive.0, manifest, destination).map_err(PrepareError::Stage)
}
