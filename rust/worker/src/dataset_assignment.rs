//! Validate every input capability against the job's immutable requested mounts.

use crate::{
    control::canonical_uuid,
    dataset_cache_store::{CachePin, CacheStore, CacheStoreError},
    dataset_download::DatasetDownloader,
    dataset_staging::{validate_manifest, DatasetManifest},
    execution::{ExecutionSpec, Input},
    runtime::{PreparedWorkspace, RuntimeError},
};
use dispatch_protocol::v1::{Assignment, ObjectVersion};
use std::fmt;

#[derive(Debug)]
pub enum InputError {
    Invalid,
    SpecMismatch,
}

impl fmt::Display for InputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Signed download URLs are bearer capabilities and never enter errors.
        f.write_str("invalid dataset input assignment")
    }
}
impl std::error::Error for InputError {}

#[derive(Debug)]
pub enum InputStageError {
    Assignment(InputError),
    Cache(CacheStoreError),
    Mount(RuntimeError),
}
impl fmt::Display for InputStageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("dataset input preparation failed")
    }
}
impl std::error::Error for InputStageError {}

pub struct ValidatedInput {
    pub dataset_id: String,
    pub dataset_name: String,
    pub mount_path: String,
    pub archive: ObjectVersion,
    pub manifest: DatasetManifest,
    pub download_url: String,
    pub expires_unix_ms: i64,
}

pub fn validate_inputs(
    assignment: &Assignment,
    expected: &[Input],
    downloader: &DatasetDownloader,
) -> Result<Vec<ValidatedInput>, InputError> {
    if expected.len() > 64 || assignment.inputs.len() != expected.len() {
        return Err(InputError::SpecMismatch);
    }
    let mut validated = Vec::with_capacity(expected.len());
    for (wire, requested) in assignment.inputs.iter().zip(expected) {
        if wire.dataset_name != requested.dataset || wire.mount_path != requested.mount_path {
            return Err(InputError::SpecMismatch);
        }
        if !canonical_uuid(&wire.dataset_id)
            || !valid_name(&wire.dataset_name)
            || !valid_mount(&wire.mount_path)
            || validated
                .iter()
                .any(|previous: &ValidatedInput| overlaps(&previous.mount_path, &wire.mount_path))
            || wire.file_manifest_json.is_empty()
            || wire.file_manifest_json.len() > 2 << 20
        {
            return Err(InputError::Invalid);
        }
        let archive = wire.archive.as_ref().ok_or(InputError::Invalid)?;
        if archive.size_bytes == 0 {
            return Err(InputError::Invalid);
        }
        let manifest: DatasetManifest =
            serde_json::from_slice(&wire.file_manifest_json).map_err(|_| InputError::Invalid)?;
        validate_manifest(&manifest).map_err(|_| InputError::Invalid)?;
        let total = manifest
            .files
            .iter()
            .try_fold(0u64, |sum, file| sum.checked_add(file.size_bytes))
            .ok_or(InputError::Invalid)?;
        if total > archive.size_bytes {
            return Err(InputError::Invalid);
        }
        // Validate the signed URL against this exact version before the agent
        // can fetch or mount anything. Transfer rechecks it at request time.
        downloader
            .validate_grant(archive, &wire.download_url, wire.expires_unix_ms)
            .map_err(|_| InputError::Invalid)?;
        validated.push(ValidatedInput {
            dataset_id: wire.dataset_id.clone(),
            dataset_name: wire.dataset_name.clone(),
            mount_path: wire.mount_path.clone(),
            archive: archive.clone(),
            manifest,
            download_url: wire.download_url.clone(),
            expires_unix_ms: wire.expires_unix_ms,
        });
    }
    Ok(validated)
}

pub fn validate_replayed_input(
    original: &Assignment,
    replay: &Assignment,
    expected: &[Input],
    index: usize,
    downloader: &DatasetDownloader,
) -> Result<ValidatedInput, InputError> {
    if index >= expected.len()
        || original.inputs.len() != expected.len()
        || stable_assignment(original) != stable_assignment(replay)
    {
        return Err(InputError::SpecMismatch);
    }
    validate_inputs(replay, expected, downloader)?
        .into_iter()
        .nth(index)
        .ok_or(InputError::Invalid)
}

fn stable_assignment(assignment: &Assignment) -> Assignment {
    let mut stable = assignment.clone();
    // Replay may recompute remaining authority and mint fresh capabilities,
    // but it must not substitute a different dataset or execution identity.
    stable.lease_duration_ms = 0;
    stable.phase_remaining_ms = 0;
    stable.server_time_unix_ms = 0;
    for input in &mut stable.inputs {
        input.download_url.clear();
        input.expires_unix_ms = 0;
    }
    stable
}

pub async fn stage_inputs(
    assignment: &Assignment,
    execution: &ExecutionSpec,
    downloader: &DatasetDownloader,
    cache: &CacheStore,
    workspace: &mut PreparedWorkspace,
) -> Result<Vec<CachePin>, InputStageError> {
    let validated = validate_inputs(assignment, &execution.job().spec.inputs, downloader)
        .map_err(InputStageError::Assignment)?;
    let mut pins = Vec::with_capacity(validated.len());
    for input in validated {
        let pin = cache
            .prepare(
                downloader,
                &input.archive,
                &input.download_url,
                input.expires_unix_ms,
                &input.manifest,
            )
            .await
            .map_err(InputStageError::Cache)?;
        // The pin belongs to the caller until physical container cleanup.
        // Failed later inputs drop earlier pins before a container can launch.
        workspace
            .bind_input(&input.mount_path, pin.path())
            .map_err(InputStageError::Mount)?;
        pins.push(pin);
    }
    Ok(pins)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn valid_mount(path: &str) -> bool {
    path.len() <= 4096
        && path.starts_with("/inputs/")
        && !path.contains(['\\', '\0'])
        && path
            .split('/')
            .skip(1)
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn overlaps(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}
