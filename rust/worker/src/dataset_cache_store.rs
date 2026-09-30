//! Process-local immutable dataset cache with attempt-lifetime pins.

use crate::{
    dataset_cache::{CacheError, CacheIndex},
    dataset_download::DatasetDownloader,
    dataset_prepare::{prepare_dataset, PrepareError},
    dataset_staging::{reopen_directories, validate_manifest, DatasetManifest},
};
use dispatch_protocol::v1::ObjectVersion;
use ring::digest::{Context, SHA256};
use std::{
    fmt, fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Debug)]
pub enum CacheStoreError {
    Policy(CacheError),
    Preparation(PrepareError),
    File,
    Poisoned,
}

impl fmt::Display for CacheStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("dataset cache operation failed")
    }
}
impl std::error::Error for CacheStoreError {}

pub struct CacheStore {
    root: PathBuf,
    index: Arc<Mutex<CacheIndex>>,
    operation: tokio::sync::Mutex<()>,
    poisoned: AtomicBool,
}

pub struct CachePin {
    path: PathBuf,
    key: String,
    index: Arc<Mutex<CacheIndex>>,
}

impl CachePin {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CachePin {
    fn drop(&mut self) {
        // A pin lives through execution cleanup, so eviction can never remove
        // a tree still mounted into a running or finalizing container.
        let mut index = self.index.lock().unwrap_or_else(|error| error.into_inner());
        let _ = index.unpin(&self.key);
    }
}

impl CacheStore {
    pub fn new(root: &Path, high_bytes: u64, low_bytes: u64) -> Result<Self, CacheStoreError> {
        let meta = fs::symlink_metadata(root).map_err(|_| CacheStoreError::File)?;
        // INVARIANT: this process starts with an empty private cache root.
        // Recovery must reconcile old mounts before a later startup reuses it.
        if !root.is_absolute()
            || !meta.is_dir()
            || meta.file_type().is_symlink()
            || meta.uid() != unsafe { libc::geteuid() }
            || meta.permissions().mode() & 0o777 != 0o700
            || fs::read_dir(root)
                .map_err(|_| CacheStoreError::File)?
                .next()
                .is_some()
        {
            return Err(CacheStoreError::File);
        }
        let canonical = root.canonicalize().map_err(|_| CacheStoreError::File)?;
        Ok(Self {
            root: canonical,
            index: Arc::new(Mutex::new(
                CacheIndex::new(high_bytes, low_bytes).map_err(CacheStoreError::Policy)?,
            )),
            operation: tokio::sync::Mutex::new(()),
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn used_bytes(&self) -> Result<u64, CacheStoreError> {
        Ok(self
            .index
            .lock()
            .map_err(|_| CacheStoreError::Poisoned)?
            .used_bytes())
    }

    pub async fn prepare(
        &self,
        downloader: &DatasetDownloader,
        object: &ObjectVersion,
        url: &str,
        expires_unix_ms: i64,
        manifest: &DatasetManifest,
    ) -> Result<CachePin, CacheStoreError> {
        let _operation = self.operation.lock().await;
        if self.poisoned.load(Ordering::Acquire) {
            return Err(CacheStoreError::Poisoned);
        }
        let key = identity(object, manifest)?;
        let destination = self.root.join(&key);
        let victims = {
            let mut index = self.index.lock().map_err(|_| CacheStoreError::Poisoned)?;
            if index.contains(&key) {
                let meta = match fs::symlink_metadata(&destination) {
                    Ok(meta) => meta,
                    Err(_) => {
                        self.poisoned.store(true, Ordering::Release);
                        return Err(CacheStoreError::Poisoned);
                    }
                };
                if !meta.is_dir()
                    || meta.file_type().is_symlink()
                    || meta.permissions().mode() & 0o777 != 0o555
                {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(CacheStoreError::Poisoned);
                }
                index.pin(&key).map_err(CacheStoreError::Policy)?;
                return Ok(CachePin {
                    path: destination,
                    key,
                    index: Arc::clone(&self.index),
                });
            }
            // Reject malformed grants and manifests before evicting reusable
            // inputs. A cache hit needs no live grant for the same identity.
            downloader
                .validate_grant(object, url, expires_unix_ms)
                .map_err(|_| CacheStoreError::File)?;
            validate_manifest(manifest).map_err(|_| CacheStoreError::File)?;
            index
                .plan_eviction(object.size_bytes)
                .map_err(CacheStoreError::Policy)?
        };
        for victim in victims {
            if delete_tree(&self.root.join(&victim)).is_err() {
                self.poisoned.store(true, Ordering::Release);
                return Err(CacheStoreError::Poisoned);
            }
            self.index
                .lock()
                .map_err(|_| CacheStoreError::Poisoned)?
                .remove(&victim)
                .map_err(CacheStoreError::Policy)?;
        }
        if let Err(error) = prepare_dataset(
            downloader,
            object,
            url,
            expires_unix_ms,
            manifest,
            &destination,
        )
        .await
        {
            if fs::symlink_metadata(&destination).is_ok() && delete_tree(&destination).is_err() {
                self.poisoned.store(true, Ordering::Release);
                return Err(CacheStoreError::Poisoned);
            }
            return Err(CacheStoreError::Preparation(error));
        }
        let indexed = (|| {
            let mut index = self.index.lock().map_err(|_| CacheStoreError::Poisoned)?;
            index
                .insert(&key, object.size_bytes)
                .map_err(CacheStoreError::Policy)?;
            index.pin(&key).map_err(CacheStoreError::Policy)
        })();
        if let Err(error) = indexed {
            if let Ok(mut index) = self.index.lock() {
                if index.contains(&key) {
                    let _ = index.remove(&key);
                }
            }
            if delete_tree(&destination).is_err() {
                self.poisoned.store(true, Ordering::Release);
            }
            return Err(error);
        }
        Ok(CachePin {
            path: destination,
            key,
            index: Arc::clone(&self.index),
        })
    }
}

fn delete_tree(path: &Path) -> Result<(), CacheStoreError> {
    let meta = fs::symlink_metadata(path).map_err(|_| CacheStoreError::File)?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(CacheStoreError::File);
    }
    reopen_directories(path).map_err(|_| CacheStoreError::File)?;
    fs::remove_dir_all(path).map_err(|_| CacheStoreError::File)
}

fn identity(object: &ObjectVersion, manifest: &DatasetManifest) -> Result<String, CacheStoreError> {
    let manifest = serde_json::to_vec(manifest).map_err(|_| CacheStoreError::File)?;
    let mut digest = Context::new(&SHA256);
    // Length-prefix object fields so distinct keys and versions cannot share
    // an identity; typed manifest serialization fixes the field order.
    for field in [
        object.key.as_bytes(),
        object.version_id.as_bytes(),
        object.sha256.as_bytes(),
    ] {
        digest.update(&(field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    digest.update(&object.size_bytes.to_be_bytes());
    digest.update(&manifest);
    Ok(digest
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
