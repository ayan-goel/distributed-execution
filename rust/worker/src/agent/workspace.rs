//! Private per-attempt preparation, with an exclusive Linux quota profile.
use super::*;
use crate::dataset_staging::reopen_directories;
use crate::runtime::PreparedWorkspace;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

#[derive(Clone)]
pub(super) struct Workspaces {
    root: PathBuf,
    strict: bool,
    #[cfg(target_os = "linux")]
    quota: Option<std::sync::Arc<std::sync::Mutex<super::quota::QuotaWorkspaces>>>,
}

impl Workspaces {
    pub fn open(root: &Path, worker: &str, strict: bool) -> Result<Self, AgentError> {
        #[cfg(target_os = "linux")]
        if strict {
            let quota = super::quota::QuotaWorkspaces::open(root, worker)?;
            return Ok(Self {
                root: quota.root().to_owned(),
                strict,
                quota: Some(std::sync::Arc::new(std::sync::Mutex::new(quota))),
            });
        }
        #[cfg(not(target_os = "linux"))]
        if strict {
            let _ = worker;
            return Err(AgentError::Runtime(RuntimeError::Unsupported));
        }
        Ok(Self {
            root: workspace(root)?,
            strict,
            #[cfg(target_os = "linux")]
            quota: None,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn strict(&self) -> bool {
        self.strict
    }

    pub fn verify(&self) -> Result<(), AgentError> {
        #[cfg(target_os = "linux")]
        if let Some(quota) = &self.quota {
            return quota.lock().map_err(|_| AgentError::Task)?.verify();
        }
        if workspace(&self.root)? != self.root {
            return Err(AgentError::File);
        }
        Ok(())
    }

    pub fn prepare(&self, attempt: &str, bytes: u64) -> Result<PreparedWorkspace, AgentError> {
        self.verify()?;
        if disk_pressure(&self.root, bytes) {
            return Err(AgentError::File);
        }
        #[cfg(target_os = "linux")]
        if let Some(quota) = &self.quota {
            return quota
                .lock()
                .map_err(|_| AgentError::Task)?
                .prepare(attempt, bytes);
        }
        prepare_attempt_workspace(&self.root, attempt, bytes)
    }

    pub fn reconcile(&self) -> Result<(), AgentError> {
        self.verify()?;
        clear_abandoned_attempts(&self.root)
    }

    pub fn remove(&self, path: &Path) -> Result<(), AgentError> {
        self.verify()?;
        if path.parent() != Some(self.root())
            || !path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(canonical_uuid)
        {
            return Err(AgentError::Configuration);
        }
        std::fs::remove_dir_all(path).map_err(|_| AgentError::File)?;
        std::fs::File::open(&self.root)
            .and_then(|file| file.sync_all())
            .map_err(|_| AgentError::File)
    }
}

pub(super) fn prepare_attempt_workspace(
    root: &Path,
    attempt: &str,
    scratch_bytes: u64,
) -> Result<PreparedWorkspace, AgentError> {
    if !canonical_uuid(attempt) || scratch_bytes == 0 || workspace(root)? != root {
        return Err(AgentError::Configuration);
    }
    if disk_pressure(root, scratch_bytes) {
        return Err(AgentError::File);
    }
    let path = root.join(attempt);
    // Creation is exclusive under a private parent. A stale or malicious path
    // cannot be reused as an execution mount after an uncertain prior attempt.
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&path)
        .map_err(|_| AgentError::File)?;
    prepare_mount_children(&path)?;
    PreparedWorkspace::soft_development(path).map_err(AgentError::Runtime)
}

pub(super) fn prepare_mount_children(path: &Path) -> Result<(), AgentError> {
    for (name, mode) in [("inputs", 0o755), ("outputs", 0o777), ("scratch", 0o777)] {
        let child = path.join(name);
        std::fs::DirBuilder::new()
            .mode(mode)
            .create(&child)
            .map_err(|_| AgentError::File)?;
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(mode))
            .map_err(|_| AgentError::File)?;
    }
    Ok(())
}

pub(super) fn clear_abandoned_attempts(root: &Path) -> Result<(), AgentError> {
    if workspace(root)? != root {
        return Err(AgentError::Configuration);
    }
    for entry in std::fs::read_dir(root).map_err(|_| AgentError::File)? {
        let entry = entry.map_err(|_| AgentError::File)?;
        let name = entry.file_name();
        let name = name.to_str().ok_or(AgentError::File)?;
        if name == ".dataset-cache" {
            if !entry.file_type().map_err(|_| AgentError::File)?.is_dir() {
                return Err(AgentError::File);
            }
            // The old incarnation's containers are absent before this call.
            // Reopen sealed trees only now; clearing them earlier could remove
            // bytes still mounted into a predecessor's running container.
            reopen_directories(&entry.path()).map_err(|_| AgentError::File)?;
            std::fs::remove_dir_all(entry.path()).map_err(|_| AgentError::File)?;
            continue;
        }
        // INVARIANT: only attempt UUID directories live under this private root.
        // Reject aliases and unknown files rather than following an unsafe path
        // or announcing readiness with unaccounted prior workspace state.
        if !canonical_uuid(name) || !entry.file_type().map_err(|_| AgentError::File)?.is_dir() {
            return Err(AgentError::File);
        }
        std::fs::remove_dir_all(entry.path()).map_err(|_| AgentError::File)?;
    }
    let cache = root.join(".dataset-cache");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&cache)
        .map_err(|_| AgentError::File)?;
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700))
        .map_err(|_| AgentError::File)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_private_unique_attempt_mounts_and_rejects_aliases() {
        let root = std::env::temp_dir().join(format!(
            "dispatch-agent-workspace-{}-{}",
            std::process::id(),
            new_uuid().unwrap()
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let root = root.canonicalize().unwrap();
        let attempt = new_uuid().unwrap();
        let prepared = prepare_attempt_workspace(&root, &attempt, 1 << 20).unwrap();
        assert_eq!(prepared.root(), root.join(&attempt));
        for (name, mode) in [("inputs", 0o755), ("outputs", 0o777), ("scratch", 0o777)] {
            assert_eq!(
                std::fs::metadata(prepared.root().join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                mode
            );
        }
        assert_eq!(
            std::fs::metadata(prepared.root())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert!(prepare_attempt_workspace(&root, &attempt, 1 << 20).is_err());
        assert!(prepare_attempt_workspace(&root, "../escape", 1 << 20).is_err());
        let alias = new_uuid().unwrap();
        std::os::unix::fs::symlink(prepared.root(), root.join(&alias)).unwrap();
        assert!(prepare_attempt_workspace(&root, &alias, 1 << 20).is_err());
        assert!(clear_abandoned_attempts(&root).is_err());
        std::fs::remove_file(root.join(&alias)).unwrap();
        clear_abandoned_attempts(&root).unwrap();
        assert!(!prepared.root().exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reconciled_startup_replaces_sealed_cache_without_following_aliases() {
        let root = std::env::temp_dir().join(format!(
            "dispatch-agent-cache-{}-{}",
            std::process::id(),
            new_uuid().unwrap()
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let root = root.canonicalize().unwrap();
        let cache = root.join(".dataset-cache");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&cache)
            .unwrap();
        let entry = cache.join("old-version");
        std::fs::create_dir(&entry).unwrap();
        std::fs::write(entry.join("input.txt"), b"old").unwrap();
        std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o555)).unwrap();
        clear_abandoned_attempts(&root).unwrap();
        assert!(cache.is_dir());
        assert_eq!(std::fs::read_dir(&cache).unwrap().count(), 0);
        assert_eq!(
            std::fs::metadata(&cache).unwrap().permissions().mode() & 0o777,
            0o700
        );
        std::fs::remove_dir(&cache).unwrap();
        let outside = root.parent().unwrap().join(format!(
            "dispatch-agent-cache-outside-{}",
            new_uuid().unwrap()
        ));
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, &cache).unwrap();
        assert!(clear_abandoned_attempts(&root).is_err());
        assert!(outside.is_dir());
        std::fs::remove_file(cache).unwrap();
        std::fs::remove_dir(root).unwrap();
        std::fs::remove_dir(outside).unwrap();
    }
}
