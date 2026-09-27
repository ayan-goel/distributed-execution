//! Private per-attempt mount preparation for the explicit soft-scratch profile.
use super::*;
use crate::runtime::PreparedWorkspace;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

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
    for (name, mode) in [("inputs", 0o755), ("outputs", 0o777), ("scratch", 0o777)] {
        let child = path.join(name);
        std::fs::DirBuilder::new()
            .mode(mode)
            .create(&child)
            .map_err(|_| AgentError::File)?;
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(mode))
            .map_err(|_| AgentError::File)?;
    }
    PreparedWorkspace::soft_development(path).map_err(AgentError::Runtime)
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
        std::fs::remove_dir_all(root).unwrap();
    }
}
