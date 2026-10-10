//! One durable allocator owns the entire dedicated ext4 workspace filesystem.
use super::{workspace::prepare_mount_children, AgentError};
use crate::{
    control::canonical_uuid,
    journal::{JournalError, ProjectIds},
    project_quota::ProjectQuota,
    runtime::PreparedWorkspace,
};
use std::{
    fs::{self, File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

pub struct QuotaWorkspaces {
    mount: PathBuf,
    directory: File,
    work: PathBuf,
    work_directory: File,
    ids: ProjectIds,
}

impl QuotaWorkspaces {
    /// Explicitly initialize an unused dedicated filesystem. Partial setup is
    /// retained on error; neither initialize nor open resets an existing counter.
    pub fn initialize(mount: &Path, worker: &str) -> Result<Self, AgentError> {
        if !canonical_uuid(worker) {
            return Err(AgentError::Configuration);
        }
        let directory = lock_filesystem(mount)?;
        layout(mount, false)?;
        for name in [".dispatch-projects", "work"] {
            fs::DirBuilder::new()
                .mode(0o700)
                .create(mount.join(name))
                .map_err(|_| AgentError::File)?;
        }
        directory.sync_all().map_err(|_| AgentError::File)?;
        let ids = ProjectIds::initialize(&mount.join(".dispatch-projects"), worker)?;
        Self::from_parts(mount, directory, ids)
    }

    pub fn open(mount: &Path, worker: &str) -> Result<Self, AgentError> {
        let directory = lock_filesystem(mount)?;
        layout(mount, true)?;
        let ids = ProjectIds::open(&mount.join(".dispatch-projects"), worker)?;
        Self::from_parts(mount, directory, ids)
    }

    fn from_parts(mount: &Path, directory: File, ids: ProjectIds) -> Result<Self, AgentError> {
        let work = mount.join("work");
        let work_directory = private_directory(&work)?;
        let store = Self {
            mount: mount.to_owned(),
            directory,
            work,
            work_directory,
            ids,
        };
        store.verify()?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.work
    }

    pub fn verify(&self) -> Result<(), AgentError> {
        for (path, pinned) in [
            (&self.mount, &self.directory),
            (&self.work, &self.work_directory),
        ] {
            let actual = private_directory(path)?;
            let actual = actual.metadata().map_err(|_| AgentError::File)?;
            let pinned = pinned.metadata().map_err(|_| AgentError::File)?;
            if (actual.dev(), actual.ino()) != (pinned.dev(), pinned.ino()) {
                return Err(AgentError::File);
            }
        }
        layout(&self.mount, true)?;
        // Control state and the common work parent must never inherit a job's
        // project: exhaustion of that job must not block allocation or recovery.
        ProjectQuota::verify_unassigned(&self.directory).map_err(|_| AgentError::File)?;
        ProjectQuota::verify_unassigned(&self.work_directory).map_err(|_| AgentError::File)?;
        let control = private_directory(&self.mount.join(".dispatch-projects"))?;
        ProjectQuota::verify_unassigned(&control).map_err(|_| AgentError::File)?;
        self.ids.verify()?;
        Ok(())
    }

    pub fn prepare(&mut self, attempt: &str, bytes: u64) -> Result<PreparedWorkspace, AgentError> {
        if !canonical_uuid(attempt) || bytes == 0 || bytes % 1024 != 0 {
            return Err(AgentError::Configuration);
        }
        self.verify()?;
        // Commit the ID before any quota setup. Failed or uncertain preparation
        // permanently skips that ID instead of colliding after a restart.
        let id = self.ids.reserve()?;
        let path = self.work.join(attempt);
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(|_| AgentError::File)?;
        self.work_directory
            .sync_all()
            .map_err(|_| AgentError::File)?;
        // Bound tiny-file overhead as well as allocated bytes. One inode per
        // 4 KiB, capped at a million, prevents a job exhausting host inode space.
        let inodes = (bytes / 4096).clamp(16, 1_000_000);
        let quota =
            ProjectQuota::attach_empty(&path, id, bytes, inodes).map_err(|_| AgentError::File)?;
        prepare_mount_children(&path)?;
        File::open(&path)
            .and_then(|file| file.sync_all())
            .map_err(|_| AgentError::File)?;
        PreparedWorkspace::project_quota(path, quota).map_err(AgentError::Runtime)
    }
}

fn private_directory(path: &Path) -> Result<File, AgentError> {
    if !path.is_absolute() || fs::canonicalize(path).ok().as_deref() != Some(path) {
        return Err(AgentError::Configuration);
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| AgentError::File)?;
    let metadata = file.metadata().map_err(|_| AgentError::File)?;
    // This release administers quotas through a root worker on a dedicated host.
    // Never allow a workload UID to traverse the filesystem/control parents.
    if unsafe { libc::geteuid() } != 0 || metadata.uid() != 0 || metadata.mode() & 0o777 != 0o700 {
        return Err(AgentError::File);
    }
    Ok(file)
}

fn lock_filesystem(mount: &Path) -> Result<File, AgentError> {
    let directory = private_directory(mount)?;
    let metadata = directory.metadata().map_err(|_| AgentError::File)?;
    // ext4 reserves inode 2 for its filesystem root. A subdirectory is not an
    // independent ID namespace; all aliases must contend on this same inode.
    // Source: https://github.com/torvalds/linux/blob/v6.12/fs/ext4/ext4.h
    if metadata.ino() != 2 {
        return Err(AgentError::Configuration);
    }
    ProjectQuota::verify_unassigned(&directory).map_err(|_| AgentError::File)?;
    // SAFETY: flock receives the live root descriptor and defined nonblocking flags.
    if unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock {
                AgentError::Journal(JournalError::Busy)
            } else {
                AgentError::File
            },
        );
    }
    Ok(directory)
}

fn layout(mount: &Path, initialized: bool) -> Result<(), AgentError> {
    let device = fs::metadata(mount).map_err(|_| AgentError::File)?.dev();
    for entry in fs::read_dir(mount).map_err(|_| AgentError::File)? {
        let entry = entry.map_err(|_| AgentError::File)?;
        let name = entry.file_name();
        let allowed = name == "lost+found"
            || (initialized && (name == ".dispatch-projects" || name == "work"));
        if !allowed {
            return Err(AgentError::Configuration);
        }
        let child = private_directory(&entry.path())?;
        if child.metadata().map_err(|_| AgentError::File)?.dev() != device
            || (name == "lost+found"
                && fs::read_dir(entry.path())
                    .map_err(|_| AgentError::File)?
                    .next()
                    .is_some())
        {
            return Err(AgentError::File);
        }
    }
    Ok(())
}
