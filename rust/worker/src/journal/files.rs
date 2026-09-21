use super::{JournalError, JournalLimits};
use crate::control::canonical_uuid;
use ring::digest::{digest, SHA256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

pub(super) const MAX_PAYLOAD: usize = 5 * 1024 * 1024;
const HEADER: usize = 48;
const MAGIC: &[u8; 8] = b"DSPJNL01";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CommitStep {
    Created,
    Written,
    Synced,
    Renamed,
    Committed,
}

pub(super) struct Directory {
    root: PathBuf,
    directory: File,
    _lock: File,
    limits: JournalLimits,
    poisoned: bool,
    #[cfg(test)]
    pub fault: Option<CommitStep>,
}

fn metadata_safe(metadata: &fs::Metadata, directory: bool) -> Result<(), JournalError> {
    // Journal bytes include workload environment and retry identities. Neither
    // another user nor a container may read/write these files through permissions
    // or a hard link; ancestor directories remain a trusted installation boundary.
    // SAFETY: geteuid reads the current process credentials without pointers.
    let owner = unsafe { libc::geteuid() };
    if metadata.uid() != owner
        || metadata.mode() & 0o777 != if directory { 0o700 } else { 0o600 }
        || (directory && !metadata.is_dir())
        || (!directory && (!metadata.is_file() || metadata.nlink() != 1))
    {
        return Err(JournalError::UnsafePath);
    }
    Ok(())
}

impl Directory {
    pub fn open(root: &Path, limits: JournalLimits) -> Result<Self, JournalError> {
        if !root.is_absolute() || fs::canonicalize(root).ok().as_deref() != Some(root) {
            return Err(JournalError::UnsafePath);
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(root)?;
        metadata_safe(&directory.metadata()?, true)?;
        let lock_path = root.join(".lock");
        if let Ok(metadata) = fs::symlink_metadata(&lock_path) {
            metadata_safe(&metadata, false)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&lock_path)?;
        metadata_safe(&lock.metadata()?, false)?;
        // The lock inode is permanent. Unlinking it could let a second process
        // lock a replacement while this process still owns the original inode.
        // SAFETY: flock receives a live file descriptor and defined flags.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            return Err(if error.kind() == std::io::ErrorKind::WouldBlock {
                JournalError::Busy
            } else {
                error.into()
            });
        }
        lock.sync_all()?;
        directory.sync_all()?;
        let store = Self {
            root: root.to_owned(),
            directory,
            _lock: lock,
            limits,
            poisoned: false,
            #[cfg(test)]
            fault: None,
        };
        // A temporary replacement is never a committed record. Discard only
        // this known private regular file after obtaining exclusive ownership.
        let pending = store.root.join(".pending");
        match fs::symlink_metadata(&pending) {
            Ok(metadata) => {
                metadata_safe(&metadata, false)?;
                fs::remove_file(pending)?;
                store.directory.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        store.inventory()?;
        Ok(store)
    }

    fn healthy(&self) -> Result<(), JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let actual = fs::symlink_metadata(&self.root)?;
        metadata_safe(&actual, true)?;
        let opened = self.directory.metadata()?;
        if actual.dev() != opened.dev() || actual.ino() != opened.ino() {
            return Err(JournalError::UnsafePath);
        }
        Ok(())
    }

    pub fn inventory(&self) -> Result<Vec<(String, u64)>, JournalError> {
        self.healthy()?;
        let mut records = Vec::new();
        let mut bytes = 0u64;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| JournalError::Corrupt)?;
            if name == ".lock" || name == ".identity" {
                continue;
            }
            if name == ".session" {
                let metadata = fs::symlink_metadata(entry.path())?;
                metadata_safe(&metadata, false)?;
                if metadata.len() > (super::session::MAX_SESSION + HEADER) as u64 {
                    return Err(JournalError::Corrupt);
                }
                continue;
            }
            let id = name
                .strip_suffix(".attempt")
                .filter(|id| canonical_uuid(id))
                .ok_or(JournalError::Corrupt)?;
            let metadata = fs::symlink_metadata(entry.path())?;
            metadata_safe(&metadata, false)?;
            if metadata.len() > (MAX_PAYLOAD + HEADER) as u64 {
                return Err(JournalError::Corrupt);
            }
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or(JournalError::Limit)?;
            records.push((id.to_string(), metadata.len()));
            if records.len() > self.limits.max_attempts || bytes > self.limits.max_bytes {
                return Err(JournalError::Limit);
            }
        }
        records.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Ok(records)
    }

    pub fn read(&self, name: &str) -> Result<Option<Vec<u8>>, JournalError> {
        self.healthy()?;
        let path = self.root.join(name);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        metadata_safe(&metadata, false)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        metadata_safe(&file.metadata()?, false)?;
        let mut bytes = Vec::new();
        file.take((MAX_PAYLOAD + HEADER + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() < HEADER || bytes.len() > MAX_PAYLOAD + HEADER || &bytes[..8] != MAGIC {
            return Err(JournalError::Corrupt);
        }
        let length =
            u64::from_be_bytes(bytes[8..16].try_into().map_err(|_| JournalError::Corrupt)?);
        let body = &bytes[HEADER..];
        if length != body.len() as u64 || digest(&SHA256, body).as_ref() != &bytes[16..HEADER] {
            return Err(JournalError::Corrupt);
        }
        Ok(Some(body.to_vec()))
    }

    pub fn write(&mut self, name: &str, payload: &[u8]) -> Result<(), JournalError> {
        self.healthy()?;
        if payload.len() > MAX_PAYLOAD {
            return Err(JournalError::Limit);
        }
        let inventory = self.inventory()?;
        if let Some(id) = name.strip_suffix(".attempt") {
            let old = inventory.iter().find(|(key, _)| key == id);
            let total: u64 = inventory.iter().map(|(_, size)| size).sum();
            if (old.is_none() && inventory.len() >= self.limits.max_attempts)
                || total - old.map_or(0, |(_, size)| *size) + (payload.len() + HEADER) as u64
                    > self.limits.max_bytes
            {
                return Err(JournalError::Limit);
            }
        }
        let outcome = self.replace(name, payload);
        // After a write/sync/rename error, visibility may differ from durability.
        // No subsequent action may rely on this writer until it is reopened.
        if outcome.is_err() {
            self.poisoned = true;
        }
        outcome
    }

    fn replace(&mut self, name: &str, payload: &[u8]) -> Result<(), JournalError> {
        let pending = self.root.join(".pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&pending)?;
        self.checkpoint(CommitStep::Created)?;
        file.write_all(MAGIC)?;
        file.write_all(&(payload.len() as u64).to_be_bytes())?;
        file.write_all(digest(&SHA256, payload).as_ref())?;
        file.write_all(payload)?;
        self.checkpoint(CommitStep::Written)?;
        file.sync_all()?;
        self.checkpoint(CommitStep::Synced)?;
        // The temporary lives on the same filesystem. File sync protects its
        // contents; rename selects the whole record; directory sync persists the
        // selected name. Returning before the final sync would overclaim durability.
        fs::rename(pending, self.root.join(name))?;
        self.checkpoint(CommitStep::Renamed)?;
        self.directory.sync_all()?;
        self.checkpoint(CommitStep::Committed)?;
        Ok(())
    }

    fn checkpoint(&mut self, step: CommitStep) -> Result<(), JournalError> {
        #[cfg(test)]
        if self.fault == Some(step) {
            return Err(JournalError::Io(std::io::ErrorKind::Other));
        }
        let _ = step;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        os::unix::fs::DirBuilderExt,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "dispatch-journal-fault-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
            Self(root.canonicalize().unwrap())
        }
        fn open(&self) -> Directory {
            Directory::open(&self.0, JournalLimits::default()).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn interrupted_replacements_recover_one_whole_record_and_poison_the_writer() {
        let name = "00000000-0000-0000-0000-000000000001.attempt";
        for step in [
            CommitStep::Created,
            CommitStep::Written,
            CommitStep::Synced,
            CommitStep::Renamed,
            CommitStep::Committed,
        ] {
            let f = Fixture::new();
            let mut directory = f.open();
            directory.write(name, b"old").unwrap();
            directory.fault = Some(step);
            assert!(matches!(
                directory.write(name, b"new"),
                Err(JournalError::Io(_))
            ));
            assert!(matches!(
                directory.write(name, b"later"),
                Err(JournalError::Poisoned)
            ));
            drop(directory);
            let recovered = f.open();
            let expected = if matches!(step, CommitStep::Renamed | CommitStep::Committed) {
                b"new"
            } else {
                b"old"
            };
            assert_eq!(recovered.read(name).unwrap().unwrap(), expected);
            assert!(!f.0.join(".pending").exists());
        }
    }

    #[test]
    fn corrupt_lengths_truncated_frames_and_future_versions_fail_closed() {
        for variant in 0..3 {
            let f = Fixture::new();
            let mut directory = f.open();
            directory.write(".identity", b"identity").unwrap();
            let path = f.0.join(".identity");
            let mut bytes = fs::read(&path).unwrap();
            match variant {
                0 => bytes[8..16].copy_from_slice(&u64::MAX.to_be_bytes()),
                1 => bytes.truncate(HEADER - 1),
                _ => bytes[7] = b'2',
            }
            fs::write(path, bytes).unwrap();
            assert!(matches!(
                directory.read(".identity"),
                Err(JournalError::Corrupt)
            ));
        }
    }
}
