//! Private bounded disk queue for sealed, immutable log objects.
use crate::{
    control::canonical_uuid,
    log_format::{LogSegment, MAX_SEGMENT_BYTES},
    runtime::PreparedWorkspace,
};
use dispatch_protocol::v1::LogStream;
use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub const MAX_SPOOL_BYTES: u64 = 256 << 20;
pub const MAX_SPOOL_SEGMENTS: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpoolError {
    Invalid,
    Identity,
    UnsafePath,
    Conflict,
    Poisoned,
    Io(std::io::ErrorKind),
}
impl fmt::Display for SpoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Paths and log content can contain workload data; report categories only.
        write!(f, "log spool error: {self:?}")
    }
}
impl std::error::Error for SpoolError {}
impl From<std::io::Error> for SpoolError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}

#[derive(Debug)]
pub struct StoredSegment {
    path: PathBuf,
    name: String,
    size: u64,
}
impl StoredSegment {
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug)]
pub enum SpoolOutcome {
    Stored(StoredSegment),
    Saturated,
}

#[derive(Debug)]
pub struct LogSpool {
    root: PathBuf,
    directory: File,
    attempt_id: String,
    cap: u64,
    used: u64,
    entries: BTreeMap<String, u64>,
    poisoned: bool,
}

fn safe_file(file: &File, size: u64) -> Result<(), SpoolError> {
    let meta = file.metadata()?;
    // INVARIANT: only this worker may read the spool, and a linked inode must
    // not let another path mutate bytes after checksum and upload preparation.
    // SAFETY: geteuid reads current process credentials without pointers.
    let owner = unsafe { libc::geteuid() };
    if !meta.is_file()
        || meta.uid() != owner
        || meta.mode() & 0o777 != 0o600
        || meta.nlink() != 1
        || meta.len() != size
    {
        return Err(SpoolError::UnsafePath);
    }
    Ok(())
}

impl LogSpool {
    pub fn create(workspace: &PreparedWorkspace, cap: u64) -> Result<Self, SpoolError> {
        let attempt_id = workspace
            .root()
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(SpoolError::UnsafePath)?;
        if !canonical_uuid(attempt_id) || cap == 0 || cap > MAX_SPOOL_BYTES {
            return Err(SpoolError::Invalid);
        }
        let root = workspace.root().join("logs");
        // This child is outside all container bind mounts. Exclusive creation
        // rejects stale state or a symlink before any object is written.
        if fs::symlink_metadata(&root).is_ok() {
            return Err(SpoolError::UnsafePath);
        }
        fs::DirBuilder::new().mode(0o700).create(&root)?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&root)?;
        let meta = directory.metadata()?;
        // SAFETY: geteuid reads current process credentials without pointers.
        if !meta.is_dir()
            || meta.mode() & 0o777 != 0o700
            || meta.uid() != unsafe { libc::geteuid() }
        {
            return Err(SpoolError::UnsafePath);
        }
        directory.sync_all()?;
        Ok(Self {
            root,
            directory,
            attempt_id: attempt_id.to_owned(),
            cap,
            used: 0,
            entries: BTreeMap::new(),
            poisoned: false,
        })
    }

    pub fn bytes_used(&self) -> u64 {
        self.used
    }

    pub fn store(
        &mut self,
        stream: LogStream,
        segment: LogSegment,
    ) -> Result<SpoolOutcome, SpoolError> {
        if self.poisoned {
            return Err(SpoolError::Poisoned);
        }
        if segment.attempt_id() != self.attempt_id || segment.stream() != stream {
            return Err(SpoolError::Identity);
        }
        let prefix = match stream {
            LogStream::Stdout => "stdout",
            LogStream::Stderr => "stderr",
            _ => return Err(SpoolError::Invalid),
        };
        let size = segment.bytes().len() as u64;
        if size == 0 || size > MAX_SEGMENT_BYTES as u64 {
            return Err(SpoolError::Invalid);
        }
        let name = format!(
            "{prefix}-{:019}-{:019}.seg",
            segment.first_sequence(),
            segment.last_sequence()
        );
        if self.entries.contains_key(&name) {
            return Err(SpoolError::Conflict);
        }
        // A byte cap alone permits millions of tiny segments and exhausts
        // inode space or the server's per-attempt upload identity budget.
        if self.entries.len() >= MAX_SPOOL_SEGMENTS {
            return Ok(SpoolOutcome::Saturated);
        }
        if size > self.cap - self.used {
            return Ok(SpoolOutcome::Saturated);
        }
        let pending = self.root.join(".pending");
        let final_path = self.root.join(&name);
        let mut created_pending = false;
        let mut linked_final = false;
        let result = (|| -> Result<(), SpoolError> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&pending)?;
            created_pending = true;
            file.write_all(segment.bytes())?;
            file.sync_all()?;
            drop(file);
            // Hard-link publication cannot overwrite an existing range. The
            // private pending alias is unlinked before the entry is readable.
            fs::hard_link(&pending, &final_path)?;
            linked_final = true;
            fs::remove_file(&pending)?;
            self.directory.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            if created_pending {
                let _ = fs::remove_file(&pending);
            }
            if linked_final {
                let _ = fs::remove_file(&final_path);
            }
            self.poisoned = true;
            return Err(error);
        }
        self.used += size;
        self.entries.insert(name.clone(), size);
        Ok(SpoolOutcome::Stored(StoredSegment {
            path: final_path,
            name,
            size,
        }))
    }

    pub fn read(&self, entry: &StoredSegment) -> Result<File, SpoolError> {
        if self.poisoned {
            return Err(SpoolError::Poisoned);
        }
        if self.entries.get(&entry.name) != Some(&entry.size)
            || entry.path != self.root.join(&entry.name)
        {
            return Err(SpoolError::Identity);
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&entry.path)?;
        safe_file(&file, entry.size)?;
        Ok(file)
    }

    pub fn remove(&mut self, entry: StoredSegment) -> Result<(), SpoolError> {
        let file = self.read(&entry)?;
        drop(file);
        if let Err(error) = fs::remove_file(&entry.path).and_then(|_| self.directory.sync_all()) {
            self.poisoned = true;
            return Err(error.into());
        }
        self.used -= entry.size;
        self.entries.remove(&entry.name);
        Ok(())
    }
}
