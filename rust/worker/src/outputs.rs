//! Bounded collection of declared files after confirmed container exit.
use crate::{execution::ExecutionSpec, runtime::PreparedWorkspace};
use ring::digest::{Context, SHA256};
use std::{
    ffi::CString,
    fmt,
    fs::{File, Metadata, OpenOptions},
    io::{Read, Seek},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
};

#[derive(Debug, PartialEq, Eq)]
pub enum CollectionError {
    Configuration,
    Missing,
    Unsafe,
    TooLarge,
    Limit,
    Changed,
    Io(std::io::ErrorKind),
}
impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Workload filenames and OS diagnostics can contain private data.
        write!(f, "output collection error: {self:?}")
    }
}
impl std::error::Error for CollectionError {}
impl From<std::io::Error> for CollectionError {
    fn from(e: std::io::Error) -> Self {
        match e.raw_os_error() {
            Some(libc::ELOOP | libc::ENOTDIR) => Self::Unsafe,
            _ if e.kind() == std::io::ErrorKind::NotFound => Self::Missing,
            _ => Self::Io(e.kind()),
        }
    }
}

#[derive(Clone, Copy)]
pub struct CollectionLimits {
    pub max_file_bytes: u64,
    pub max_total_bytes: u64,
}
impl Default for CollectionLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 64 << 20,
            max_total_bytes: 8 << 30,
        }
    }
}

pub struct CollectedOutput {
    name: String,
    size: u64,
    sha256: String,
    file: File,
}
impl fmt::Debug for CollectedOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectedOutput").finish_non_exhaustive()
    }
}
impl CollectedOutput {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn size_bytes(&self) -> u64 {
        self.size
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn into_file(self) -> File {
        self.file
    }
}

/// Synchronous bounded I/O: callers must run this off lease/heartbeat tasks and
/// only after runtime stop is confirmed. Open handles do not freeze file contents;
/// transfers must retain the declared checksum and exact-size verification.
pub fn collect_outputs(
    workspace: &PreparedWorkspace,
    execution: &ExecutionSpec,
    limits: CollectionLimits,
) -> Result<Vec<CollectedOutput>, CollectionError> {
    if limits.max_file_bytes == 0
        || limits.max_file_bytes > 64 << 20
        || limits.max_total_bytes == 0
        || limits.max_total_bytes > 8 << 30
    {
        return Err(CollectionError::Configuration);
    }
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(workspace.root())?;
    let metadata = root.metadata()?;
    // SAFETY: geteuid reads the current credentials and takes no pointers.
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o022 != 0 {
        return Err(CollectionError::Unsafe);
    }
    let outputs = open_child(&root, "outputs", true)?;
    let mut collected = Vec::new();
    let mut total = 0u64;
    // ExecutionSpec already bounds this list to 64 disjoint, lexical /outputs
    // paths. Walk only those declarations; never enumerate arbitrary job files.
    for declaration in &execution.job().spec.outputs {
        let mut file = match open_declared(&outputs, &declaration.path[9..]) {
            Ok(file) => file,
            Err(CollectionError::Missing) if !declaration.required => continue,
            Err(error) => return Err(error),
        };
        let before = file.metadata()?;
        regular(&before)?;
        if before.len() > declaration.max_bytes.min(limits.max_file_bytes) {
            return Err(CollectionError::TooLarge);
        }
        total = total
            .checked_add(before.len())
            .ok_or(CollectionError::Limit)?;
        if total > limits.max_total_bytes {
            return Err(CollectionError::Limit);
        }
        let mut hash = Context::new(&SHA256);
        let mut read = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        // Read one byte beyond observed length to detect growth without following
        // an expanding file forever. Memory remains fixed regardless of file size.
        let mut source = (&mut file).take(before.len() + 1);
        loop {
            let count = source.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            read += count as u64;
            if read > before.len() {
                return Err(CollectionError::Changed);
            }
            hash.update(&buffer[..count]);
        }
        let after = file.metadata()?;
        regular(&after)?;
        if read != before.len() || stamp(&before) != stamp(&after) {
            return Err(CollectionError::Changed);
        }
        file.rewind()?;
        collected.push(CollectedOutput {
            name: declaration.name.clone(),
            size: read,
            sha256: hash
                .finish()
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            file,
        });
    }
    Ok(collected)
}

fn regular(metadata: &Metadata) -> Result<(), CollectionError> {
    // Special files may block or expose devices. Hard links could alias an input
    // or another declared output, so each accepted file must have one directory link.
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(CollectionError::Unsafe);
    }
    Ok(())
}
fn stamp(m: &Metadata) -> (u64, i64, i64, i64, i64) {
    (
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}
fn open_declared(outputs: &File, path: &str) -> Result<File, CollectionError> {
    let mut components = path.split('/').peekable();
    let mut parent = outputs.try_clone()?;
    while let Some(component) = components.next() {
        parent = open_child(&parent, component, components.peek().is_some())?;
    }
    Ok(parent)
}
fn open_child(parent: &File, component: &str, directory: bool) -> Result<File, CollectionError> {
    let name = CString::new(component).map_err(|_| CollectionError::Unsafe)?;
    // Every component is opened relative to a held directory descriptor. Reject
    // symlinks at all levels; O_NONBLOCK keeps a FIFO leaf from stalling validation.
    let flags = libc::O_RDONLY
        | libc::O_NOFOLLOW
        | libc::O_CLOEXEC
        | libc::O_NONBLOCK
        | if directory { libc::O_DIRECTORY } else { 0 };
    // SAFETY: parent owns a live directory fd; name is NUL-terminated and openat
    // retains neither pointer. Successful fd ownership transfers exactly once.
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: openat returned a newly owned valid descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}
