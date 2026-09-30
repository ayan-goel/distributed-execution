//! Verification and atomic publication of a registered dataset archive.

use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub format: String,
    pub files: Vec<DatasetFile>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetFile {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug)]
pub enum StageError {
    InvalidManifest,
    InvalidArchive,
    InvalidWorkspace,
    AlreadyPublished,
    Io(std::io::Error),
}

impl fmt::Display for StageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Paths and storage errors may reveal tenant names or worker layout.
        f.write_str("dataset staging failed")
    }
}

impl std::error::Error for StageError {}

impl From<std::io::Error> for StageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

struct PendingDirectory(PathBuf);

impl Drop for PendingDirectory {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = reopen_directories(&self.0);
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

pub fn stage_archive(
    archive_path: &Path,
    manifest: &DatasetManifest,
    destination: &Path,
) -> Result<(), StageError> {
    validate_manifest(manifest)?;
    let parent = destination.parent().ok_or(StageError::InvalidWorkspace)?;
    let parent_meta = fs::symlink_metadata(parent).map_err(|_| StageError::InvalidWorkspace)?;
    // INVARIANT: extraction happens under a worker-owned, private parent so
    // another user cannot replace path components during verification.
    if !parent_meta.is_dir()
        || parent_meta.file_type().is_symlink()
        || parent_meta.permissions().mode() & 0o077 != 0
        || parent_meta.uid() != unsafe { libc::geteuid() }
        || destination.exists()
        || fs::symlink_metadata(destination).is_ok()
    {
        return Err(StageError::InvalidWorkspace);
    }
    let archive_meta = fs::symlink_metadata(archive_path)?;
    if !archive_meta.is_file()
        || archive_meta.file_type().is_symlink()
        || archive_meta.len() > MAX_ARCHIVE_BYTES
    {
        return Err(StageError::InvalidArchive);
    }
    let nonce = crate::journal::new_uuid().map_err(|_| StageError::InvalidWorkspace)?;
    let mut staging = PendingDirectory(parent.join(format!(".staging-{nonce}")));
    fs::create_dir(&staging.0)?;
    fs::set_permissions(&staging.0, fs::Permissions::from_mode(0o700))?;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(archive_path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > MAX_ARCHIVE_BYTES {
        return Err(StageError::InvalidArchive);
    }
    let mut archive = tar::Archive::new(file);
    let mut count = 0;
    for item in archive.entries().map_err(|_| StageError::InvalidArchive)? {
        let mut entry = item.map_err(|_| StageError::InvalidArchive)?;
        let declared = manifest
            .files
            .get(count)
            .ok_or(StageError::InvalidArchive)?;
        let path = entry.path().map_err(|_| StageError::InvalidArchive)?;
        if !entry.header().entry_type().is_file()
            || path.to_str() != Some(declared.path.as_str())
            || entry.size() != declared.size_bytes
        {
            return Err(StageError::InvalidArchive);
        }
        let target = staging.0.join(&declared.path);
        if let Some(dir) = target.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)?;
        output.set_permissions(fs::Permissions::from_mode(0o600))?;
        let mut hash = Context::new(&SHA256);
        let mut copied = 0u64;
        let mut buffer = [0u8; 32 * 1024];
        loop {
            let n = entry
                .read(&mut buffer)
                .map_err(|_| StageError::InvalidArchive)?;
            if n == 0 {
                break;
            }
            copied = copied
                .checked_add(n as u64)
                .ok_or(StageError::InvalidArchive)?;
            if copied > declared.size_bytes {
                return Err(StageError::InvalidArchive);
            }
            hash.update(&buffer[..n]);
            output.write_all(&buffer[..n])?;
        }
        if copied != declared.size_bytes || hex_sha(hash.finish().as_ref()) != declared.sha256 {
            return Err(StageError::InvalidArchive);
        }
        output.sync_all()?;
        output.set_permissions(fs::Permissions::from_mode(0o444))?;
        count += 1;
    }
    if count != manifest.files.len() {
        return Err(StageError::InvalidArchive);
    }
    seal_directories(&staging.0)?;
    // Publication is a same-directory rename; readers see all verified files
    // together, and an existing immutable cache entry cannot be replaced.
    rename_noreplace(&staging.0, destination)?;
    staging.0 = PathBuf::new();
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(crate) fn validate_manifest(manifest: &DatasetManifest) -> Result<(), StageError> {
    if manifest.format != "tar.v1" || manifest.files.is_empty() || manifest.files.len() > 1024 {
        return Err(StageError::InvalidManifest);
    }
    let mut previous = "";
    let mut total = 0u64;
    let mut files = HashSet::new();
    for file in &manifest.files {
        let path = file.path.as_str();
        if path.len() > 512
            || path.is_empty()
            || path <= previous
            || path.contains(['\\', '\0'])
            || path.chars().any(char::is_control)
            || path
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || file.sha256.len() != 64
            || !file
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(StageError::InvalidManifest);
        }
        let mut ancestor = Path::new(path).parent();
        while let Some(dir) = ancestor {
            if files.contains(dir.to_str().ok_or(StageError::InvalidManifest)?) {
                return Err(StageError::InvalidManifest);
            }
            ancestor = dir.parent();
        }
        total = total
            .checked_add(file.size_bytes)
            .ok_or(StageError::InvalidManifest)?;
        if total > MAX_ARCHIVE_BYTES {
            return Err(StageError::InvalidManifest);
        }
        files.insert(path);
        previous = path;
    }
    Ok(())
}

fn seal_directories(root: &Path) -> Result<(), StageError> {
    for item in fs::read_dir(root)? {
        let path = item?.path();
        if fs::symlink_metadata(&path)?.is_dir() {
            seal_directories(&path)?;
        }
    }
    File::open(root)?.sync_all()?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o555))?;
    Ok(())
}

pub(crate) fn reopen_directories(root: &Path) -> std::io::Result<()> {
    if !fs::symlink_metadata(root)?.is_dir() {
        return Ok(());
    }
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    for item in fs::read_dir(root)? {
        let path = item?.path();
        if fs::symlink_metadata(&path)?.is_dir() {
            reopen_directories(&path)?;
        }
    }
    Ok(())
}

fn hex_sha(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(result, "{byte:02x}");
    }
    result
}

pub(crate) fn rename_noreplace(source: &Path, destination: &Path) -> Result<(), StageError> {
    let from = std::ffi::CString::new(source.as_os_str().as_bytes())
        .map_err(|_| StageError::InvalidWorkspace)?;
    let to = std::ffi::CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| StageError::InvalidWorkspace)?;
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    compile_error!("dataset staging requires an atomic no-replace rename");
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        Err(StageError::AlreadyPublished)
    } else {
        Err(StageError::Io(error))
    }
}
