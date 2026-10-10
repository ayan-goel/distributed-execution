//! Linux ext4 project-quota primitive. The caller exclusively owns the empty
//! directory and project-ID namespace; never reuse IDs after uncertain setup.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Error, ErrorKind},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
};

// Linux UAPI linux/fs.h: preserve unrelated attributes when enabling project
// inheritance. These fixed-width fields also avoid native-long ABI ambiguity.
#[repr(C)]
#[derive(Default)]
struct Fsxattr {
    xflags: u32,
    extsize: u32,
    nextents: u32,
    projid: u32,
    cowextsize: u32,
    pad: [u8; 8],
}
const PROJECT_INHERIT: u32 = 0x200;
const PROJECT_QUOTA: libc::c_int = 2;

// Q_XGETQSTATV version 1 is a 160-byte, 8-byte-aligned Linux UAPI structure.
// Only its header is needed; retain space for all quota counters and padding.
#[repr(C, align(8))]
#[derive(Default)]
struct QuotaState {
    version: u8,
    pad: u8,
    flags: u16,
    incore: u32,
    unused: [u64; 19],
}

#[derive(Debug)]
pub struct ProjectQuota {
    directory: File,
    project_id: u32,
    bytes: u64,
    inodes: u64,
}

#[derive(Debug)]
pub struct QuotaUsage {
    pub bytes: u64,
    pub inodes: u64,
}

impl ProjectQuota {
    /// Configure a new, private, empty directory before creating mount children.
    /// Requires ext4 project enforcement and CAP_SYS_ADMIN on Linux >= 5.14.
    /// Failure retains any installed limits; the caller must quarantine the ID.
    pub fn attach_empty(path: &Path, project_id: u32, bytes: u64, inodes: u64) -> io::Result<Self> {
        // Zero means unlimited to quotactl. Reject rounding and signed-ID overflow
        // so the configured hard limit cannot exceed the admitted reservation.
        if project_id == 0
            || project_id > i32::MAX as u32
            || bytes == 0
            || bytes % 1024 != 0
            || inodes == 0
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid project quota limits",
            ));
        }
        if !path.is_absolute() || fs::canonicalize(path)? != path {
            return Err(invalid("quota directory must be canonical"));
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let metadata = directory.metadata()?;
        // SAFETY: geteuid has no pointer arguments and reads process credentials.
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o777 != 0o700
            || fs::read_dir(path)?.next().is_some()
        {
            return Err(invalid("quota directory must be private, owned, and empty"));
        }
        Self::verify_filesystem(&directory)?;
        let quota = Self {
            directory,
            project_id,
            bytes,
            inodes,
        };
        let mut attributes = quota.attributes()?;
        if attributes.projid != 0 || attributes.xflags & PROJECT_INHERIT != 0 {
            return Err(invalid("directory already belongs to a project"));
        }
        let previous = quota.read_quota()?;
        // INVARIANT: an existing limit or charged inode belongs to another claim.
        // Refuse collisions even when that project's current byte usage is zero.
        if previous.dqb_bhardlimit != 0
            || previous.dqb_bsoftlimit != 0
            || previous.dqb_ihardlimit != 0
            || previous.dqb_isoftlimit != 0
            || previous.dqb_curspace != 0
            || previous.dqb_curinodes != 0
        {
            return Err(invalid("project ID already claimed"));
        }
        let mut limits = empty_quota();
        limits.dqb_bhardlimit = bytes / 1024;
        limits.dqb_ihardlimit = inodes;
        limits.dqb_valid = libc::QIF_LIMITS;
        quota.control(libc::Q_SETQUOTA, &mut limits)?;
        quota.verify_limits()?;
        attributes.projid = project_id;
        attributes.xflags |= PROJECT_INHERIT;
        // SAFETY: FSSETXATTR copies the fixed-size Linux UAPI struct from this
        // live buffer. Only the new worker-owned directory is modified.
        if unsafe {
            libc::ioctl(
                quota.directory.as_raw_fd(),
                libc::_IOW::<Fsxattr>(b'X' as u32, 32),
                &attributes,
            )
        } != 0
        {
            return Err(Error::last_os_error());
        }
        quota.directory.sync_all()?;
        quota.verify()?;
        Ok(quota)
    }

    pub fn verify_filesystem(directory: &File) -> io::Result<()> {
        // SAFETY: zero initializes an integer-only output struct; fstatfs receives
        // a live descriptor and a correctly sized writable buffer.
        let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstatfs(directory.as_raw_fd(), &mut filesystem) } != 0 {
            return Err(Error::last_os_error());
        }
        if filesystem.f_type != libc::EXT4_SUPER_MAGIC {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "strict scratch requires ext4",
            ));
        }
        Self::enforcement(directory)
    }

    pub(crate) fn verify_unassigned(directory: &File) -> io::Result<()> {
        Self::verify_filesystem(directory)?;
        let attributes = Self::read_attributes(directory)?;
        if attributes.projid != 0 || attributes.xflags & PROJECT_INHERIT != 0 {
            return Err(invalid("directory already belongs to a project"));
        }
        Ok(())
    }

    pub fn verify(&self) -> io::Result<()> {
        let attributes = self.attributes()?;
        if attributes.projid != self.project_id || attributes.xflags & PROJECT_INHERIT == 0 {
            return Err(invalid("project inheritance changed"));
        }
        self.verify_limits()
    }

    pub fn byte_limit(&self) -> u64 {
        self.bytes
    }

    pub fn verify_directory(&self, path: &Path) -> io::Result<()> {
        let pinned = self.directory.metadata()?;
        let actual = fs::symlink_metadata(path)?;
        // Bind the quota proof to the inode Docker will mount. A renamed or
        // replaced path must not inherit the old descriptor's enforcement claim.
        if !path.is_absolute()
            || fs::canonicalize(path)? != path
            || !actual.is_dir()
            || (pinned.dev(), pinned.ino()) != (actual.dev(), actual.ino())
        {
            return Err(invalid("quota directory identity changed"));
        }
        self.verify()
    }

    pub fn usage(&self) -> io::Result<QuotaUsage> {
        let quota = self.read_quota()?;
        Ok(QuotaUsage {
            bytes: quota.dqb_curspace,
            inodes: quota.dqb_curinodes,
        })
    }

    fn verify_limits(&self) -> io::Result<()> {
        self.verify_enforcement()?;
        let quota = self.read_quota()?;
        if quota.dqb_bhardlimit != self.bytes / 1024
            || quota.dqb_bsoftlimit != 0
            || quota.dqb_ihardlimit != self.inodes
            || quota.dqb_isoftlimit != 0
        {
            return Err(invalid("project quota limits changed"));
        }
        Ok(())
    }

    fn verify_enforcement(&self) -> io::Result<()> {
        Self::enforcement(&self.directory)
    }

    fn enforcement(directory: &File) -> io::Result<()> {
        let mut state = QuotaState {
            version: 1,
            ..Default::default()
        };
        // INVARIANT: ext4 accounting can remain active with enforcement disabled.
        // Read back both project flags rather than trusting stored hard limits.
        // SAFETY: version 1 copies the fixed-size UAPI state into this aligned
        // live buffer; the descriptor pins the filesystem under verification.
        let result = unsafe {
            libc::syscall(
                libc::SYS_quotactl_fd,
                directory.as_raw_fd(),
                libc::QCMD(((b'X' as i32) << 8) + 8, PROJECT_QUOTA),
                0,
                &mut state as *mut QuotaState,
            )
        };
        if result != 0 {
            return Err(Error::last_os_error());
        }
        if state.version != 1 || state.flags & 0x30 != 0x30 {
            return Err(invalid("project quota enforcement is disabled"));
        }
        Ok(())
    }

    fn attributes(&self) -> io::Result<Fsxattr> {
        Self::read_attributes(&self.directory)
    }

    fn read_attributes(directory: &File) -> io::Result<Fsxattr> {
        let mut attributes = Fsxattr::default();
        // SAFETY: FSGETXATTR writes exactly the Linux UAPI structure into this
        // live, correctly aligned buffer; the descriptor pins our directory.
        if unsafe {
            libc::ioctl(
                directory.as_raw_fd(),
                libc::_IOR::<Fsxattr>(b'X' as u32, 31),
                &mut attributes,
            )
        } != 0
        {
            return Err(Error::last_os_error());
        }
        Ok(attributes)
    }

    fn read_quota(&self) -> io::Result<libc::dqblk> {
        let mut quota = empty_quota();
        self.control(libc::Q_GETQUOTA, &mut quota)?;
        Ok(quota)
    }

    fn control(&self, operation: libc::c_int, quota: &mut libc::dqblk) -> io::Result<()> {
        // quotactl_fd binds the operation to this filesystem without parsing
        // device names. An unsupported syscall or disabled quotas fails closed.
        // SAFETY: the syscall copies a Linux dqblk through a live buffer; its
        // operation and project type are fixed by this module, never job input.
        let result = unsafe {
            libc::syscall(
                libc::SYS_quotactl_fd,
                self.directory.as_raw_fd(),
                libc::QCMD(operation, PROJECT_QUOTA),
                self.project_id,
                quota as *mut libc::dqblk,
            )
        };
        if result != 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }
}

fn empty_quota() -> libc::dqblk {
    // SAFETY: Linux dqblk contains only integer fields; zero is a valid initial
    // output buffer and leaves usage fields untouched when setting QIF_LIMITS.
    unsafe { std::mem::zeroed() }
}

fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}
