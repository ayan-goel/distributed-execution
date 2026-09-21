//! Durable evidence and retry identities, never recovered execution authority.
use crate::{
    control::{canonical_uuid, lower_hash, validate_phase_request},
    execution::ExecutionSpec,
};
use dispatch_protocol::v1::{Assignment, AttemptState, ReportPhaseRequest};
use prost::Message;
use ring::rand::{SecureRandom, SystemRandom};
use std::{fmt, path::Path};

mod files;
mod session;
pub use session::StoredSession;

#[derive(Debug, PartialEq, Eq)]
pub enum JournalError {
    Invalid,
    Identity,
    Conflict,
    Corrupt,
    UnsafePath,
    Busy,
    Limit,
    Poisoned,
    Random,
    Io(std::io::ErrorKind),
}
impl From<std::io::Error> for JournalError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}
impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never forward parser or OS messages containing job environment or paths.
        write!(f, "worker journal error: {self:?}")
    }
}
impl std::error::Error for JournalError {}

#[derive(Clone, Copy, Debug)]
pub struct JournalLimits {
    pub max_attempts: usize,
    pub max_bytes: u64,
}
impl Default for JournalLimits {
    fn default() -> Self {
        Self {
            max_attempts: 4096,
            max_bytes: 256 << 20,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct ExitEvidence {
    #[prost(int32, tag = "1")]
    pub exit_code: i32,
    #[prost(bool, tag = "2")]
    pub oom_killed: bool,
}

#[derive(Clone, PartialEq, Message)]
struct Record {
    #[prost(message, required, tag = "1")]
    assignment: Assignment,
    #[prost(string, tag = "2")]
    container_id: String,
    #[prost(message, optional, tag = "3")]
    exit: Option<ExitEvidence>,
    #[prost(message, repeated, tag = "4")]
    phase_reports: Vec<ReportPhaseRequest>,
}

pub struct RecoveredAttempt(Record);
impl fmt::Debug for RecoveredAttempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveredAttempt").finish_non_exhaustive()
    }
}
impl RecoveredAttempt {
    pub fn assignment(&self) -> &Assignment {
        &self.0.assignment
    }
    pub fn container_id(&self) -> Option<&str> {
        if self.0.container_id.is_empty() {
            None
        } else {
            Some(&self.0.container_id)
        }
    }
    pub fn exit(&self) -> Option<&ExitEvidence> {
        self.0.exit.as_ref()
    }
    pub fn phase_reports(&self) -> &[ReportPhaseRequest] {
        &self.0.phase_reports
    }
}

pub struct Journal {
    directory: files::Directory,
    worker_id: String,
    incarnation: Option<String>,
}
impl Journal {
    pub fn open(
        root: impl AsRef<Path>,
        worker_id: &str,
        limits: JournalLimits,
    ) -> Result<Self, JournalError> {
        if !canonical_uuid(worker_id)
            || limits.max_attempts == 0
            || limits.max_attempts > 4096
            || limits.max_bytes == 0
            || limits.max_bytes > 32u64 << 30
        {
            return Err(JournalError::Invalid);
        }
        let mut directory = files::Directory::open(root.as_ref(), limits)?;
        match directory.read(".identity")? {
            Some(saved) if saved == worker_id.as_bytes() => {}
            Some(_) => return Err(JournalError::Identity),
            None => {
                if !directory.inventory()?.is_empty() || directory.read(".session")?.is_some() {
                    return Err(JournalError::Corrupt);
                }
                directory.write(".identity", worker_id.as_bytes())?;
            }
        }
        Ok(Self {
            directory,
            worker_id: worker_id.to_string(),
            incarnation: None,
        })
    }

    pub fn attempt_ids(&self) -> Result<Vec<String>, JournalError> {
        Ok(self
            .directory
            .inventory()?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }

    pub fn load_attempt(&self, id: &str) -> Result<Option<RecoveredAttempt>, JournalError> {
        if !canonical_uuid(id) {
            return Err(JournalError::Invalid);
        }
        let Some(bytes) = self.directory.read(&format!("{id}.attempt"))? else {
            return Ok(None);
        };
        let record = Record::decode(bytes.as_slice()).map_err(|_| JournalError::Corrupt)?;
        record
            .validate(&self.worker_id, id)
            .map_err(|_| JournalError::Corrupt)?;
        // Reject unknown/duplicate/noncanonical fields even with a valid checksum;
        // future formats need an explicit version instead of silently losing data.
        if record.encode_to_vec() != bytes {
            return Err(JournalError::Corrupt);
        }
        Ok(Some(RecoveredAttempt(record)))
    }

    pub fn persist_assignment(&mut self, assignment: &Assignment) -> Result<(), JournalError> {
        let a = assignment.authority.as_ref().ok_or(JournalError::Invalid)?;
        if a.worker_id != self.worker_id {
            return Err(JournalError::Identity);
        }
        let mut normalized = assignment.clone();
        // Stored transport timing is stale after a crash. Zero it before writing
        // so a recovered record cannot masquerade as a fresh server grant.
        normalized.lease_duration_ms = 0;
        normalized.phase_remaining_ms = 0;
        normalized.server_time_unix_ms = 0;
        let record = Record {
            assignment: normalized,
            container_id: String::new(),
            exit: None,
            phase_reports: Vec::new(),
        };
        record.validate(&self.worker_id, &a.attempt_id)?;
        if let Some(saved) = self.load_attempt(&a.attempt_id)? {
            return if saved.0.assignment == record.assignment {
                Ok(())
            } else {
                Err(JournalError::Conflict)
            };
        }
        self.save(&a.attempt_id, &record)
    }

    pub fn bind_container(&mut self, id: &str, container_id: &str) -> Result<(), JournalError> {
        if !lower_hash(container_id) {
            return Err(JournalError::Invalid);
        }
        let mut record = self.required(id)?;
        if record.phase_reports.is_empty() {
            return Err(JournalError::Conflict);
        }
        if !record.container_id.is_empty() {
            return if record.container_id == container_id {
                Ok(())
            } else {
                Err(JournalError::Conflict)
            };
        }
        record.container_id = container_id.to_string();
        self.save(id, &record)
    }

    pub fn record_exit(
        &mut self,
        id: &str,
        exit_code: i32,
        oom_killed: bool,
    ) -> Result<(), JournalError> {
        if !(0..=255).contains(&exit_code) {
            return Err(JournalError::Invalid);
        }
        let mut record = self.required(id)?;
        if record.container_id.is_empty() {
            return Err(JournalError::Conflict);
        }
        let evidence = ExitEvidence {
            exit_code,
            oom_killed,
        };
        if let Some(old) = &record.exit {
            return if old == &evidence {
                Ok(())
            } else {
                Err(JournalError::Conflict)
            };
        }
        record.exit = Some(evidence);
        self.save(id, &record)
    }

    pub fn prepare_phase(
        &mut self,
        id: &str,
        phase: AttemptState,
    ) -> Result<ReportPhaseRequest, JournalError> {
        let mut record = self.required(id)?;
        if let Some(saved) = record
            .phase_reports
            .iter()
            .find(|r| r.phase == phase as i32)
        {
            return Ok(saved.clone());
        }
        let allowed = match phase {
            AttemptState::Starting => record.phase_reports.is_empty(),
            AttemptState::Running => {
                !record.phase_reports.is_empty()
                    && !record.container_id.is_empty()
                    && record.exit.is_none()
            }
            AttemptState::Finalizing => record.exit.is_some(),
            _ => return Err(JournalError::Invalid),
        };
        if !allowed {
            return Err(JournalError::Conflict);
        }
        let request = ReportPhaseRequest {
            authority: record.assignment.authority.clone(),
            event_id: new_uuid()?,
            phase: phase as i32,
            container_id: if phase == AttemptState::Starting {
                String::new()
            } else {
                record.container_id.clone()
            },
            exit_code: if phase == AttemptState::Finalizing {
                record.exit.as_ref().map(|exit| exit.exit_code)
            } else {
                None
            },
        };
        record.phase_reports.push(request.clone());
        self.save(id, &record)?;
        Ok(request)
    }

    fn required(&self, id: &str) -> Result<Record, JournalError> {
        self.load_attempt(id)?
            .map(|r| r.0)
            .ok_or(JournalError::Invalid)
    }
    fn save(&mut self, id: &str, record: &Record) -> Result<(), JournalError> {
        record.validate(&self.worker_id, id)?;
        if record.encoded_len() > files::MAX_PAYLOAD {
            return Err(JournalError::Limit);
        }
        self.directory
            .write(&format!("{id}.attempt"), &record.encode_to_vec())
    }
}

impl Record {
    fn validate(&self, worker: &str, id: &str) -> Result<(), JournalError> {
        let a = self
            .assignment
            .authority
            .as_ref()
            .ok_or(JournalError::Invalid)?;
        if a.worker_id != worker
            || a.attempt_id != id
            || !canonical_uuid(id)
            || !canonical_uuid(&a.job_id)
            || !canonical_uuid(&a.session_id)
            || a.generation == 0
            || a.generation > i64::MAX as u64
            || self.assignment.lease_duration_ms != 0
            || self.assignment.phase_remaining_ms != 0
            || self.assignment.server_time_unix_ms != 0
        {
            return Err(JournalError::Invalid);
        }
        ExecutionSpec::from_assignment(&self.assignment).map_err(|_| JournalError::Invalid)?;
        if self.phase_reports.len() > 3
            || (!self.container_id.is_empty()
                && (!lower_hash(&self.container_id) || self.phase_reports.is_empty()))
            || self.exit.as_ref().is_some_and(|exit| {
                self.container_id.is_empty() || !(0..=255).contains(&exit.exit_code)
            })
        {
            return Err(JournalError::Invalid);
        }
        let mut previous = 0;
        let mut ids = std::collections::HashSet::new();
        for (index, r) in self.phase_reports.iter().enumerate() {
            validate_phase_request(r).map_err(|_| JournalError::Invalid)?;
            if r.authority.as_ref() != Some(a)
                || r.phase <= previous
                || (index == 0 && r.phase != AttemptState::Starting as i32)
                || !ids.insert(&r.event_id)
                || (r.phase != AttemptState::Starting as i32 && r.container_id != self.container_id)
                || (r.phase == AttemptState::Finalizing as i32
                    && r.exit_code != self.exit.as_ref().map(|exit| exit.exit_code))
            {
                return Err(JournalError::Invalid);
            }
            previous = r.phase;
        }
        Ok(())
    }
}

pub(crate) fn new_uuid() -> Result<String, JournalError> {
    let mut bytes = [0u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| JournalError::Random)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}
