use super::{new_uuid, Journal, JournalError};
use crate::control::canonical_uuid;
use dispatch_protocol::{
    v1::{RegisterWorkerRequest, RegisterWorkerResponse},
    VERSION,
};
use prost::Message;
use std::{collections::HashSet, fmt};

pub(super) const MAX_SESSION: usize = 64 * 1024;

#[derive(Clone, PartialEq, Message)]
struct SessionRecord {
    #[prost(message, required, tag = "1")]
    registration: RegisterWorkerRequest,
    #[prost(uint64, tag = "2")]
    generation: u64,
    #[prost(string, tag = "3")]
    previous_session_id: String,
}

pub struct StoredSession(SessionRecord);
impl fmt::Debug for StoredSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredSession").finish_non_exhaustive()
    }
}
impl StoredSession {
    pub fn registration(&self) -> &RegisterWorkerRequest {
        &self.0.registration
    }
    pub fn generation(&self) -> Option<u64> {
        (self.0.generation != 0).then_some(self.0.generation)
    }
    pub fn previous_session_id(&self) -> Option<&str> {
        (!self.0.previous_session_id.is_empty()).then_some(&self.0.previous_session_id)
    }
}

impl Journal {
    pub fn load_session(&self) -> Result<Option<StoredSession>, JournalError> {
        let Some(bytes) = self.directory.read(".session")? else {
            return Ok(None);
        };
        if bytes.len() > MAX_SESSION {
            return Err(JournalError::Corrupt);
        }
        let record = SessionRecord::decode(bytes.as_slice()).map_err(|_| JournalError::Corrupt)?;
        validate(&record.registration, &self.worker_id).map_err(|_| JournalError::Corrupt)?;
        if record.generation > i64::MAX as u64
            || (!record.previous_session_id.is_empty()
                && (!canonical_uuid(&record.previous_session_id)
                    || record.previous_session_id == record.registration.requested_session_id))
        {
            return Err(JournalError::Corrupt);
        }
        Ok(Some(StoredSession(record)))
    }

    pub fn begin_incarnation(
        &mut self,
        mut claims: RegisterWorkerRequest,
    ) -> Result<StoredSession, JournalError> {
        if self.incarnation.is_some() {
            return Err(JournalError::Conflict);
        }
        if !claims.request_id.is_empty() || !claims.requested_session_id.is_empty() {
            return Err(JournalError::Invalid);
        }
        let previous = self.load_session()?;
        // Process restarts never resume a saved incarnation, even when its
        // registration result was uncertain. The server fences it through recovery.
        claims.request_id = new_uuid()?;
        claims.requested_session_id = new_uuid()?;
        validate(&claims, &self.worker_id)?;
        let record = SessionRecord {
            registration: claims,
            generation: 0,
            previous_session_id: previous
                .map(|p| p.0.registration.requested_session_id)
                .unwrap_or_default(),
        };
        self.save_session(&record)?;
        self.incarnation = Some(record.registration.requested_session_id.clone());
        Ok(StoredSession(record))
    }

    pub fn record_registration(
        &mut self,
        response: &RegisterWorkerResponse,
    ) -> Result<(), JournalError> {
        let current = self.incarnation.as_ref().ok_or(JournalError::Conflict)?;
        let mut saved = self.load_session()?.ok_or(JournalError::Corrupt)?.0;
        let session = response.session.as_ref().ok_or(JournalError::Invalid)?;
        if &saved.registration.requested_session_id != current
            || &session.session_id != current
            || session.worker_id != self.worker_id
        {
            return Err(JournalError::Conflict);
        }
        if response.protocol_version != VERSION
            || response.session_generation == 0
            || response.session_generation > i64::MAX as u64
        {
            return Err(JournalError::Invalid);
        }
        if saved.generation != 0 {
            return if saved.generation == response.session_generation {
                Ok(())
            } else {
                Err(JournalError::Conflict)
            };
        }
        // Registration acceptance records identity only. Readiness and cleanup
        // instructions are current server state and must never be restored here.
        saved.generation = response.session_generation;
        self.save_session(&saved)
    }

    fn save_session(&mut self, record: &SessionRecord) -> Result<(), JournalError> {
        if record.encoded_len() > MAX_SESSION {
            return Err(JournalError::Limit);
        }
        self.directory.write(".session", &record.encode_to_vec())
    }
}

fn validate(r: &RegisterWorkerRequest, worker: &str) -> Result<(), JournalError> {
    let resources = r.allocatable.as_ref().ok_or(JournalError::Invalid)?;
    if r.worker_id != worker
        || !canonical_uuid(&r.request_id)
        || !canonical_uuid(&r.requested_session_id)
        || r.protocol_version != VERSION
        || !(1..=1000).contains(&r.execution_slots)
        || !(1..=1_024_000).contains(&resources.cpu_millis)
        || !(1 << 20..=16_777_216u64 << 20).contains(&resources.memory_bytes)
        || !(1 << 20..=1_073_741_824u64 << 20).contains(&resources.scratch_bytes)
        || resources.memory_bytes % (1 << 20) != 0
        || resources.scratch_bytes % (1 << 20) != 0
        || r.labels.len() > 64
        || r.labels.get("os").map(String::as_str) != Some("linux")
        || !matches!(
            r.labels.get("architecture").map(String::as_str),
            Some("amd64" | "arm64")
        )
        || r.labels.iter().any(|(key, value)| {
            key.is_empty()
                || key.len() > 128
                || !key.bytes().enumerate().all(|(n, b)| {
                    b.is_ascii_alphanumeric() || (n > 0 && matches!(b, b'.' | b'_' | b'-'))
                })
                || value.is_empty()
                || value.len() > 256
                || value.chars().any(char::is_control)
        })
    {
        return Err(JournalError::Invalid);
    }
    if r.capabilities.len() != 5 {
        return Err(JournalError::Invalid);
    }
    let caps: HashSet<_> = r.capabilities.iter().map(String::as_str).collect();
    if caps.len() != 5
        || !["docker.v1", "cpu.hard", "memory.hard", "pids.hard"]
            .iter()
            .all(|c| caps.contains(c))
        || caps.contains("scratch.soft") == caps.contains("scratch.quota")
    {
        return Err(JournalError::Invalid);
    }
    Ok(())
}
