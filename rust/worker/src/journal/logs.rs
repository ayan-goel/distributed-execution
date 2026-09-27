use super::{new_uuid, AsyncJournal, Journal, JournalError, Record};
use crate::control::{
    canonical_uuid,
    logs::{validate_request, validate_response},
    scoped_key, validate_artifact, validate_create, validate_finalization, validate_grant,
};
use dispatch_protocol::v1::{
    ArtifactKind, AttemptState, CreateUploadRequest, CreateUploadResponse, Decision,
    FinalizeUploadRequest, FinalizeUploadResponse, LogGap, LogStream, MutationResponse,
    ObjectVersion, RegisterLogSegmentRequest,
};
use prost::Message;
use std::{collections::HashSet, fmt};

#[derive(Clone, PartialEq, Message)]
#[prost(skip_debug)]
pub struct LogUpload {
    #[prost(message, required, tag = "1")]
    declaration: CreateUploadRequest,
    #[prost(int32, tag = "2")]
    stream: i32,
    #[prost(uint64, tag = "3")]
    first_sequence: u64,
    #[prost(uint64, tag = "4")]
    last_sequence: u64,
    #[prost(message, repeated, tag = "5")]
    gaps: Vec<LogGap>,
    #[prost(string, tag = "6")]
    upload_id: String,
    #[prost(string, tag = "7")]
    object_key: String,
    #[prost(message, optional, tag = "8")]
    finalization: Option<FinalizeUploadRequest>,
    #[prost(message, optional, tag = "9")]
    response: Option<FinalizeUploadResponse>,
    #[prost(message, optional, tag = "10")]
    registration: Option<RegisterLogSegmentRequest>,
    #[prost(bool, tag = "11")]
    registered: bool,
}
impl fmt::Debug for LogUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogUpload").finish_non_exhaustive()
    }
}
impl LogUpload {
    pub fn declaration(&self) -> &CreateUploadRequest {
        &self.declaration
    }
    pub fn stream(&self) -> LogStream {
        LogStream::try_from(self.stream).expect("validated log stream")
    }
    pub fn first_sequence(&self) -> u64 {
        self.first_sequence
    }
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }
    pub fn gaps(&self) -> &[LogGap] {
        &self.gaps
    }
    pub fn upload_id(&self) -> Option<&str> {
        (!self.upload_id.is_empty()).then_some(&self.upload_id)
    }
    pub fn object_key(&self) -> Option<&str> {
        (!self.object_key.is_empty()).then_some(&self.object_key)
    }
    pub fn finalization(&self) -> Option<&FinalizeUploadRequest> {
        self.finalization.as_ref()
    }
    pub fn response(&self) -> Option<&FinalizeUploadResponse> {
        self.response.as_ref()
    }
    pub fn registration(&self) -> Option<&RegisterLogSegmentRequest> {
        self.registration.as_ref()
    }
    pub fn registered(&self) -> bool {
        self.registered
    }
}

impl Journal {
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_log(
        &mut self,
        id: &str,
        stream: LogStream,
        first: u64,
        last: u64,
        gaps: &[LogGap],
        size: u64,
        sha256: &str,
    ) -> Result<CreateUploadRequest, JournalError> {
        let mut record = self.required(id)?;
        if let Some(saved) = record
            .logs
            .iter()
            .find(|s| s.stream == stream as i32 && s.first_sequence == first)
        {
            return if saved.last_sequence == last
                && saved.gaps == gaps
                && saved.declaration.size_bytes == size
                && saved.declaration.sha256 == sha256
            {
                Ok(saved.declaration.clone())
            } else {
                Err(JournalError::Conflict)
            };
        }
        // INVARIANT: a later segment cannot jump ahead of a missing or uncertain
        // registration. The server catalog must see contiguous ranges per stream.
        let previous = record.logs.iter().rev().find(|s| s.stream == stream as i32);
        let phase = record.phase_reports.last().map(|r| r.phase);
        if record.completion.is_some()
            || record.logs.len() >= 1024
            || !matches!(phase, Some(p) if p == AttemptState::Starting as i32 || p == AttemptState::Running as i32 || p == AttemptState::Finalizing as i32)
            || previous
                .is_some_and(|p| !p.registered || p.last_sequence.checked_add(1) != Some(first))
            || (previous.is_none() && first != 1)
            || size == 0
            || size > 1 << 20
        {
            return Err(JournalError::Invalid);
        }
        let request = CreateUploadRequest {
            authority: record.assignment.authority.clone(),
            request_id: new_uuid()?,
            name: match stream {
                LogStream::Stdout => "stdout",
                LogStream::Stderr => "stderr",
                _ => return Err(JournalError::Invalid),
            }
            .into(),
            kind: ArtifactKind::Log as i32,
            size_bytes: size,
            sha256: sha256.into(),
            part_count: 1,
        };
        let probe = RegisterLogSegmentRequest {
            authority: request.authority.clone(),
            request_id: request.request_id.clone(),
            artifact_id: request.request_id.clone(),
            stream: stream as i32,
            first_sequence: first,
            last_sequence: last,
            gaps: gaps.to_vec(),
        };
        validate_create(&request).map_err(|_| JournalError::Invalid)?;
        validate_request(&probe).map_err(|_| JournalError::Invalid)?;
        record.logs.push(LogUpload {
            declaration: request.clone(),
            stream: stream as i32,
            first_sequence: first,
            last_sequence: last,
            gaps: gaps.to_vec(),
            upload_id: String::new(),
            object_key: String::new(),
            finalization: None,
            response: None,
            registration: None,
            registered: false,
        });
        self.save(id, &record)?;
        Ok(request)
    }

    pub fn record_log_grant(
        &mut self,
        request: &CreateUploadRequest,
        grant: &CreateUploadResponse,
    ) -> Result<(), JournalError> {
        validate_grant(request, grant).map_err(|_| JournalError::Invalid)?;
        let id = &request
            .authority
            .as_ref()
            .ok_or(JournalError::Invalid)?
            .attempt_id;
        let mut record = self.required(id)?;
        let sealed = record.completion.is_some();
        let saved = record
            .logs
            .iter_mut()
            .find(|s| &s.declaration == request)
            .ok_or(JournalError::Conflict)?;
        if !saved.upload_id.is_empty() {
            return if saved.upload_id == grant.upload_id && saved.object_key == grant.object_key {
                Ok(())
            } else {
                Err(JournalError::Conflict)
            };
        }
        if sealed {
            return Err(JournalError::Conflict);
        }
        // Persist only stable scope. Signed URLs are short-lived bearer grants.
        saved.upload_id = grant.upload_id.clone();
        saved.object_key = grant.object_key.clone();
        self.save(id, &record)
    }

    pub fn prepare_log_finalization(
        &mut self,
        id: &str,
        request_id: &str,
        object: &ObjectVersion,
    ) -> Result<FinalizeUploadRequest, JournalError> {
        let mut record = self.required(id)?;
        let sealed = record.completion.is_some();
        let saved = record
            .logs
            .iter_mut()
            .find(|s| s.declaration.request_id == request_id)
            .ok_or(JournalError::Invalid)?;
        if let Some(request) = &saved.finalization {
            return if request.object.as_ref() == Some(object) {
                Ok(request.clone())
            } else {
                Err(JournalError::Conflict)
            };
        }
        if sealed
            || saved.upload_id.is_empty()
            || object.key != saved.object_key
            || object.size_bytes != saved.declaration.size_bytes
            || object.sha256 != saved.declaration.sha256
        {
            return Err(JournalError::Conflict);
        }
        let request = FinalizeUploadRequest {
            authority: saved.declaration.authority.clone(),
            request_id: new_uuid()?,
            upload_id: saved.upload_id.clone(),
            object: Some(object.clone()),
            parts: Vec::new(),
        };
        validate_finalization(&request).map_err(|_| JournalError::Invalid)?;
        // Pin the observed version before verification; never re-PUT after an
        // uncertain finalize reply, which would mint a different version.
        saved.finalization = Some(request.clone());
        self.save(id, &record)?;
        Ok(request)
    }

    pub fn record_log_response(
        &mut self,
        request: &FinalizeUploadRequest,
        response: &FinalizeUploadResponse,
    ) -> Result<(), JournalError> {
        validate_artifact(request, response).map_err(|_| JournalError::Invalid)?;
        let id = &request
            .authority
            .as_ref()
            .ok_or(JournalError::Invalid)?
            .attempt_id;
        let mut record = self.required(id)?;
        let saved = record
            .logs
            .iter_mut()
            .find(|s| s.finalization.as_ref() == Some(request))
            .ok_or(JournalError::Conflict)?;
        if let Some(old) = &saved.response {
            return if old == response {
                Ok(())
            } else {
                Err(JournalError::Conflict)
            };
        }
        saved.response = Some(response.clone());
        self.save(id, &record)
    }

    pub fn prepare_log_registration(
        &mut self,
        id: &str,
        request_id: &str,
    ) -> Result<RegisterLogSegmentRequest, JournalError> {
        let mut record = self.required(id)?;
        let sealed = record.completion.is_some();
        let saved = record
            .logs
            .iter_mut()
            .find(|s| s.declaration.request_id == request_id)
            .ok_or(JournalError::Invalid)?;
        if let Some(request) = &saved.registration {
            return Ok(request.clone());
        }
        if sealed {
            return Err(JournalError::Conflict);
        }
        let response = saved.response.as_ref().ok_or(JournalError::Conflict)?;
        let request = RegisterLogSegmentRequest {
            authority: saved.declaration.authority.clone(),
            request_id: new_uuid()?,
            artifact_id: response.artifact_id.clone(),
            stream: saved.stream,
            first_sequence: saved.first_sequence,
            last_sequence: saved.last_sequence,
            gaps: saved.gaps.clone(),
        };
        validate_request(&request).map_err(|_| JournalError::Invalid)?;
        saved.registration = Some(request.clone());
        self.save(id, &record)?;
        Ok(request)
    }

    pub fn record_log_registration(
        &mut self,
        request: &RegisterLogSegmentRequest,
        decision: Decision,
        state: AttemptState,
    ) -> Result<(), JournalError> {
        validate_response(
            request,
            MutationResponse {
                decision: decision as i32,
                state: state as i32,
            },
        )
        .map_err(|_| JournalError::Invalid)?;
        if decision != Decision::Accepted {
            return Err(JournalError::Conflict);
        }
        let id = &request
            .authority
            .as_ref()
            .ok_or(JournalError::Invalid)?
            .attempt_id;
        let mut record = self.required(id)?;
        let saved = record
            .logs
            .iter_mut()
            .find(|s| s.registration.as_ref() == Some(request))
            .ok_or(JournalError::Conflict)?;
        if saved.registered {
            return Ok(());
        }
        saved.registered = true;
        self.save(id, &record)
    }
}

impl Record {
    pub(super) fn validate_logs(&self) -> Result<(), JournalError> {
        // INVARIANT: bound durable metadata independently of spool turnover;
        // unbounded historical segments would exhaust the 5 MiB record budget.
        if self.logs.len() > 1024 || (!self.logs.is_empty() && self.phase_reports.is_empty()) {
            return Err(JournalError::Invalid);
        }
        let mut ids = HashSet::new();
        let mut uploads = HashSet::new();
        let mut artifacts = HashSet::new();
        let mut last = [0u64; 2];
        let mut previous_registered = [true; 2];
        for log in &self.logs {
            let r = &log.declaration;
            validate_create(r).map_err(|_| JournalError::Invalid)?;
            let index = match LogStream::try_from(log.stream) {
                Ok(LogStream::Stdout) => 0,
                Ok(LogStream::Stderr) => 1,
                _ => return Err(JournalError::Invalid),
            };
            if r.authority != self.assignment.authority
                || r.kind != ArtifactKind::Log as i32
                || r.name != if index == 0 { "stdout" } else { "stderr" }
                || r.size_bytes == 0
                || r.size_bytes > 1 << 20
                || !ids.insert(&r.request_id)
                || log.first_sequence != last[index] + 1
                || !previous_registered[index]
            {
                return Err(JournalError::Invalid);
            }
            last[index] = log.last_sequence;
            previous_registered[index] = log.registered;
            let probe = RegisterLogSegmentRequest {
                authority: r.authority.clone(),
                request_id: r.request_id.clone(),
                artifact_id: r.request_id.clone(),
                stream: log.stream,
                first_sequence: log.first_sequence,
                last_sequence: log.last_sequence,
                gaps: log.gaps.clone(),
            };
            validate_request(&probe).map_err(|_| JournalError::Invalid)?;
            if log.upload_id.is_empty() {
                if !log.object_key.is_empty()
                    || log.finalization.is_some()
                    || log.response.is_some()
                    || log.registration.is_some()
                    || log.registered
                {
                    return Err(JournalError::Invalid);
                }
                continue;
            }
            let a = r.authority.as_ref().ok_or(JournalError::Invalid)?;
            if !canonical_uuid(&log.upload_id)
                || !scoped_key(&log.object_key, a, &log.upload_id)
                || !uploads.insert(&log.upload_id)
            {
                return Err(JournalError::Invalid);
            }
            let Some(finalize) = &log.finalization else {
                if log.response.is_some() || log.registration.is_some() || log.registered {
                    return Err(JournalError::Invalid);
                }
                continue;
            };
            validate_finalization(finalize).map_err(|_| JournalError::Invalid)?;
            let object = finalize.object.as_ref().ok_or(JournalError::Invalid)?;
            if finalize.authority != r.authority
                || finalize.upload_id != log.upload_id
                || object.key != log.object_key
                || object.size_bytes != r.size_bytes
                || object.sha256 != r.sha256
                || !ids.insert(&finalize.request_id)
            {
                return Err(JournalError::Invalid);
            }
            let Some(response) = &log.response else {
                if log.registration.is_some() || log.registered {
                    return Err(JournalError::Invalid);
                }
                continue;
            };
            validate_artifact(finalize, response).map_err(|_| JournalError::Invalid)?;
            if !artifacts.insert(&response.artifact_id) {
                return Err(JournalError::Invalid);
            }
            let Some(register) = &log.registration else {
                if log.registered {
                    return Err(JournalError::Invalid);
                }
                continue;
            };
            validate_request(register).map_err(|_| JournalError::Invalid)?;
            if register.authority != r.authority
                || register.artifact_id != response.artifact_id
                || register.stream != log.stream
                || register.first_sequence != log.first_sequence
                || register.last_sequence != log.last_sequence
                || register.gaps != log.gaps
                || !ids.insert(&register.request_id)
            {
                return Err(JournalError::Invalid);
            }
        }
        Ok(())
    }
}

impl AsyncJournal {
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_log(
        &self,
        id: String,
        stream: LogStream,
        first: u64,
        last: u64,
        gaps: Vec<LogGap>,
        size: u64,
        sha256: String,
    ) -> Result<CreateUploadRequest, JournalError> {
        self.apply(move |j| j.prepare_log(&id, stream, first, last, &gaps, size, &sha256))
            .await
    }
    pub async fn record_log_grant(
        &self,
        request: CreateUploadRequest,
        grant: CreateUploadResponse,
    ) -> Result<(), JournalError> {
        self.apply(move |j| j.record_log_grant(&request, &grant))
            .await
    }
    pub async fn prepare_log_finalization(
        &self,
        id: String,
        request_id: String,
        object: ObjectVersion,
    ) -> Result<FinalizeUploadRequest, JournalError> {
        self.apply(move |j| j.prepare_log_finalization(&id, &request_id, &object))
            .await
    }
    pub async fn record_log_response(
        &self,
        request: FinalizeUploadRequest,
        response: FinalizeUploadResponse,
    ) -> Result<(), JournalError> {
        self.apply(move |j| j.record_log_response(&request, &response))
            .await
    }
    pub async fn prepare_log_registration(
        &self,
        id: String,
        request_id: String,
    ) -> Result<RegisterLogSegmentRequest, JournalError> {
        self.apply(move |j| j.prepare_log_registration(&id, &request_id))
            .await
    }
    pub async fn record_log_registration(
        &self,
        request: RegisterLogSegmentRequest,
        decision: Decision,
        state: AttemptState,
    ) -> Result<(), JournalError> {
        self.apply(move |j| j.record_log_registration(&request, decision, state))
            .await
    }
}
