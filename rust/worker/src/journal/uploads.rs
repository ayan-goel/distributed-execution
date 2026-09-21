use super::{new_uuid, AsyncJournal, Journal, JournalError, Record};
use crate::{
    control::{
        canonical_uuid, scoped_key, validate_artifact, validate_create, validate_finalization,
        validate_grant,
    },
    execution::ExecutionSpec,
};
use dispatch_protocol::v1::{
    ArtifactKind, AttemptState, CreateUploadRequest, CreateUploadResponse, FinalizeUploadRequest,
    FinalizeUploadResponse, ObjectVersion,
};
use prost::Message;
use std::{collections::HashSet, fmt};

#[derive(Clone, PartialEq, Message)]
#[prost(skip_debug)]
pub struct OutputUpload {
    #[prost(message, required, tag = "1")]
    declaration: CreateUploadRequest,
    #[prost(string, tag = "2")]
    upload_id: String,
    #[prost(string, tag = "3")]
    object_key: String,
    #[prost(message, optional, tag = "4")]
    finalization: Option<FinalizeUploadRequest>,
    #[prost(message, optional, tag = "5")]
    response: Option<FinalizeUploadResponse>,
}
impl fmt::Debug for OutputUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputUpload").finish_non_exhaustive()
    }
}
impl OutputUpload {
    pub fn declaration(&self) -> &CreateUploadRequest {
        &self.declaration
    }
    pub fn upload_id(&self) -> Option<&str> {
        (!self.upload_id.is_empty()).then_some(self.upload_id.as_str())
    }
    pub fn object_key(&self) -> Option<&str> {
        (!self.object_key.is_empty()).then_some(self.object_key.as_str())
    }
    pub fn finalization(&self) -> Option<&FinalizeUploadRequest> {
        self.finalization.as_ref()
    }
    pub fn response(&self) -> Option<&FinalizeUploadResponse> {
        self.response.as_ref()
    }
}

impl Journal {
    pub fn prepare_output(
        &mut self,
        id: &str,
        name: &str,
        size: u64,
        sha256: &str,
    ) -> Result<CreateUploadRequest, JournalError> {
        let mut record = self.required(id)?;
        if let Some(saved) = record.outputs.iter().find(|u| u.declaration.name == name) {
            return if saved.declaration.size_bytes == size && saved.declaration.sha256 == sha256 {
                Ok(saved.declaration.clone())
            } else {
                Err(JournalError::Conflict)
            };
        }
        // Once completion evidence is sealed, a new upload cannot revise its
        // output set. Exact retries remain readable for resolving old operations.
        if record.completion.is_some() {
            return Err(JournalError::Conflict);
        }
        let request = CreateUploadRequest {
            authority: record.assignment.authority.clone(),
            request_id: new_uuid()?,
            name: name.into(),
            kind: ArtifactKind::Output as i32,
            size_bytes: size,
            sha256: sha256.into(),
            part_count: 1,
        };
        record.outputs.push(OutputUpload {
            declaration: request.clone(),
            upload_id: String::new(),
            object_key: String::new(),
            finalization: None,
            response: None,
        });
        self.save(id, &record)?;
        Ok(request)
    }

    pub fn record_output_grant(
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
            .outputs
            .iter_mut()
            .find(|u| &u.declaration == request)
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
        // Keep only stable scope. Expiring URLs and signed headers are bearer
        // capabilities; recovery obtains a fresh grant for this same declaration.
        saved.upload_id = grant.upload_id.clone();
        saved.object_key = grant.object_key.clone();
        self.save(id, &record)
    }

    pub fn prepare_output_finalization(
        &mut self,
        id: &str,
        request_id: &str,
        object: &ObjectVersion,
    ) -> Result<FinalizeUploadRequest, JournalError> {
        let mut record = self.required(id)?;
        let sealed = record.completion.is_some();
        let saved = record
            .outputs
            .iter_mut()
            .find(|u| u.declaration.request_id == request_id)
            .ok_or(JournalError::Invalid)?;
        if let Some(request) = &saved.finalization {
            return if request.object.as_ref() == Some(object) {
                Ok(request.clone())
            } else {
                Err(JournalError::Conflict)
            };
        }
        if sealed {
            return Err(JournalError::Conflict);
        }
        let request = FinalizeUploadRequest {
            authority: saved.declaration.authority.clone(),
            request_id: new_uuid()?,
            upload_id: saved.upload_id.clone(),
            object: Some(object.clone()),
            parts: Vec::new(),
        };
        // Save the exact observed version before any verification RPC. A lost
        // reply must never select the latest key version or repeat the HTTP PUT.
        saved.finalization = Some(request.clone());
        self.save(id, &record)?;
        Ok(request)
    }

    pub fn record_output_response(
        &mut self,
        request: &FinalizeUploadRequest,
        response: &FinalizeUploadResponse,
    ) -> Result<(), JournalError> {
        let id = &request
            .authority
            .as_ref()
            .ok_or(JournalError::Invalid)?
            .attempt_id;
        let mut record = self.required(id)?;
        let saved = record
            .outputs
            .iter_mut()
            .find(|u| u.finalization.as_ref() == Some(request))
            .ok_or(JournalError::Conflict)?;
        validate_artifact(request, response).map_err(|_| JournalError::Invalid)?;
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
}

impl Record {
    pub(super) fn validate_outputs(&self) -> Result<(), JournalError> {
        if self.outputs.is_empty() {
            return Ok(());
        }
        if self.outputs.len() > 64
            || self.phase_reports.last().map(|r| r.phase) != Some(AttemptState::Finalizing as i32)
        {
            return Err(JournalError::Invalid);
        }
        let execution =
            ExecutionSpec::from_assignment(&self.assignment).map_err(|_| JournalError::Invalid)?;
        let mut names = HashSet::new();
        let mut requests = HashSet::new();
        let mut uploads = HashSet::new();
        let mut artifacts = HashSet::new();
        for output in &self.outputs {
            let r = &output.declaration;
            validate_create(r).map_err(|_| JournalError::Invalid)?;
            let declared = execution
                .job()
                .spec
                .outputs
                .iter()
                .find(|o| o.name == r.name)
                .ok_or(JournalError::Invalid)?;
            if r.authority != self.assignment.authority
                || r.kind != ArtifactKind::Output as i32
                || r.size_bytes > declared.max_bytes
                || !names.insert(&r.name)
                || !requests.insert(&r.request_id)
            {
                return Err(JournalError::Invalid);
            }
            if output.upload_id.is_empty() {
                if !output.object_key.is_empty()
                    || output.finalization.is_some()
                    || output.response.is_some()
                {
                    return Err(JournalError::Invalid);
                }
                continue;
            }
            let a = r.authority.as_ref().ok_or(JournalError::Invalid)?;
            if !canonical_uuid(&output.upload_id)
                || !scoped_key(&output.object_key, a, &output.upload_id)
                || !uploads.insert(&output.upload_id)
            {
                return Err(JournalError::Invalid);
            }
            let Some(finalize) = &output.finalization else {
                if output.response.is_some() {
                    return Err(JournalError::Invalid);
                }
                continue;
            };
            validate_finalization(finalize).map_err(|_| JournalError::Invalid)?;
            let object = finalize.object.as_ref().ok_or(JournalError::Invalid)?;
            if finalize.authority != r.authority
                || finalize.upload_id != output.upload_id
                || object.key != output.object_key
                || object.size_bytes != r.size_bytes
                || object.sha256 != r.sha256
                || !requests.insert(&finalize.request_id)
            {
                return Err(JournalError::Invalid);
            }
            if let Some(response) = &output.response {
                validate_artifact(finalize, response).map_err(|_| JournalError::Invalid)?;
                if !artifacts.insert(&response.artifact_id) {
                    return Err(JournalError::Invalid);
                }
            }
        }
        Ok(())
    }
}

impl AsyncJournal {
    pub async fn prepare_output(
        &self,
        id: String,
        name: String,
        size: u64,
        sha256: String,
    ) -> Result<CreateUploadRequest, JournalError> {
        self.apply(move |j| j.prepare_output(&id, &name, size, &sha256))
            .await
    }
    pub async fn record_output_grant(
        &self,
        request: CreateUploadRequest,
        grant: CreateUploadResponse,
    ) -> Result<(), JournalError> {
        self.apply(move |j| j.record_output_grant(&request, &grant))
            .await
    }
    pub async fn prepare_output_finalization(
        &self,
        id: String,
        request_id: String,
        object: ObjectVersion,
    ) -> Result<FinalizeUploadRequest, JournalError> {
        self.apply(move |j| j.prepare_output_finalization(&id, &request_id, &object))
            .await
    }
    pub async fn record_output_response(
        &self,
        request: FinalizeUploadRequest,
        response: FinalizeUploadResponse,
    ) -> Result<(), JournalError> {
        self.apply(move |j| j.record_output_response(&request, &response))
            .await
    }
}
