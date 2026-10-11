use super::*;
use crate::control::{lower_hash, valid_etag, validate_part_request};
use dispatch_protocol::v1::GrantUploadPartRequest;

impl OutputUpload {
    pub(super) fn validate_parts(&self) -> Result<(), JournalError> {
        let mut previous = 0;
        if self.parts.len() > self.declaration.part_count as usize
            || self.declaration.part_count == 1 && !self.parts.is_empty()
        {
            return Err(JournalError::Invalid);
        }
        for part in &self.parts {
            if part.number <= previous
                || part.number > self.declaration.part_count
                || !lower_hash(&part.sha256)
                || !part.etag.is_empty() && !valid_etag(&part.etag)
            {
                return Err(JournalError::Invalid);
            }
            previous = part.number;
        }
        Ok(())
    }
}

impl Journal {
    pub fn prepare_output_part(
        &mut self,
        id: &str,
        request_id: &str,
        number: u32,
        sha256: &str,
    ) -> Result<GrantUploadPartRequest, JournalError> {
        let mut record = self.required(id)?;
        let sealed = record.completion.is_some();
        let saved = record
            .outputs
            .iter_mut()
            .find(|u| u.declaration.request_id == request_id)
            .ok_or(JournalError::Invalid)?;
        let request = GrantUploadPartRequest {
            authority: saved.declaration.authority.clone(),
            upload_id: saved.upload_id.clone(),
            number,
            sha256: sha256.into(),
        };
        validate_part_request(&request).map_err(|_| JournalError::Invalid)?;
        if saved.declaration.part_count < 2 || number > saved.declaration.part_count {
            return Err(JournalError::Invalid);
        }
        match saved.parts.binary_search_by_key(&number, |p| p.number) {
            Ok(index) => {
                return if saved.parts[index].sha256 == sha256 {
                    Ok(request)
                } else {
                    Err(JournalError::Conflict)
                }
            }
            Err(index) => {
                if sealed || saved.finalization.is_some() {
                    return Err(JournalError::Conflict);
                }
                // Persist content identity before requesting a capability. A lost
                // PUT reply may only retry the same bytes at this part number.
                saved.parts.insert(
                    index,
                    CompletedPart {
                        number,
                        etag: String::new(),
                        sha256: sha256.into(),
                    },
                );
            }
        }
        self.save(id, &record)?;
        Ok(request)
    }

    pub fn record_output_part(
        &mut self,
        request: &GrantUploadPartRequest,
        etag: &str,
    ) -> Result<(), JournalError> {
        validate_part_request(request).map_err(|_| JournalError::Invalid)?;
        if !valid_etag(etag) {
            return Err(JournalError::Invalid);
        }
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
            .find(|u| {
                u.upload_id == request.upload_id && u.declaration.authority == request.authority
            })
            .ok_or(JournalError::Conflict)?;
        let part = saved
            .parts
            .iter_mut()
            .find(|p| p.number == request.number && p.sha256 == request.sha256)
            .ok_or(JournalError::Conflict)?;
        if !part.etag.is_empty() {
            return if part.etag == etag {
                Ok(())
            } else {
                Err(JournalError::Conflict)
            };
        }
        if sealed || saved.finalization.is_some() {
            return Err(JournalError::Conflict);
        }
        // Prepared -> uploaded: save the observed ETag before completing storage.
        // The ETag is completion evidence, never a content hash or object version.
        part.etag = etag.into();
        self.save(id, &record)
    }

    pub fn prepare_multipart_finalization(
        &mut self,
        id: &str,
        request_id: &str,
    ) -> Result<FinalizeUploadRequest, JournalError> {
        let mut record = self.required(id)?;
        let sealed = record.completion.is_some();
        let saved = record
            .outputs
            .iter_mut()
            .find(|u| u.declaration.request_id == request_id)
            .ok_or(JournalError::Invalid)?;
        if saved.declaration.part_count < 2 {
            return Err(JournalError::Invalid);
        }
        if let Some(finalize) = &saved.finalization {
            return Ok(finalize.clone());
        }
        if sealed || saved.parts.len() != saved.declaration.part_count as usize {
            return Err(JournalError::Conflict);
        }
        let finalize = FinalizeUploadRequest {
            authority: saved.declaration.authority.clone(),
            request_id: new_uuid()?,
            upload_id: saved.upload_id.clone(),
            object: Some(ObjectVersion {
                key: saved.object_key.clone(),
                version_id: String::new(),
                size_bytes: saved.declaration.size_bytes,
                sha256: saved.declaration.sha256.clone(),
            }),
            parts: saved.parts.clone(),
        };
        validate_finalization(&finalize).map_err(|_| JournalError::Invalid)?;
        // Uploaded -> completing: seal one ordered completion request before RPC.
        // Recovery retries it without uploading parts or choosing a latest version.
        saved.finalization = Some(finalize.clone());
        self.save(id, &record)?;
        Ok(finalize)
    }
}

impl AsyncJournal {
    pub async fn prepare_output_plan(
        &self,
        id: String,
        name: String,
        size: u64,
        sha256: String,
        part_size: u64,
    ) -> Result<CreateUploadRequest, JournalError> {
        self.apply(move |j| j.prepare_output_plan(&id, &name, size, &sha256, part_size))
            .await
    }
    pub async fn prepare_output_part(
        &self,
        id: String,
        request_id: String,
        number: u32,
        sha256: String,
    ) -> Result<GrantUploadPartRequest, JournalError> {
        self.apply(move |j| j.prepare_output_part(&id, &request_id, number, &sha256))
            .await
    }
    pub async fn record_output_part(
        &self,
        request: GrantUploadPartRequest,
        etag: String,
    ) -> Result<(), JournalError> {
        self.apply(move |j| j.record_output_part(&request, &etag))
            .await
    }
    pub async fn prepare_multipart_finalization(
        &self,
        id: String,
        request_id: String,
    ) -> Result<FinalizeUploadRequest, JournalError> {
        self.apply(move |j| j.prepare_multipart_finalization(&id, &request_id))
            .await
    }
}
