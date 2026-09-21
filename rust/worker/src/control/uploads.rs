use super::{canonical_uuid, lower_hash, rpc_request, ClientError, ControlClient, RPC_TIMEOUT};
use dispatch_protocol::v1::{
    ArtifactKind, AttemptAuthority, CreateUploadRequest, CreateUploadResponse,
    FinalizeUploadRequest, FinalizeUploadResponse,
};

impl ControlClient {
    pub async fn create_upload(
        &mut self,
        request: &CreateUploadRequest,
    ) -> Result<CreateUploadResponse, ClientError> {
        validate_create(request)?;
        // The caller owns durable request IDs. A retry must preserve the complete
        // declaration; this RPC never allocates a second logical upload itself.
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.create_upload(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|s| ClientError::Rpc(Box::new(s)))?
        .into_inner();
        validate_grant(request, &response)?;
        Ok(response)
    }

    pub async fn finalize_upload(
        &mut self,
        request: &FinalizeUploadRequest,
    ) -> Result<FinalizeUploadResponse, ClientError> {
        validate_finalization(request)?;
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.finalize_upload(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|s| ClientError::Rpc(Box::new(s)))?
        .into_inner();
        // A reply acknowledges only this immutable object version. It grants no
        // new execution authority and does not mean that a job result is accepted.
        validate_artifact(request, &response)?;
        Ok(response)
    }
}

pub(crate) fn validate_finalization(request: &FinalizeUploadRequest) -> Result<(), ClientError> {
    let a = request
        .authority
        .as_ref()
        .filter(|a| valid_authority(a))
        .ok_or(ClientError::Configuration)?;
    let o = request.object.as_ref().ok_or(ClientError::Configuration)?;
    if !canonical_uuid(&request.request_id)
        || !canonical_uuid(&request.upload_id)
        || !request.parts.is_empty()
        || !scoped_key(&o.key, a, &request.upload_id)
        || o.size_bytes > 64 << 20
        || !lower_hash(&o.sha256)
        || !valid_version(&o.version_id)
    {
        return Err(ClientError::Configuration);
    }
    Ok(())
}

pub(crate) fn validate_artifact(
    request: &FinalizeUploadRequest,
    response: &FinalizeUploadResponse,
) -> Result<(), ClientError> {
    if !canonical_uuid(&response.artifact_id) || response.object != request.object {
        return Err(ClientError::Response);
    }
    Ok(())
}

fn valid_authority(a: &AttemptAuthority) -> bool {
    [&a.job_id, &a.attempt_id, &a.worker_id, &a.session_id]
        .iter()
        .all(|s| canonical_uuid(s))
        && a.generation > 0
        && a.generation <= i64::MAX as u64
}
pub(crate) fn validate_create(r: &CreateUploadRequest) -> Result<(), ClientError> {
    let name = r.name.as_bytes();
    if !r.authority.as_ref().is_some_and(valid_authority)
        || !canonical_uuid(&r.request_id)
        || !matches!(
            ArtifactKind::try_from(r.kind),
            Ok(ArtifactKind::Output | ArtifactKind::Log | ArtifactKind::Manifest)
        )
        || r.part_count != 1
        || r.size_bytes > 64 << 20
        || !lower_hash(&r.sha256)
        || name.is_empty()
        || name.len() > 128
        || !name[0].is_ascii_alphanumeric()
        || !name
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(b))
    {
        return Err(ClientError::Configuration);
    }
    Ok(())
}
pub(crate) fn scoped_key(key: &str, a: &AttemptAuthority, upload: &str) -> bool {
    let parts: Vec<_> = key.split('/').collect();
    parts.len() == 8
        && parts[0] == "projects"
        && canonical_uuid(parts[1])
        && parts[2] == "jobs"
        && parts[3] == a.job_id
        && parts[4] == "attempts"
        && parts[5] == a.attempt_id
        && parts[6] == "uploads"
        && parts[7] == upload
}
pub(crate) fn validate_grant(
    r: &CreateUploadRequest,
    g: &CreateUploadResponse,
) -> Result<(), ClientError> {
    let a = r.authority.as_ref().ok_or(ClientError::Configuration)?;
    if !canonical_uuid(&g.upload_id)
        || !scoped_key(&g.object_key, a, &g.upload_id)
        || !g.parts.is_empty()
        || g.upload_url.is_empty()
        || g.upload_url.len() > 16 << 10
        || g.required_headers.len() > 64
        || g.expires_unix_ms <= 0
        || g.required_headers
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
            > 64 << 10
    {
        return Err(ClientError::Response);
    }
    Ok(())
}
pub(crate) fn valid_version(version: &str) -> bool {
    !version.is_empty()
        && version != "null"
        && version.len() <= 1024
        && version.bytes().all(|b| (33..=126).contains(&b))
}
