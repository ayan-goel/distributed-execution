use super::{canonical_uuid, lower_hash, rpc_request, ClientError, ControlClient, RPC_TIMEOUT};
use dispatch_protocol::v1::{
    ArtifactKind, AttemptAuthority, CreateUploadRequest, CreateUploadResponse,
    FinalizeUploadRequest, FinalizeUploadResponse, GrantUploadPartRequest, UploadPart,
};

impl ControlClient {
    pub async fn grant_upload_part(
        &mut self,
        request: &GrantUploadPartRequest,
    ) -> Result<UploadPart, ClientError> {
        validate_part_request(request)?;
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.grant_upload_part(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|s| ClientError::Rpc(Box::new(s)))?
        .into_inner();
        validate_part_grant(request, &response)?;
        Ok(response)
    }
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
        || !valid_completion_parts(request)
        || !scoped_key(&o.key, a, &request.upload_id)
        || o.size_bytes > 64 << 20
        || !lower_hash(&o.sha256)
        || (request.parts.is_empty() && !valid_version(&o.version_id))
        || (!request.parts.is_empty() && !o.version_id.is_empty())
    {
        return Err(ClientError::Configuration);
    }
    Ok(())
}

pub(crate) fn validate_artifact(
    request: &FinalizeUploadRequest,
    response: &FinalizeUploadResponse,
) -> Result<(), ClientError> {
    let matches = if request.parts.is_empty() {
        response.object == request.object
    } else {
        response
            .object
            .as_ref()
            .zip(request.object.as_ref())
            .is_some_and(|(actual, expected)| {
                actual.key == expected.key
                    && actual.size_bytes == expected.size_bytes
                    && actual.sha256 == expected.sha256
                    && valid_version(&actual.version_id)
            })
    };
    if !canonical_uuid(&response.artifact_id) || !matches {
        return Err(ClientError::Response);
    }
    Ok(())
}

pub(crate) fn valid_authority(a: &AttemptAuthority) -> bool {
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
        || !valid_part_plan(r)
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
    {
        return Err(ClientError::Response);
    }
    if r.part_count > 1 {
        if !g.upload_url.is_empty()
            || !g.required_headers.is_empty()
            || g.expires_unix_ms != 0
            || g.part_count != r.part_count
            || g.part_size_bytes != r.part_size_bytes
        {
            return Err(ClientError::Response);
        }
    } else if g.part_count != 0
        || g.part_size_bytes != 0
        || !bounded_capability(&g.upload_url, &g.required_headers, g.expires_unix_ms)
    {
        return Err(ClientError::Response);
    }
    Ok(())
}

fn valid_part_plan(r: &CreateUploadRequest) -> bool {
    if r.part_count == 1 {
        return r.part_size_bytes == 0;
    }
    r.kind == ArtifactKind::Output as i32
        && (2..=10000).contains(&r.part_count)
        && r.size_bytes > 0
        && (5 << 20..=64 << 20).contains(&r.part_size_bytes)
        && u64::from(r.part_count) == 1 + (r.size_bytes - 1) / r.part_size_bytes
}

fn valid_completion_parts(r: &FinalizeUploadRequest) -> bool {
    r.parts.is_empty()
        || ((2..=10000).contains(&r.parts.len())
            && r.parts.iter().enumerate().all(|(i, p)| {
                p.number == i as u32 + 1 && valid_etag(&p.etag) && lower_hash(&p.sha256)
            }))
}

fn bounded_capability(
    url: &str,
    headers: &std::collections::HashMap<String, String>,
    expires: i64,
) -> bool {
    !url.is_empty()
        && url.len() <= 16 << 10
        && headers.len() <= 64
        && expires > 0
        && headers
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
            <= 64 << 10
}

pub(crate) fn validate_part_request(r: &GrantUploadPartRequest) -> Result<(), ClientError> {
    if !r.authority.as_ref().is_some_and(valid_authority)
        || !canonical_uuid(&r.upload_id)
        || !(1..=10000).contains(&r.number)
        || !lower_hash(&r.sha256)
    {
        return Err(ClientError::Configuration);
    }
    Ok(())
}

fn validate_part_grant(r: &GrantUploadPartRequest, g: &UploadPart) -> Result<(), ClientError> {
    if g.number != r.number || !bounded_capability(&g.url, &g.required_headers, g.expires_unix_ms) {
        return Err(ClientError::Response);
    }
    Ok(())
}
pub(crate) fn valid_version(version: &str) -> bool {
    version != "null" && valid_etag(version)
}

pub(crate) fn valid_etag(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && value.bytes().all(|b| (33..=126).contains(&b))
}

#[cfg(test)]
mod multipart_tests {
    use super::*;
    use dispatch_protocol::v1::{CompletedPart, ObjectVersion};

    fn declaration() -> CreateUploadRequest {
        CreateUploadRequest {
            authority: Some(AttemptAuthority {
                job_id: "00000000-0000-0000-0000-000000000001".into(),
                attempt_id: "00000000-0000-0000-0000-000000000002".into(),
                worker_id: "00000000-0000-0000-0000-000000000003".into(),
                session_id: "00000000-0000-0000-0000-000000000004".into(),
                generation: 1,
            }),
            request_id: "00000000-0000-0000-0000-000000000005".into(),
            name: "result".into(),
            kind: ArtifactKind::Output as i32,
            size_bytes: (5 << 20) + 3,
            sha256: "a".repeat(64),
            part_count: 2,
            part_size_bytes: 5 << 20,
        }
    }

    #[test]
    fn multipart_initialization_accepts_only_matching_bounded_plan() {
        let r = declaration();
        assert!(validate_create(&r).is_ok());
        let a = r.authority.as_ref().unwrap();
        let mut g = CreateUploadResponse {
            upload_id: "00000000-0000-0000-0000-000000000006".into(),
            object_key: format!(
                "projects/00000000-0000-0000-0000-000000000007/jobs/{}/attempts/{}/uploads/00000000-0000-0000-0000-000000000006",
                a.job_id, a.attempt_id
            ),
            part_count: r.part_count,
            part_size_bytes: r.part_size_bytes,
            ..Default::default()
        };
        assert!(validate_grant(&r, &g).is_ok());
        g.upload_url = "https://storage.example/signed".into();
        assert!(validate_grant(&r, &g).is_err());
        g.upload_url.clear();
        g.part_count = 3;
        assert!(validate_grant(&r, &g).is_err());
        let mut bad = r.clone();
        bad.part_size_bytes = 0;
        assert!(validate_create(&bad).is_err());
        bad = r.clone();
        bad.kind = ArtifactKind::Log as i32;
        assert!(validate_create(&bad).is_err());
    }

    #[test]
    fn multipart_completion_requires_ordered_checksums_and_server_version() {
        let d = declaration();
        let a = d.authority.as_ref().unwrap();
        let mut r = FinalizeUploadRequest {
            authority: d.authority.clone(),
            request_id: d.request_id.clone(),
            upload_id: "00000000-0000-0000-0000-000000000006".into(),
            object: Some(ObjectVersion {
                key: format!(
                    "projects/00000000-0000-0000-0000-000000000007/jobs/{}/attempts/{}/uploads/00000000-0000-0000-0000-000000000006",
                    a.job_id, a.attempt_id
                ),
                version_id: String::new(),
                size_bytes: d.size_bytes,
                sha256: d.sha256.clone(),
            }),
            parts: vec![
                CompletedPart { number: 1, etag: "first".into(), sha256: d.sha256.clone() },
                CompletedPart { number: 2, etag: "last".into(), sha256: d.sha256.clone() },
            ],
        };
        assert!(validate_finalization(&r).is_ok());
        let mut object = r.object.clone().unwrap();
        object.version_id = "exact-version".into();
        let mut response = FinalizeUploadResponse {
            artifact_id: "00000000-0000-0000-0000-000000000008".into(),
            object: Some(object),
        };
        assert!(validate_artifact(&r, &response).is_ok());
        response.object.as_mut().unwrap().sha256 = "b".repeat(64);
        assert!(validate_artifact(&r, &response).is_err());
        r.parts.swap(0, 1);
        assert!(validate_finalization(&r).is_err());
        r.parts.swap(0, 1);
        r.parts[1].sha256.clear();
        assert!(validate_finalization(&r).is_err());
    }

    #[test]
    fn part_grant_preserves_part_number_and_bounded_capability() {
        let d = declaration();
        let r = GrantUploadPartRequest {
            authority: d.authority,
            upload_id: "00000000-0000-0000-0000-000000000006".into(),
            number: 2,
            sha256: d.sha256,
        };
        assert!(validate_part_request(&r).is_ok());
        let mut g = UploadPart {
            number: 2,
            url: "https://storage.example/signed".into(),
            expires_unix_ms: 1,
            required_headers: Default::default(),
        };
        assert!(validate_part_grant(&r, &g).is_ok());
        g.number = 1;
        assert!(validate_part_grant(&r, &g).is_err());
    }
}
