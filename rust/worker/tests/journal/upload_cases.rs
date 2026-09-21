use super::*;
use dispatch_protocol::v1::{
    CreateUploadRequest, CreateUploadResponse, FinalizeUploadResponse, ObjectVersion,
};

const SHA: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const UPLOAD: &str = "00000000-0000-0000-0000-000000000007";

fn output_assignment() -> Assignment {
    let mut a = assignment();
    let mut job: serde_json::Value = serde_json::from_slice(&a.canonical_job_spec_json).unwrap();
    job["spec"]["outputs"] = serde_json::json!([{"name":"result", "path":"/outputs/result", "maxBytes":3, "required":true}]);
    a.canonical_job_spec_json = serde_json::to_vec(&job).unwrap();
    a.spec_sha256 = digest(&SHA256, &a.canonical_job_spec_json)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    a
}
fn ready(j: &mut Journal) {
    j.persist_assignment(&output_assignment()).unwrap();
    j.prepare_phase(ATTEMPT, AttemptState::Starting).unwrap();
    j.bind_container(ATTEMPT, &"a".repeat(64)).unwrap();
    j.record_exit(ATTEMPT, 0, false).unwrap();
    j.prepare_phase(ATTEMPT, AttemptState::Finalizing).unwrap();
}
fn grant(request: &CreateUploadRequest) -> CreateUploadResponse {
    let a = request.authority.as_ref().unwrap();
    CreateUploadResponse {
        upload_id: UPLOAD.into(),
        object_key: format!(
            "projects/00000000-0000-0000-0000-000000000006/jobs/{}/attempts/{}/uploads/{UPLOAD}",
            a.job_id, a.attempt_id
        ),
        upload_url: "https://storage.example/private?secret=never-journal-this".into(),
        expires_unix_ms: 123,
        ..Default::default()
    }
}
fn object(g: &CreateUploadResponse) -> ObjectVersion {
    ObjectVersion {
        key: g.object_key.clone(),
        version_id: "immutable-version-1".into(),
        size_bytes: 3,
        sha256: SHA.into(),
    }
}

#[tokio::test]
async fn upload_evidence_survives_reopen_without_persisting_capabilities() {
    let f = Fixture::new();
    let mut j = f.open();
    ready(&mut j);
    let request = j.prepare_output(ATTEMPT, "result", 3, SHA).unwrap();
    assert_eq!(
        j.prepare_output(ATTEMPT, "result", 3, SHA).unwrap(),
        request
    );
    let g = grant(&request);
    j.record_output_grant(&request, &g).unwrap();
    let finalize = j
        .prepare_output_finalization(ATTEMPT, &request.request_id, &object(&g))
        .unwrap();
    drop(j);
    let j = AsyncJournal::new(f.open());
    assert_eq!(
        j.prepare_output(ATTEMPT.into(), "result".into(), 3, SHA.into())
            .await
            .unwrap(),
        request
    );
    let reply = FinalizeUploadResponse {
        artifact_id: "00000000-0000-0000-0000-000000000008".into(),
        object: finalize.object.clone(),
    };
    j.record_output_response(finalize.clone(), reply.clone())
        .await
        .unwrap();
    drop(j);
    let j = f.open();
    let saved = j.load_attempt(ATTEMPT).unwrap().unwrap();
    let upload = &saved.outputs()[0];
    assert_eq!(upload.declaration(), &request);
    assert_eq!(upload.finalization(), Some(&finalize));
    assert_eq!(upload.response(), Some(&reply));
    let raw = fs::read(f.0.join(format!("{ATTEMPT}.attempt"))).unwrap();
    assert!(!raw.windows(18).any(|w| w == b"never-journal-this"));
    assert!(!String::from_utf8_lossy(&raw).contains("storage.example"));
    assert!(!format!("{upload:?}").contains(SHA));
}

#[test]
fn upload_evidence_rejects_changed_content_scope_versions_and_acknowledgements() {
    let f = Fixture::new();
    let mut j = f.open();
    j.persist_assignment(&output_assignment()).unwrap();
    assert!(j.prepare_output(ATTEMPT, "result", 3, SHA).is_err());
    ready(&mut j);
    assert!(j.prepare_output(ATTEMPT, "undeclared", 3, SHA).is_err());
    assert!(j.prepare_output(ATTEMPT, "result", 4, SHA).is_err());
    let request = j.prepare_output(ATTEMPT, "result", 3, SHA).unwrap();
    assert!(matches!(
        j.prepare_output(ATTEMPT, "result", 2, SHA),
        Err(JournalError::Conflict)
    ));
    let g = grant(&request);
    assert!(j
        .prepare_output_finalization(ATTEMPT, &request.request_id, &object(&g))
        .is_err());
    let mut bad = request.clone();
    bad.authority.as_mut().unwrap().generation += 1;
    assert!(j.record_output_grant(&bad, &g).is_err());
    j.record_output_grant(&request, &g).unwrap();
    j.record_output_grant(&request, &g).unwrap();
    let mut other = g.clone();
    other.upload_id = "00000000-0000-0000-0000-000000000009".into();
    other.object_key = other.object_key.replace(UPLOAD, &other.upload_id);
    assert!(matches!(
        j.record_output_grant(&request, &other),
        Err(JournalError::Conflict)
    ));
    let mut wrong = object(&g);
    wrong.sha256 = "b".repeat(64);
    assert!(j
        .prepare_output_finalization(ATTEMPT, &request.request_id, &wrong)
        .is_err());
    let finalize = j
        .prepare_output_finalization(ATTEMPT, &request.request_id, &object(&g))
        .unwrap();
    assert_eq!(
        j.prepare_output_finalization(ATTEMPT, &request.request_id, &object(&g))
            .unwrap(),
        finalize
    );
    wrong = object(&g);
    wrong.version_id = "different-version".into();
    assert!(matches!(
        j.prepare_output_finalization(ATTEMPT, &request.request_id, &wrong),
        Err(JournalError::Conflict)
    ));
    let mut response = FinalizeUploadResponse {
        artifact_id: "00000000-0000-0000-0000-000000000008".into(),
        object: Some(wrong),
    };
    assert!(j.record_output_response(&finalize, &response).is_err());
    response.object = finalize.object.clone();
    j.record_output_response(&finalize, &response).unwrap();
    j.record_output_response(&finalize, &response).unwrap();
    response.artifact_id = "00000000-0000-0000-0000-000000000009".into();
    assert!(matches!(
        j.record_output_response(&finalize, &response),
        Err(JournalError::Conflict)
    ));
}
