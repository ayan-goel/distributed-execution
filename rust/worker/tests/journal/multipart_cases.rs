use super::*;
use dispatch_protocol::v1::{CreateUploadResponse, FinalizeUploadResponse};

const SIZE: u64 = (5 << 20) + 3;
const PART: u64 = 5 << 20;

fn ready(j: &mut Journal) {
    let mut a = assignment();
    let mut job: serde_json::Value = serde_json::from_slice(&a.canonical_job_spec_json).unwrap();
    job["spec"]["outputs"] = serde_json::json!([{"name":"result", "path":"/outputs/result", "maxBytes":SIZE, "required":true}]);
    a.canonical_job_spec_json = serde_json::to_vec(&job).unwrap();
    a.spec_sha256 = digest(&SHA256, &a.canonical_job_spec_json)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    j.persist_assignment(&a).unwrap();
    j.prepare_phase(ATTEMPT, AttemptState::Starting).unwrap();
    j.bind_container(ATTEMPT, &"a".repeat(64)).unwrap();
    j.record_exit(ATTEMPT, 0, false).unwrap();
    j.prepare_phase(ATTEMPT, AttemptState::Finalizing).unwrap();
}

fn grant() -> CreateUploadResponse {
    CreateUploadResponse {
        upload_id: "00000000-0000-0000-0000-000000000007".into(),
        object_key: format!("projects/00000000-0000-0000-0000-000000000006/jobs/00000000-0000-0000-0000-000000000003/attempts/{ATTEMPT}/uploads/00000000-0000-0000-0000-000000000007"),
        part_count: 2,
        part_size_bytes: PART,
        ..Default::default()
    }
}

#[tokio::test]
async fn multipart_parts_and_completion_survive_reopen() {
    let f = Fixture::new();
    let mut j = f.open();
    ready(&mut j);
    let declaration = j
        .prepare_output_plan(ATTEMPT, "result", SIZE, &"a".repeat(64), PART)
        .unwrap();
    assert_eq!(declaration.part_count, 2);
    j.record_output_grant(&declaration, &grant()).unwrap();
    let second = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 2, &"c".repeat(64))
        .unwrap();
    j.record_output_part(&second, "\"last\"").unwrap();
    let first = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"b".repeat(64))
        .unwrap();
    let mut unprepared = first.clone();
    unprepared.number = 2;
    assert!(j.record_output_part(&unprepared, "\"first\"").is_err());
    drop(j);

    let j = AsyncJournal::new(f.open());
    let saved = j.load_attempt(ATTEMPT.into()).await.unwrap().unwrap();
    let parts = saved.outputs()[0].parts();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].number, 1);
    assert!(parts[0].etag.is_empty());
    assert_eq!(parts[1].etag, "\"last\"");
    assert_eq!(
        j.prepare_output_part(
            ATTEMPT.into(),
            declaration.request_id.clone(),
            1,
            "b".repeat(64)
        )
        .await
        .unwrap(),
        first
    );
    j.record_output_part(first.clone(), "\"first\"".into())
        .await
        .unwrap();
    let finalize = j
        .prepare_multipart_finalization(ATTEMPT.into(), declaration.request_id.clone())
        .await
        .unwrap();
    assert_eq!(finalize.parts[0].etag, "\"first\"");
    assert_eq!(finalize.parts[1].sha256, "c".repeat(64));
    assert!(finalize.object.as_ref().unwrap().version_id.is_empty());
    drop(j);

    let mut j = f.open();
    assert_eq!(
        j.prepare_multipart_finalization(ATTEMPT, &declaration.request_id)
            .unwrap(),
        finalize
    );
    let mut object = finalize.object.clone().unwrap();
    object.version_id = "exact-completed-version".into();
    let response = FinalizeUploadResponse {
        artifact_id: "00000000-0000-0000-0000-000000000008".into(),
        object: Some(object),
    };
    j.record_output_response(&finalize, &response).unwrap();
    drop(j);
    assert_eq!(
        f.open().load_attempt(ATTEMPT).unwrap().unwrap().outputs()[0].response(),
        Some(&response)
    );
}

#[test]
fn multipart_journal_rejects_changed_or_incomplete_evidence() {
    let f = Fixture::new();
    let mut j = f.open();
    ready(&mut j);
    let declaration = j
        .prepare_output_plan(ATTEMPT, "result", SIZE, &"a".repeat(64), PART)
        .unwrap();
    assert!(j
        .prepare_output_plan(ATTEMPT, "result", SIZE, &"a".repeat(64), 0)
        .is_err());
    assert!(j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"b".repeat(64))
        .is_err());
    j.record_output_grant(&declaration, &grant()).unwrap();
    assert!(j
        .prepare_multipart_finalization(ATTEMPT, &declaration.request_id)
        .is_err());
    for number in [0, 3] {
        assert!(j
            .prepare_output_part(ATTEMPT, &declaration.request_id, number, &"b".repeat(64))
            .is_err());
    }
    let first = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"b".repeat(64))
        .unwrap();
    assert!(j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"c".repeat(64))
        .is_err());
    assert!(j.record_output_part(&first, "bad\nheader").is_err());
    j.record_output_part(&first, "\"first\"").unwrap();
    assert!(j.record_output_part(&first, "\"changed\"").is_err());
    assert!(j
        .prepare_multipart_finalization(ATTEMPT, &declaration.request_id)
        .is_err());
    let second = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 2, &"c".repeat(64))
        .unwrap();
    assert!(j
        .prepare_multipart_finalization(ATTEMPT, &declaration.request_id)
        .is_err());
    j.record_output_part(&second, "\"last\"").unwrap();
    let finalize = j
        .prepare_multipart_finalization(ATTEMPT, &declaration.request_id)
        .unwrap();
    let mut wrong = finalize.object.clone().unwrap();
    wrong.version_id = "client-selected-version".into();
    assert!(j
        .prepare_output_finalization(ATTEMPT, &declaration.request_id, &wrong)
        .is_err());
    let mut foreign = first.clone();
    foreign.authority.as_mut().unwrap().generation += 1;
    assert!(j.record_output_part(&foreign, "\"first\"").is_err());
}

#[tokio::test]
async fn multipart_concurrent_part_acknowledgements_preserve_both_records() {
    let f = Fixture::new();
    let mut j = f.open();
    ready(&mut j);
    let declaration = j
        .prepare_output_plan(ATTEMPT, "result", SIZE, &"a".repeat(64), PART)
        .unwrap();
    j.record_output_grant(&declaration, &grant()).unwrap();
    let first = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"b".repeat(64))
        .unwrap();
    let second = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 2, &"c".repeat(64))
        .unwrap();
    let j = AsyncJournal::new(j);
    let (a, b) = tokio::join!(
        j.record_output_part(first, "\"first\"".into()),
        j.record_output_part(second, "\"last\"".into()),
    );
    a.unwrap();
    b.unwrap();
    let finalize = j
        .prepare_multipart_finalization(ATTEMPT.into(), declaration.request_id)
        .await
        .unwrap();
    assert_eq!(finalize.parts.len(), 2);
    assert_eq!(finalize.parts[0].etag, "\"first\"");
    assert_eq!(finalize.parts[1].etag, "\"last\"");
}

#[test]
fn multipart_completion_seal_blocks_late_part_mutation() {
    let f = Fixture::new();
    let mut j = f.open();
    ready(&mut j);
    let declaration = j
        .prepare_output_plan(ATTEMPT, "result", SIZE, &"a".repeat(64), PART)
        .unwrap();
    j.record_output_grant(&declaration, &grant()).unwrap();
    let first = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"b".repeat(64))
        .unwrap();
    let mut completion = completion();
    completion.exit_code = Some(0);
    completion.payload_sha256 = completion_digest(&completion).unwrap();
    j.persist_completion(&completion).unwrap();
    assert!(j.record_output_part(&first, "\"late\"").is_err());
    assert!(j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 2, &"c".repeat(64))
        .is_err());
    assert!(j
        .prepare_multipart_finalization(ATTEMPT, &declaration.request_id)
        .is_err());
    assert_eq!(
        j.prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"b".repeat(64))
            .unwrap(),
        first
    );
}

#[test]
fn multipart_part_corruption_fails_even_with_recomputed_frame_checksum() {
    let f = Fixture::new();
    let mut j = f.open();
    ready(&mut j);
    let declaration = j
        .prepare_output_plan(ATTEMPT, "result", SIZE, &"a".repeat(64), PART)
        .unwrap();
    j.record_output_grant(&declaration, &grant()).unwrap();
    let part = j
        .prepare_output_part(ATTEMPT, &declaration.request_id, 1, &"b".repeat(64))
        .unwrap();
    j.record_output_part(&part, "\"unique-etag\"").unwrap();
    drop(j);
    let path = f.0.join(format!("{ATTEMPT}.attempt"));
    let mut bytes = fs::read(&path).unwrap();
    let offset = bytes.windows(11).position(|w| w == b"unique-etag").unwrap();
    bytes[offset] = 0;
    let checksum = digest(&SHA256, &bytes[48..]);
    bytes[16..48].copy_from_slice(checksum.as_ref());
    fs::write(path, bytes).unwrap();
    assert!(matches!(
        f.open().load_attempt(ATTEMPT),
        Err(JournalError::Corrupt)
    ));
}
