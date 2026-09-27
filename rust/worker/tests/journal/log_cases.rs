use super::*;
use dispatch_protocol::v1::{
    CreateUploadResponse, Decision, FinalizeUploadResponse, LogGap, LogStream, ObjectVersion,
};

const SHA: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const UPLOAD: &str = "00000000-0000-0000-0000-000000000007";
const ARTIFACT: &str = "00000000-0000-0000-0000-000000000008";

fn ready(j: &mut Journal) {
    j.persist_assignment(&assignment()).unwrap();
    j.prepare_phase(ATTEMPT, AttemptState::Starting).unwrap();
    j.bind_container(ATTEMPT, &"a".repeat(64)).unwrap();
    j.prepare_phase(ATTEMPT, AttemptState::Running).unwrap();
}

#[test]
fn log_evidence_survives_reopen_and_replays_exact_identity() {
    let fixture = Fixture::new();
    let mut journal = fixture.open();
    ready(&mut journal);
    let gaps = vec![LogGap {
        stream: LogStream::Stdout as i32,
        first_sequence: 2,
        last_sequence: 2,
    }];
    let create = journal
        .prepare_log(ATTEMPT, LogStream::Stdout, 1, 3, &gaps, 3, SHA)
        .unwrap();
    assert_eq!(
        journal
            .prepare_log(ATTEMPT, LogStream::Stdout, 1, 3, &gaps, 3, SHA)
            .unwrap(),
        create
    );
    assert!(matches!(
        journal.prepare_log(ATTEMPT, LogStream::Stdout, 1, 3, &[], 3, SHA),
        Err(JournalError::Conflict)
    ));
    let authority = create.authority.as_ref().unwrap();
    let grant = CreateUploadResponse {
        upload_id: UPLOAD.into(),
        object_key: format!(
            "projects/00000000-0000-0000-0000-000000000006/jobs/{}/attempts/{}/uploads/{UPLOAD}",
            authority.job_id, authority.attempt_id
        ),
        upload_url: "https://storage.example/never-journal-this".into(),
        expires_unix_ms: 123,
        ..Default::default()
    };
    journal.record_log_grant(&create, &grant).unwrap();
    assert!(journal
        .record_log_grant(
            &create,
            &CreateUploadResponse {
                upload_id: ARTIFACT.into(),
                ..grant.clone()
            }
        )
        .is_err());
    let object = ObjectVersion {
        key: grant.object_key.clone(),
        version_id: "version-1".into(),
        size_bytes: 3,
        sha256: SHA.into(),
    };
    let finalize = journal
        .prepare_log_finalization(ATTEMPT, &create.request_id, &object)
        .unwrap();
    let mut changed = object.clone();
    changed.version_id = "version-2".into();
    assert!(matches!(
        journal.prepare_log_finalization(ATTEMPT, &create.request_id, &changed),
        Err(JournalError::Conflict)
    ));
    let reply = FinalizeUploadResponse {
        artifact_id: ARTIFACT.into(),
        object: Some(object.clone()),
    };
    journal.record_log_response(&finalize, &reply).unwrap();
    let register = journal
        .prepare_log_registration(ATTEMPT, &create.request_id)
        .unwrap();
    assert_eq!(register.artifact_id, ARTIFACT);
    assert_eq!(register.gaps, gaps);
    drop(journal);
    let mut journal = fixture.open();
    assert_eq!(
        journal
            .prepare_log_registration(ATTEMPT, &create.request_id)
            .unwrap(),
        register
    );
    journal
        .record_log_registration(&register, Decision::Accepted, AttemptState::Running)
        .unwrap();
    journal
        .record_log_registration(&register, Decision::Accepted, AttemptState::Succeeded)
        .unwrap();
    assert!(journal
        .record_log_registration(&register, Decision::Fenced, AttemptState::Running)
        .is_err());
    let next = journal
        .prepare_log(ATTEMPT, LogStream::Stdout, 4, 4, &[], 3, SHA)
        .unwrap();
    assert_ne!(next.request_id, create.request_id);
    let saved = journal.load_attempt(ATTEMPT).unwrap().unwrap();
    assert_eq!(saved.logs().len(), 2);
    assert_eq!(saved.logs()[0].registration(), Some(&register));
    assert!(saved.logs()[0].registered());
    let raw = fs::read(fixture.0.join(format!("{ATTEMPT}.attempt"))).unwrap();
    assert!(!String::from_utf8_lossy(&raw).contains("never-journal-this"));
}

#[test]
fn log_journal_rejects_out_of_order_or_changed_evidence() {
    let fixture = Fixture::new();
    let mut journal = fixture.open();
    ready(&mut journal);
    let create = journal
        .prepare_log(ATTEMPT, LogStream::Stderr, 1, 1, &[], 3, SHA)
        .unwrap();
    assert!(journal
        .prepare_log_registration(ATTEMPT, &create.request_id)
        .is_err());
    assert!(journal
        .prepare_log(ATTEMPT, LogStream::Stderr, 3, 3, &[], 3, SHA)
        .is_err());
    assert!(journal
        .prepare_log(ATTEMPT, LogStream::Stderr, 1, 1, &[], 4, SHA)
        .is_err());
}
