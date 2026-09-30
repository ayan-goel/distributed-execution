#![cfg(unix)]

use dispatch_protocol::v1::{Assignment, InputManifest, ObjectVersion};
use dispatch_worker::{
    dataset_assignment::{validate_inputs, validate_replayed_input},
    dataset_download::DatasetDownloader,
    execution::Input,
};
use ring::digest::{digest, SHA256};

fn sha(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn expiry() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000
}

fn assignment() -> (Assignment, Vec<Input>) {
    let files = format!("{{\"format\":\"tar.v1\",\"files\":[{{\"path\":\"data.txt\",\"sizeBytes\":4,\"sha256\":\"{}\"}}]}}", sha(b"data"));
    let input = InputManifest {
        dataset_id: "00000000-0000-0000-0000-000000000005".into(),
        dataset_name: "dataset-v1".into(),
        mount_path: "/inputs/data".into(),
        archive: Some(ObjectVersion {
            key: "projects/p/datasets/data".into(),
            version_id: "v1".into(),
            size_bytes: 4096,
            sha256: sha(b"archive"),
        }),
        file_manifest_json: files.into_bytes(),
        download_url: "http://127.0.0.1:1/projects/p/datasets/data?versionId=v1".into(),
        expires_unix_ms: expiry(),
    };
    (
        Assignment {
            inputs: vec![input],
            ..Default::default()
        },
        vec![Input {
            dataset: "dataset-v1".into(),
            mount_path: "/inputs/data".into(),
        }],
    )
}

#[test]
fn validates_assignment_against_hashed_job_inputs() {
    let downloader = DatasetDownloader::new(true).unwrap();
    let (assignment, expected) = assignment();
    let validated = validate_inputs(&assignment, &expected, &downloader).unwrap();
    assert_eq!(validated.len(), 1);
    assert_eq!(validated[0].manifest.files[0].path, "data.txt");
    assert_eq!(validated[0].archive.version_id, "v1");
    assert_eq!(validated[0].mount_path, "/inputs/data");
}

#[test]
fn rejects_mismatched_identity_manifest_and_grant() {
    let downloader = DatasetDownloader::new(true).unwrap();
    let (original, expected) = assignment();
    let mutators: [fn(&mut Assignment); 6] = [
        |a: &mut Assignment| a.inputs[0].dataset_name = "other".into(),
        |a: &mut Assignment| a.inputs[0].mount_path = "/inputs/other".into(),
        |a: &mut Assignment| a.inputs[0].file_manifest_json = b"{}".to_vec(),
        |a: &mut Assignment| {
            a.inputs[0].download_url =
                "http://127.0.0.1:1/projects/p/datasets/data?versionId=v2".into()
        },
        |a: &mut Assignment| a.inputs[0].expires_unix_ms = 1,
        |a: &mut Assignment| a.inputs[0].archive = None,
    ];
    for mutate in mutators {
        let mut changed = original.clone();
        mutate(&mut changed);
        assert!(validate_inputs(&changed, &expected, &downloader).is_err());
    }
}

#[test]
fn replay_refreshes_only_the_capability_not_the_frozen_binding() {
    let downloader = DatasetDownloader::new(true).unwrap();
    let (original, expected) = assignment();
    let mut replay = original.clone();
    replay.inputs[0].download_url =
        "http://127.0.0.1:2/projects/p/datasets/data?versionId=v1".into();
    replay.inputs[0].expires_unix_ms = expiry();
    replay.lease_duration_ms = 25_000;
    replay.phase_remaining_ms = 50_000;
    replay.server_time_unix_ms = 1234;
    assert!(validate_replayed_input(&original, &replay, &expected, 0, &downloader).is_ok());
    for changed in [
        |a: &mut Assignment| a.inputs[0].archive.as_mut().unwrap().version_id = "v2".into(),
        |a: &mut Assignment| a.inputs[0].dataset_id = "00000000-0000-0000-0000-000000000006".into(),
        |a: &mut Assignment| a.spec_sha256 = "other".into(),
    ] {
        let mut forged = replay.clone();
        changed(&mut forged);
        assert!(validate_replayed_input(&original, &forged, &expected, 0, &downloader).is_err());
    }
}
