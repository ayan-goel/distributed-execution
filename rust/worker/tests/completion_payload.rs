use dispatch_protocol::v1::{
    AttemptAuthority, CompleteAttemptRequest, FailureReason, LogGap, LogStream, OutputReference,
};
use dispatch_worker::control::completion_digest;

fn id(n: u8) -> String {
    format!("00000000-0000-0000-0000-{n:012}")
}
fn request() -> CompleteAttemptRequest {
    CompleteAttemptRequest {
        authority: Some(AttemptAuthority {
            job_id: id(1),
            attempt_id: id(2),
            worker_id: id(3),
            session_id: id(4),
            generation: 7,
        }),
        completion_id: id(5),
        payload_sha256: String::new(),
        exit_code: Some(0),
        reason: 0,
        stopped: true,
        outputs: vec![OutputReference {
            name: "result".into(),
            artifact_id: id(6),
        }],
        logs_complete: false,
        gaps: vec![LogGap {
            stream: LogStream::Stderr as i32,
            first_sequence: 1,
            last_sequence: 2,
        }],
        metrics_json: br#"{"loss":0.25}"#.to_vec(),
    }
}

#[test]
fn completion_digest_matches_go_golden_and_excludes_envelope() {
    let mut r = request();
    let expected = "10bae250e1af14636713d63053f8b8939cd09e3267514d40fa1ac43ebd8c81f8";
    assert_eq!(completion_digest(&r).unwrap(), expected);
    r.completion_id = id(9);
    r.payload_sha256 = "f".repeat(64);
    assert_eq!(completion_digest(&r).unwrap(), expected);
    r.authority.as_mut().unwrap().generation += 1;
    assert_ne!(completion_digest(&r).unwrap(), expected);
}

#[test]
fn completion_sets_are_sorted_without_changing_caller_evidence() {
    let mut r = request();
    r.outputs.push(OutputReference {
        name: "another".into(),
        artifact_id: id(8),
    });
    r.gaps.push(LogGap {
        stream: LogStream::Stdout as i32,
        first_sequence: 5,
        last_sequence: 9,
    });
    let original = r.clone();
    let first = completion_digest(&r).unwrap();
    assert_eq!(r, original);
    r.outputs.reverse();
    r.gaps.reverse();
    assert_eq!(completion_digest(&r).unwrap(), first);
    r.metrics_json = br#"{ "loss":0.25 }"#.to_vec();
    assert_ne!(completion_digest(&r).unwrap(), first);
}

#[test]
fn completion_rejects_invalid_authority_and_result_evidence() {
    for change in [
        |r: &mut CompleteAttemptRequest| r.authority = None,
        |r: &mut CompleteAttemptRequest| r.authority.as_mut().unwrap().generation = u64::MAX,
        |r: &mut CompleteAttemptRequest| r.completion_id = "invalid".into(),
        |r: &mut CompleteAttemptRequest| r.exit_code = None,
        |r: &mut CompleteAttemptRequest| r.exit_code = Some(256),
        |r: &mut CompleteAttemptRequest| r.stopped = false,
        |r: &mut CompleteAttemptRequest| r.reason = FailureReason::WorkerLost as i32,
        |r: &mut CompleteAttemptRequest| r.reason = FailureReason::ApplicationExit as i32,
        |r: &mut CompleteAttemptRequest| r.reason = 999,
        |r: &mut CompleteAttemptRequest| r.outputs.push(r.outputs[0].clone()),
        |r: &mut CompleteAttemptRequest| r.outputs[0].name = "../result".into(),
        |r: &mut CompleteAttemptRequest| r.logs_complete = true,
        |r: &mut CompleteAttemptRequest| r.gaps[0].stream = 0,
        |r: &mut CompleteAttemptRequest| r.gaps[0].first_sequence = 0,
        |r: &mut CompleteAttemptRequest| r.gaps[0].last_sequence = u64::MAX,
        |r: &mut CompleteAttemptRequest| r.gaps.push(r.gaps[0]),
    ] {
        let mut r = request();
        change(&mut r);
        assert!(completion_digest(&r).is_err());
    }
    let mut r = request();
    r.reason = FailureReason::RuntimeUnavailable as i32;
    r.exit_code = None;
    r.stopped = false;
    assert!(completion_digest(&r).is_ok());
}

#[test]
fn completion_metrics_are_bounded_numeric_and_duplicate_free() {
    for json in [
        r#"{"x":1,"x":2}"#,
        r#"{"x":1,"\u0078":2}"#,
        r#"{"x":true}"#,
        r#"{"x":null}"#,
        r#"{"x":"1"}"#,
        r#"{"x":[]}"#,
        r#"{"x":{}}"#,
        r#"{"x":1e99999}"#,
        r#"{"x":1e-99999}"#,
        r#"{"bad name":1}"#,
        r#"[]"#,
        r#"null"#,
        r#"{} {}"#,
    ] {
        let mut r = request();
        r.metrics_json = json.as_bytes().to_vec();
        assert!(completion_digest(&r).is_err(), "accepted {json}");
    }
    let mut r = request();
    r.metrics_json = vec![b' '; (64 << 10) + 1];
    assert!(completion_digest(&r).is_err());
    r.metrics_json = vec![0xff];
    assert!(completion_digest(&r).is_err());
    r.metrics_json =
        br#"{"big":9007199254740993,"zero":-0.00e-99999,"small":5e-324,"exponent":1E+03}"#.to_vec();
    assert!(completion_digest(&r).is_ok());
    r.metrics_json = format!(
        "{{{}}}",
        (0..256)
            .map(|n| format!("\"m{n}\":{n}"))
            .collect::<Vec<_>>()
            .join(",")
    )
    .into_bytes();
    assert!(completion_digest(&r).is_ok());
    r.metrics_json.splice(1..1, b"\"extra\":1,".iter().copied());
    assert!(completion_digest(&r).is_err());
}

#[test]
fn completion_enforces_collection_and_number_text_limits() {
    let mut r = request();
    r.outputs = (1..=64)
        .map(|n| OutputReference {
            name: format!("o{n}"),
            artifact_id: id(n),
        })
        .collect();
    r.gaps = (0..1024)
        .map(|n| LogGap {
            stream: LogStream::Stdout as i32,
            first_sequence: n * 2 + 1,
            last_sequence: n * 2 + 1,
        })
        .collect();
    assert!(completion_digest(&r).is_ok());
    let mut too_many = r.clone();
    too_many.outputs.push(OutputReference {
        name: "extra".into(),
        artifact_id: id(65),
    });
    assert!(completion_digest(&too_many).is_err());
    r.gaps.push(LogGap {
        stream: LogStream::Stderr as i32,
        first_sequence: 1,
        last_sequence: 1,
    });
    assert!(completion_digest(&r).is_err());
    let mut r = request();
    r.metrics_json = format!("{{\"x\":1.{}}}", "0".repeat(1022)).into_bytes();
    assert!(completion_digest(&r).is_ok());
    r.metrics_json = format!("{{\"x\":1.{}}}", "0".repeat(1023)).into_bytes();
    assert!(completion_digest(&r).is_err());
}
