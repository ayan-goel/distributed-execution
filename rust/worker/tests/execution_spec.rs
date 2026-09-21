use dispatch_protocol::v1::{Assignment, Resources};
use dispatch_worker::execution::ExecutionSpec;
use ring::digest::{digest, SHA256};
use serde_json::{json, Value};

fn document() -> Value {
    json!({
        "apiVersion": "dispatch.dev/v1alpha1", "kind": "Job",
        "metadata": {"name":"test", "project":"research"},
        "spec": {
            "image": format!("example.org/test@sha256:{}", "a".repeat(64)),
            "command":["python", "run.py"], "args":["λ", "", "$(literal)"],
            "env":{"SEED":"1", "MESSAGE":"<tag> & λ"},
            "resources":{"cpuMillis":2000, "memoryMiB":4096, "scratchMiB":8192},
            "placement":{"labels":{"architecture":"arm64"}},
            "outputs":[{"name":"metrics", "path":"/outputs/metrics.json", "required":true, "maxBytes":1048576}],
            "timeouts":{"startupSeconds":300, "executionSeconds":1800, "finalizationSeconds":300},
            "retry":{"maxAttempts":3, "on":["WORKER_LOST"], "initialBackoffSeconds":5, "maxBackoffSeconds":60},
            "terminationGraceSeconds":10, "network":"disabled"
        }
    })
}

fn assignment(raw: Vec<u8>) -> Assignment {
    let hash = digest(&SHA256, &raw)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Assignment {
        canonical_job_spec_json: raw,
        spec_sha256: hash,
        image_digest: format!("example.org/test@sha256:{}", "a".repeat(64)),
        argv: vec![
            "python".into(),
            "run.py".into(),
            "λ".into(),
            "".into(),
            "$(literal)".into(),
        ],
        resources: Some(Resources {
            cpu_millis: 2000,
            memory_bytes: 4096 << 20,
            scratch_bytes: 8192 << 20,
        }),
        ..Default::default()
    }
}

#[test]
fn binds_exact_bytes_and_preserves_execution_settings() {
    let mut a = assignment(serde_json::to_vec(&document()).unwrap());
    let checked = ExecutionSpec::from_assignment(&a).unwrap();
    assert_eq!(checked.job().spec.env["MESSAGE"], "<tag> & λ");
    assert_eq!(checked.job().spec.args, ["λ", "", "$(literal)"]);
    assert_eq!(checked.job().spec.outputs[0].max_bytes, 1048576);
    assert_eq!(checked.job().spec.timeouts.execution_seconds, 1800);
    a.canonical_job_spec_json.push(b' ');
    assert!(ExecutionSpec::from_assignment(&a).is_err());
    for change in [
        |a: &mut Assignment| a.image_digest.push('a'),
        |a: &mut Assignment| a.argv.reverse(),
        |a: &mut Assignment| a.resources.as_mut().unwrap().cpu_millis += 1,
        |a: &mut Assignment| a.resources.as_mut().unwrap().memory_bytes += 1,
        |a: &mut Assignment| a.resources.as_mut().unwrap().scratch_bytes += 1,
    ] {
        let mut a = assignment(serde_json::to_vec(&document()).unwrap());
        change(&mut a);
        assert!(ExecutionSpec::from_assignment(&a).is_err());
    }
}

#[test]
fn rejects_unsafe_or_unsupported_settings_even_with_matching_hash() {
    for (pointer, invalid) in [
        ("/spec/network", json!("host")),
        ("/spec/env", json!({"DISPATCH_ATTEMPT_ID":"forged"})),
        ("/spec/env", json!({"BAD=NAME":"value"})),
        ("/spec/env", json!({"SEED":3})),
        ("/spec/env", json!({"VALUE":"bad\u{0}value"})),
        ("/spec/timeouts/executionSeconds", json!(604801)),
        ("/spec/timeouts/startupSeconds", json!(0)),
        ("/spec/terminationGraceSeconds", json!(-1)),
        ("/spec/resources/memoryMiB", json!(u64::MAX)),
        ("/spec/retry/maxAttempts", json!(11)),
        ("/spec/retry/on", json!(["WORKER_LOST", "WORKER_LOST"])),
        ("/spec/retry/on", json!(["APPLICATION_FAILED"])),
        ("/spec/outputs/0/path", json!("/outputs/../etc/passwd")),
        ("/spec/outputs/0/path", json!("/outputs//metrics")),
        ("/spec/outputs/0/path", json!("/outputs/metrics/")),
        ("/spec/outputs/0/path", json!("/outputs/metrics\\file")),
        ("/spec/outputs/0/maxBytes", json!(0)),
        ("/spec/outputs/0/required", Value::Null),
        ("/spec/placement", json!([])),
    ] {
        let mut doc = document();
        *doc.pointer_mut(pointer).unwrap() = invalid;
        let a = assignment(serde_json::to_vec(&doc).unwrap());
        assert!(ExecutionSpec::from_assignment(&a).is_err(), "{pointer}");
    }
    for paths in [
        ["/outputs/data", "/outputs/data/file"],
        ["/outputs/data/file", "/outputs/data"],
    ] {
        let mut doc = document();
        doc["spec"]["outputs"] = json!([
            {"name":"a", "path":paths[0], "required":true, "maxBytes":1},
            {"name":"b", "path":paths[1], "required":true, "maxBytes":1}
        ]);
        assert!(
            ExecutionSpec::from_assignment(&assignment(serde_json::to_vec(&doc).unwrap())).is_err()
        );
    }
}

#[test]
fn rejects_ambiguous_json_and_unimplemented_mounts() {
    let raw = serde_json::to_string(&document()).unwrap();
    for bad in [
        raw.replace("\"kind\":\"Job\"", "\"kind\":\"Job\",\"kind\":\"Job\""),
        raw.replace("\"SEED\":\"1\"", "\"SEED\":\"1\",\"SEED\":\"2\""),
        raw.replace(
            "\"network\":\"disabled\"",
            "\"network\":\"disabled\",\"privileged\":true",
        ),
        raw.replace("\"network\":\"disabled\"", "\"Network\":\"disabled\""),
        raw.replace(
            "\"placement\":",
            "\"inputs\":[{\"dataset\":\"data\",\"mountPath\":\"/inputs/data\"}],\"placement\":",
        ),
        format!("{raw} {{}}"),
    ] {
        assert!(ExecutionSpec::from_assignment(&assignment(bad.into_bytes())).is_err());
    }
    assert!(ExecutionSpec::from_assignment(&assignment(vec![b' '; 2 * 1024 * 1024 + 1])).is_err());
}
