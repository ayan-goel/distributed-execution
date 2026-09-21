use super::{completion_digest, lower_hash, rpc_request, ClientError, ControlClient, RPC_TIMEOUT};
use dispatch_protocol::v1::{
    AttemptState, CompleteAttemptRequest, CompleteAttemptResponse, Decision, FailureReason,
};
use serde::Deserialize;

pub struct CompletionStatus {
    pub decision: Decision,
    pub state: AttemptState,
    pub accepted_manifest_json: Vec<u8>,
}

impl ControlClient {
    pub async fn complete_attempt(
        &mut self,
        request: &CompleteAttemptRequest,
    ) -> Result<CompletionStatus, ClientError> {
        validate_request(request)?;
        // The caller persists the complete request before delivery. Lost replies
        // must replay that identity and evidence, never regenerate either here.
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.complete_attempt(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        validate_response(request, response)
    }
}

pub(crate) fn validate_request(r: &CompleteAttemptRequest) -> Result<(), ClientError> {
    if !lower_hash(&r.payload_sha256) || completion_digest(r)? != r.payload_sha256 {
        return Err(ClientError::Configuration);
    }
    Ok(())
}

pub(crate) fn validate_response(
    request: &CompleteAttemptRequest,
    response: CompleteAttemptResponse,
) -> Result<CompletionStatus, ClientError> {
    let decision = Decision::try_from(response.decision).map_err(|_| ClientError::Response)?;
    let state = AttemptState::try_from(response.state).map_err(|_| ClientError::Response)?;
    let active = matches!(
        state,
        AttemptState::Assigned
            | AttemptState::Starting
            | AttemptState::Running
            | AttemptState::Finalizing
    );
    let terminal = matches!(
        state,
        AttemptState::Succeeded
            | AttemptState::Failed
            | AttemptState::Cancelled
            | AttemptState::Lost
    );
    let expected = match FailureReason::try_from(request.reason) {
        Ok(FailureReason::Unspecified) => AttemptState::Succeeded,
        Ok(FailureReason::UserCancelled) => AttemptState::Cancelled,
        Ok(_) => AttemptState::Failed,
        Err(_) => return Err(ClientError::Response),
    };
    let valid = match decision {
        Decision::Accepted => state == expected,
        Decision::AlreadyTerminal => terminal,
        Decision::Fenced => active || state == AttemptState::Unspecified,
        Decision::StopRequested => active,
        _ => false,
    };
    if !valid {
        return Err(ClientError::Response);
    }
    // A terminal attempt alone does not acknowledge this completion. Only an
    // accepted success may carry the immutable result for this exact authority.
    if decision == Decision::Accepted && state == AttemptState::Succeeded {
        validate_manifest(request, &response.accepted_manifest_json)?;
    } else if !response.accepted_manifest_json.is_empty() {
        return Err(ClientError::Response);
    }
    Ok(CompletionStatus {
        decision,
        state,
        accepted_manifest_json: response.accepted_manifest_json,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    version: u32,
    authority: Authority,
    state: String,
    exit_code: Option<i32>,
    reason: String,
    cleanup_pending: bool,
    outputs: Vec<Output>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Authority {
    job_id: String,
    attempt_id: String,
    generation: u64,
    worker_id: String,
    session_id: String,
}

#[derive(Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
struct Output {
    name: String,
    artifact_id: String,
}

fn validate_manifest(request: &CompleteAttemptRequest, raw: &[u8]) -> Result<(), ClientError> {
    // Check only the acknowledgement binding. The server owns full manifest
    // validation; retain its bytes unchanged, including exact metric numbers.
    if raw.is_empty() || raw.len() > 2 << 20 {
        return Err(ClientError::Response);
    }
    // Serde also decodes structs from positional arrays. The wire contract
    // requires a JSON object, even if an array would populate the same fields.
    if raw.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err(ClientError::Response);
    }
    let mut m: Manifest = serde_json::from_slice(raw).map_err(|_| ClientError::Response)?;
    let a = request.authority.as_ref().ok_or(ClientError::Response)?;
    if m.version != 1
        || m.state != "SUCCEEDED"
        || m.exit_code != Some(0)
        || !m.reason.is_empty()
        || m.cleanup_pending
        || m.authority.job_id != a.job_id
        || m.authority.attempt_id != a.attempt_id
        || m.authority.generation != a.generation
        || m.authority.worker_id != a.worker_id
        || m.authority.session_id != a.session_id
        || m.outputs.len() != request.outputs.len()
    {
        return Err(ClientError::Response);
    }
    let mut expected: Vec<_> = request
        .outputs
        .iter()
        .map(|o| Output {
            name: o.name.clone(),
            artifact_id: o.artifact_id.clone(),
        })
        .collect();
    m.outputs.sort();
    expected.sort();
    if m.outputs != expected {
        return Err(ClientError::Response);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatch_protocol::v1::{AttemptAuthority, OutputReference};
    use serde_json::json;

    fn request() -> CompleteAttemptRequest {
        let mut r = CompleteAttemptRequest {
            authority: Some(AttemptAuthority {
                job_id: "00000000-0000-0000-0000-000000000001".into(),
                attempt_id: "00000000-0000-0000-0000-000000000002".into(),
                worker_id: "00000000-0000-0000-0000-000000000003".into(),
                session_id: "00000000-0000-0000-0000-000000000004".into(),
                generation: 7,
            }),
            completion_id: "00000000-0000-0000-0000-000000000005".into(),
            exit_code: Some(0),
            stopped: true,
            logs_complete: true,
            outputs: vec![OutputReference {
                name: "result".into(),
                artifact_id: "00000000-0000-0000-0000-000000000006".into(),
            }],
            ..Default::default()
        };
        r.payload_sha256 = completion_digest(&r).unwrap();
        r
    }

    fn manifest(r: &CompleteAttemptRequest) -> serde_json::Value {
        let a = r.authority.as_ref().unwrap();
        json!({
            "version": 1,
            "authority": {"jobId": a.job_id, "attemptId": a.attempt_id,
                "workerId": a.worker_id, "sessionId": a.session_id, "generation": a.generation},
            "state": "SUCCEEDED", "exitCode": 0, "reason": "", "cleanupPending": false,
            "outputs": [{"name": "result", "artifactId": r.outputs[0].artifact_id}],
            "metrics": {"large": 9007199254740993u64}
        })
    }

    fn response(manifest: Vec<u8>) -> CompleteAttemptResponse {
        CompleteAttemptResponse {
            decision: Decision::Accepted as i32,
            state: AttemptState::Succeeded as i32,
            accepted_manifest_json: manifest,
        }
    }

    #[test]
    fn request_digest_must_match_without_rewriting_evidence() {
        let r = request();
        assert!(validate_request(&r).is_ok());
        let mut changed = r.clone();
        changed.logs_complete = false;
        assert!(matches!(
            validate_request(&changed),
            Err(ClientError::Configuration)
        ));
        assert_eq!(changed.payload_sha256, r.payload_sha256);
        for digest in [
            String::new(),
            "0".repeat(64),
            r.payload_sha256.to_uppercase(),
        ] {
            changed = r.clone();
            changed.payload_sha256 = digest;
            assert!(validate_request(&changed).is_err());
        }
    }

    #[test]
    fn accepted_manifest_preserves_bytes_and_binds_authority_and_outputs() {
        let r = request();
        let raw = serde_json::to_vec_pretty(&manifest(&r)).unwrap();
        let accepted = validate_response(&r, response(raw.clone())).unwrap();
        assert_eq!(accepted.decision, Decision::Accepted);
        assert_eq!(accepted.state, AttemptState::Succeeded);
        assert_eq!(accepted.accepted_manifest_json, raw);
        for (path, value) in [
            ("/version", json!(2)),
            ("/authority/jobId", json!("other")),
            ("/authority/attemptId", json!("other")),
            ("/authority/workerId", json!("other")),
            ("/authority/sessionId", json!("other")),
            ("/authority/generation", json!(8)),
            ("/state", json!("FAILED")),
            ("/exitCode", json!(1)),
            ("/reason", json!("OOM")),
            ("/cleanupPending", json!(true)),
            ("/outputs/0/name", json!("other")),
            ("/outputs/0/artifactId", json!("other")),
            ("/outputs", json!([])),
            ("/outputs", json!(null)),
        ] {
            let mut m = manifest(&r);
            *m.pointer_mut(path).unwrap() = value;
            assert!(
                validate_response(&r, response(serde_json::to_vec(&m).unwrap())).is_err(),
                "{path}"
            );
        }
        for raw in [
            vec![],
            b"null".to_vec(),
            b"[]".to_vec(),
            b"{}".to_vec(),
            b"{broken".to_vec(),
            vec![b' '; (2 << 20) + 1],
        ] {
            assert!(validate_response(&r, response(raw)).is_err());
        }
        let duplicate = String::from_utf8(serde_json::to_vec(&manifest(&r)).unwrap())
            .unwrap()
            .replacen("{", "{\"version\":1,", 1);
        assert!(validate_response(&r, response(duplicate.into_bytes())).is_err());
    }

    #[test]
    fn manifest_requires_an_object_not_a_positional_struct_encoding() {
        let r = request();
        let m = manifest(&r);
        let positional = json!([
            m["version"],
            m["authority"],
            m["state"],
            m["exitCode"],
            m["reason"],
            m["cleanupPending"],
            m["outputs"]
        ]);
        assert!(validate_response(&r, response(serde_json::to_vec(&positional).unwrap())).is_err());
    }

    #[test]
    fn decisions_cannot_claim_acceptance_or_carry_unaccepted_manifests() {
        let r = request();
        for (decision, state) in [
            (Decision::Fenced, AttemptState::Unspecified),
            (Decision::Fenced, AttemptState::Finalizing),
            (Decision::StopRequested, AttemptState::Running),
            (Decision::AlreadyTerminal, AttemptState::Succeeded),
            (Decision::AlreadyTerminal, AttemptState::Failed),
            (Decision::AlreadyTerminal, AttemptState::Lost),
            (Decision::AlreadyTerminal, AttemptState::Cancelled),
        ] {
            let mut reply = CompleteAttemptResponse {
                decision: decision as i32,
                state: state as i32,
                accepted_manifest_json: vec![],
            };
            assert!(validate_response(&r, reply.clone()).is_ok());
            reply.accepted_manifest_json = serde_json::to_vec(&manifest(&r)).unwrap();
            assert!(validate_response(&r, reply).is_err());
        }
        for (decision, state) in [
            (0, 0),
            (999, 999),
            (Decision::Accepted as i32, 999),
            (Decision::Accepted as i32, AttemptState::Failed as i32),
            (Decision::Accepted as i32, AttemptState::Cancelled as i32),
            (Decision::Accepted as i32, AttemptState::Finalizing as i32),
            (Decision::Fenced as i32, AttemptState::Succeeded as i32),
            (Decision::StopRequested as i32, 0),
            (
                Decision::AlreadyTerminal as i32,
                AttemptState::Running as i32,
            ),
        ] {
            assert!(validate_response(
                &r,
                CompleteAttemptResponse {
                    decision,
                    state,
                    accepted_manifest_json: vec![]
                }
            )
            .is_err());
        }
        for (reason, state) in [
            (FailureReason::Oom, AttemptState::Failed),
            (FailureReason::UserCancelled, AttemptState::Cancelled),
        ] {
            let mut failure = r.clone();
            failure.reason = reason as i32;
            let reply = CompleteAttemptResponse {
                decision: Decision::Accepted as i32,
                state: state as i32,
                accepted_manifest_json: vec![],
            };
            assert!(validate_response(&failure, reply).is_ok());
            assert!(validate_response(
                &failure,
                response(serde_json::to_vec(&manifest(&r)).unwrap())
            )
            .is_err());
        }
    }
}
