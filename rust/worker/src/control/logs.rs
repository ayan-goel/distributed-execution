use super::{
    canonical_uuid, rpc_request, uploads::valid_authority, ClientError, ControlClient, RPC_TIMEOUT,
};
use dispatch_protocol::v1::{
    AttemptState, Decision, LogStream, MutationResponse, RegisterLogSegmentRequest,
};

#[derive(Debug)]
pub struct LogStatus {
    pub decision: Decision,
    pub state: AttemptState,
}

impl ControlClient {
    pub async fn register_log_segment(
        &mut self,
        request: &RegisterLogSegmentRequest,
    ) -> Result<LogStatus, ClientError> {
        validate_request(request)?;
        // The caller owns the durable request UUID and must replay the exact
        // range and artifact after a lost reply; this RPC creates no retry ID.
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner
                .register_log_segment(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        validate_response(request, response)
    }
}

pub(crate) fn validate_request(r: &RegisterLogSegmentRequest) -> Result<(), ClientError> {
    if !r.authority.as_ref().is_some_and(valid_authority)
        || !canonical_uuid(&r.request_id)
        || !canonical_uuid(&r.artifact_id)
        || !matches!(
            LogStream::try_from(r.stream),
            Ok(LogStream::Stdout | LogStream::Stderr)
        )
        || r.first_sequence == 0
        || r.last_sequence < r.first_sequence
        || r.last_sequence > i64::MAX as u64
        || r.gaps.len() > 1024
    {
        return Err(ClientError::Configuration);
    }
    let mut previous = r.first_sequence - 1;
    let mut dropped = 0u64;
    for gap in &r.gaps {
        if gap.stream != r.stream
            || gap.first_sequence <= previous
            || gap.first_sequence < r.first_sequence
            || gap.last_sequence < gap.first_sequence
            || gap.last_sequence > r.last_sequence
        {
            return Err(ClientError::Configuration);
        }
        dropped += gap.last_sequence - gap.first_sequence + 1;
        previous = gap.last_sequence;
    }
    // A registered object must contain captured records; all-dropped ranges
    // belong in the completion gap claim, not an empty segment upload.
    if dropped > r.last_sequence - r.first_sequence {
        return Err(ClientError::Configuration);
    }
    Ok(())
}

pub(crate) fn validate_response(
    _: &RegisterLogSegmentRequest,
    response: MutationResponse,
) -> Result<LogStatus, ClientError> {
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
            | AttemptState::Lost
            | AttemptState::Cancelled
    );
    let valid = match decision {
        // The store may accept an exact historical replay after terminalization.
        Decision::Accepted => {
            matches!(
                state,
                AttemptState::Starting | AttemptState::Running | AttemptState::Finalizing
            ) || terminal
        }
        Decision::Fenced => active || state == AttemptState::Unspecified,
        Decision::StopRequested => active,
        Decision::AlreadyTerminal => terminal,
        _ => false,
    };
    if !valid {
        return Err(ClientError::Response);
    }
    Ok(LogStatus { decision, state })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatch_protocol::v1::{AttemptAuthority, LogGap, LogStream};

    fn request() -> RegisterLogSegmentRequest {
        RegisterLogSegmentRequest {
            authority: Some(AttemptAuthority {
                worker_id: "00000000-0000-0000-0000-000000000001".into(),
                session_id: "00000000-0000-0000-0000-000000000002".into(),
                job_id: "00000000-0000-0000-0000-000000000003".into(),
                attempt_id: "00000000-0000-0000-0000-000000000004".into(),
                generation: 1,
            }),
            request_id: "00000000-0000-0000-0000-000000000005".into(),
            artifact_id: "00000000-0000-0000-0000-000000000006".into(),
            stream: LogStream::Stdout as i32,
            first_sequence: 1,
            last_sequence: 3,
            gaps: vec![LogGap {
                stream: LogStream::Stdout as i32,
                first_sequence: 2,
                last_sequence: 2,
            }],
        }
    }

    #[test]
    fn registration_requires_bounded_scoped_monotone_evidence() {
        let base = request();
        assert!(validate_request(&base).is_ok());
        let mut invalid = base.clone();
        invalid.first_sequence = 0;
        assert!(validate_request(&invalid).is_err());
        invalid = base.clone();
        invalid.last_sequence = i64::MAX as u64 + 1;
        assert!(validate_request(&invalid).is_err());
        invalid = base.clone();
        invalid.gaps[0].stream = LogStream::Stderr as i32;
        assert!(validate_request(&invalid).is_err());
        invalid = base.clone();
        invalid.gaps = vec![LogGap {
            stream: LogStream::Stdout as i32,
            first_sequence: 1,
            last_sequence: 3,
        }];
        assert!(validate_request(&invalid).is_err());
        invalid = base.clone();
        invalid.artifact_id = "bad".into();
        assert!(validate_request(&invalid).is_err());
        invalid = base.clone();
        invalid.authority.as_mut().unwrap().generation = i64::MAX as u64 + 1;
        assert!(validate_request(&invalid).is_err());
        invalid = base.clone();
        invalid.gaps = vec![
            LogGap {
                stream: LogStream::Stdout as i32,
                first_sequence: 1,
                last_sequence: 1,
            },
            LogGap {
                stream: LogStream::Stdout as i32,
                first_sequence: 1,
                last_sequence: 2,
            },
        ];
        assert!(validate_request(&invalid).is_err());
        invalid = base.clone();
        invalid.gaps = vec![base.gaps[0]; 1025];
        assert!(validate_request(&invalid).is_err());
    }

    #[test]
    fn registration_reply_never_invents_progress_or_rejection() {
        let request = request();
        let accepted = MutationResponse {
            decision: Decision::Accepted as i32,
            state: AttemptState::Running as i32,
        };
        let status = validate_response(&request, accepted).unwrap();
        assert_eq!(status.decision, Decision::Accepted);
        assert_eq!(status.state, AttemptState::Running);
        for response in [
            MutationResponse {
                decision: Decision::Accepted as i32,
                state: AttemptState::Assigned as i32,
            },
            MutationResponse {
                decision: Decision::StopRequested as i32,
                state: AttemptState::Succeeded as i32,
            },
            MutationResponse {
                decision: Decision::AlreadyTerminal as i32,
                state: AttemptState::Running as i32,
            },
            MutationResponse {
                decision: 99,
                state: AttemptState::Running as i32,
            },
        ] {
            assert!(validate_response(&request, response).is_err());
        }
        let replay = MutationResponse {
            decision: Decision::Accepted as i32,
            state: AttemptState::Succeeded as i32,
        };
        assert_eq!(
            validate_response(&request, replay).unwrap().state,
            AttemptState::Succeeded
        );
        let fenced = MutationResponse {
            decision: Decision::Fenced as i32,
            state: AttemptState::Unspecified as i32,
        };
        assert_eq!(
            validate_response(&request, fenced).unwrap().decision,
            Decision::Fenced
        );
    }
}
