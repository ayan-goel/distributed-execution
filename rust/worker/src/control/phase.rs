use super::{
    rpc_request,
    work::{canonical_uuid, lower_hash},
    ClientError, ControlClient, RPC_TIMEOUT,
};
use dispatch_protocol::v1::{AttemptState, Decision, MutationResponse, ReportPhaseRequest};

#[derive(Debug)]
pub struct PhaseStatus {
    pub decision: Decision,
    pub state: AttemptState,
}

impl ControlClient {
    pub async fn report_phase(
        &mut self,
        request: &ReportPhaseRequest,
    ) -> Result<PhaseStatus, ClientError> {
        validate_request(request)?;
        // The caller journals the event UUID and payload before sending. A
        // transport retry uses that same identity and never invents new evidence.
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.report_phase(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        // An acknowledgement carries progress only. It cannot update a local
        // execution window; the caller must separately obtain a fresh renewal.
        validate_response(request, response)
    }
}

pub(crate) fn validate_request(r: &ReportPhaseRequest) -> Result<(), ClientError> {
    let a = r.authority.as_ref().ok_or(ClientError::Configuration)?;
    if !canonical_uuid(&r.event_id)
        || [&a.worker_id, &a.session_id, &a.job_id, &a.attempt_id]
            .iter()
            .any(|id| !canonical_uuid(id))
        || a.generation == 0
        || a.generation > i64::MAX as u64
    {
        return Err(ClientError::Configuration);
    }
    let valid = match AttemptState::try_from(r.phase) {
        Ok(AttemptState::Starting) => r.container_id.is_empty() && r.exit_code.is_none(),
        Ok(AttemptState::Running) => lower_hash(&r.container_id) && r.exit_code.is_none(),
        Ok(AttemptState::Finalizing) => {
            lower_hash(&r.container_id) && r.exit_code.is_some_and(|code| (0..=255).contains(&code))
        }
        _ => false,
    };
    if !valid {
        return Err(ClientError::Configuration);
    }
    Ok(())
}

fn validate_response(
    request: &ReportPhaseRequest,
    response: MutationResponse,
) -> Result<PhaseStatus, ClientError> {
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
        // A replay can acknowledge later progress, but never earlier progress.
        // The wire's active phase values have fixed increasing numeric order.
        Decision::Accepted => {
            active && state != AttemptState::Assigned && response.state >= request.phase
        }
        Decision::Fenced => active || state == AttemptState::Unspecified,
        Decision::StopRequested => active,
        Decision::AlreadyTerminal => terminal,
        _ => false,
    };
    if !valid {
        return Err(ClientError::Response);
    }
    Ok(PhaseStatus { decision, state })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatch_protocol::v1::AttemptAuthority;

    fn request(phase: AttemptState) -> ReportPhaseRequest {
        ReportPhaseRequest {
            authority: Some(AttemptAuthority {
                worker_id: "00000000-0000-0000-0000-000000000001".into(),
                session_id: "00000000-0000-0000-0000-000000000002".into(),
                job_id: "00000000-0000-0000-0000-000000000003".into(),
                attempt_id: "00000000-0000-0000-0000-000000000004".into(),
                generation: 1,
            }),
            event_id: "00000000-0000-0000-0000-000000000005".into(),
            phase: phase as i32,
            container_id: if phase == AttemptState::Starting {
                String::new()
            } else {
                "a".repeat(64)
            },
            exit_code: if phase == AttemptState::Finalizing {
                Some(0)
            } else {
                None
            },
        }
    }

    #[test]
    fn phase_requests_require_valid_authority_and_explicit_exit_evidence() {
        for phase in [
            AttemptState::Starting,
            AttemptState::Running,
            AttemptState::Finalizing,
        ] {
            assert!(validate_request(&request(phase)).is_ok());
        }
        for change in [
            |r: &mut ReportPhaseRequest| r.authority = None,
            |r: &mut ReportPhaseRequest| r.authority.as_mut().unwrap().generation = u64::MAX,
            |r: &mut ReportPhaseRequest| r.authority.as_mut().unwrap().generation = 0,
            |r: &mut ReportPhaseRequest| r.authority.as_mut().unwrap().worker_id = "bad".into(),
            |r: &mut ReportPhaseRequest| r.authority.as_mut().unwrap().session_id = "bad".into(),
            |r: &mut ReportPhaseRequest| r.event_id = "bad".into(),
            |r: &mut ReportPhaseRequest| r.phase = AttemptState::Succeeded as i32,
            |r: &mut ReportPhaseRequest| r.phase = 999,
            |r: &mut ReportPhaseRequest| r.container_id = "abcd".into(),
            |r: &mut ReportPhaseRequest| r.exit_code = None,
            |r: &mut ReportPhaseRequest| r.exit_code = Some(-1),
            |r: &mut ReportPhaseRequest| r.exit_code = Some(256),
        ] {
            let mut r = request(AttemptState::Finalizing);
            change(&mut r);
            assert!(matches!(
                validate_request(&r),
                Err(ClientError::Configuration)
            ));
        }
        let mut r = request(AttemptState::Starting);
        r.exit_code = Some(0);
        assert!(validate_request(&r).is_err());
        let mut r = request(AttemptState::Running);
        r.exit_code = Some(0);
        assert!(validate_request(&r).is_err());
    }

    #[test]
    fn phase_replies_cannot_invent_progress_or_regress_an_acknowledgement() {
        let r = request(AttemptState::Running);
        for (decision, state) in [
            (Decision::Accepted, AttemptState::Running),
            (Decision::Accepted, AttemptState::Finalizing),
            (Decision::Fenced, AttemptState::Unspecified),
            (Decision::StopRequested, AttemptState::Starting),
            (Decision::AlreadyTerminal, AttemptState::Failed),
        ] {
            let status = validate_response(
                &r,
                MutationResponse {
                    decision: decision as i32,
                    state: state as i32,
                },
            )
            .unwrap();
            assert_eq!(status.decision, decision);
            assert_eq!(status.state, state);
        }
        for (decision, state) in [
            (Decision::Accepted as i32, AttemptState::Starting as i32),
            (Decision::Accepted as i32, AttemptState::Succeeded as i32),
            (Decision::Accepted as i32, 0),
            (0, 0),
            (999, AttemptState::Running as i32),
            (Decision::Accepted as i32, 999),
            (Decision::StopRequested as i32, 0),
            (
                Decision::AlreadyTerminal as i32,
                AttemptState::Running as i32,
            ),
            (Decision::Fenced as i32, AttemptState::Cancelled as i32),
        ] {
            assert!(matches!(
                validate_response(&r, MutationResponse { decision, state }),
                Err(ClientError::Response)
            ));
        }
    }
}
