use super::{
    rpc_request,
    work::{canonical_uuid, valid_session},
    ClientError, ControlClient, RPC_TIMEOUT,
};
use crate::lease::{AuthorityWindow, LeaseError, MonoTime};
use dispatch_protocol::v1::{AttemptAuthority, Decision, RenewLeasesRequest, RenewLeasesResponse};
use std::collections::HashSet;

#[cfg(unix)]
mod maintain;

#[derive(Debug)]
pub struct RenewedLease {
    identity: AttemptAuthority,
    authority: AuthorityWindow,
}

impl RenewedLease {
    pub fn identity(&self) -> &AttemptAuthority {
        &self.identity
    }
    pub fn authority(&self) -> &AuthorityWindow {
        &self.authority
    }
}

#[derive(Debug)]
pub enum RenewalOutcome {
    Renewed(RenewedLease),
    Rejected {
        identity: AttemptAuthority,
        decision: Decision,
    },
    Expired(AttemptAuthority),
}

impl ControlClient {
    pub async fn renew(
        &mut self,
        request: &RenewLeasesRequest,
    ) -> Result<Vec<RenewalOutcome>, ClientError> {
        validate_request(request)?;
        let sent = MonoTime::now().map_err(|_| ClientError::Clock)?;
        // The caller owns the batch UUID: retry this exact payload after an
        // ambiguous response, then use a new UUID for the next renewal period.
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.renew_leases(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        let received = MonoTime::now().map_err(|_| ClientError::Clock)?;
        let mut outcomes = decode_renewals(request, response, sent, received)?;
        // Validate the entire batch before publishing any grants. Processing
        // time consumes authority too; a late member retains only cleanup identity.
        for outcome in &mut outcomes {
            if let RenewalOutcome::Renewed(grant) = outcome {
                match grant.authority.remaining() {
                    Ok(_) => {}
                    Err(LeaseError::Expired) => {
                        *outcome = RenewalOutcome::Expired(grant.identity.clone())
                    }
                    Err(LeaseError::Clock) => return Err(ClientError::Clock),
                    Err(LeaseError::Invalid) => return Err(ClientError::Response),
                }
            }
        }
        Ok(outcomes)
    }
}

fn validate_request(request: &RenewLeasesRequest) -> Result<(), ClientError> {
    let session = valid_session(request.session.as_ref())?;
    if !canonical_uuid(&request.request_id)
        || request.attempts.is_empty()
        || request.attempts.len() > 64
    {
        return Err(ClientError::Configuration);
    }
    let mut seen = HashSet::new();
    for a in &request.attempts {
        if a.worker_id != session.worker_id
            || a.session_id != session.session_id
            || !canonical_uuid(&a.job_id)
            || !canonical_uuid(&a.attempt_id)
            || a.generation == 0
            || a.generation > i64::MAX as u64
            || !seen.insert(&a.attempt_id)
        {
            return Err(ClientError::Configuration);
        }
    }
    Ok(())
}

fn decode_renewals(
    request: &RenewLeasesRequest,
    response: RenewLeasesResponse,
    sent: MonoTime,
    received: MonoTime,
) -> Result<Vec<RenewalOutcome>, ClientError> {
    if response.results.len() != request.attempts.len() {
        return Err(ClientError::Response);
    }
    response
        .results
        .into_iter()
        .zip(&request.attempts)
        .map(|(g, expected)| {
            let identity = g.authority.ok_or(ClientError::Response)?;
            // The wire contract preserves input order. Full-tuple equality also
            // rejects duplicate, foreign, missing, and substituted result identities.
            if &identity != expected {
                return Err(ClientError::Response);
            }
            match Decision::try_from(g.decision) {
                Ok(Decision::Accepted) => match AuthorityWindow::from_grant(
                    sent,
                    received,
                    g.remaining_ms,
                    g.phase_remaining_ms,
                ) {
                    Ok(authority) => Ok(RenewalOutcome::Renewed(RenewedLease {
                        identity,
                        authority,
                    })),
                    Err(LeaseError::Expired) => Ok(RenewalOutcome::Expired(identity)),
                    Err(LeaseError::Clock) => Err(ClientError::Clock),
                    Err(LeaseError::Invalid) => Err(ClientError::Response),
                },
                Ok(
                    decision @ (Decision::Fenced
                    | Decision::StopRequested
                    | Decision::AlreadyTerminal),
                ) if g.remaining_ms == 0 && g.phase_remaining_ms == 0 => {
                    Ok(RenewalOutcome::Rejected { identity, decision })
                }
                _ => Err(ClientError::Response),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatch_protocol::v1::{LeaseResult, WorkerSession};

    fn request() -> RenewLeasesRequest {
        RenewLeasesRequest {
            session: Some(WorkerSession {
                worker_id: "00000000-0000-0000-0000-000000000001".into(),
                session_id: "00000000-0000-0000-0000-000000000002".into(),
            }),
            request_id: "00000000-0000-0000-0000-000000000003".into(),
            attempts: vec![AttemptAuthority {
                worker_id: "00000000-0000-0000-0000-000000000001".into(),
                session_id: "00000000-0000-0000-0000-000000000002".into(),
                job_id: "00000000-0000-0000-0000-000000000004".into(),
                attempt_id: "00000000-0000-0000-0000-000000000005".into(),
                generation: 1,
            }],
        }
    }

    fn response(r: &RenewLeasesRequest) -> RenewLeasesResponse {
        RenewLeasesResponse {
            results: r
                .attempts
                .iter()
                .map(|a| LeaseResult {
                    authority: Some(a.clone()),
                    decision: Decision::Accepted as i32,
                    remaining_ms: 30_000,
                    phase_remaining_ms: 300_000,
                    server_time_unix_ms: 1,
                })
                .collect(),
        }
    }

    #[test]
    fn request_validation_prevents_ambiguous_or_cross_session_batches() {
        assert!(validate_request(&request()).is_ok());
        for change in [
            |r: &mut RenewLeasesRequest| r.attempts.clear(),
            |r: &mut RenewLeasesRequest| r.attempts.push(r.attempts[0].clone()),
            |r: &mut RenewLeasesRequest| r.attempts = vec![r.attempts[0].clone(); 65],
            |r: &mut RenewLeasesRequest| r.request_id = "bad".into(),
            |r: &mut RenewLeasesRequest| r.attempts[0].generation = 0,
            |r: &mut RenewLeasesRequest| r.attempts[0].generation = u64::MAX,
            |r: &mut RenewLeasesRequest| r.attempts[0].session_id = r.request_id.clone(),
            |r: &mut RenewLeasesRequest| r.attempts[0].worker_id = r.request_id.clone(),
            |r: &mut RenewLeasesRequest| r.attempts[0].job_id = "bad".into(),
            |r: &mut RenewLeasesRequest| r.attempts[0].attempt_id = "bad".into(),
        ] {
            let mut r = request();
            change(&mut r);
            assert!(matches!(
                validate_request(&r),
                Err(ClientError::Configuration)
            ));
        }
    }

    #[test]
    fn responses_bind_every_member_and_reject_malformed_authority() {
        let r = request();
        let now = MonoTime::now().unwrap();
        for change in [
            |r: &mut RenewLeasesResponse| r.results.clear(),
            |r: &mut RenewLeasesResponse| r.results.push(r.results[0].clone()),
            |r: &mut RenewLeasesResponse| r.results[0].authority = None,
            |r: &mut RenewLeasesResponse| r.results[0].authority.as_mut().unwrap().generation += 1,
            |r: &mut RenewLeasesResponse| {
                r.results[0].authority.as_mut().unwrap().job_id = "different".into()
            },
            |r: &mut RenewLeasesResponse| r.results[0].decision = 999,
            |r: &mut RenewLeasesResponse| r.results[0].decision = Decision::Unspecified as i32,
            |r: &mut RenewLeasesResponse| r.results[0].remaining_ms = 30_001,
            |r: &mut RenewLeasesResponse| r.results[0].phase_remaining_ms = 604_800_001,
            |r: &mut RenewLeasesResponse| r.results[0].remaining_ms = 0,
            |r: &mut RenewLeasesResponse| r.results[0].decision = Decision::Fenced as i32,
        ] {
            let mut reply = response(&r);
            change(&mut reply);
            assert!(matches!(
                decode_renewals(&r, reply, now, now),
                Err(ClientError::Response)
            ));
        }
    }

    #[test]
    fn mixed_results_keep_expiry_and_rejection_separate_from_authority() {
        let mut r = request();
        for n in 6..10 {
            let mut a = r.attempts[0].clone();
            a.attempt_id = format!("00000000-0000-0000-0000-{n:012}");
            r.attempts.push(a);
        }
        let mut reply = response(&r);
        reply.results[1].remaining_ms = 5000;
        for (g, decision) in reply.results[2..].iter_mut().zip([
            Decision::Fenced,
            Decision::StopRequested,
            Decision::AlreadyTerminal,
        ]) {
            g.decision = decision as i32;
            g.remaining_ms = 0;
            g.phase_remaining_ms = 0;
        }
        let now = MonoTime::now().unwrap();
        let results = decode_renewals(&r, reply, now, now).unwrap();
        let RenewalOutcome::Renewed(grant) = &results[0] else {
            panic!("missing renewal")
        };
        assert_eq!(grant.identity(), &r.attempts[0]);
        assert_eq!(
            grant.authority().remaining_at(now).unwrap(),
            std::time::Duration::from_secs(25)
        );
        assert!(matches!(&results[1],RenewalOutcome::Expired(a) if a==&r.attempts[1]));
        for (outcome, decision) in results[2..].iter().zip([
            Decision::Fenced,
            Decision::StopRequested,
            Decision::AlreadyTerminal,
        ]) {
            assert!(matches!(outcome,RenewalOutcome::Rejected{decision:d,..} if *d==decision));
        }
        let mut swapped = response(&r);
        swapped.results.swap(0, 1);
        assert!(matches!(
            decode_renewals(&r, swapped, now, now),
            Err(ClientError::Response)
        ));
    }
}
