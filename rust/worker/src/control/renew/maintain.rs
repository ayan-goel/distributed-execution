use super::{ClientError, ControlClient, RenewLeasesRequest, RenewalOutcome};
use crate::supervisor::{AuthorityController, StopReason};
use dispatch_protocol::v1::{AttemptAuthority, WorkerSession};
use std::{future::Future, time::Duration};

struct Timing {
    period: Duration,
    retry: Duration,
    rpc: Duration,
}

trait RenewalRpc {
    fn renew_batch(
        &mut self,
        request: &RenewLeasesRequest,
    ) -> impl Future<Output = Result<Vec<RenewalOutcome>, ClientError>> + Send;
}
impl RenewalRpc for ControlClient {
    async fn renew_batch(
        &mut self,
        request: &RenewLeasesRequest,
    ) -> Result<Vec<RenewalOutcome>, ClientError> {
        self.renew(request).await
    }
}

impl ControlClient {
    /// Maintain one same-session batch until every consumer finishes or loses authority.
    /// Run independently of heartbeat, launch, filesystem, and runtime observation tasks.
    pub fn maintain_leases(
        &mut self,
        controllers: Vec<AuthorityController>,
    ) -> impl Future<Output = Result<(), ClientError>> + Send + '_ {
        maintain(
            self,
            controllers,
            Timing {
                period: Duration::from_secs(5),
                retry: Duration::from_secs(1),
                rpc: super::RPC_TIMEOUT,
            },
        )
    }
}

struct Batch(Vec<AuthorityController>);
impl Batch {
    fn stop(&self, reason: StopReason) {
        for controller in &self.0 {
            controller.stop(reason);
        }
    }
    fn request(&self, active_only: bool) -> Result<RenewLeasesRequest, ClientError> {
        let first = self.0.first().ok_or(ClientError::Configuration)?.identity();
        Ok(RenewLeasesRequest {
            session: Some(WorkerSession {
                worker_id: first.worker_id.clone(),
                session_id: first.session_id.clone(),
            }),
            request_id: crate::journal::new_uuid().map_err(|_| ClientError::Random)?,
            attempts: self
                .0
                .iter()
                .filter(|c| !active_only || c.active())
                .map(|c| c.identity().clone())
                .collect(),
        })
    }
}
impl Drop for Batch {
    fn drop(&mut self) {
        // Cancelling the renewal future must close authority even if a launch
        // coordinator still holds a cloned sender. Watchdogs perform the cleanup.
        self.stop(StopReason::ControllerLost);
    }
}

fn maintain(
    rpc: &mut (impl RenewalRpc + Send),
    controllers: Vec<AuthorityController>,
    timing: Timing,
) -> impl Future<Output = Result<(), ClientError>> + Send + '_ {
    // Construct the guard before returning the future: an executor may cancel
    // a queued task before its first poll, while other senders remain alive.
    let batch = Batch(controllers);
    async move {
        if batch.0.is_empty() || batch.0.len() > 64 {
            return Err(ClientError::Configuration);
        }
        super::validate_request(&batch.request(false)?)?;
        let mut pending = None;
        loop {
            if !batch.0.iter().any(AuthorityController::active) {
                return Ok(());
            }
            if pending.is_none() {
                pending = Some(batch.request(true)?);
            }
            let request = pending.as_ref().unwrap();
            if request.attempts.is_empty() {
                return Ok(());
            }
            let started = tokio::time::Instant::now();
            let result = tokio::time::timeout(timing.rpc, rpc.renew_batch(request))
                .await
                .unwrap_or(Err(ClientError::Deadline));
            let delay = match result {
                Ok(outcomes) => {
                    // Validate the whole batch before applying anything. An ambiguous
                    // retry retains its original members even when some have exited.
                    if outcomes.len() != request.attempts.len()
                        || outcomes
                            .iter()
                            .zip(&request.attempts)
                            .any(|(outcome, expected)| outcome_identity(outcome) != expected)
                    {
                        batch.stop(StopReason::RenewalFailed);
                        return Err(ClientError::Response);
                    }
                    for outcome in &outcomes {
                        let controller = batch
                            .0
                            .iter()
                            .find(|c| c.identity() == outcome_identity(outcome))
                            .ok_or(ClientError::Response)?;
                        if controller.apply(outcome).is_err() {
                            batch.stop(StopReason::RenewalFailed);
                            return Err(ClientError::Response);
                        }
                    }
                    pending = None;
                    timing.period.saturating_sub(started.elapsed())
                }
                Err(error) if error.retryable() => timing.retry,
                Err(error) => {
                    batch.stop(StopReason::RenewalFailed);
                    return Err(error);
                }
            };
            // A transport failure never changes the held windows. The independent
            // watchdog expires them while this task waits or retries the same UUID.
            if !batch.0.iter().any(AuthorityController::active) {
                return Ok(());
            }
            tokio::time::sleep(delay).await;
        }
    }
}

fn outcome_identity(outcome: &RenewalOutcome) -> &AttemptAuthority {
    match outcome {
        RenewalOutcome::Renewed(grant) => grant.identity(),
        RenewalOutcome::Rejected { identity, .. } | RenewalOutcome::Expired(identity) => identity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        control::RenewedLease,
        lease::{AuthorityWindow, MonoTime},
        supervisor::{authority_channel, SupervisedAuthority},
    };
    use dispatch_protocol::v1::Decision;
    use std::collections::VecDeque;

    enum Action {
        Renew,
        Reject,
        Retry,
        Fatal,
        Stall,
        Malformed,
    }
    struct Fake {
        actions: VecDeque<Action>,
        requests: Vec<RenewLeasesRequest>,
        retire: Option<SupervisedAuthority>,
    }
    impl RenewalRpc for Fake {
        async fn renew_batch(
            &mut self,
            request: &RenewLeasesRequest,
        ) -> Result<Vec<RenewalOutcome>, ClientError> {
            self.requests.push(request.clone());
            match self.actions.pop_front().unwrap_or(Action::Retry) {
                Action::Retry => {
                    self.retire.take();
                    Err(ClientError::Connection)
                }
                Action::Fatal => Err(ClientError::Response),
                Action::Stall => std::future::pending().await,
                Action::Malformed => Ok(vec![]),
                action => Ok(request
                    .attempts
                    .iter()
                    .map(|identity| match action {
                        Action::Renew => RenewalOutcome::Renewed(RenewedLease {
                            identity: identity.clone(),
                            authority: window(1000),
                        }),
                        _ => RenewalOutcome::Rejected {
                            identity: identity.clone(),
                            decision: Decision::Fenced,
                        },
                    })
                    .collect()),
            }
        }
    }
    fn fake(actions: impl IntoIterator<Item = Action>) -> Fake {
        Fake {
            actions: actions.into_iter().collect(),
            requests: Vec::new(),
            retire: None,
        }
    }
    fn window(ms: u64) -> AuthorityWindow {
        let now = MonoTime::now().unwrap();
        AuthorityWindow::from_grant(now, now, 30_000, ms).unwrap()
    }
    fn controller(n: u64, ms: u64) -> (AuthorityController, SupervisedAuthority) {
        authority_channel(
            AttemptAuthority {
                worker_id: "00000000-0000-0000-0000-000000000001".into(),
                session_id: "00000000-0000-0000-0000-000000000002".into(),
                job_id: format!("00000000-0000-0000-0000-{n:012x}"),
                attempt_id: format!("00000000-0000-0000-0000-{n:012x}"),
                generation: 1,
            },
            window(ms),
        )
        .unwrap()
    }
    fn timing() -> Timing {
        Timing {
            period: Duration::from_millis(20),
            retry: Duration::from_millis(10),
            rpc: Duration::from_millis(30),
        }
    }

    #[tokio::test]
    async fn uncertain_reply_replays_exact_batch_then_allocates_a_new_period_identity() {
        let (a, _a_watch) = controller(3, 1000);
        let (b, _b_watch) = controller(4, 1000);
        let mut rpc = fake([Action::Retry, Action::Renew, Action::Reject]);
        maintain(&mut rpc, vec![a.clone(), b.clone()], timing())
            .await
            .unwrap();
        assert_eq!(rpc.requests.len(), 3);
        assert_eq!(rpc.requests[0], rpc.requests[1]);
        assert_ne!(rpc.requests[1].request_id, rpc.requests[2].request_id);
        assert_eq!(rpc.requests[0].attempts.len(), 2);
        assert!(!a.active() && !b.active());
    }

    #[tokio::test]
    async fn outages_and_stalled_rpcs_cannot_create_execution_time() {
        let (a, _watch) = controller(3, 100);
        let mut rpc = fake([Action::Stall]);
        let started = tokio::time::Instant::now();
        maintain(&mut rpc, vec![a.clone()], timing()).await.unwrap();
        assert!(!a.active());
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(rpc.requests.len() >= 2);
        assert!(rpc.requests.iter().all(|r| r == &rpc.requests[0]));
    }

    #[tokio::test]
    async fn membership_changes_wait_for_the_uncertain_batch_to_resolve() {
        let (a, a_watch) = controller(3, 1000);
        let (b, _b_watch) = controller(4, 1000);
        let mut rpc = fake([Action::Retry, Action::Renew, Action::Reject]);
        rpc.retire = Some(a_watch);
        maintain(&mut rpc, vec![a, b.clone()], timing())
            .await
            .unwrap();
        assert_eq!(rpc.requests.len(), 3);
        assert_eq!(rpc.requests[0], rpc.requests[1]);
        assert_eq!(rpc.requests[2].attempts, vec![b.identity().clone()]);
    }

    #[tokio::test]
    async fn fatal_response_stops_authority_even_when_the_caller_retains_a_sender() {
        for action in [Action::Fatal, Action::Malformed] {
            let (a, _watch) = controller(3, 1000);
            let mut rpc = fake([action]);
            assert!(matches!(
                maintain(&mut rpc, vec![a.clone()], timing()).await,
                Err(ClientError::Response)
            ));
            assert!(!a.active());
        }
    }

    #[tokio::test]
    async fn dropping_renewal_future_stops_authority_and_closed_watchers_retire() {
        let (a, watch) = controller(3, 1000);
        let mut rpc = fake([Action::Stall]);
        let controllers = vec![a.clone()];
        let mut task = Box::pin(maintain(&mut rpc, controllers, timing()));
        tokio::select! {
            _ = &mut task => panic!("stalled request completed early"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
        drop(task);
        assert!(!a.active());
        drop(watch);

        let (b, watch) = controller(4, 1000);
        drop(watch);
        let mut rpc = fake([]);
        maintain(&mut rpc, vec![b], timing()).await.unwrap();
        assert!(rpc.requests.is_empty());
    }

    #[tokio::test]
    async fn cancellation_before_the_first_poll_also_closes_retained_authority() {
        let (a, _watch) = controller(3, 1000);
        let mut rpc = fake([]);
        let task = maintain(&mut rpc, vec![a.clone()], timing());
        drop(task);
        assert!(!a.active());
        assert!(rpc.requests.is_empty());
    }

    #[tokio::test]
    async fn invalid_batches_are_rejected_before_network_io() {
        let mut rpc = fake([]);
        let (a, _watch) = controller(3, 1000);
        assert!(matches!(
            maintain(&mut rpc, vec![a.clone(), a], timing()).await,
            Err(ClientError::Configuration)
        ));
        assert!(matches!(
            maintain(&mut rpc, vec![], timing()).await,
            Err(ClientError::Configuration)
        ));
        let pairs: Vec<_> = (1..=65).map(|n| controller(n, 1000)).collect();
        let controls = pairs.iter().map(|(c, _)| c.clone()).collect();
        assert!(matches!(
            maintain(&mut rpc, controls, timing()).await,
            Err(ClientError::Configuration)
        ));
        assert!(rpc.requests.is_empty());
    }

    #[tokio::test]
    async fn batch_limit_accepts_64_but_never_crosses_worker_sessions() {
        let pairs: Vec<_> = (1..=64).map(|n| controller(n, 1000)).collect();
        let controls = pairs.iter().map(|(c, _)| c.clone()).collect();
        let mut rpc = fake([Action::Reject]);
        maintain(&mut rpc, controls, timing()).await.unwrap();
        assert_eq!(rpc.requests[0].attempts.len(), 64);

        let (a, _a_watch) = controller(100, 1000);
        let mut other = a.identity().clone();
        other.session_id = "00000000-0000-0000-0000-000000000099".into();
        other.attempt_id = "00000000-0000-0000-0000-000000000098".into();
        let (b, _b_watch) = authority_channel(other, window(1000)).unwrap();
        let mut rpc = fake([]);
        assert!(matches!(
            maintain(&mut rpc, vec![a, b], timing()).await,
            Err(ClientError::Configuration)
        ));
        assert!(rpc.requests.is_empty());
    }
}
