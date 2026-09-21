use super::{rpc_request, ClientError, ControlClient, RPC_TIMEOUT};
use crate::lease::{AuthorityWindow, LeaseError, MonoTime};
use dispatch_protocol::v1::{
    acquire_work_response, AcquireWorkRequest, AcquireWorkResponse, Assignment, AttemptAuthority,
    Decision, ListAssignmentsRequest, ListAssignmentsResponse, NoWorkReason, WorkerSession,
};

#[derive(Debug)]
pub struct GrantedAssignment {
    assignment: Box<Assignment>,
    authority: AuthorityWindow,
}
impl GrantedAssignment {
    pub fn assignment(&self) -> &Assignment {
        &self.assignment
    }
    pub fn authority(&self) -> &AuthorityWindow {
        &self.authority
    }
}

#[derive(Debug)]
pub enum WorkOutcome {
    Assignment(GrantedAssignment),
    NoWork(NoWorkReason),
    Rejected(Decision),
    Expired(AttemptAuthority),
}

#[derive(Debug)]
pub struct AssignmentPage {
    pub assignments: Vec<GrantedAssignment>,
    pub expired: Vec<AttemptAuthority>,
    pub next_after_job_id: String,
}

impl ControlClient {
    pub async fn acquire(
        &mut self,
        request: &AcquireWorkRequest,
    ) -> Result<WorkOutcome, ClientError> {
        let session = valid_session(request.session.as_ref())?;
        if !canonical_uuid(&request.request_id) {
            return Err(ClientError::Configuration);
        }
        let sent = MonoTime::now().map_err(|_| ClientError::Clock)?;
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.acquire_work(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        let received = MonoTime::now().map_err(|_| ClientError::Clock)?;
        // Retry callers retain the original UUID. A response is interpreted from
        // this request's send sample, including connection/queue/network delay.
        decode_acquisition(session, response, sent, received)
    }

    pub async fn list_assignments(
        &mut self,
        request: &ListAssignmentsRequest,
    ) -> Result<AssignmentPage, ClientError> {
        valid_session(request.session.as_ref())?;
        if request.page_size > 64
            || (!request.after_job_id.is_empty() && !canonical_uuid(&request.after_job_id))
        {
            return Err(ClientError::Configuration);
        }
        let sent = MonoTime::now().map_err(|_| ClientError::Clock)?;
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.list_assignments(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        let received = MonoTime::now().map_err(|_| ClientError::Clock)?;
        decode_page(request, response, sent, received)
    }
}

fn valid_session(session: Option<&WorkerSession>) -> Result<&WorkerSession, ClientError> {
    session
        .filter(|s| canonical_uuid(&s.worker_id) && canonical_uuid(&s.session_id))
        .ok_or(ClientError::Configuration)
}

fn decode_acquisition(
    session: &WorkerSession,
    response: AcquireWorkResponse,
    sent: MonoTime,
    received: MonoTime,
) -> Result<WorkOutcome, ClientError> {
    use acquire_work_response::Outcome;
    match response.outcome.ok_or(ClientError::Response)? {
        Outcome::Assignment(assignment) => check_assignment(session, assignment, sent, received),
        Outcome::NoWork(value) => match NoWorkReason::try_from(value) {
            Ok(reason) if reason != NoWorkReason::Unspecified => Ok(WorkOutcome::NoWork(reason)),
            _ => Err(ClientError::Response),
        },
        Outcome::Rejected(value) => match Decision::try_from(value) {
            Ok(
                decision @ (Decision::Fenced | Decision::StopRequested | Decision::AlreadyTerminal),
            ) => Ok(WorkOutcome::Rejected(decision)),
            _ => Err(ClientError::Response),
        },
    }
}

fn check_assignment(
    session: &WorkerSession,
    assignment: Box<Assignment>,
    sent: MonoTime,
    received: MonoTime,
) -> Result<WorkOutcome, ClientError> {
    let a = assignment.authority.as_ref().ok_or(ClientError::Response)?;
    let r = assignment.resources.as_ref().ok_or(ClientError::Response)?;
    let pinned = assignment
        .image_digest
        .rsplit_once("@sha256:")
        .is_some_and(|(name, digest)| !name.is_empty() && lower_hash(digest));
    if a.worker_id != session.worker_id
        || a.session_id != session.session_id
        || !canonical_uuid(&a.job_id)
        || !canonical_uuid(&a.attempt_id)
        || a.generation == 0
        || a.generation > i64::MAX as u64
        || r.cpu_millis == 0
        || r.cpu_millis > 1_024_000
        || r.memory_bytes < (1 << 20)
        || r.memory_bytes > (16_777_216u64 << 20)
        || r.memory_bytes % (1 << 20) != 0
        || r.scratch_bytes < (1 << 20)
        || r.scratch_bytes > (1_073_741_824u64 << 20)
        || r.scratch_bytes % (1 << 20) != 0
        || assignment.argv.is_empty()
        || assignment.argv.len() > 256
        || assignment.argv[0].is_empty()
        || assignment
            .argv
            .iter()
            .any(|arg| arg.len() > 8192 || arg.contains('\0'))
        || !pinned
        || assignment.image_digest.len() > 1024
        || assignment
            .image_digest
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        || assignment.canonical_job_spec_json.is_empty()
        || assignment.canonical_job_spec_json.len() > 2 * 1024 * 1024
        || !lower_hash(&assignment.spec_sha256)
        || !assignment.inputs.is_empty()
    {
        return Err(ClientError::Response);
    }
    // Expiry preserves only the authority tuple for reconciliation/cleanup. It
    // never returns a live window that a caller could use to start the workload.
    match AuthorityWindow::from_grant(
        sent,
        received,
        assignment.lease_duration_ms,
        assignment.phase_remaining_ms,
    ) {
        Ok(authority) => Ok(WorkOutcome::Assignment(GrantedAssignment {
            assignment,
            authority,
        })),
        Err(LeaseError::Expired) => Ok(WorkOutcome::Expired(a.clone())),
        Err(LeaseError::Clock) => Err(ClientError::Clock),
        Err(LeaseError::Invalid) => Err(ClientError::Response),
    }
}

fn decode_page(
    request: &ListAssignmentsRequest,
    response: ListAssignmentsResponse,
    sent: MonoTime,
    received: MonoTime,
) -> Result<AssignmentPage, ClientError> {
    let session = valid_session(request.session.as_ref())?;
    let limit = if request.page_size == 0 {
        32
    } else {
        request.page_size
    };
    if limit > 64 || response.assignments.len() > limit as usize {
        return Err(ClientError::Response);
    }
    let mut previous = request.after_job_id.clone();
    let mut page = AssignmentPage {
        assignments: Vec::new(),
        expired: Vec::new(),
        next_after_job_id: response.next_after_job_id,
    };
    for assignment in response.assignments {
        let job = &assignment
            .authority
            .as_ref()
            .ok_or(ClientError::Response)?
            .job_id;
        if job <= &previous {
            return Err(ClientError::Response);
        }
        previous = job.clone();
        match check_assignment(session, Box::new(assignment), sent, received)? {
            WorkOutcome::Assignment(grant) => page.assignments.push(grant),
            WorkOutcome::Expired(authority) => page.expired.push(authority),
            _ => return Err(ClientError::Response),
        }
    }
    // Cursors may advance past expired candidates omitted by the server. They
    // must still advance strictly past the request and never behind a returned job.
    if !page.next_after_job_id.is_empty()
        && (!canonical_uuid(&page.next_after_job_id)
            || page.next_after_job_id <= request.after_job_id
            || page.next_after_job_id < previous)
    {
        return Err(ClientError::Response);
    }
    Ok(page)
}

fn lower_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(n, b)| {
            if [8, 13, 18, 23].contains(&n) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatch_protocol::v1::{AttemptAuthority, Resources};

    fn session() -> WorkerSession {
        WorkerSession {
            worker_id: "00000000-0000-0000-0000-000000000001".into(),
            session_id: "00000000-0000-0000-0000-000000000002".into(),
        }
    }
    fn assignment() -> Assignment {
        let session = session();
        Assignment {
            authority: Some(AttemptAuthority {
                worker_id: session.worker_id,
                session_id: session.session_id,
                job_id: "00000000-0000-0000-0000-000000000003".into(),
                attempt_id: "00000000-0000-0000-0000-000000000004".into(),
                generation: 1,
            }),
            resources: Some(Resources {
                cpu_millis: 2000,
                memory_bytes: 4096 << 20,
                scratch_bytes: 8192 << 20,
            }),
            image_digest: format!("example.org/test@sha256:{}", "a".repeat(64)),
            argv: vec!["true".into()],
            canonical_job_spec_json: b"{\"kind\":\"Job\"}".to_vec(),
            spec_sha256: "b".repeat(64),
            lease_duration_ms: 30_000,
            phase_remaining_ms: 300_000,
            ..Default::default()
        }
    }
    fn response(a: Assignment) -> AcquireWorkResponse {
        AcquireWorkResponse {
            outcome: Some(acquire_work_response::Outcome::Assignment(Box::new(a))),
        }
    }

    #[test]
    fn received_assignments_bind_session_and_validate_resource_bounds() {
        let now = MonoTime::now().unwrap();
        assert!(matches!(
            decode_acquisition(&session(), response(assignment()), now, now).unwrap(),
            WorkOutcome::Assignment(_)
        ));
        for change in [
            |a: &mut Assignment| {
                a.authority.as_mut().unwrap().worker_id =
                    "00000000-0000-0000-0000-000000000099".into()
            },
            |a: &mut Assignment| {
                a.authority.as_mut().unwrap().session_id =
                    "00000000-0000-0000-0000-000000000099".into()
            },
            |a: &mut Assignment| a.authority.as_mut().unwrap().generation = u64::MAX,
            |a: &mut Assignment| a.authority.as_mut().unwrap().job_id = "invalid".into(),
            |a: &mut Assignment| a.resources.as_mut().unwrap().memory_bytes = u64::MAX,
            |a: &mut Assignment| a.resources.as_mut().unwrap().scratch_bytes = 0,
            |a: &mut Assignment| a.argv[0] = "bad\0command".into(),
            |a: &mut Assignment| a.spec_sha256 = "bad".into(),
            |a: &mut Assignment| a.image_digest = "image:latest".into(),
            |a: &mut Assignment| a.image_digest.insert(1, '\0'),
        ] {
            let mut a = assignment();
            change(&mut a);
            assert!(decode_acquisition(&session(), response(a), now, now).is_err());
        }
    }

    #[test]
    fn expired_grants_preserve_identity_without_execution_authority() {
        let now = MonoTime::now().unwrap();
        let mut a = assignment();
        a.lease_duration_ms = 5_000;
        assert!(matches!(
            decode_acquisition(&session(), response(a), now, now).unwrap(),
            WorkOutcome::Expired(_)
        ));
        for outcome in [
            None,
            Some(acquire_work_response::Outcome::NoWork(0)),
            Some(acquire_work_response::Outcome::NoWork(999)),
            Some(acquire_work_response::Outcome::Rejected(1)),
            Some(acquire_work_response::Outcome::Rejected(999)),
        ] {
            assert!(
                decode_acquisition(&session(), AcquireWorkResponse { outcome }, now, now).is_err()
            );
        }
    }

    #[test]
    fn recovery_validates_order_count_and_cursor_progress() {
        let now = MonoTime::now().unwrap();
        let request = ListAssignmentsRequest {
            session: Some(session()),
            page_size: 1,
            after_job_id: String::new(),
        };
        let a = assignment();
        let cursor = a.authority.as_ref().unwrap().job_id.clone();
        let page = ListAssignmentsResponse {
            assignments: vec![a.clone()],
            next_after_job_id: cursor.clone(),
        };
        assert_eq!(
            decode_page(&request, page.clone(), now, now)
                .unwrap()
                .assignments
                .len(),
            1
        );
        let mut duplicate = page.clone();
        duplicate.assignments.push(a);
        assert!(decode_page(&request, duplicate.clone(), now, now).is_err());
        let larger_request = ListAssignmentsRequest {
            page_size: 2,
            ..request.clone()
        };
        assert!(decode_page(&larger_request, duplicate, now, now).is_err());
        let mut invalid_cursor = page.clone();
        invalid_cursor.next_after_job_id = "invalid".into();
        assert!(decode_page(&request, invalid_cursor, now, now).is_err());
        let mut expired = page.clone();
        expired.assignments[0].lease_duration_ms = 5_000;
        let expired = decode_page(&request, expired, now, now).unwrap();
        assert!(expired.assignments.is_empty());
        assert_eq!(expired.expired.len(), 1);
        assert_eq!(expired.next_after_job_id, cursor);
        let request = ListAssignmentsRequest {
            after_job_id: cursor,
            ..request
        };
        assert!(decode_page(&request, page, now, now).is_err());
        let empty = ListAssignmentsResponse {
            assignments: vec![],
            next_after_job_id: "00000000-0000-0000-0000-000000000099".into(),
        };
        assert!(!decode_page(&request, empty, now, now)
            .unwrap()
            .next_after_job_id
            .is_empty());
    }
}
