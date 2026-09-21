use dispatch_protocol::{
    v1::{
        worker_service_client::WorkerServiceClient, HeartbeatRequest, HeartbeatResponse,
        RegisterWorkerRequest, RegisterWorkerResponse,
    },
    MAX_MESSAGE_BYTES, VERSION,
};
use std::{fmt, time::Duration};
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
    Code, Request,
};

const RPC_TIMEOUT: Duration = Duration::from_secs(5);

mod work;
pub use work::{AssignmentPage, GrantedAssignment, WorkOutcome};
mod renew;
pub use renew::{RenewalOutcome, RenewedLease};
mod phase;
pub(crate) use phase::validate_request as validate_phase_request;
pub use phase::PhaseStatus;
pub(crate) use work::{canonical_uuid, lower_hash};

#[derive(Debug)]
pub enum ClientError {
    Configuration,
    Connection,
    Deadline,
    Response,
    Clock,
    Random,
    Rpc(Box<tonic::Status>),
}

impl ClientError {
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Connection | Self::Deadline)
            || matches!(self, Self::Rpc(status) if matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded))
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Transport diagnostics can contain credentials or endpoint contents.
        // Expose only categories; RPC messages are escaped before terminal output.
        match self {
            Self::Configuration => write!(f, "invalid worker connection configuration"),
            Self::Connection => write!(f, "worker connection failed"),
            Self::Deadline => write!(f, "worker request deadline exceeded"),
            Self::Response => write!(f, "invalid control-plane response"),
            Self::Clock => write!(f, "worker monotonic clock failed"),
            Self::Random => write!(f, "worker operation identity generation failed"),
            Self::Rpc(status) => {
                write!(f, "worker RPC {:?}: {:?}", status.code(), status.message())
            }
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Clone)]
pub struct ControlClient {
    inner: WorkerServiceClient<Channel>,
}

impl ControlClient {
    pub async fn connect(
        address: &str,
        ca_pem: &[u8],
        certificate_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, ClientError> {
        // Only the configured CA authenticates the control plane. Client identity
        // stays inside TLS; there is no insecure or system-root fallback.
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca_pem))
            .identity(Identity::from_pem(certificate_pem, private_key_pem))
            .timeout(RPC_TIMEOUT);
        let endpoint = endpoint(address)?
            .tls_config(tls)
            .map_err(|_| ClientError::Configuration)?;
        let channel = tokio::time::timeout(RPC_TIMEOUT, endpoint.connect())
            .await
            .map_err(|_| ClientError::Deadline)?
            .map_err(|_| ClientError::Connection)?;
        Ok(Self {
            inner: WorkerServiceClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        })
    }

    pub async fn register(
        &mut self,
        request: &RegisterWorkerRequest,
    ) -> Result<RegisterWorkerResponse, ClientError> {
        // The caller owns the durable operation identity. Retrying this method
        // must not allocate a new session or alter the registration payload.
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.register_worker(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        validate_registration(request, &response)?;
        Ok(response)
    }

    pub async fn heartbeat(
        &mut self,
        request: &HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClientError> {
        let response = tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner.heartbeat(rpc_request(request.clone())),
        )
        .await
        .map_err(|_| ClientError::Deadline)?
        .map_err(|status| ClientError::Rpc(Box::new(status)))?
        .into_inner();
        validate_heartbeat(request, &response)?;
        Ok(response)
    }
}

fn rpc_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    // The wire deadline bounds server work; the outer timeout also bounds local
    // connection readiness and response delivery when the transport stalls.
    request.set_timeout(RPC_TIMEOUT);
    request
}

pub(crate) fn endpoint(address: &str) -> Result<Endpoint, ClientError> {
    if address.contains(['@', '?', '#']) {
        return Err(ClientError::Configuration);
    }
    let endpoint =
        Endpoint::from_shared(address.to_owned()).map_err(|_| ClientError::Configuration)?;
    let uri = endpoint.uri();
    if uri.scheme_str() != Some("https") || uri.host().is_none() || !matches!(uri.path(), "" | "/")
    {
        return Err(ClientError::Configuration);
    }
    Ok(endpoint.connect_timeout(RPC_TIMEOUT))
}

fn validate_registration(
    request: &RegisterWorkerRequest,
    response: &RegisterWorkerResponse,
) -> Result<(), ClientError> {
    let session = response.session.as_ref().ok_or(ClientError::Response)?;
    if session.worker_id != request.worker_id
        || session.session_id != request.requested_session_id
        || session.worker_id.is_empty()
        || session.session_id.is_empty()
        || response.session_generation == 0
        || response.session_generation > i64::MAX as u64
        || response.protocol_version != VERSION
    {
        return Err(ClientError::Response);
    }
    Ok(())
}

fn validate_heartbeat(
    request: &HeartbeatRequest,
    response: &HeartbeatResponse,
) -> Result<(), ClientError> {
    let session = request.session.as_ref().ok_or(ClientError::Configuration)?;
    // Stops may refer to an older session during reconciliation, but never
    // another host. Runtime cleanup must additionally match container labels.
    if response.stop.len() > 2048
        || response.stop.iter().any(|authority| {
            authority.worker_id != session.worker_id
                || authority.generation == 0
                || authority.generation > i64::MAX as u64
                || authority.job_id.is_empty()
                || authority.attempt_id.is_empty()
                || authority.session_id.is_empty()
        })
    {
        return Err(ClientError::Response);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatch_protocol::v1::{RegisterWorkerRequest, RegisterWorkerResponse, WorkerSession};

    #[test]
    fn endpoints_require_https_without_credentials_paths_or_queries() {
        for url in [
            "https://dispatch.example:8444",
            "https://127.0.0.1:8444/",
            "https://[::1]:8444",
        ] {
            assert!(endpoint(url).is_ok(), "{url}");
        }
        for url in [
            "http://127.0.0.1:8444",
            "https://user:password@dispatch.example",
            "https://dispatch.example?token=secret",
            "https://dispatch.example/path",
            "https://dispatch.example/#fragment",
            "not-a-url",
        ] {
            assert!(endpoint(url).is_err(), "{url}");
        }
    }

    #[test]
    fn registration_rejects_changed_authority_and_protocol() {
        let request = RegisterWorkerRequest {
            worker_id: "host".into(),
            requested_session_id: "session".into(),
            ..Default::default()
        };
        let mut response = RegisterWorkerResponse {
            session: Some(WorkerSession {
                worker_id: "host".into(),
                session_id: "session".into(),
            }),
            session_generation: 1,
            protocol_version: 1,
            cleanup_required: true,
        };
        assert!(validate_registration(&request, &response).is_ok());
        response.session_generation = 0;
        assert!(validate_registration(&request, &response).is_err());
        response.session_generation = 1;
        response.session.as_mut().unwrap().session_id = "different".into();
        assert!(validate_registration(&request, &response).is_err());
        response.session.as_mut().unwrap().session_id = "session".into();
        response.protocol_version = 2;
        assert!(validate_registration(&request, &response).is_err());
    }

    #[test]
    fn terminal_session_errors_are_not_retried_as_transport_failures() {
        assert!(ClientError::Connection.retryable());
        assert!(
            ClientError::Rpc(Box::new(tonic::Status::unavailable("database unavailable")))
                .retryable()
        );
        assert!(
            !ClientError::Rpc(Box::new(tonic::Status::failed_precondition(
                "SESSION_FENCED"
            )))
            .retryable()
        );
        assert!(!ClientError::Rpc(Box::new(tonic::Status::unauthenticated("revoked"))).retryable());
        assert!(!ClientError::Rpc(Box::new(tonic::Status::aborted("STALE_HEARTBEAT"))).retryable());
    }

    #[test]
    fn cleanup_accepts_previous_sessions_but_rejects_other_hosts_and_invalid_authority() {
        use dispatch_protocol::v1::AttemptAuthority;
        let request = HeartbeatRequest {
            session: Some(WorkerSession {
                worker_id: "host".into(),
                session_id: "current".into(),
            }),
            ..Default::default()
        };
        let stop = AttemptAuthority {
            worker_id: "host".into(),
            session_id: "previous".into(),
            job_id: "job".into(),
            attempt_id: "attempt".into(),
            generation: 1,
        };
        let mut response = HeartbeatResponse {
            stop: vec![stop.clone()],
            ..Default::default()
        };
        assert!(validate_heartbeat(&request, &response).is_ok());
        response.stop[0].worker_id = "other".into();
        assert!(validate_heartbeat(&request, &response).is_err());
        response.stop[0] = stop.clone();
        for generation in [0, u64::MAX] {
            response.stop[0].generation = generation;
            assert!(validate_heartbeat(&request, &response).is_err());
        }
        response.stop = vec![stop; 2049];
        assert!(validate_heartbeat(&request, &response).is_err());
    }
}
