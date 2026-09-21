use super::*;
use crate::control::{canonical_uuid, lower_hash};
use std::collections::HashSet;

const MAX_INVENTORY: usize = 1024;

#[derive(Debug, PartialEq, Eq)]
struct Fingerprint {
    id: String,
    authority: AttemptAuthority,
    image_id: String,
    spec_sha256: String,
    scratch_policy: String,
}

/// Discovery supplies cleanup evidence only; this type cannot be passed to start.
#[derive(Debug)]
pub struct RecoveredContainer {
    fingerprint: Fingerprint,
    status: ContainerStatus,
}
impl RecoveredContainer {
    pub fn id(&self) -> &str {
        &self.fingerprint.id
    }
    pub fn authority(&self) -> &AttemptAuthority {
        &self.fingerprint.authority
    }
    pub fn status(&self) -> &ContainerStatus {
        &self.status
    }
}

pub trait RecoveryRuntime {
    fn inventory(
        &self,
        worker_id: &str,
    ) -> impl Future<Output = Result<Vec<RecoveredContainer>, RuntimeError>> + Send;
    fn remove_previous(
        &self,
        container: &RecoveredContainer,
        current_session: &str,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send;
}

impl RecoveryRuntime for DockerRuntime {
    async fn inventory(&self, worker_id: &str) -> Result<Vec<RecoveredContainer>, RuntimeError> {
        if !canonical_uuid(worker_id) {
            return Err(RuntimeError::Identity);
        }
        // Include exited/created containers and request one extra result so a
        // truncated snapshot can never be reported as complete reconciliation.
        let options = ListContainersOptionsBuilder::default()
            .all(true)
            .limit((MAX_INVENTORY + 1) as i32)
            .filters(&HashMap::from([(
                "label",
                vec![format!("dev.dispatch.worker={worker_id}")],
            )]))
            .build();
        tokio::time::timeout(RPC_TIMEOUT, async {
            let summaries = self.docker.list_containers(Some(options)).await?;
            if summaries.len() > MAX_INVENTORY {
                return Err(RuntimeError::InventoryLimit);
            }
            let mut ids = HashSet::new();
            let mut inventory = Vec::with_capacity(summaries.len());
            for summary in summaries {
                let id = summary.id.ok_or(RuntimeError::Identity)?;
                if !lower_hash(&id) || !ids.insert(id.clone()) {
                    return Err(RuntimeError::Identity);
                }
                // List metadata may be stale. Inspect by full ID and validate
                // ownership again; absence is safe, a daemon error is not absence.
                let actual = match self.docker.inspect_container(&id, None).await {
                    Ok(actual) => actual,
                    Err(DockerError::DockerResponseServerError {
                        status_code: 404, ..
                    }) => continue,
                    Err(error) => return Err(error.into()),
                };
                inventory.push(recover(actual, worker_id, &id)?);
            }
            inventory.sort_unstable_by(|a, b| a.id().cmp(b.id()));
            Ok(inventory)
        })
        .await
        .map_err(|_| RuntimeError::Deadline)?
    }

    async fn remove_previous(
        &self,
        container: &RecoveredContainer,
        current_session: &str,
    ) -> Result<(), RuntimeError> {
        // Caller must first register the new incarnation, fencing the previous
        // one. Never use recovery cleanup for this session's completion workflow.
        if !canonical_uuid(current_session) || container.authority().session_id == current_session {
            return Err(RuntimeError::Identity);
        }
        let actual = match bounded(
            RPC_TIMEOUT,
            self.docker.inspect_container(container.id(), None),
        )
        .await
        {
            Ok(actual) => actual,
            Err(RuntimeError::Daemon(404)) => return Ok(()),
            Err(error) => return Err(error),
        };
        let checked = recover(actual, &container.authority().worker_id, container.id())?;
        if checked.fingerprint != container.fingerprint {
            return Err(RuntimeError::Identity);
        }
        // Fenced executions must not resume. Forced removal kills running or
        // paused containers without unpausing/adopting them; volume deletion is
        // disabled so cleanup cannot discard separate stored workload evidence.
        let options = RemoveContainerOptionsBuilder::default()
            .force(true)
            .v(false)
            .build();
        match bounded(
            RPC_TIMEOUT,
            self.docker.remove_container(container.id(), Some(options)),
        )
        .await
        {
            Ok(()) => {}
            Err(RuntimeError::Daemon(404)) => return Ok(()),
            Err(error) => return Err(error),
        }
        match bounded(
            RPC_TIMEOUT,
            self.docker.inspect_container(container.id(), None),
        )
        .await
        {
            Err(RuntimeError::Daemon(404)) => Ok(()),
            Err(error) => Err(error),
            Ok(_) => Err(RuntimeError::Transport),
        }
    }
}

fn recover(
    actual: ContainerInspectResponse,
    worker: &str,
    id: &str,
) -> Result<RecoveredContainer, RuntimeError> {
    let labels = actual
        .config
        .as_ref()
        .and_then(|c| c.labels.as_ref())
        .ok_or(RuntimeError::Identity)?;
    let label = |key: &str| labels.get(key).cloned().ok_or(RuntimeError::Identity);
    let generation = label("dev.dispatch.generation")?;
    let authority = AttemptAuthority {
        worker_id: label("dev.dispatch.worker")?,
        session_id: label("dev.dispatch.session")?,
        job_id: label("dev.dispatch.job")?,
        attempt_id: label("dev.dispatch.attempt")?,
        generation: generation.parse().map_err(|_| RuntimeError::Identity)?,
    };
    let image_id = actual.image.clone().ok_or(RuntimeError::Identity)?;
    let spec_sha256 = label("dev.dispatch.spec-sha256")?;
    let scratch_policy = label("dev.dispatch.scratch-policy")?;
    if actual.id.as_deref() != Some(id)
        || !lower_hash(id)
        || authority.worker_id != worker
        || [
            &authority.worker_id,
            &authority.session_id,
            &authority.job_id,
            &authority.attempt_id,
        ]
        .iter()
        .any(|v| !canonical_uuid(v))
        || authority.generation == 0
        || authority.generation > i64::MAX as u64
        || authority.generation.to_string() != generation
        || !image_id.strip_prefix("sha256:").is_some_and(lower_hash)
        || !lower_hash(&spec_sha256)
        || scratch_policy != "soft-development"
        || actual.name.as_deref() != Some(&format!("/dispatch-{}", authority.attempt_id))
    {
        return Err(RuntimeError::Identity);
    }
    Ok(RecoveredContainer {
        fingerprint: Fingerprint {
            id: id.to_owned(),
            authority,
            image_id,
            spec_sha256,
            scratch_policy,
        },
        status: container_status(actual)?,
    })
}
