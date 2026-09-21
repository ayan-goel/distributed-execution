use super::{Journal, JournalError, RecoveredAttempt};
use dispatch_protocol::v1::{
    Assignment, AttemptState, CompleteAttemptRequest, CompleteAttemptResponse,
    RegisterWorkerResponse, ReportPhaseRequest, WorkerSession,
};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct AsyncJournal {
    journal: Arc<Mutex<Journal>>,
    gate: Arc<Semaphore>,
}
impl AsyncJournal {
    pub fn new(journal: Journal) -> Self {
        Self {
            journal: Arc::new(Mutex::new(journal)),
            gate: Arc::new(Semaphore::new(1)),
        }
    }

    pub(crate) async fn apply<T, F>(&self, operation: F) -> Result<T, JournalError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Journal) -> Result<T, JournalError> + Send + 'static,
    {
        // Acquire asynchronously before allocating a blocking task. Waiting
        // operations cannot exhaust Tokio's blocking pool or delay lease timers.
        let permit = self
            .gate
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| JournalError::Poisoned)?;
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || {
            // Blocking I/O cannot be aborted once started. Both the permit and
            // journal ownership stay here even if the awaiting future is dropped.
            let _permit = permit;
            let mut journal = journal.lock().map_err(|_| JournalError::Poisoned)?;
            operation(&mut journal)
        })
        .await
        .map_err(|_| JournalError::Poisoned)?
    }

    pub async fn record_registration(
        &self,
        reply: RegisterWorkerResponse,
    ) -> Result<(), JournalError> {
        self.apply(move |journal| journal.record_registration(&reply))
            .await
    }
    pub(crate) async fn prepare_launch(
        &self,
        assignment: Assignment,
        session: WorkerSession,
    ) -> Result<ReportPhaseRequest, JournalError> {
        self.apply(move |journal| {
            let identity = assignment.authority.as_ref().ok_or(JournalError::Invalid)?;
            let saved = journal.load_session()?.ok_or(JournalError::Identity)?;
            // Only the acknowledged incarnation begun by this open owner may
            // launch. Reopening historical session metadata restores no authority.
            if journal.incarnation.as_deref() != Some(session.session_id.as_str())
                || journal.worker_id != session.worker_id
                || saved.generation().is_none()
                || saved.registration().requested_session_id != session.session_id
                || identity.worker_id != session.worker_id
                || identity.session_id != session.session_id
            {
                return Err(JournalError::Identity);
            }
            // Claim the attempt while serialized with all journal mutations.
            // Even an incomplete earlier launch requires reconciliation, not replay.
            if journal.load_attempt(&identity.attempt_id)?.is_some() {
                return Err(JournalError::Conflict);
            }
            journal.persist_assignment(&assignment)?;
            journal.prepare_phase(&identity.attempt_id, AttemptState::Starting)
        })
        .await
    }
    pub async fn persist_assignment(&self, assignment: Assignment) -> Result<(), JournalError> {
        self.apply(move |journal| journal.persist_assignment(&assignment))
            .await
    }
    pub async fn load_attempt(&self, id: String) -> Result<Option<RecoveredAttempt>, JournalError> {
        self.apply(move |journal| journal.load_attempt(&id)).await
    }
    pub async fn persist_completion(
        &self,
        request: CompleteAttemptRequest,
    ) -> Result<(), JournalError> {
        self.apply(move |journal| journal.persist_completion(&request))
            .await
    }
    pub async fn record_completion_response(
        &self,
        request: CompleteAttemptRequest,
        response: CompleteAttemptResponse,
    ) -> Result<(), JournalError> {
        self.apply(move |journal| journal.record_completion_response(&request, &response))
            .await
    }
    pub async fn bind_container(
        &self,
        id: String,
        container_id: String,
    ) -> Result<(), JournalError> {
        self.apply(move |journal| journal.bind_container(&id, &container_id))
            .await
    }
    pub async fn record_exit(&self, id: String, code: i32, oom: bool) -> Result<(), JournalError> {
        self.apply(move |journal| journal.record_exit(&id, code, oom))
            .await
    }
    pub async fn prepare_phase(
        &self,
        id: String,
        phase: AttemptState,
    ) -> Result<ReportPhaseRequest, JournalError> {
        self.apply(move |journal| journal.prepare_phase(&id, phase))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::JournalLimits;
    use std::{
        fs,
        os::unix::fs::DirBuilderExt,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    const WORKER: &str = "00000000-0000-0000-0000-000000000001";
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "dispatch-async-journal-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
            Self(root.canonicalize().unwrap())
        }
        fn journal(&self) -> Journal {
            Journal::open(&self.0, WORKER, JournalLimits::default()).unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_write_keeps_its_permit_and_lock_after_awaiting_task_is_cancelled() {
        let fixture = Fixture::new();
        let journal = AsyncJournal::new(fixture.journal());
        let blocked = journal.clone();
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            blocked
                .apply(move |_| {
                    entered.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                    Ok(())
                })
                .await
        });
        entered_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(matches!(
            Journal::open(&fixture.0, WORKER, JournalLimits::default()),
            Err(JournalError::Busy)
        ));

        let second = journal.clone();
        let (ran, mut ran_rx) = tokio::sync::oneshot::channel();
        let next = tokio::spawn(async move {
            second
                .apply(move |_| {
                    let _ = ran.send(());
                    Ok(())
                })
                .await
        });
        // This timer must keep progressing on a single-thread runtime while the
        // journal's blocking closure is stalled and a second operation is queued.
        assert!(tokio::time::timeout(Duration::from_millis(25), &mut ran_rx)
            .await
            .is_err());
        drop(journal);
        release.send(()).unwrap();
        next.await.unwrap().unwrap();
        ran_rx.await.unwrap();
        fixture.journal();
    }

    #[tokio::test]
    async fn panic_poisoning_prevents_later_journal_work() {
        let fixture = Fixture::new();
        let journal = AsyncJournal::new(fixture.journal());
        let error = journal
            .apply::<(), _>(|_| panic!("injected journal failure"))
            .await;
        assert!(matches!(error, Err(JournalError::Poisoned)));
        assert!(matches!(
            journal.apply(|_| Ok(())).await,
            Err(JournalError::Poisoned)
        ));
    }
}
