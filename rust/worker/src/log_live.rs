//! Drain Docker output while preserving a bounded, private log spool.
use crate::{
    log_assembler::{AssembleError, LogAssembler, FLUSH_INTERVAL},
    log_capture::{CaptureCounts, CapturedChunk, LogQueue, QUEUE_CHUNKS},
    log_spool::MAX_SPOOL_BYTES,
    runtime::{PreparedWorkspace, Runtime, RuntimeError},
};
use dispatch_protocol::v1::LogStream;
use std::{fmt, future::Future, sync::Arc};
use tokio::sync::{mpsc, Mutex};

#[derive(Debug)]
pub enum LiveLogError {
    Assembly(AssembleError),
    Capture,
}
impl fmt::Display for LiveLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("live log collector failed")
    }
}
impl std::error::Error for LiveLogError {}
impl From<AssembleError> for LiveLogError {
    fn from(error: AssembleError) -> Self {
        Self::Assembly(error)
    }
}

pub struct LiveCapture {
    pub assembler: SharedAssembler,
    pub counts: [u64; 2],
    pub complete: bool,
}

pub type SharedAssembler = Arc<Mutex<LogAssembler>>;

pub fn new_shared(
    workspace: &PreparedWorkspace,
    spool_cap: u64,
) -> Result<SharedAssembler, LiveLogError> {
    Ok(Arc::new(Mutex::new(LogAssembler::new(
        workspace, spool_cap,
    )?)))
}

pub async fn capture_running<R: Runtime>(
    runtime: &R,
    handle: &R::Handle,
    workspace: &PreparedWorkspace,
) -> Result<LiveCapture, LiveLogError> {
    let (queue, receiver, counts) =
        LogQueue::bounded(QUEUE_CHUNKS).map_err(|_| LiveLogError::Capture)?;
    collect_capture(
        workspace,
        MAX_SPOOL_BYTES,
        receiver,
        counts,
        runtime.follow_logs(handle, queue),
    )
    .await
}

pub async fn collect_capture<F>(
    workspace: &PreparedWorkspace,
    spool_cap: u64,
    receiver: mpsc::Receiver<CapturedChunk>,
    counts: CaptureCounts,
    producer: F,
) -> Result<LiveCapture, LiveLogError>
where
    F: Future<Output = Result<(), RuntimeError>>,
{
    let assembler = new_shared(workspace, spool_cap)?;
    collect_capture_shared(assembler, receiver, counts, producer).await
}

pub async fn collect_capture_shared<F>(
    assembler: SharedAssembler,
    mut receiver: mpsc::Receiver<CapturedChunk>,
    counts: CaptureCounts,
    producer: F,
) -> Result<LiveCapture, LiveLogError>
where
    F: Future<Output = Result<(), RuntimeError>>,
{
    let mut producer = std::pin::pin!(producer);
    let mut producer_result = None;
    let mut receiver_closed = false;
    let mut flush =
        tokio::time::interval_at(tokio::time::Instant::now() + FLUSH_INTERVAL, FLUSH_INTERVAL);
    loop {
        if receiver_closed && producer_result.is_some() {
            break;
        }
        tokio::select! {
            result = &mut producer, if producer_result.is_none() => producer_result = Some(result),
            item = receiver.recv(), if !receiver_closed => match item {
                Some(chunk) => assembler.lock().await.ingest(chunk)?,
                None => receiver_closed = true,
            },
            _ = flush.tick() => assembler.lock().await.flush_due()?,
        }
    }
    assembler.lock().await.flush_all()?;
    // Count dropped tail chunks even when no later record exists to bridge
    // their sequence gap in a sealed segment.
    let totals = [
        counts
            .last_sequence(LogStream::Stdout)
            .map_err(|_| LiveLogError::Capture)?,
        counts
            .last_sequence(LogStream::Stderr)
            .map_err(|_| LiveLogError::Capture)?,
    ];
    Ok(LiveCapture {
        assembler,
        counts: totals,
        complete: matches!(producer_result, Some(Ok(()))),
    })
}
