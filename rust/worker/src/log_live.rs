//! Drain Docker output while preserving a bounded, private log spool.
use crate::{
    log_assembler::{AssembleError, LogAssembler, FLUSH_INTERVAL},
    log_capture::{CaptureCounts, CapturedChunk},
    runtime::{PreparedWorkspace, RuntimeError},
};
use dispatch_protocol::v1::LogStream;
use std::{fmt, future::Future};
use tokio::sync::mpsc;

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
    pub assembler: LogAssembler,
    pub counts: [u64; 2],
    pub complete: bool,
}

pub async fn collect_capture<F>(
    workspace: &PreparedWorkspace,
    spool_cap: u64,
    mut receiver: mpsc::Receiver<CapturedChunk>,
    counts: CaptureCounts,
    producer: F,
) -> Result<LiveCapture, LiveLogError>
where
    F: Future<Output = Result<(), RuntimeError>>,
{
    let mut assembler = LogAssembler::new(workspace, spool_cap)?;
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
                Some(chunk) => assembler.ingest(chunk)?,
                None => receiver_closed = true,
            },
            _ = flush.tick() => assembler.flush_due()?,
        }
    }
    assembler.flush_all()?;
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
