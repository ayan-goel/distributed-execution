//! Bounded, nonblocking handoff between Docker output and segment encoding.
use dispatch_protocol::v1::LogStream;
use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;

pub const MAX_CHUNK_BYTES: usize = 64 << 10;
pub const QUEUE_CHUNKS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureError {
    Invalid,
    Clock,
    Closed,
    Sequence,
}
impl fmt::Display for CaptureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "log capture error: {self:?}")
    }
}
impl std::error::Error for CaptureError {}

#[derive(Debug, PartialEq, Eq)]
pub struct CapturedChunk {
    pub stream: LogStream,
    pub sequence: u64,
    pub capture_unix_nanos: i64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct CaptureCounts(Arc<[AtomicU64; 2]>);
impl CaptureCounts {
    pub fn last_sequence(&self, stream: LogStream) -> Result<u64, CaptureError> {
        Ok(self.0[index(stream)?].load(Ordering::Acquire))
    }
}

pub struct LogQueue {
    sender: mpsc::Sender<CapturedChunk>,
    counts: CaptureCounts,
}
impl LogQueue {
    pub fn bounded(
        capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<CapturedChunk>, CaptureCounts), CaptureError> {
        if capacity == 0 || capacity > QUEUE_CHUNKS {
            return Err(CaptureError::Invalid);
        }
        let (sender, receiver) = mpsc::channel(capacity);
        let counts = CaptureCounts(Arc::new([AtomicU64::new(0), AtomicU64::new(0)]));
        Ok((
            Self {
                sender,
                counts: counts.clone(),
            },
            receiver,
            counts,
        ))
    }

    pub fn push_frame(&self, stream: LogStream, bytes: &[u8]) -> Result<(), CaptureError> {
        let index = index(stream)?;
        for payload in bytes.chunks(MAX_CHUNK_BYTES) {
            let capture_unix_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| CaptureError::Clock)?
                .as_nanos();
            let capture_unix_nanos =
                i64::try_from(capture_unix_nanos).map_err(|_| CaptureError::Clock)?;
            let sequence = self.counts.0[index]
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                    old.checked_add(1).filter(|next| *next <= i64::MAX as u64)
                })
                .map_err(|_| CaptureError::Sequence)?
                + 1;
            let item = CapturedChunk {
                stream,
                sequence,
                capture_unix_nanos,
                payload: payload.to_vec(),
            };
            match self.sender.try_send(item) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(CaptureError::Closed),
            }
            // INVARIANT: full queues drop only this chunk, never block Docker's
            // stdout pipe. The advanced sequence exposes the loss to the reader.
        }
        Ok(())
    }
}
fn index(stream: LogStream) -> Result<usize, CaptureError> {
    match stream {
        LogStream::Stdout => Ok(0),
        LogStream::Stderr => Ok(1),
        _ => Err(CaptureError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturated_queue_never_blocks_and_retains_dropped_tail_count() {
        let (queue, mut receiver, counts) = LogQueue::bounded(1).unwrap();
        queue.push_frame(LogStream::Stdout, b"first").unwrap();
        queue.push_frame(LogStream::Stdout, b"dropped").unwrap();
        assert_eq!(receiver.try_recv().unwrap().sequence, 1);
        queue.push_frame(LogStream::Stdout, b"third").unwrap();
        let next = receiver.try_recv().unwrap();
        assert_eq!((next.sequence, next.payload), (3, b"third".to_vec()));
        queue.push_frame(LogStream::Stdout, b"tail lost").unwrap();
        assert_eq!(counts.last_sequence(LogStream::Stdout).unwrap(), 4);
        assert_eq!(counts.last_sequence(LogStream::Stderr).unwrap(), 0);
    }

    #[test]
    fn frame_splitting_bounds_memory_and_keeps_binary_bytes() {
        let (queue, mut receiver, _) = LogQueue::bounded(2).unwrap();
        let mut bytes = vec![0xff; MAX_CHUNK_BYTES + 1];
        bytes[MAX_CHUNK_BYTES] = 0;
        queue.push_frame(LogStream::Stderr, &bytes).unwrap();
        let first = receiver.try_recv().unwrap();
        let second = receiver.try_recv().unwrap();
        assert_eq!(first.payload.len(), MAX_CHUNK_BYTES);
        assert_eq!((second.sequence, second.payload), (2, vec![0]));
        assert_eq!(first.stream, LogStream::Stderr);
        assert!(first.capture_unix_nanos > 0);
        assert!(LogQueue::bounded(QUEUE_CHUNKS + 1).is_err());
    }
}
