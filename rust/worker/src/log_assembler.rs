//! Seal bounded raw log chunks into private, immutable upload candidates.
use crate::{
    log_capture::CapturedChunk,
    log_format::{LogFormatError, LogSegmentWriter},
    log_spool::{LogSpool, SpoolError, SpoolOutcome, StoredSegment},
    runtime::PreparedWorkspace,
};
use dispatch_protocol::v1::{LogGap, LogStream};
use ring::digest::{digest, SHA256};
use std::{
    collections::VecDeque,
    fmt,
    fs::File,
    time::{Duration, Instant},
};

pub const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub enum AssembleError {
    Invalid,
    Format(LogFormatError),
    Spool(SpoolError),
}
impl fmt::Display for AssembleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "log assembly error: {self:?}")
    }
}
impl std::error::Error for AssembleError {}
impl From<LogFormatError> for AssembleError {
    fn from(error: LogFormatError) -> Self {
        Self::Format(error)
    }
}
impl From<SpoolError> for AssembleError {
    fn from(error: SpoolError) -> Self {
        Self::Spool(error)
    }
}

#[derive(Debug)]
pub struct PendingSegment {
    entry: StoredSegment,
    stream: LogStream,
    first_sequence: u64,
    last_sequence: u64,
    gaps: Vec<LogGap>,
    sha256: String,
}
impl PendingSegment {
    pub fn stream(&self) -> LogStream {
        self.stream
    }
    pub fn first_sequence(&self) -> u64 {
        self.first_sequence
    }
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }
    pub fn gaps(&self) -> &[LogGap] {
        &self.gaps
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn size(&self) -> u64 {
        self.entry.size()
    }
}

struct OpenSegment {
    writer: LogSegmentWriter,
    opened: Instant,
}

pub struct LogAssembler {
    attempt_id: String,
    spool: LogSpool,
    open: [Option<OpenSegment>; 2],
    last_seen: [u64; 2],
    last_stored: [u64; 2],
    pending: VecDeque<PendingSegment>,
}
impl LogAssembler {
    pub fn new(workspace: &PreparedWorkspace, cap: u64) -> Result<Self, AssembleError> {
        let attempt_id = workspace
            .root()
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or(AssembleError::Invalid)?
            .to_owned();
        Ok(Self {
            attempt_id,
            spool: LogSpool::create(workspace, cap)?,
            open: [None, None],
            last_seen: [0; 2],
            last_stored: [0; 2],
            pending: VecDeque::new(),
        })
    }

    pub fn ingest(&mut self, chunk: CapturedChunk) -> Result<(), AssembleError> {
        let index = index(chunk.stream)?;
        if chunk.sequence <= self.last_seen[index] || chunk.payload.is_empty() {
            return Err(AssembleError::Invalid);
        }
        if self.open[index].is_none() {
            self.open[index] = Some(self.fresh(chunk.stream, index)?);
        }
        let append = self.open[index].as_mut().unwrap().writer.append(
            chunk.sequence,
            chunk.capture_unix_nanos,
            &chunk.payload,
        );
        if matches!(append, Err(LogFormatError::Full | LogFormatError::GapLimit)) {
            self.flush_stream(chunk.stream)?;
            self.open[index] = Some(self.fresh(chunk.stream, index)?);
            self.open[index].as_mut().unwrap().writer.append(
                chunk.sequence,
                chunk.capture_unix_nanos,
                &chunk.payload,
            )?;
        } else {
            append?;
        }
        self.last_seen[index] = chunk.sequence;
        Ok(())
    }

    pub fn flush_due(&mut self) -> Result<(), AssembleError> {
        let now = Instant::now();
        for stream in [LogStream::Stdout, LogStream::Stderr] {
            if self.open[index(stream)?]
                .as_ref()
                .is_some_and(|s| now.duration_since(s.opened) >= FLUSH_INTERVAL)
            {
                self.flush_stream(stream)?;
            }
        }
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<(), AssembleError> {
        for stream in [LogStream::Stdout, LogStream::Stderr] {
            self.flush_stream(stream)?;
        }
        Ok(())
    }

    pub fn front(&self) -> Option<&PendingSegment> {
        self.pending.front()
    }
    pub fn read_front(&self) -> Result<Option<File>, AssembleError> {
        self.pending
            .front()
            .map(|segment| self.spool.read(&segment.entry).map_err(Into::into))
            .transpose()
    }
    pub fn acknowledge_front(&mut self) -> Result<(), AssembleError> {
        let entry = self
            .pending
            .front()
            .ok_or(AssembleError::Invalid)?
            .entry
            .clone();
        self.spool.remove(entry)?;
        self.pending.pop_front();
        Ok(())
    }
    pub fn last_stored(&self, stream: LogStream) -> Result<u64, AssembleError> {
        Ok(self.last_stored[index(stream)?])
    }

    fn fresh(&self, stream: LogStream, index: usize) -> Result<OpenSegment, AssembleError> {
        Ok(OpenSegment {
            writer: LogSegmentWriter::new(&self.attempt_id, stream, self.last_stored[index] + 1)?,
            opened: Instant::now(),
        })
    }
    fn flush_stream(&mut self, stream: LogStream) -> Result<(), AssembleError> {
        let index = index(stream)?;
        let Some(open) = self.open[index].take() else {
            return Ok(());
        };
        let segment = open.writer.finish()?;
        let first = segment.first_sequence();
        let last = segment.last_sequence();
        let gaps = segment
            .gaps()
            .iter()
            .map(|gap| LogGap {
                stream: stream as i32,
                first_sequence: gap.first,
                last_sequence: gap.last,
            })
            .collect();
        let sha256 = digest(&SHA256, segment.bytes())
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        // A full spool drops this whole candidate. The next writer starts at
        // the last stored range so its sequence gap covers these lost bytes.
        if let SpoolOutcome::Stored(entry) = self.spool.store(stream, segment)? {
            self.last_stored[index] = last;
            self.pending.push_back(PendingSegment {
                entry,
                stream,
                first_sequence: first,
                last_sequence: last,
                gaps,
                sha256,
            });
        }
        Ok(())
    }
}

fn index(stream: LogStream) -> Result<usize, AssembleError> {
    match stream {
        LogStream::Stdout => Ok(0),
        LogStream::Stderr => Ok(1),
        _ => Err(AssembleError::Invalid),
    }
}
