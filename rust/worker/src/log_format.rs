//! Bounded binary segment encoding for one attempt and one output stream.
use crate::control::canonical_uuid;
use dispatch_protocol::v1::LogStream;
use std::fmt;

pub const MAX_SEGMENT_BYTES: usize = 1 << 20;
const HEADER_BYTES: usize = 48;
const RECORD_HEADER_BYTES: usize = 20;
const MAX_GAPS: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormatError {
    Invalid,
    Sequence,
    Full,
    GapLimit,
}

impl fmt::Display for LogFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid log segment: {self:?}")
    }
}
impl std::error::Error for LogFormatError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceGap {
    pub first: u64,
    pub last: u64,
}

#[derive(Debug)]
pub struct LogSegment {
    bytes: Vec<u8>,
    attempt_id: String,
    stream: LogStream,
    first_sequence: u64,
    last_sequence: u64,
    gaps: Vec<SequenceGap>,
}
impl LogSegment {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }
    pub fn stream(&self) -> LogStream {
        self.stream
    }
    pub fn first_sequence(&self) -> u64 {
        self.first_sequence
    }
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }
    pub fn gaps(&self) -> &[SequenceGap] {
        &self.gaps
    }
}

pub struct LogSegmentWriter {
    bytes: Vec<u8>,
    attempt_id: String,
    stream: LogStream,
    first_sequence: u64,
    last_sequence: Option<u64>,
    gaps: Vec<SequenceGap>,
}

impl LogSegmentWriter {
    pub fn new(
        attempt_id: &str,
        stream: LogStream,
        first_sequence: u64,
    ) -> Result<Self, LogFormatError> {
        let stream_byte = match stream {
            LogStream::Stdout => 1,
            LogStream::Stderr => 2,
            _ => return Err(LogFormatError::Invalid),
        };
        if !canonical_uuid(attempt_id) || first_sequence == 0 || first_sequence > i64::MAX as u64 {
            return Err(LogFormatError::Invalid);
        }
        let mut bytes = Vec::with_capacity(4096);
        bytes.extend_from_slice(b"DSPLOG01");
        bytes.extend_from_slice(attempt_id.as_bytes());
        bytes.extend_from_slice(&[stream_byte, 0, 0, 0]);
        debug_assert_eq!(bytes.len(), HEADER_BYTES);
        Ok(Self {
            bytes,
            attempt_id: attempt_id.to_owned(),
            stream,
            first_sequence,
            last_sequence: None,
            gaps: Vec::new(),
        })
    }

    pub fn append(
        &mut self,
        sequence: u64,
        capture_unix_nanos: i64,
        payload: &[u8],
    ) -> Result<(), LogFormatError> {
        if sequence == 0
            || sequence > i64::MAX as u64
            || capture_unix_nanos < 0
            || payload.is_empty()
        {
            return Err(LogFormatError::Invalid);
        }
        let expected = self
            .last_sequence
            .map_or(self.first_sequence, |last| last + 1);
        if sequence < expected {
            return Err(LogFormatError::Sequence);
        }
        if sequence > expected && self.gaps.len() == MAX_GAPS {
            return Err(LogFormatError::GapLimit);
        }
        let available = MAX_SEGMENT_BYTES - self.bytes.len();
        if available < RECORD_HEADER_BYTES || payload.len() > available - RECORD_HEADER_BYTES {
            return Err(LogFormatError::Full);
        }
        // Only commit a gap and sequence after the whole framed record fits.
        // A full segment can be flushed and retried in a new segment unchanged.
        if sequence > expected {
            self.gaps.push(SequenceGap {
                first: expected,
                last: sequence - 1,
            });
        }
        self.bytes.extend_from_slice(&sequence.to_be_bytes());
        self.bytes
            .extend_from_slice(&capture_unix_nanos.to_be_bytes());
        self.bytes
            .extend_from_slice(&(payload.len() as u32).to_be_bytes());
        self.bytes.extend_from_slice(payload);
        self.last_sequence = Some(sequence);
        Ok(())
    }

    pub fn finish(self) -> Result<LogSegment, LogFormatError> {
        let last_sequence = self.last_sequence.ok_or(LogFormatError::Invalid)?;
        Ok(LogSegment {
            bytes: self.bytes,
            attempt_id: self.attempt_id,
            stream: self.stream,
            first_sequence: self.first_sequence,
            last_sequence,
            gaps: self.gaps,
        })
    }
}
