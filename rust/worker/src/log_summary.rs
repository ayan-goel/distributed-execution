//! Build conservative completion claims from registered log evidence.
use crate::journal::LogUpload;
use dispatch_protocol::v1::{LogGap, LogStream};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummaryError {
    Invalid,
    Limit,
}
impl fmt::Display for SummaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "log summary error: {self:?}")
    }
}
impl std::error::Error for SummaryError {}

#[derive(Debug, PartialEq, Eq)]
pub struct LogSummary {
    pub complete: bool,
    pub gaps: Vec<LogGap>,
}

#[derive(Clone)]
struct Range {
    stream: LogStream,
    first: u64,
    last: u64,
    gaps: Vec<LogGap>,
    registered: bool,
}

pub fn summarize(
    logs: &[LogUpload],
    captured: [u64; 2],
    capture_finished: bool,
) -> Result<LogSummary, SummaryError> {
    let ranges = logs
        .iter()
        .map(|log| Range {
            stream: log.stream(),
            first: log.first_sequence(),
            last: log.last_sequence(),
            gaps: log.gaps().to_vec(),
            registered: log.registered(),
        })
        .collect();
    summarize_ranges(ranges, captured, capture_finished)
}

fn summarize_ranges(
    ranges: Vec<Range>,
    captured: [u64; 2],
    capture_finished: bool,
) -> Result<LogSummary, SummaryError> {
    let mut registered = [0u64; 2];
    let mut unregistered = [false; 2];
    let mut gaps = Vec::new();
    for range in ranges {
        let index = match range.stream {
            LogStream::Stdout => 0,
            LogStream::Stderr => 1,
            _ => return Err(SummaryError::Invalid),
        };
        if range.first == 0
            || range.last < range.first
            || range.last > i64::MAX as u64
            || (range.registered && (unregistered[index] || range.first != registered[index] + 1))
        {
            return Err(SummaryError::Invalid);
        }
        if range.registered {
            registered[index] = range.last;
            gaps.extend(range.gaps);
        } else {
            unregistered[index] = true;
        }
    }
    for (index, total) in captured.into_iter().enumerate() {
        if total > i64::MAX as u64 || total < registered[index] {
            return Err(SummaryError::Invalid);
        }
        if total > registered[index] {
            gaps.push(LogGap {
                stream: if index == 0 {
                    LogStream::Stdout
                } else {
                    LogStream::Stderr
                } as i32,
                first_sequence: registered[index] + 1,
                last_sequence: total,
            });
        }
    }
    gaps.sort_by_key(|gap| (gap.stream, gap.first_sequence));
    let mut merged: Vec<LogGap> = Vec::new();
    for gap in gaps {
        if gap.first_sequence == 0 || gap.last_sequence < gap.first_sequence {
            return Err(SummaryError::Invalid);
        }
        if let Some(previous) = merged.last_mut() {
            if previous.stream == gap.stream
                && gap.first_sequence <= previous.last_sequence.saturating_add(1)
            {
                previous.last_sequence = previous.last_sequence.max(gap.last_sequence);
                continue;
            }
        }
        merged.push(gap);
        // Completion has a fixed 1024-gap contract. Never silently omit
        // registered loss from its frozen manifest.
        if merged.len() > 1024 {
            return Err(SummaryError::Limit);
        }
    }
    Ok(LogSummary {
        complete: capture_finished && !unregistered[0] && !unregistered[1] && merged.is_empty(),
        gaps: merged,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn range(
        stream: LogStream,
        first: u64,
        last: u64,
        registered: bool,
        gaps: Vec<LogGap>,
    ) -> Range {
        Range {
            stream,
            first,
            last,
            registered,
            gaps,
        }
    }
    fn gap(stream: LogStream, first: u64, last: u64) -> LogGap {
        LogGap {
            stream: stream as i32,
            first_sequence: first,
            last_sequence: last,
        }
    }
    #[test]
    fn frozen_claim_covers_registered_internal_and_unregistered_tail_loss() {
        let summary = summarize_ranges(
            vec![
                range(
                    LogStream::Stdout,
                    1,
                    3,
                    true,
                    vec![gap(LogStream::Stdout, 2, 2)],
                ),
                range(LogStream::Stdout, 4, 5, false, vec![]),
            ],
            [7, 0],
            true,
        )
        .unwrap();
        assert!(!summary.complete);
        assert_eq!(
            summary.gaps,
            vec![gap(LogStream::Stdout, 2, 2), gap(LogStream::Stdout, 4, 7)]
        );
    }
    #[test]
    fn clean_and_interrupted_capture_are_distinct() {
        assert!(summarize_ranges(vec![], [0, 0], true).unwrap().complete);
        assert!(!summarize_ranges(vec![], [0, 0], false).unwrap().complete);
        assert_eq!(
            summarize_ranges(vec![], [3, 0], true).unwrap().gaps,
            vec![gap(LogStream::Stdout, 1, 3)]
        );
        let streams = summarize_ranges(
            vec![
                range(LogStream::Stdout, 1, 1, false, vec![]),
                range(LogStream::Stderr, 1, 1, true, vec![]),
            ],
            [1, 1],
            true,
        )
        .unwrap();
        assert_eq!(streams.gaps, vec![gap(LogStream::Stdout, 1, 1)]);
    }
    #[test]
    fn inconsistent_ranges_and_excess_gaps_are_rejected() {
        assert!(summarize_ranges(
            vec![range(LogStream::Stdout, 2, 2, true, vec![])],
            [2, 0],
            true
        )
        .is_err());
        let ranges = (0..1025)
            .map(|n| {
                range(
                    LogStream::Stdout,
                    n * 2 + 1,
                    n * 2 + 2,
                    true,
                    vec![gap(LogStream::Stdout, n * 2 + 1, n * 2 + 1)],
                )
            })
            .collect();
        assert_eq!(
            summarize_ranges(ranges, [2050, 0], true),
            Err(SummaryError::Limit)
        );
    }
}
