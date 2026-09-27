use dispatch_protocol::v1::LogStream;
use dispatch_worker::log_format::{
    LogFormatError, LogSegmentWriter, SequenceGap, MAX_SEGMENT_BYTES,
};

const ATTEMPT: &str = "123e4567-e89b-12d3-a456-426614174000";
const VECTOR: &str = "4453504c4f47303131323365343536372d653839622d313264332d613435362d34323636313431373430303001000000000000000000000117979cfe362a00000000000500ff1b5b41000000000000000317979cfe362a002a000000010a";

fn from_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn binary_records_preserve_identity_time_bytes_and_sequence_gaps() {
    let mut writer = LogSegmentWriter::new(ATTEMPT, LogStream::Stdout, 1).unwrap();
    writer
        .append(1, 1_700_000_000_000_000_000, b"\x00\xff\x1b[A")
        .unwrap();
    writer.append(3, 1_700_000_000_000_000_042, b"\n").unwrap();
    let segment = writer.finish().unwrap();
    assert_eq!(segment.bytes, from_hex(VECTOR));
    assert_eq!(segment.first_sequence, 1);
    assert_eq!(segment.last_sequence, 3);
    assert_eq!(segment.gaps, vec![SequenceGap { first: 2, last: 2 }]);
}

#[test]
fn invalid_records_and_full_segments_never_advance_the_sequence() {
    assert!(LogSegmentWriter::new("bad", LogStream::Stdout, 1).is_err());
    assert!(LogSegmentWriter::new(ATTEMPT, LogStream::Stdout, 0).is_err());
    let mut writer = LogSegmentWriter::new(ATTEMPT, LogStream::Stderr, 1).unwrap();
    assert_eq!(writer.append(1, -1, b"x"), Err(LogFormatError::Invalid));
    assert_eq!(writer.append(1, 1, b""), Err(LogFormatError::Invalid));
    assert_eq!(
        writer.append(1, 1, &vec![b'x'; MAX_SEGMENT_BYTES]),
        Err(LogFormatError::Full)
    );
    writer.append(1, 1, b"x").unwrap();
    assert_eq!(writer.append(1, 2, b"x"), Err(LogFormatError::Sequence));
    let segment = writer.finish().unwrap();
    assert_eq!(segment.last_sequence, 1);
    assert_eq!(segment.bytes[44], 2);
}

#[test]
fn a_segment_has_an_exact_one_mebibyte_ceiling() {
    let mut writer = LogSegmentWriter::new(ATTEMPT, LogStream::Stdout, 1).unwrap();
    writer
        .append(1, 1, &vec![b'x'; MAX_SEGMENT_BYTES - 48 - 20])
        .unwrap();
    assert_eq!(writer.append(2, 2, b"x"), Err(LogFormatError::Full));
    assert_eq!(writer.finish().unwrap().bytes.len(), MAX_SEGMENT_BYTES);
}

#[test]
fn gap_count_is_bounded_without_consuming_a_rejected_record() {
    let mut writer = LogSegmentWriter::new(ATTEMPT, LogStream::Stdout, 1).unwrap();
    for index in 0..=1024 {
        writer.append(1 + index * 2, 1, b"x").unwrap();
    }
    assert_eq!(writer.append(2051, 1, b"x"), Err(LogFormatError::GapLimit));
    writer.append(2050, 1, b"x").unwrap();
    assert_eq!(writer.finish().unwrap().gaps.len(), 1024);
}
