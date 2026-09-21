use super::{canonical_uuid, ClientError};
use dispatch_protocol::v1::{CompleteAttemptRequest, FailureReason, LogStream};
use ring::digest::{digest, Context, SHA256};
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::value::RawValue;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Authority<'a> {
    job_id: &'a str,
    attempt_id: &'a str,
    generation: u64,
    worker_id: &'a str,
    session_id: &'a str,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Output<'a> {
    name: &'a str,
    artifact_id: &'a str,
}
#[derive(Serialize)]
struct Gap {
    stream: &'static str,
    first: u64,
    last: u64,
}
#[derive(Default, Serialize)]
struct Metrics(BTreeMap<String, Box<RawValue>>);
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Payload<'a> {
    authority: Authority<'a>,
    exit_code: Option<i32>,
    reason: &'static str,
    stopped: bool,
    outputs: Vec<Output<'a>>,
    logs_complete: bool,
    gaps: Vec<Gap>,
    metrics: Metrics,
    metrics_source_sha256: String,
}

/// Bind completion evidence to the server's documented byte contract. Caller-owned
/// request identity and claimed digest are excluded; source metrics bytes remain bound.
pub fn completion_digest(r: &CompleteAttemptRequest) -> Result<String, ClientError> {
    let a = r.authority.as_ref().ok_or(ClientError::Configuration)?;
    // Match server bounds before allocating normalized copies. Workload-provided
    // metrics and gap reports must not create unbounded completion payloads.
    if [
        &a.job_id,
        &a.attempt_id,
        &a.worker_id,
        &a.session_id,
        &r.completion_id,
    ]
    .iter()
    .any(|id| !canonical_uuid(id))
        || a.generation == 0
        || a.generation > i64::MAX as u64
        || r.outputs.len() > 64
        || r.gaps.len() > 1024
        || r.metrics_json.len() > 64 << 10
        || r.exit_code.is_some_and(|exit| !(0..=255).contains(&exit))
    {
        return Err(ClientError::Configuration);
    }
    let reason = FailureReason::try_from(r.reason).map_err(|_| ClientError::Configuration)?;
    if reason == FailureReason::WorkerLost
        || reason == FailureReason::Unspecified && (r.exit_code != Some(0) || !r.stopped)
        || reason == FailureReason::ApplicationExit && r.exit_code.is_none_or(|exit| exit == 0)
    {
        return Err(ClientError::Configuration);
    }
    let reason = if reason == FailureReason::Unspecified {
        ""
    } else {
        reason.as_str_name()
    };
    let mut outputs = Vec::with_capacity(r.outputs.len());
    let mut ids = BTreeSet::new();
    for output in &r.outputs {
        if !bounded_name(&output.name, false)
            || !canonical_uuid(&output.artifact_id)
            || !ids.insert(&output.artifact_id)
        {
            return Err(ClientError::Configuration);
        }
        outputs.push(Output {
            name: &output.name,
            artifact_id: &output.artifact_id,
        });
    }
    outputs.sort_by_key(|output| output.name);
    if outputs.windows(2).any(|pair| pair[0].name == pair[1].name)
        || r.logs_complete && !r.gaps.is_empty()
    {
        return Err(ClientError::Configuration);
    }
    let mut gaps = Vec::with_capacity(r.gaps.len());
    for gap in &r.gaps {
        let stream = match LogStream::try_from(gap.stream) {
            Ok(LogStream::Stdout) => "stdout",
            Ok(LogStream::Stderr) => "stderr",
            _ => return Err(ClientError::Configuration),
        };
        if gap.first_sequence == 0
            || gap.last_sequence < gap.first_sequence
            || gap.last_sequence > i64::MAX as u64
        {
            return Err(ClientError::Configuration);
        }
        gaps.push(Gap {
            stream,
            first: gap.first_sequence,
            last: gap.last_sequence,
        });
    }
    gaps.sort_by_key(|gap| (gap.stream, gap.first));
    if gaps
        .windows(2)
        .any(|pair| pair[0].stream == pair[1].stream && pair[0].last >= pair[1].first)
    {
        return Err(ClientError::Configuration);
    }
    let metrics = if r.metrics_json.is_empty() {
        Metrics::default()
    } else {
        serde_json::from_slice(&r.metrics_json).map_err(|_| ClientError::Configuration)?
    };
    let payload = Payload {
        authority: Authority {
            job_id: &a.job_id,
            attempt_id: &a.attempt_id,
            generation: a.generation,
            worker_id: &a.worker_id,
            session_id: &a.session_id,
        },
        exit_code: r.exit_code,
        reason,
        stopped: r.stopped,
        outputs,
        logs_complete: r.logs_complete,
        gaps,
        metrics,
        metrics_source_sha256: if r.metrics_json.is_empty() {
            String::new()
        } else {
            hex(digest(&SHA256, &r.metrics_json).as_ref())
        },
    };
    // Struct field order matches Go; ASCII names avoid differing HTML escaping.
    // Raw numeric JSON preserves lexical exponents and integers above 2^53.
    let body = serde_json::to_vec(&payload).map_err(|_| ClientError::Configuration)?;
    let mut hash = Context::new(&SHA256);
    hash.update(b"dispatch.worker.v1.CompleteAttempt\n");
    hash.update(&body);
    Ok(hex(hash.finish().as_ref()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn bounded_name(name: &str, metric: bool) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric() || b"_.-".contains(&b) || metric && b":/".contains(&b)
        })
}

impl<'de> Deserialize<'de> for Metrics {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MetricsVisitor;
        impl<'de> Visitor<'de> for MetricsVisitor {
            type Value = Metrics;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("bounded numeric metrics object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Metrics, M::Error> {
                // Read entries directly: a generic JSON map would silently
                // overwrite duplicate keys before we could reject ambiguous metrics.
                let mut values = BTreeMap::new();
                while let Some((key, mut raw)) = map.next_entry::<String, Box<RawValue>>()? {
                    if values.len() >= 256 || !bounded_name(&key, true) || values.contains_key(&key)
                    {
                        return Err(de::Error::custom("invalid or duplicate metric name"));
                    }
                    let number = raw.get();
                    if number.len() > 1024
                        || !matches!(number.as_bytes().first(), Some(b'-' | b'0'..=b'9'))
                    {
                        return Err(de::Error::custom("invalid metric number"));
                    }
                    let value = number
                        .parse::<f64>()
                        .map_err(|_| de::Error::custom("invalid metric number"))?;
                    if !value.is_finite() {
                        return Err(de::Error::custom("nonfinite metric"));
                    }
                    if value == 0.0 {
                        // Nonzero underflow must not silently become zero. True
                        // zero normalizes to a bounded encoding, just as in Go.
                        let mantissa = number.split(['e', 'E']).next().unwrap_or("");
                        if mantissa.bytes().any(|b| !b"-+.0".contains(&b)) {
                            return Err(de::Error::custom("metric underflow"));
                        }
                        raw = RawValue::from_string("0".into()).map_err(de::Error::custom)?;
                    }
                    values.insert(key, raw);
                }
                Ok(Metrics(values))
            }
        }
        deserializer.deserialize_map(MetricsVisitor)
    }
}
