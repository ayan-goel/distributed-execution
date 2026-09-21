//! Validated execution settings. Parsing grants neither a lease nor runtime capacity.
use dispatch_protocol::v1::Assignment;
use ring::digest::{digest, SHA256};
use serde::{de, Deserialize, Deserializer};
use std::{collections::BTreeMap, fmt};

#[derive(Debug)]
pub struct ExecutionSpec {
    job: Job,
    sha256: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SpecError {
    Document,
    Checksum,
    WireMismatch,
    InputsUnsupported,
}
impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Documents and parser diagnostics can include environment values. Keep
        // error categories stable without copying workload contents into logs.
        f.write_str(match self {
            Self::Document => "invalid execution specification",
            Self::Checksum => "execution specification checksum mismatch",
            Self::WireMismatch => "execution specification disagrees with assignment",
            Self::InputsUnsupported => "input staging is not yet supported",
        })
    }
}
impl std::error::Error for SpecError {}

impl ExecutionSpec {
    pub fn from_assignment(assignment: &Assignment) -> Result<Self, SpecError> {
        let raw = &assignment.canonical_job_spec_json;
        if raw.is_empty() || raw.len() > 2 * 1024 * 1024 {
            return Err(SpecError::Document);
        }
        // INVARIANT: identity hashes the received Go bytes, never a Rust
        // reserialization whose field order or Unicode escaping may differ.
        let hash: String = digest(&SHA256, raw)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        if hash != assignment.spec_sha256 {
            return Err(SpecError::Checksum);
        }
        check_shape(raw)?;
        // Parse original bytes so duplicate struct/map keys cannot disappear in
        // the shape preflight's Value representation before strict decoding.
        let job: Job = serde_json::from_slice(raw).map_err(|_| SpecError::Document)?;
        job.validate()?;
        if !job.spec.inputs.is_empty() || !assignment.inputs.is_empty() {
            return Err(SpecError::InputsUnsupported);
        }
        let s = &job.spec;
        let resources = assignment
            .resources
            .as_ref()
            .ok_or(SpecError::WireMismatch)?;
        if s.image != assignment.image_digest
            || !s.command.iter().chain(&s.args).eq(assignment.argv.iter())
            || s.resources.cpu_millis != resources.cpu_millis as u64
            || (s.resources.memory_mib << 20) != resources.memory_bytes
            || (s.resources.scratch_mib << 20) != resources.scratch_bytes
        {
            return Err(SpecError::WireMismatch);
        }
        Ok(Self { job, sha256: hash })
    }

    pub fn job(&self) -> &Job {
        &self.job
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Job {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: JobSettings,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub name: String,
    pub project: String,
    #[serde(default, deserialize_with = "unique_map")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobSettings {
    pub image: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default, deserialize_with = "unique_map")]
    pub env: BTreeMap<String, String>,
    pub resources: Resources,
    pub placement: Placement,
    #[serde(default)]
    pub inputs: Vec<Input>,
    #[serde(default)]
    pub outputs: Vec<Output>,
    pub timeouts: Timeouts,
    pub retry: Retry,
    pub termination_grace_seconds: u64,
    pub network: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    #[serde(rename = "cpuMillis")]
    pub cpu_millis: u64,
    #[serde(rename = "memoryMiB")]
    pub memory_mib: u64,
    #[serde(rename = "scratchMiB")]
    pub scratch_mib: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    #[serde(default, deserialize_with = "unique_map")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Input {
    pub dataset: String,
    pub mount_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Output {
    pub name: String,
    pub path: String,
    pub required: bool,
    pub max_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Timeouts {
    pub startup_seconds: u64,
    pub execution_seconds: u64,
    pub finalization_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Retry {
    pub max_attempts: u64,
    #[serde(default)]
    pub on: Vec<String>,
    pub initial_backoff_seconds: u64,
    pub max_backoff_seconds: u64,
}

impl Job {
    fn validate(&self) -> Result<(), SpecError> {
        let s = &self.spec;
        let pinned = s.image.rsplit_once("@sha256:").is_some_and(|(name, hash)| {
            !name.is_empty()
                && hash.len() == 64
                && hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
        // Bound resource arithmetic and all runtime deadlines before constructing
        // settings. Admission quotas and actual host capability are separate gates.
        if self.api_version != "dispatch.dev/v1alpha1"
            || self.kind != "Job"
            || !identifier(&self.metadata.name, false)
            || !identifier(&self.metadata.project, false)
            || !valid_map(&self.metadata.labels, false)
            || !valid_map(&s.env, true)
            || !valid_map(&s.placement.labels, false)
            || !pinned
            || s.image.len() > 1024
            || s.image.chars().any(|c| c.is_whitespace() || c.is_control())
            || s.command.is_empty()
            || s.command[0].is_empty()
            || s.command.len() + s.args.len() > 256
            || s.command
                .iter()
                .chain(&s.args)
                .any(|a| a.len() > 8192 || a.contains('\0'))
            || !(1..=1_024_000).contains(&s.resources.cpu_millis)
            || !(1..=16_777_216).contains(&s.resources.memory_mib)
            || !(1..=1_073_741_824).contains(&s.resources.scratch_mib)
            || !(1..=3600).contains(&s.timeouts.startup_seconds)
            || !(1..=604800).contains(&s.timeouts.execution_seconds)
            || !(1..=3600).contains(&s.timeouts.finalization_seconds)
            || s.termination_grace_seconds > 300
            || s.network != "disabled"
            || !(1..=10).contains(&s.retry.max_attempts)
            || !(1..=3600).contains(&s.retry.initial_backoff_seconds)
            || !(s.retry.initial_backoff_seconds..=3600).contains(&s.retry.max_backoff_seconds)
            || s.outputs.len() > 64
            || s.inputs.len() > 64
        {
            return Err(SpecError::Document);
        }
        for (i, reason) in s.retry.on.iter().enumerate() {
            if !["WORKER_LOST", "RUNTIME_UNAVAILABLE", "TRANSFER_FAILED"].contains(&reason.as_str())
                || s.retry.on[..i].contains(reason)
            {
                return Err(SpecError::Document);
            }
        }
        for (i, output) in s.outputs.iter().enumerate() {
            // Lexical containment blocks host paths and traversal. Collection must
            // additionally resolve real files without following escaping symlinks.
            if !identifier(&output.name, false)
                || !output_path(&output.path)
                || !(1..=1u64 << 40).contains(&output.max_bytes)
                || s.outputs[..i].iter().any(|previous| {
                    previous.name == output.name || overlaps(&previous.path, &output.path)
                })
            {
                return Err(SpecError::Document);
            }
        }
        Ok(())
    }
}

fn identifier(value: &str, environment: bool) -> bool {
    if value.is_empty() || value.len() > 128 {
        return false;
    }
    let first = value.as_bytes()[0];
    if environment {
        (first.is_ascii_alphabetic() || first == b'_')
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            && !value.starts_with("DISPATCH_")
    } else {
        first.is_ascii_alphanumeric()
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    }
}

fn valid_map(map: &BTreeMap<String, String>, environment: bool) -> bool {
    map.len() <= 128
        && map
            .iter()
            .all(|(k, v)| identifier(k, environment) && v.len() <= 8192 && !v.contains('\0'))
}

fn output_path(value: &str) -> bool {
    value.len() <= 4096
        && value.starts_with("/outputs/")
        && !value.contains(['\\', '\0'])
        && value[1..]
            .split('/')
            .all(|part| !matches!(part, "" | "." | ".."))
}

fn overlaps(a: &str, b: &str) -> bool {
    a == b
        || a.strip_prefix(b).is_some_and(|rest| rest.starts_with('/'))
        || b.strip_prefix(a).is_some_and(|rest| rest.starts_with('/'))
}

pub(crate) fn unique_map<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    struct Visitor;
    impl<'de> de::Visitor<'de> for Visitor {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded string map with unique keys")
        }
        fn visit_map<M: de::MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = access.next_entry::<String, String>()? {
                // Default map decoding overwrites duplicates; reject them so a
                // hashed document never has two competing environment values.
                if result.len() >= 128 || result.insert(key, value).is_some() {
                    return Err(de::Error::custom("duplicate or oversized map"));
                }
            }
            Ok(result)
        }
    }
    decoder.deserialize_map(Visitor)
}

fn check_shape(raw: &[u8]) -> Result<(), SpecError> {
    let value: serde_json::Value = serde_json::from_slice(raw).map_err(|_| SpecError::Document)?;
    // Serde structs also accept positional arrays. The public contract requires
    // named objects, and bounded nesting limits work before typed validation.
    for pointer in [
        "",
        "/metadata",
        "/spec",
        "/spec/resources",
        "/spec/placement",
        "/spec/timeouts",
        "/spec/retry",
    ] {
        if !value
            .pointer(pointer)
            .is_some_and(serde_json::Value::is_object)
        {
            return Err(SpecError::Document);
        }
    }
    for field in ["inputs", "outputs"] {
        if let Some(items) = value["spec"]
            .get(field)
            .and_then(serde_json::Value::as_array)
        {
            if items.iter().any(|v| !v.is_object()) {
                return Err(SpecError::Document);
            }
        }
    }
    fn bounded(value: &serde_json::Value, depth: usize) -> bool {
        depth <= 32
            && match value {
                serde_json::Value::Null => false,
                serde_json::Value::Array(values) => values.iter().all(|v| bounded(v, depth + 1)),
                serde_json::Value::Object(values) => values.values().all(|v| bounded(v, depth + 1)),
                _ => true,
            }
    }
    if !bounded(&value, 0) {
        return Err(SpecError::Document);
    }
    Ok(())
}
