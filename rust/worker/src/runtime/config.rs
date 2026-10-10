use super::*;

pub(super) fn build(
    identity: &AttemptAuthority,
    execution: &ExecutionSpec,
    workspace: &PreparedWorkspace,
) -> Result<ContainerCreateBody, RuntimeError> {
    if identity.generation == 0
        || identity.generation > i64::MAX as u64
        || [
            &identity.worker_id,
            &identity.session_id,
            &identity.job_id,
            &identity.attempt_id,
        ]
        .iter()
        .any(|s| !uuid(s))
    {
        return Err(RuntimeError::Identity);
    }
    let s = &execution.job().spec;
    if s.inputs.len() != workspace.inputs.len()
        || s.inputs
            .iter()
            .zip(&workspace.inputs)
            .any(|(requested, prepared)| requested.mount_path != prepared.target)
    {
        return Err(RuntimeError::Configuration);
    }
    let labels = HashMap::from([
        ("dev.dispatch.worker".into(), identity.worker_id.clone()),
        ("dev.dispatch.session".into(), identity.session_id.clone()),
        ("dev.dispatch.job".into(), identity.job_id.clone()),
        ("dev.dispatch.attempt".into(), identity.attempt_id.clone()),
        (
            "dev.dispatch.generation".into(),
            identity.generation.to_string(),
        ),
        ("dev.dispatch.spec-sha256".into(), execution.sha256().into()),
        (
            "dev.dispatch.scratch-policy".into(),
            workspace.scratch_policy().into(),
        ),
    ]);
    let mut env: Vec<_> = s.env.iter().map(|(k, v)| format!("{k}={v}")).collect();
    env.extend([
        format!("DISPATCH_JOB_ID={}", identity.job_id),
        format!("DISPATCH_ATTEMPT_ID={}", identity.attempt_id),
        format!("DISPATCH_WORKER_ID={}", identity.worker_id),
        format!("DISPATCH_SESSION_ID={}", identity.session_id),
        format!("DISPATCH_GENERATION={}", identity.generation),
    ]);
    let mut mounts: Vec<_> = ["inputs", "outputs", "scratch"]
        .iter()
        .map(|name| Mount {
            target: Some(format!("/{name}")),
            source: Some(workspace.root.join(name).to_str().unwrap().into()),
            typ: Some(MountType::BIND),
            read_only: Some(*name == "inputs"),
            ..Default::default()
        })
        .collect();
    for input in &workspace.inputs {
        mounts.push(Mount {
            target: Some(input.target.clone()),
            source: Some(
                input
                    .source
                    .to_str()
                    .ok_or(RuntimeError::Configuration)?
                    .to_owned(),
            ),
            typ: Some(MountType::BIND),
            read_only: Some(true),
            ..Default::default()
        });
    }
    // Linux CFS has a minimum 1-ms quota. A 1-second period represents sub-10
    // milliCPU jobs exactly instead of rounding above the admitted reservation.
    let period = if s.resources.cpu_millis < 10 {
        1_000_000
    } else {
        100_000
    };
    let memory = (s.resources.memory_mib << 20) as i64;
    let temporary = (memory / 4).min(64 << 20);
    Ok(ContainerCreateBody {
        image: Some(s.image.clone()),
        user: Some("65532:65532".into()),
        entrypoint: Some(s.command.clone()),
        cmd: Some(s.args.clone()),
        env: Some(env),
        working_dir: Some("/scratch".into()),
        network_disabled: Some(true),
        tty: Some(false),
        open_stdin: Some(false),
        labels: Some(labels),
        stop_signal: Some("SIGTERM".into()),
        stop_timeout: Some(s.termination_grace_seconds as i64),
        // Image healthchecks can execute extra processes after the main workload
        // starts; only the agent's lifecycle governs this attempt.
        healthcheck: Some(HealthConfig {
            test: Some(vec!["NONE".into()]),
            ..Default::default()
        }),
        host_config: Some(HostConfig {
            privileged: Some(false),
            readonly_rootfs: Some(true),
            network_mode: Some("none".into()),
            cap_drop: Some(vec!["ALL".into()]),
            security_opt: Some(vec!["no-new-privileges:true".into()]),
            memory: Some(memory),
            memory_swap: Some(memory),
            cpu_period: Some(period as i64),
            cpu_quota: Some((s.resources.cpu_millis * period / 1000) as i64),
            pids_limit: Some(256),
            init: Some(true),
            auto_remove: Some(false),
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::NO),
                maximum_retry_count: Some(0),
            }),
            mounts: Some(mounts),
            tmpfs: Some(HashMap::from([(
                "/tmp".into(),
                format!("rw,nosuid,nodev,noexec,size={temporary},mode=1777"),
            )])),
            shm_size: Some(temporary),
            // Bound daemon-side retention when the agent is disconnected. An
            // early burst can precede follower polling, and JSON escaping can
            // expand binary bytes sixfold; 1 MiB rotation lost real log data.
            // The collector still needs a pre-start attachment to close that
            // remaining gap without growing this bound indefinitely.
            log_config: Some(HostConfigLogConfig {
                typ: Some("json-file".into()),
                config: Some(HashMap::from([
                    ("max-size".into(), "8m".into()),
                    ("max-file".into(), "2".into()),
                ])),
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}

pub(super) fn verify(
    actual: &ContainerInspectResponse,
    expected: &ContainerCreateBody,
    image_id: &str,
) -> Result<(), RuntimeError> {
    verify_identity(actual, expected, image_id)?;
    let c = actual.config.as_ref().ok_or(RuntimeError::Identity)?;
    let h = actual.host_config.as_ref().ok_or(RuntimeError::Identity)?;
    let e = expected
        .host_config
        .as_ref()
        .ok_or(RuntimeError::Identity)?;
    let env = c.env.as_ref().ok_or(RuntimeError::Identity)?;
    // Labels bind ownership, while inspection of limits and mounts prevents
    // adopting a same-name container whose actual execution settings differ.
    if c.user != expected.user
        || c.entrypoint != expected.entrypoint
        || c.cmd.as_deref().unwrap_or_default() != expected.cmd.as_deref().unwrap_or_default()
        || c.working_dir != expected.working_dir
        || c.tty != Some(false)
        || expected
            .env
            .as_ref()
            .unwrap()
            .iter()
            .any(|v| !env.contains(v))
        || c.healthcheck.as_ref().and_then(|v| v.test.as_ref())
            != expected.healthcheck.as_ref().and_then(|v| v.test.as_ref())
        || h.privileged != Some(false)
        || h.readonly_rootfs != Some(true)
        || h.network_mode != e.network_mode
        || h.cap_drop != e.cap_drop
        || h.security_opt != e.security_opt
        || h.memory != e.memory
        || h.memory_swap != e.memory_swap
        || h.cpu_period != e.cpu_period
        || h.cpu_quota != e.cpu_quota
        || h.pids_limit != e.pids_limit
        || h.init != e.init
        || h.auto_remove != Some(false)
        || h.restart_policy.as_ref().and_then(|v| v.name) != Some(RestartPolicyNameEnum::NO)
        || h.tmpfs != e.tmpfs
        || h.shm_size != e.shm_size
        || h.log_config != e.log_config
        || h.binds.as_ref().is_some_and(|v| !v.is_empty())
        || h.volumes_from.as_ref().is_some_and(|v| !v.is_empty())
        || h.cap_add.as_ref().is_some_and(|v| !v.is_empty())
        || h.devices.as_ref().is_some_and(|v| !v.is_empty())
        || h.device_requests.as_ref().is_some_and(|v| !v.is_empty())
        || h.pid_mode.as_deref().is_some_and(|v| !v.is_empty())
        // A host override disables the daemon remapping required by strict
        // quotas; only Docker's default namespace policy is allowed.
        || h.userns_mode.as_deref().is_some_and(|v| !v.is_empty())
        || h.ipc_mode
            .as_deref()
            .is_some_and(|v| !matches!(v, "private" | ""))
    {
        return Err(RuntimeError::Identity);
    }
    let mounts = h.mounts.as_ref().ok_or(RuntimeError::Identity)?;
    let wanted = e.mounts.as_ref().unwrap();
    if mounts.len() != wanted.len()
        || wanted.iter().any(|expected| {
            !mounts.iter().any(|m| {
                m.target == expected.target
                    && m.source == expected.source
                    && m.typ == expected.typ
                    && m.read_only.unwrap_or(false) == expected.read_only.unwrap_or(false)
            })
        })
    {
        return Err(RuntimeError::Identity);
    }
    Ok(())
}

pub(super) fn verify_identity(
    actual: &ContainerInspectResponse,
    expected: &ContainerCreateBody,
    image_id: &str,
) -> Result<(), RuntimeError> {
    let labels = actual
        .config
        .as_ref()
        .and_then(|c| c.labels.as_ref())
        .ok_or(RuntimeError::Identity)?;
    // Cleanup uses immutable ownership even when an operator changed mutable
    // resource limits. Configuration drift must not prevent stopping owned work.
    if actual.image.as_deref() != Some(image_id)
        || expected
            .labels
            .as_ref()
            .unwrap()
            .iter()
            .any(|(k, v)| labels.get(k) != Some(v))
    {
        return Err(RuntimeError::Identity);
    }
    Ok(())
}

fn uuid(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

#[cfg(test)]
mod input_tests {
    use super::*;
    use dispatch_protocol::v1::{Assignment, InputManifest, Resources};
    use ring::digest::{digest, SHA256};
    use std::{
        fs,
        os::unix::fs::{DirBuilderExt, PermissionsExt},
    };

    #[test]
    fn declared_input_requires_exact_sealed_read_only_bind() {
        let root = std::env::temp_dir().join(format!(
            "dispatch-runtime-input-{}",
            crate::journal::new_uuid().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let work = root.join("attempt");
        fs::DirBuilder::new().mode(0o700).create(&work).unwrap();
        for name in ["inputs", "outputs", "scratch"] {
            fs::create_dir(work.join(name)).unwrap();
        }
        let cache = root.join("cache");
        fs::DirBuilder::new().mode(0o700).create(&cache).unwrap();
        let source = cache.join("version");
        fs::create_dir(&source).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o555)).unwrap();
        let image = format!("example.org/test@sha256:{}", "a".repeat(64));
        let raw = serde_json::to_vec(&serde_json::json!({
            "apiVersion":"dispatch.dev/v1alpha1", "kind":"Job", "metadata":{"name":"test", "project":"research"},
            "spec":{"image":image,"command":["true"],"resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},
            "placement":{},"inputs":[{"dataset":"data","mountPath":"/inputs/data"}],"network":"disabled",
            "timeouts":{"startupSeconds":30,"executionSeconds":30,"finalizationSeconds":30},
            "retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},"terminationGraceSeconds":1}
        })).unwrap();
        let assignment = Assignment {
            image_digest: image,
            argv: vec!["true".into()],
            resources: Some(Resources {
                cpu_millis: 1000,
                memory_bytes: 128 << 20,
                scratch_bytes: 64 << 20,
            }),
            spec_sha256: digest(&SHA256, &raw)
                .as_ref()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
            canonical_job_spec_json: raw,
            inputs: vec![InputManifest {
                dataset_name: "data".into(),
                mount_path: "/inputs/data".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let execution = ExecutionSpec::from_assignment(&assignment).unwrap();
        let identity = AttemptAuthority {
            worker_id: "00000000-0000-0000-0000-000000000001".into(),
            session_id: "00000000-0000-0000-0000-000000000002".into(),
            job_id: "00000000-0000-0000-0000-000000000003".into(),
            attempt_id: "00000000-0000-0000-0000-000000000004".into(),
            generation: 1,
        };
        let mut workspace = PreparedWorkspace::soft_development(&work).unwrap();
        assert!(build(&identity, &execution, &workspace).is_err());
        assert!(workspace.bind_input("/inputs/data", &source).is_ok());
        assert!(work.join("inputs/data").is_dir());
        let config = build(&identity, &execution, &workspace).unwrap();
        let image_id = format!("sha256:{}", "b".repeat(64));
        let mut actual = ContainerInspectResponse {
            image: Some(image_id.clone()),
            config: Some(serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap()),
            host_config: config.host_config.clone(),
            ..Default::default()
        };
        assert!(verify(&actual, &config, &image_id).is_ok());
        actual.host_config.as_mut().unwrap().userns_mode = Some("host".into());
        assert_eq!(
            verify(&actual, &config, &image_id),
            Err(RuntimeError::Identity)
        );
        let mounts = config.host_config.unwrap().mounts.unwrap();
        assert!(mounts
            .iter()
            .any(|mount| mount.target.as_deref() == Some("/inputs/data")
                && mount.source.as_deref() == source.to_str()
                && mount.read_only == Some(true)));
        assert!(workspace
            .bind_input("/inputs/data/nested", &source)
            .is_err());
        assert!(workspace.bind_input("/inputs/../escape", &source).is_err());
        let alias = cache.join("alias");
        std::os::unix::fs::symlink(&source, &alias).unwrap();
        let mut another = PreparedWorkspace::soft_development(&work).unwrap();
        assert!(another.bind_input("/inputs/data", &alias).is_err());
        fs::remove_file(alias).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}
