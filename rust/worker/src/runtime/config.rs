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
            "soft-development".into(),
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
    let mounts = ["inputs", "outputs", "scratch"]
        .iter()
        .map(|name| Mount {
            target: Some(format!("/{name}")),
            source: Some(workspace.root.join(name).to_str().unwrap().into()),
            typ: Some(MountType::BIND),
            read_only: Some(*name == "inputs"),
            ..Default::default()
        })
        .collect();
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
            // Bound daemon-side retention even when the agent is disconnected.
            // D15 will additionally stream/spool logs and report retention gaps.
            log_config: Some(HostConfigLogConfig {
                typ: Some("json-file".into()),
                config: Some(HashMap::from([
                    ("max-size".into(), "1m".into()),
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
