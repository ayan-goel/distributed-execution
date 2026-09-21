#![cfg(unix)]
use dispatch_worker::agent::AgentConfig;

fn config() -> serde_json::Value {
    serde_json::json!({
        "worker_id":"00000000-0000-0000-0000-000000000001",
        "server_url":"https://localhost:8444", "ca_cert":"/worker/ca.pem",
        "client_cert":"/worker/client.pem", "client_key":"/worker/key.pem",
        "journal_dir":"/worker/state", "workspace_root":"/worker/work",
        "docker_socket":"/var/run/docker.sock", "cpu_millis":2000,
        "memory_mib":512, "scratch_mib":1024, "execution_slots":2,
        "labels":{"os":"linux","architecture":"arm64"}
    })
}

#[test]
fn worker_configuration_is_bounded_strict_and_has_explicit_local_paths() {
    assert!(AgentConfig::decode(&serde_json::to_vec(&config()).unwrap()).is_ok());
    for (key, value) in [
        ("worker_id", serde_json::json!("bad")),
        ("server_url", serde_json::json!("http://localhost:8444")),
        ("cpu_millis", serde_json::json!(0)),
        ("memory_mib", serde_json::json!(u64::MAX)),
        ("execution_slots", serde_json::json!(1001)),
        ("client_key", serde_json::json!("relative.pem")),
        ("extra", serde_json::json!(true)),
    ] {
        let mut bad = config();
        bad[key] = value;
        assert!(
            AgentConfig::decode(&serde_json::to_vec(&bad).unwrap()).is_err(),
            "{key}"
        );
    }
    assert!(AgentConfig::decode(&vec![b' '; 65537]).is_err());
    let duplicate =
        serde_json::to_string(&config())
            .unwrap()
            .replacen("{", "{\"worker_id\":\"duplicate\",", 1);
    assert!(AgentConfig::decode(duplicate.as_bytes()).is_err());
    let duplicate_label = serde_json::to_string(&config())
        .unwrap()
        .replace("\"os\":\"linux\"", "\"os\":\"linux\",\"os\":\"linux\"");
    assert!(AgentConfig::decode(duplicate_label.as_bytes()).is_err());
}

#[test]
fn worker_run_requires_explicit_development_scratch_profile() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_dispatch-worker"))
        .args(["run", "--config", "/missing.json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--dev-soft-scratch"));
}
