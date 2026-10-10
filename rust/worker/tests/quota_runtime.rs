#![cfg(target_os = "linux")]

use dispatch_protocol::v1::{Assignment, AttemptAuthority, InputManifest, Resources};
use dispatch_worker::{
    execution::ExecutionSpec,
    lease::{AuthorityWindow, MonoTime},
    project_quota::ProjectQuota,
    runtime::{DockerRuntime, PreparedWorkspace, RecoveryRuntime, Runtime, RuntimeError},
};
use ring::digest::{digest, SHA256};
use std::{fs, io::Write, os::unix::fs::PermissionsExt, path::Path};

#[tokio::test]
#[ignore = "requires a dedicated Docker daemon with userns-remap disabled"]
async fn daemon_without_remapping_rejects_strict_runtime() {
    let runtime = DockerRuntime::connect("/var/run/docker.sock")
        .await
        .unwrap();
    assert_eq!(
        runtime.check_quota_support().await,
        Err(RuntimeError::Unsupported)
    );
}

#[tokio::test]
#[ignore = "requires the dedicated Linux quota/Docker fixture"]
async fn real_container_cannot_escape_its_project_quota() {
    let root = std::env::var_os("DISPATCH_QUOTA_TEST_ROOT").expect("quota fixture");
    let root = Path::new(&root);
    let work = root.join("container");
    fs::create_dir(&work).unwrap();
    fs::set_permissions(&work, fs::Permissions::from_mode(0o700)).unwrap();
    let quota = ProjectQuota::attach_empty(&work, 2001, 64 << 20, 4096).unwrap();
    assert!(quota.verify_directory(root).is_err());
    for name in ["inputs", "outputs", "scratch"] {
        fs::create_dir(work.join(name)).unwrap();
        fs::set_permissions(work.join(name), fs::Permissions::from_mode(0o777)).unwrap();
    }
    let source = root.parent().unwrap().parent().unwrap().join("probe");
    fs::create_dir(&source).unwrap();
    fs::copy(std::env::current_exe().unwrap(), source.join("probe")).unwrap();
    fs::set_permissions(source.join("probe"), fs::Permissions::from_mode(0o555)).unwrap();
    fs::set_permissions(&source, fs::Permissions::from_mode(0o555)).unwrap();
    let mut workspace = PreparedWorkspace::project_quota(&work, quota).unwrap();
    workspace.bind_input("/inputs/probe", &source).unwrap();
    let image = std::env::var("DISPATCH_QUOTA_TEST_IMAGE").expect("pinned Debian image");
    let argv = [
        "/inputs/probe/probe",
        "--ignored",
        "--exact",
        "quota_container_writer",
        "--nocapture",
    ];
    let raw = serde_json::to_vec(&serde_json::json!({
        "apiVersion":"dispatch.dev/v1alpha1", "kind":"Job",
        "metadata":{"name":"quota-test", "project":"research"},
        "spec":{"image":image,"command":argv,
        "resources":{"cpuMillis":1000,"memoryMiB":128,"scratchMiB":64},
        "placement":{},"inputs":[{"dataset":"probe","mountPath":"/inputs/probe"}],"network":"disabled",
        "timeouts":{"startupSeconds":30,"executionSeconds":30,"finalizationSeconds":30},
        "retry":{"maxAttempts":1,"initialBackoffSeconds":1,"maxBackoffSeconds":1},"terminationGraceSeconds":1}
    })).unwrap();
    let assignment = Assignment {
        image_digest: image,
        argv: argv.iter().map(|s| (*s).into()).collect(),
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
            dataset_name: "probe".into(),
            mount_path: "/inputs/probe".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let spec = ExecutionSpec::from_assignment(&assignment).unwrap();
    let identity = AttemptAuthority {
        worker_id: "00000000-0000-0000-0000-000000002001".into(),
        session_id: "00000000-0000-0000-0000-000000002002".into(),
        job_id: std::env::var("DISPATCH_QUOTA_TEST_JOB").unwrap(),
        attempt_id: std::env::var("DISPATCH_QUOTA_TEST_JOB").unwrap(),
        generation: 1,
    };
    let runtime = DockerRuntime::connect("/var/run/docker.sock")
        .await
        .unwrap();
    runtime.check_quota_support().await.unwrap();
    let now = MonoTime::now().unwrap();
    let lease = AuthorityWindow::from_grant(now, now, 30_000, 30_000).unwrap();
    let mut mismatch = assignment.clone();
    let mut document: serde_json::Value =
        serde_json::from_slice(&mismatch.canonical_job_spec_json).unwrap();
    document["spec"]["resources"]["scratchMiB"] = 32.into();
    mismatch.canonical_job_spec_json = serde_json::to_vec(&document).unwrap();
    mismatch.spec_sha256 = digest(&SHA256, &mismatch.canonical_job_spec_json)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    mismatch.resources.as_mut().unwrap().scratch_bytes = 32 << 20;
    let mismatch = ExecutionSpec::from_assignment(&mismatch).unwrap();
    assert!(matches!(
        runtime
            .create(&identity, &mismatch, &workspace, &lease)
            .await,
        Err(RuntimeError::Configuration)
    ));
    let handle = runtime
        .create(&identity, &spec, &workspace, &lease)
        .await
        .unwrap();
    let inventory = runtime.inventory(&identity.worker_id).await.unwrap();
    assert!(inventory
        .iter()
        .any(|container| container.id() == handle.id()));
    runtime.start(&handle, &lease).await.unwrap();
    let exit = runtime.wait(&handle).await.unwrap();
    let logs = runtime.logs(&handle, 8192).await.unwrap();
    runtime.remove(&handle).await.unwrap();
    assert_eq!(exit.exit_code, Some(0), "{logs:?}");
    assert!(
        String::from_utf8_lossy(&logs.stdout)
            .contains("retagging blocked; inheritance protected; EDQUOT"),
        "{logs:?}"
    );
    fs::remove_dir_all(work).unwrap();
    fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
    fs::remove_dir_all(source).unwrap();
}

#[test]
#[ignore = "executes only inside the fixture's unprivileged container"]
fn quota_container_writer() {
    assert_eq!(unsafe { libc::geteuid() }, 65532);
    let mapping = fs::read_to_string("/proc/self/uid_map").unwrap();
    assert_ne!(mapping.split_whitespace().nth(1), Some("0"));
    fs::create_dir("/scratch/nested").unwrap();
    assert_project_attributes_protected(Path::new("/scratch/nested"));
    let mut file = fs::File::create("/scratch/nested/data").unwrap();
    for _ in 0..2048 {
        match file
            .write_all(&[0x5a; 64 * 1024])
            .and_then(|_| file.sync_all())
        {
            Ok(()) => {}
            Err(error) => {
                assert_eq!(error.raw_os_error(), Some(libc::EDQUOT));
                let output_error = fs::File::create("/outputs/after-full")
                    .and_then(|mut file| file.write_all(&[1; 64 * 1024]))
                    .unwrap_err();
                assert_eq!(output_error.raw_os_error(), Some(libc::EDQUOT));
                println!("retagging blocked; inheritance protected; EDQUOT");
                return;
            }
        }
    }
    panic!("container exceeded its 64 MiB quota");
}

fn assert_project_attributes_protected(path: &Path) {
    use std::os::fd::AsRawFd;
    #[repr(C)]
    #[derive(Default)]
    struct Attributes {
        flags: u32,
        extsize: u32,
        nextents: u32,
        project: u32,
        cowextsize: u32,
        pad: [u8; 8],
    }
    let directory = fs::File::open(path).unwrap();
    let fd = directory.as_raw_fd();
    let mut attributes = Attributes::default();
    // Exercise both Linux attribute interfaces on a directory owned by the job.
    // Testing only the host-owned mount root would hide the retagging bypass.
    assert_eq!(
        unsafe {
            libc::ioctl(
                fd,
                libc::_IOR::<Attributes>(b'X' as u32, 31),
                &mut attributes,
            )
        },
        0
    );
    assert_eq!(attributes.project, 2001);
    assert_ne!(attributes.flags & 0x200, 0);
    attributes.project = 0;
    assert_eq!(
        unsafe { libc::ioctl(fd, libc::_IOW::<Attributes>(b'X' as u32, 32), &attributes) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EINVAL)
    );
    attributes.project = 2001;
    attributes.flags &= !0x200;
    assert_eq!(
        unsafe { libc::ioctl(fd, libc::_IOW::<Attributes>(b'X' as u32, 32), &attributes) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EINVAL)
    );
    let mut flags: libc::c_long = 0;
    assert_eq!(
        unsafe { libc::ioctl(fd, libc::_IOR::<libc::c_long>(b'f' as u32, 1), &mut flags) },
        0
    );
    flags &= !0x20000000;
    assert_eq!(
        unsafe { libc::ioctl(fd, libc::_IOW::<libc::c_long>(b'f' as u32, 2), &flags) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EINVAL)
    );
}
