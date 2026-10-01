// Test fixtures use panics to fail the test.
#![allow(clippy::unwrap_used)]
//! `WALGIT__` overrides fail closed through the real binaries: a typo'd
//! override stops `config check` and server startup (before anything binds),
//! naming the variable; a valid one is applied.

use std::process::{Command, Output};

const TYPO: (&str, &str) = ("WALGIT__SERVER__LSITEN", "127.0.0.1:0");
const HINT: &str = "WALGIT__SERVER__LSITEN: unknown key `lsiten` in [server]; did you mean WALGIT__SERVER__LISTEN?";

/// A binary with `--config /dev/null` (defaults + env, D39) and only `env`:
/// nothing from the test runner's environment leaks in.
fn run(bin: &str, args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(bin);
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .args(["--config", "/dev/null"])
        .args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().unwrap()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

#[test]
fn server_startup_refuses_a_typod_override() {
    // Spawned, not `output()`: a server that ignored the typo would serve forever.
    let mut child = Command::new(env!("CARGO_BIN_EXE_walgit-server"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env(TYPO.0, TYPO.1)
        .args(["--config", "/dev/null"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() > deadline {
            child.kill().unwrap();
            panic!("walgit-server started with {} set", TYPO.0);
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(stderr(&out).contains(HINT), "{}", stderr(&out));
}

#[test]
fn config_check_refuses_a_typod_override_in_the_env_and_in_an_env_file() {
    let walgit = env!("CARGO_BIN_EXE_walgit");
    let out = run(walgit, &["config", "check"], &[TYPO]);
    assert!(!out.status.success(), "process env typo accepted");
    assert!(stderr(&out).contains(HINT), "{}", stderr(&out));

    let dir = tempfile::tempdir().unwrap();
    let env_file = dir.path().join("walgit.env");
    std::fs::write(
        &env_file,
        format!(
            "# host overrides\nWALGIT__WAL__MAX_BATCH=7\n{}={}\n",
            TYPO.0, TYPO.1
        ),
    )
    .unwrap();
    let path = env_file.to_str().unwrap();
    let out = run(walgit, &["config", "check", "--env-file", path], &[]);
    assert!(!out.status.success(), "env-file typo accepted");
    assert!(stderr(&out).contains(HINT), "{}", stderr(&out));
    assert!(!String::from_utf8_lossy(&out.stdout).contains("config OK"));

    std::fs::write(&env_file, "WALGIT__WAL__MAX_BATCH=7\n").unwrap();
    let out = run(walgit, &["config", "check", "--env-file", path], &[]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(String::from_utf8_lossy(&out.stdout).contains("config OK"));
}

#[test]
fn a_valid_override_is_applied() {
    let out = run(
        env!("CARGO_BIN_EXE_walgit"),
        &["config", "dump"],
        &[("WALGIT__WAL__MAX_BATCH", "7")],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    let dump = String::from_utf8_lossy(&out.stdout);
    assert!(dump.contains("max_batch = 7"), "{dump}");
}
