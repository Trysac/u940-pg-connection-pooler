#![cfg(feature = "conformance")]
//! Conformance test running `pgbench` against a live PostgreSQL server through `Pooler`.

use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::thread;

use pg_connection_pooler::{Config, Connector, Pooler, TcpConnector};

fn pgbench_available() -> bool {
    Command::new("pgbench").arg("--version").output().is_ok()
}

#[test]
fn pgbench_simple_mode_through_pooler() {
    if !pgbench_available() {
        eprintln!("skipping conformance test: pgbench not found on PATH");
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind pooler listener");
    let port = listener.local_addr().expect("local_addr").port();

    let config = Config {
        pool_size: 4,
        users: vec![("bench".to_string(), "pw".to_string())],
        ..Config::default()
    };
    let connector: Arc<dyn Connector> = Arc::new(TcpConnector::new("127.0.0.1:5432", None));
    let pooler = Pooler::new(config, connector);

    thread::spawn(move || {
        let _ = pooler.serve(listener);
    });

    let init_out = Command::new("pgbench")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-U",
            "bench",
            "-i",
            "-s",
            "1",
            "app",
        ])
        .env("PGPASSWORD", "pw")
        .output()
        .expect("failed to execute pgbench -i");

    assert!(
        init_out.status.success(),
        "pgbench -i failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&init_out.stdout),
        String::from_utf8_lossy(&init_out.stderr)
    );

    let run_out = Command::new("pgbench")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-U",
            "bench",
            "-c",
            "8",
            "-j",
            "2",
            "-t",
            "200",
            "-M",
            "simple",
            "app",
        ])
        .env("PGPASSWORD", "pw")
        .output()
        .expect("failed to execute pgbench run");

    let stdout = String::from_utf8_lossy(&run_out.stdout);
    let stderr = String::from_utf8_lossy(&run_out.stderr);
    assert!(
        run_out.status.success(),
        "pgbench run failed:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("number of failed transactions: 0"),
        "expected 'number of failed transactions: 0' in pgbench output:\nstdout: {stdout}\nstderr: {stderr}"
    );
}
