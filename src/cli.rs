//! Command-line interface for running the PostgreSQL transaction-mode pooler.

use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::sync::Arc;

use crate::connector::TcpConnector;
use crate::pool::{Config, Pooler};

const MAX_USERS_FILE_BYTES: usize = 1_048_576;

/// Parsed command-line options for the `serve` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeArgs {
    /// Address to bind the pooler listener to.
    pub listen: String,
    /// Backend PostgreSQL server address.
    pub server: String,
    /// Maximum backend connections in the pool.
    pub pool_size: usize,
    /// Optional path to a `user:password` file.
    pub users_file: Option<String>,
    /// Optional backend server password.
    pub server_password: Option<String>,
}

/// Parses CLI arguments for `serve`.
///
/// # Errors
///
/// Returns a descriptive error string if the arguments are invalid.
pub fn parse_args(args: &[String]) -> Result<ServeArgs, String> {
    if args.is_empty() {
        return Err("missing subcommand (expected 'serve')".to_string());
    }
    if args[0] != "serve" {
        return Err(format!(
            "unknown subcommand '{}' (expected 'serve')",
            args[0]
        ));
    }

    let mut listen = "127.0.0.1:6432".to_string();
    let mut server = "127.0.0.1:5432".to_string();
    let mut pool_size: usize = 10;
    let mut users_file: Option<String> = None;
    let mut server_password: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        let flag = args[i].as_str();
        let val = args
            .get(i + 1)
            .ok_or_else(|| format!("missing value for flag '{flag}'"))?;
        match flag {
            "--listen" => listen.clone_from(val),
            "--server" => server.clone_from(val),
            "--pool-size" => {
                pool_size = val
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --pool-size '{val}'"))?;
                if pool_size == 0 {
                    return Err("--pool-size must be >= 1".to_string());
                }
            }
            "--users" => users_file = Some(val.clone()),
            "--server-password" => server_password = Some(val.clone()),
            other => return Err(format!("unknown flag '{other}'")),
        }
        i += 2;
    }

    Ok(ServeArgs {
        listen,
        server,
        pool_size,
        users_file,
        server_password,
    })
}

/// Parses a `users.txt` content string with lines of the form `user:password`.
///
/// Blank lines and lines starting with `#` are ignored.
///
/// # Errors
///
/// Returns an error string if the content exceeds 1 MiB or a line is malformed.
pub fn parse_users_content(content: &str) -> Result<Vec<(String, String)>, String> {
    if content.len() > MAX_USERS_FILE_BYTES {
        return Err("users file exceeds maximum size of 1 MiB".to_string());
    }
    let mut users = Vec::new();
    for (idx, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((user, pw)) = line.split_once(':') else {
            return Err(format!("invalid user entry on line {}", idx + 1));
        };
        if user.is_empty() {
            return Err(format!("empty username on line {}", idx + 1));
        }
        users.push((user.to_string(), pw.to_string()));
    }
    Ok(users)
}

/// Runs the CLI with `args` (excluding `argv[0]`), writing output to `stdout` and `stderr`.
///
/// Exit codes:
/// - `0`: clean execution
/// - `1`: domain failure
/// - `2`: usage, parse, or I/O error
pub fn run(args: &[String], stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
    if args.first().is_some_and(|a| a == "--help" || a == "-h") {
        let _ = writeln!(
            stdout,
            "Usage: pg-connection-pooler serve [--listen ADDR] [--server ADDR] [--pool-size N] [--users FILE]"
        );
        return 0;
    }

    let parsed = match parse_args(args) {
        Ok(p) => p,
        Err(msg) => {
            let _ = writeln!(stderr, "error: {msg}");
            return 2;
        }
    };

    let users = if let Some(path) = &parsed.users_file {
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                let _ = writeln!(stderr, "error reading users file: {}", e.kind());
                return 2;
            }
        };
        match parse_users_content(&content) {
            Ok(u) => u,
            Err(msg) => {
                let _ = writeln!(stderr, "error: {msg}");
                return 2;
            }
        }
    } else {
        Vec::new()
    };

    let listener = match TcpListener::bind(&parsed.listen) {
        Ok(l) => l,
        Err(e) => {
            let _ = writeln!(stderr, "error binding listener: {}", e.kind());
            return 2;
        }
    };

    let config = Config {
        pool_size: parsed.pool_size,
        users,
        ..Config::default()
    };
    let connector = Arc::new(TcpConnector::new(parsed.server, parsed.server_password));
    let pooler = Pooler::new(config, connector);

    let _ = writeln!(stdout, "listening on {}", parsed.listen);
    match pooler.serve(listener) {
        Ok(()) => 0,
        Err(e) => {
            let _ = writeln!(stderr, "pooler failed: {}", e.kind());
            1
        }
    }
}
