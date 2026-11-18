//! Server stream trait and TCP PostgreSQL backend connector.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;

use crate::codec::{Message, read_message, write_message, write_startup};
use crate::error::PoolError;
use crate::md5::md5_password;

const DEFAULT_MAX_STARTUP_MESSAGE: usize = 16_777_216;

/// A bidirectional byte stream to a PostgreSQL backend server.
pub trait ServerStream: Read + Write + Send {
    /// `ParameterStatus` (`'S'`) messages collected during backend startup, if available.
    fn initial_parameters(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}

impl ServerStream for TcpStream {}

/// A stream wrapper that carries the backend's initial `ParameterStatus` pairs.
pub struct HandshakedTcpStream {
    stream: TcpStream,
    params: Vec<(String, String)>,
}

impl Read for HandshakedTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for HandshakedTcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl ServerStream for HandshakedTcpStream {
    fn initial_parameters(&self) -> Vec<(String, String)> {
        self.params.clone()
    }
}

/// Factory for establishing authenticated connections to a PostgreSQL backend server.
pub trait Connector: Send + Sync {
    /// Opens a new connection to the backend for `(user, database)` and completes the startup
    /// handshake through the initial `ReadyForQuery`.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] if the connection or handshake fails. To preserve a specific
    /// [`PoolError`] across the `io::Error` boundary, wrap it with `io::Error::other(pool_error)`.
    fn connect(&self, user: &str, database: &str) -> io::Result<Box<dyn ServerStream>>;
}

/// Connects to a PostgreSQL server over TCP and performs the v3.0 startup handshake.
#[derive(Debug)]
pub struct TcpConnector {
    /// Backend socket address (e.g. `"127.0.0.1:5432"`).
    pub addr: String,
    /// Optional password used when the backend requests cleartext (3) or MD5 (5) authentication.
    pub password: Option<String>,
    cached_params: Mutex<Vec<(String, String)>>,
}

impl TcpConnector {
    /// Creates a new [`TcpConnector`] for `addr` and optional `password`.
    #[must_use]
    pub fn new(addr: impl Into<String>, password: Option<String>) -> Self {
        Self {
            addr: addr.into(),
            password,
            cached_params: Mutex::new(Vec::new()),
        }
    }

    /// Returns a snapshot of the `ParameterStatus` key-value pairs observed during server handshakes.
    #[must_use]
    pub fn cached_parameters(&self) -> Vec<(String, String)> {
        self.cached_params
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }
}

/// Performs the PostgreSQL backend startup handshake on an existing stream `s`.
///
/// Handles `AuthenticationOk` (0), `AuthenticationCleartextPassword` (3), and
/// `AuthenticationMD5Password` (5), collects `ParameterStatus` (`'S'`) and ignores
/// `BackendKeyData` (`'K'`) and `NoticeResponse` (`'N'`) until `ReadyForQuery` (`'Z'`).
///
/// # Errors
///
/// Returns a [`PoolError`] if authentication fails, an unsupported authentication method is
/// requested, or the wire protocol is violated.
pub fn handshake_server(
    s: &mut dyn ServerStream,
    user: &str,
    database: &str,
    password: Option<&str>,
    max_message_bytes: usize,
) -> Result<Vec<(String, String)>, PoolError> {
    write_startup(s, user, database).map_err(|e| PoolError::io(&e))?;
    s.flush().map_err(|e| PoolError::io(&e))?;

    let mut params = Vec::new();
    loop {
        let msg = read_message(s, max_message_bytes)?;
        match msg.tag {
            b'R' => {
                if msg.body.len() < 4 {
                    return Err(PoolError::protocol_violation("short Authentication body"));
                }
                let code = i32::from_be_bytes([msg.body[0], msg.body[1], msg.body[2], msg.body[3]]);
                match code {
                    0 => {}
                    3 => {
                        let pw = password.ok_or_else(PoolError::auth_failed)?;
                        let mut body = Vec::with_capacity(pw.len() + 1);
                        body.extend_from_slice(pw.as_bytes());
                        body.push(0);
                        write_message(s, &Message { tag: b'p', body })
                            .map_err(|e| PoolError::io(&e))?;
                        s.flush().map_err(|e| PoolError::io(&e))?;
                    }
                    5 => {
                        if msg.body.len() < 8 {
                            return Err(PoolError::protocol_violation("short MD5 salt"));
                        }
                        let pw = password.ok_or_else(PoolError::auth_failed)?;
                        let salt = [msg.body[4], msg.body[5], msg.body[6], msg.body[7]];
                        let digest = md5_password(user, pw, salt);
                        let mut body = Vec::with_capacity(digest.len() + 1);
                        body.extend_from_slice(digest.as_bytes());
                        body.push(0);
                        write_message(s, &Message { tag: b'p', body })
                            .map_err(|e| PoolError::io(&e))?;
                        s.flush().map_err(|e| PoolError::io(&e))?;
                    }
                    other => return Err(PoolError::unsupported_auth(other)),
                }
            }
            b'S' => {
                if let Some((k, v)) = parse_parameter_status(&msg.body) {
                    params.push((k, v));
                }
            }
            b'K' | b'N' => {}
            b'Z' => {
                return Ok(params);
            }
            b'E' => {
                return Err(PoolError::auth_failed());
            }
            _ => {
                return Err(PoolError::protocol_violation(
                    "unexpected backend message during startup",
                ));
            }
        }
    }
}

pub(crate) fn parse_parameter_status(body: &[u8]) -> Option<(String, String)> {
    let mut parts = body.split(|&b| b == 0);
    let k = std::str::from_utf8(parts.next()?).ok()?.to_string();
    let v = std::str::from_utf8(parts.next()?).ok()?.to_string();
    Some((k, v))
}

impl Connector for TcpConnector {
    fn connect(&self, user: &str, database: &str) -> io::Result<Box<dyn ServerStream>> {
        let mut stream = TcpStream::connect(&self.addr)?;
        let _ = stream.set_nodelay(true);
        let params = handshake_server(
            &mut stream,
            user,
            database,
            self.password.as_deref(),
            DEFAULT_MAX_STARTUP_MESSAGE,
        )
        .map_err(io::Error::other)?;
        if let Ok(mut guard) = self.cached_params.lock()
            && guard.is_empty()
        {
            (*guard).clone_from(&params);
        }
        Ok(Box::new(HandshakedTcpStream { stream, params }))
    }
}
