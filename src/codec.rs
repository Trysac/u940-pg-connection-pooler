//! PostgreSQL v3.0 wire protocol startup and message framing.

use std::io::{self, Read, Write};

use crate::error::PoolError;

/// PostgreSQL protocol v3.0 version number (`(3 << 16) | 0`).
pub const PROTOCOL_V3: i32 = 196_608;
/// PostgreSQL `SSLRequest` magic code (`80877103`).
pub const SSL_REQUEST_CODE: i32 = 80_877_103;
/// PostgreSQL `GSSENCRequest` magic code (`80877104`).
pub const GSS_REQUEST_CODE: i32 = 80_877_104;
/// PostgreSQL `CancelRequest` magic code (`80877102`).
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;

/// Parsed client startup packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Startup {
    /// `SSLRequest` packet.
    Ssl,
    /// `GSSENCRequest` packet.
    Gss,
    /// `CancelRequest` packet.
    Cancel {
        /// Target backend process ID.
        pid: i32,
        /// Target backend secret key.
        key: i32,
    },
    /// Normal v3.0 `StartupMessage`.
    Params {
        /// Database role name.
        user: String,
        /// Target database name (defaults to `user` if omitted in packet).
        database: String,
        /// All key/value startup parameters in wire order.
        params: Vec<(String, String)>,
    },
}

/// A framed PostgreSQL frontend or backend message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Single-byte message type tag (e.g. `b'Q'`, `b'Z'`, `b'E'`).
    pub tag: u8,
    /// Message payload excluding the 1-byte tag and 4-byte length prefix.
    pub body: Vec<u8>,
}

/// Reads an initial untagged startup packet from `r`, enforcing a maximum frame size of `max` bytes.
///
/// # Errors
///
/// Returns [`PoolError`] with:
/// - `message_too_large` if the 4-byte length prefix exceeds `max`
/// - `protocol_violation` if the length is smaller than 8, the protocol code is unknown,
///   strings are not valid UTF-8 / NUL-terminated, or `user` is missing
/// - `server_closed` or `io` on read failures
pub fn read_startup(r: &mut dyn Read, max: usize) -> Result<Startup, PoolError> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let raw_len = i32::from_be_bytes(len_buf);
    if raw_len < 8 {
        return Err(PoolError::protocol_violation("startup length < 8"));
    }
    let total_len = usize::try_from(raw_len)
        .map_err(|_| PoolError::protocol_violation("negative startup length"))?;
    if total_len > max {
        return Err(PoolError::message_too_large(total_len, max));
    }

    let payload_len = total_len - 4;
    let mut buf = vec![0u8; payload_len];
    r.read_exact(&mut buf)?;

    let code = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    match code {
        SSL_REQUEST_CODE => {
            if total_len != 8 {
                return Err(PoolError::protocol_violation("invalid SSLRequest length"));
            }
            Ok(Startup::Ssl)
        }
        GSS_REQUEST_CODE => {
            if total_len != 8 {
                return Err(PoolError::protocol_violation(
                    "invalid GSSENCRequest length",
                ));
            }
            Ok(Startup::Gss)
        }
        CANCEL_REQUEST_CODE => {
            if total_len != 16 {
                return Err(PoolError::protocol_violation(
                    "invalid CancelRequest length",
                ));
            }
            let pid = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let key = i32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
            Ok(Startup::Cancel { pid, key })
        }
        PROTOCOL_V3 => parse_startup_params(&buf[4..]),
        _ => Err(PoolError::protocol_violation(
            "unsupported protocol version",
        )),
    }
}

fn parse_startup_params(rest: &[u8]) -> Result<Startup, PoolError> {
    if rest.last() != Some(&0) {
        return Err(PoolError::protocol_violation(
            "startup parameters not NUL-terminated",
        ));
    }
    let mut parts = rest.split(|&b| b == 0);
    let mut params = Vec::new();
    let mut user: Option<String> = None;
    let mut database: Option<String> = None;

    while let Some(key_bytes) = parts.next() {
        if key_bytes.is_empty() {
            break;
        }
        let val_bytes = parts
            .next()
            .ok_or_else(|| PoolError::protocol_violation("missing startup parameter value"))?;
        let key = std::str::from_utf8(key_bytes)
            .map_err(|_| PoolError::protocol_violation("invalid UTF-8 in startup key"))?
            .to_string();
        let val = std::str::from_utf8(val_bytes)
            .map_err(|_| PoolError::protocol_violation("invalid UTF-8 in startup value"))?
            .to_string();
        if key == "user" && !val.is_empty() {
            user = Some(val.clone());
        } else if key == "database" && !val.is_empty() {
            database = Some(val.clone());
        }
        params.push((key, val));
    }

    let Some(user) = user else {
        return Err(PoolError::protocol_violation(
            "startup message missing user parameter",
        ));
    };
    let database = database.unwrap_or_else(|| user.clone());
    Ok(Startup::Params {
        user,
        database,
        params,
    })
}

/// Writes a v3.0 `StartupMessage` for `user`, `database`, and extra parameters to `w`.
///
/// # Errors
///
/// Returns an [`io::Error`] if writing to `w` fails.
pub fn write_startup(w: &mut dyn Write, user: &str, database: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    payload.extend_from_slice(b"user\0");
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"database\0");
    payload.extend_from_slice(database.as_bytes());
    payload.push(0);
    payload.push(0);

    let total_len = i32::try_from(payload.len() + 4)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "startup message too large"))?;
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&total_len.to_be_bytes());
    frame.extend_from_slice(&payload);
    w.write_all(&frame)
}

/// Reads a single tagged message from `r`, enforcing that the 4-byte length field is in `4..=max`.
///
/// # Errors
///
/// Returns [`PoolError`] with:
/// - `message_too_large` if the length field exceeds `max`
/// - `protocol_violation` if the length field is less than 4
/// - `server_closed` on EOF or `io` on other I/O errors
pub fn read_message(r: &mut dyn Read, max: usize) -> Result<Message, PoolError> {
    let mut header = [0u8; 5];
    r.read_exact(&mut header)?;
    let tag = header[0];
    let raw_len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    if raw_len < 4 {
        return Err(PoolError::protocol_violation("message length < 4"));
    }
    let len = usize::try_from(raw_len)
        .map_err(|_| PoolError::protocol_violation("negative message length"))?;
    if len > max {
        return Err(PoolError::message_too_large(len, max));
    }
    let body_len = len - 4;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body)?;
    Ok(Message { tag, body })
}

/// Writes a single tagged message `m` to `w`.
///
/// # Errors
///
/// Returns an [`io::Error`] if writing to `w` fails or the message body is too large.
pub fn write_message(w: &mut dyn Write, m: &Message) -> io::Result<()> {
    let len = i32::try_from(m.body.len() + 4)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "message body too large"))?;
    let mut buf = Vec::with_capacity(m.body.len() + 5);
    buf.push(m.tag);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&m.body);
    w.write_all(&buf)
}

/// Builds a PostgreSQL `ErrorResponse` (`'E'`) message with `S`, `V`, `C`, and `M` fields.
#[must_use]
pub fn error_response(sqlstate: &str, message: &str) -> Message {
    let mut body = Vec::with_capacity(32 + sqlstate.len() + message.len());
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'V');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    Message { tag: b'E', body }
}

/// Builds `AuthenticationOk` (`'R'`, code 0).
#[must_use]
pub fn auth_ok() -> Message {
    Message {
        tag: b'R',
        body: 0i32.to_be_bytes().to_vec(),
    }
}

/// Builds `AuthenticationMD5Password` (`'R'`, code 5, 4-byte salt).
#[must_use]
pub fn auth_md5_challenge(salt: [u8; 4]) -> Message {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&5i32.to_be_bytes());
    body.extend_from_slice(&salt);
    Message { tag: b'R', body }
}

/// Builds `BackendKeyData` (`'K'`, pid + secret key).
#[must_use]
pub fn backend_key_data(pid: i32, key: i32) -> Message {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&key.to_be_bytes());
    Message { tag: b'K', body }
}

/// Builds `ReadyForQuery` (`'Z'`, status byte `'I'`, `'T'`, or `'E'`).
#[must_use]
pub fn ready_for_query(status: u8) -> Message {
    Message {
        tag: b'Z',
        body: vec![status],
    }
}

/// Extracts the NUL-terminated string from a `PasswordMessage` (`'p'`) body.
pub(crate) fn parse_password_body(body: &[u8]) -> Result<&str, PoolError> {
    let raw = body.strip_suffix(&[0]).unwrap_or(body);
    std::str::from_utf8(raw)
        .map_err(|_| PoolError::protocol_violation("invalid UTF-8 in password message"))
}
