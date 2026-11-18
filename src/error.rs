//! Error types for the connection pooler.

use std::fmt;
use std::io;

/// Machine-readable error category returned by [`PoolError::code`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// Underlying I/O failure.
    Io(String),
    /// A frame exceeded the configured `max_message_bytes`.
    MessageTooLarge {
        /// Declared frame length.
        length: usize,
        /// Configured maximum frame length.
        max: usize,
    },
    /// Wire protocol violation (invalid length, version, or structure).
    ProtocolViolation(&'static str),
    /// Client or server authentication failed.
    AuthFailed,
    /// Server requested an unsupported authentication mechanism (e.g. SASL).
    UnsupportedAuth(i32),
    /// Timed out waiting for an available server connection in the pool.
    PoolTimeout,
    /// Client rejected because `max_clients` is already reached.
    TooManyClients,
    /// Backend server closed the connection unexpectedly.
    ServerClosed,
}

/// Error returned by codec, connector, and pooler operations.
///
/// Its [`Display`](fmt::Display) implementation never prints passwords or query text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolError {
    kind: ErrorKind,
}

impl PoolError {
    /// Creates an I/O error from an [`io::Error`].
    #[must_use]
    pub fn io(err: &io::Error) -> Self {
        let mut msg = err.kind().to_string();
        if msg.len() > 120 {
            msg.truncate(120);
        }
        Self {
            kind: ErrorKind::Io(msg),
        }
    }

    /// Creates a `message_too_large` error.
    #[must_use]
    pub const fn message_too_large(length: usize, max: usize) -> Self {
        Self {
            kind: ErrorKind::MessageTooLarge { length, max },
        }
    }

    /// Creates a `protocol_violation` error with a static reason.
    #[must_use]
    pub const fn protocol_violation(reason: &'static str) -> Self {
        Self {
            kind: ErrorKind::ProtocolViolation(reason),
        }
    }

    /// Creates an `auth_failed` error.
    #[must_use]
    pub const fn auth_failed() -> Self {
        Self {
            kind: ErrorKind::AuthFailed,
        }
    }

    /// Creates an `unsupported_auth` error with the server's authentication code.
    #[must_use]
    pub const fn unsupported_auth(auth_code: i32) -> Self {
        Self {
            kind: ErrorKind::UnsupportedAuth(auth_code),
        }
    }

    /// Creates a `pool_timeout` error.
    #[must_use]
    pub const fn pool_timeout() -> Self {
        Self {
            kind: ErrorKind::PoolTimeout,
        }
    }

    /// Creates a `too_many_clients` error.
    #[must_use]
    pub const fn too_many_clients() -> Self {
        Self {
            kind: ErrorKind::TooManyClients,
        }
    }

    /// Creates a `server_closed` error.
    #[must_use]
    pub const fn server_closed() -> Self {
        Self {
            kind: ErrorKind::ServerClosed,
        }
    }

    /// Returns the stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self.kind {
            ErrorKind::Io(_) => "io",
            ErrorKind::MessageTooLarge { .. } => "message_too_large",
            ErrorKind::ProtocolViolation(_) => "protocol_violation",
            ErrorKind::AuthFailed => "auth_failed",
            ErrorKind::UnsupportedAuth(_) => "unsupported_auth",
            ErrorKind::PoolTimeout => "pool_timeout",
            ErrorKind::TooManyClients => "too_many_clients",
            ErrorKind::ServerClosed => "server_closed",
        }
    }

    /// Returns the error category.
    #[must_use]
    pub const fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Io(kind) => write!(f, "io error: {kind}"),
            ErrorKind::MessageTooLarge { length, max } => {
                write!(f, "message length {length} exceeds maximum {max}")
            }
            ErrorKind::ProtocolViolation(reason) => write!(f, "protocol violation: {reason}"),
            ErrorKind::AuthFailed => f.write_str("authentication failed"),
            ErrorKind::UnsupportedAuth(code) => {
                write!(f, "unsupported authentication method {code}")
            }
            ErrorKind::PoolTimeout => f.write_str("pool timeout"),
            ErrorKind::TooManyClients => f.write_str("too many clients"),
            ErrorKind::ServerClosed => f.write_str("server connection closed unexpectedly"),
        }
    }
}

impl std::error::Error for PoolError {}

impl From<io::Error> for PoolError {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            Self::server_closed()
        } else {
            Self::io(&err)
        }
    }
}
