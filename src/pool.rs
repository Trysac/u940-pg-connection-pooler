//! Transaction-mode PostgreSQL connection pooler.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::codec::{
    Message, Startup, auth_md5_challenge, auth_ok, backend_key_data, error_response,
    parse_password_body, read_message, read_startup, ready_for_query, write_message,
};
use crate::connector::{Connector, ServerStream, parse_parameter_status};
use crate::error::{ErrorKind, PoolError};
use crate::md5::md5_password;

const POOLER_BACKEND_PID: i32 = 12_345;
const POOLER_BACKEND_KEY: i32 = 67_890;

/// A bidirectional stream from a PostgreSQL client.
pub trait ClientStream: Read + Write + Send {}
impl<T: Read + Write + Send> ClientStream for T {}

/// Configuration for [`Pooler`].
pub struct Config {
    /// Maximum total server connections (idle + active) across the pool.
    pub pool_size: usize,
    /// Maximum concurrent connected clients.
    pub max_clients: usize,
    /// Maximum duration a client waits for a free server connection.
    pub pool_timeout: Duration,
    /// Maximum wire message size in bytes.
    pub max_message_bytes: usize,
    /// Allowed `(user, password)` pairs. An empty list enables `trust` mode.
    pub users: Vec<(String, String)>,
    /// Source of 4-byte random salts for MD5 authentication challenges.
    pub salt_source: Box<dyn Fn() -> [u8; 4] + Send + Sync>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pool_size: 10,
            max_clients: 100,
            pool_timeout: Duration::from_secs(5),
            max_message_bytes: 16_777_216,
            users: Vec::new(),
            salt_source: Box::new(|| [1, 2, 3, 4]),
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("pool_size", &self.pool_size)
            .field("max_clients", &self.max_clients)
            .field("pool_timeout", &self.pool_timeout)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("users_count", &self.users.len())
            .finish_non_exhaustive()
    }
}

/// Point-in-time pool metrics returned by [`Pooler::stats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Currently connected clients.
    pub clients: usize,
    /// Server connections sitting idle in the pool.
    pub servers_idle: usize,
    /// Server connections currently checked out or being established.
    pub servers_active: usize,
    /// Clients currently blocked waiting for a server connection.
    pub waiting: usize,
    /// Total number of backend `Connector::connect` calls started.
    pub connects: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PoolKey {
    user: String,
    database: String,
}

struct PoolState {
    clients: usize,
    idle: HashMap<PoolKey, VecDeque<Box<dyn ServerStream>>>,
    idle_count: usize,
    active_count: usize,
    waiting: usize,
    cached_params: Vec<(String, String)>,
}

impl PoolState {
    fn new() -> Self {
        Self {
            clients: 0,
            idle: HashMap::new(),
            idle_count: 0,
            active_count: 0,
            waiting: 0,
            cached_params: Vec::new(),
        }
    }
}

struct Inner {
    config: Config,
    connector: Arc<dyn Connector>,
    state: Mutex<PoolState>,
    cvar: Condvar,
    connects: AtomicU64,
}

/// PostgreSQL transaction-mode connection pooler.
///
/// Note on transaction-mode semantics:
/// - `CancelRequest` packets are accepted and dropped.
/// - Named prepared statements in `Parse` (`'P'`) are rejected with SQLSTATE `0A000`;
///   unnamed prepared statements work across pooled transactions.
/// - Session state modified with `SET` leaks across clients sharing a backend connection,
///   matching standard transaction-mode pooling behavior (as in `PgBouncer`).
#[derive(Clone)]
pub struct Pooler {
    inner: Arc<Inner>,
}

impl Pooler {
    /// Creates a new [`Pooler`] with `config` and `connector`.
    #[must_use]
    pub fn new(config: Config, connector: Arc<dyn Connector>) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                connector,
                state: Mutex::new(PoolState::new()),
                cvar: Condvar::new(),
                connects: AtomicU64::new(0),
            }),
        }
    }

    /// Returns current pool statistics.
    #[must_use]
    pub fn stats(&self) -> Stats {
        let guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Stats {
            clients: guard.clients,
            servers_idle: guard.idle_count,
            servers_active: guard.active_count,
            waiting: guard.waiting,
            connects: self.inner.connects.load(Ordering::SeqCst),
        }
    }

    /// Accepts TCP connections from `listener` in a loop, spawning one thread per client.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] if `listener.accept()` fails permanently.
    #[allow(clippy::needless_pass_by_value)]
    pub fn serve(&self, listener: TcpListener) -> io::Result<()> {
        for incoming in listener.incoming() {
            let stream = incoming?;
            let _ = stream.set_nodelay(true);
            let pooler = self.clone();
            thread::spawn(move || {
                let _ = pooler.handle_client(Box::new(stream));
            });
        }
        Ok(())
    }

    /// Handles the full lifecycle of a single client connection.
    ///
    /// # Errors
    ///
    /// Returns a [`PoolError`] if startup, authentication, or message handling fails.
    pub fn handle_client(&self, mut stream: Box<dyn ClientStream>) -> Result<(), PoolError> {
        {
            let mut guard = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if guard.clients >= self.inner.config.max_clients {
                drop(guard);
                let _ = send_error_and_flush(&mut *stream, "53300", "too many clients");
                return Err(PoolError::too_many_clients());
            }
            guard.clients += 1;
        }

        let result = self.run_client_session(&mut *stream);

        {
            let mut guard = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.clients = guard.clients.saturating_sub(1);
        }
        result
    }

    fn run_client_session(&self, client: &mut dyn ClientStream) -> Result<(), PoolError> {
        let max_bytes = self.inner.config.max_message_bytes;
        let (user, database) = loop {
            match read_startup(client, max_bytes) {
                Ok(Startup::Ssl | Startup::Gss) => {
                    client.write_all(b"N").map_err(|e| PoolError::io(&e))?;
                    client.flush().map_err(|e| PoolError::io(&e))?;
                }
                Ok(Startup::Cancel { .. }) => {
                    return Ok(());
                }
                Ok(Startup::Params { user, database, .. }) => {
                    break (user, database);
                }
                Err(err) => {
                    if matches!(
                        err.kind(),
                        ErrorKind::MessageTooLarge { .. } | ErrorKind::ProtocolViolation(_)
                    ) {
                        let _ = send_error_and_flush(client, "08P01", &err.to_string());
                    }
                    return Err(err);
                }
            }
        };

        self.authenticate_client(client, &user)?;

        write_message(client, &auth_ok()).map_err(|e| PoolError::io(&e))?;
        let cached_params = {
            let guard = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.cached_params.clone()
        };
        for (k, v) in cached_params {
            let mut body = Vec::with_capacity(k.len() + v.len() + 2);
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
            write_message(client, &Message { tag: b'S', body }).map_err(|e| PoolError::io(&e))?;
        }
        write_message(
            client,
            &backend_key_data(POOLER_BACKEND_PID, POOLER_BACKEND_KEY),
        )
        .map_err(|e| PoolError::io(&e))?;
        write_message(client, &ready_for_query(b'I')).map_err(|e| PoolError::io(&e))?;
        client.flush().map_err(|e| PoolError::io(&e))?;

        let key = PoolKey { user, database };
        let mut bound_server: Option<(Box<dyn ServerStream>, u8)> = None;

        let loop_res = self.message_loop(client, &key, &mut bound_server);

        if let Some((server, status)) = bound_server.take() {
            if status == b'I' {
                self.release_server(&key, server);
            } else {
                self.discard_server(server);
            }
        }

        loop_res
    }

    fn authenticate_client(
        &self,
        client: &mut dyn ClientStream,
        user: &str,
    ) -> Result<(), PoolError> {
        if self.inner.config.users.is_empty() {
            return Ok(());
        }
        let expected_pw = self
            .inner
            .config
            .users
            .iter()
            .find(|(u, _)| u == user)
            .map(|(_, p)| p.as_str());

        let salt = (self.inner.config.salt_source)();
        write_message(client, &auth_md5_challenge(salt)).map_err(|e| PoolError::io(&e))?;
        client.flush().map_err(|e| PoolError::io(&e))?;

        let msg = match read_message(client, self.inner.config.max_message_bytes) {
            Ok(m) => m,
            Err(err) => {
                if matches!(err.kind(), ErrorKind::MessageTooLarge { .. }) {
                    let _ = send_error_and_flush(client, "08P01", &err.to_string());
                }
                return Err(err);
            }
        };
        if msg.tag != b'p' {
            let _ = send_error_and_flush(client, "08P01", "expected PasswordMessage");
            return Err(PoolError::protocol_violation("expected PasswordMessage"));
        }

        let given = parse_password_body(&msg.body)?;
        let Some(pw) = expected_pw else {
            let _ = send_error_and_flush(client, "28P01", "password authentication failed");
            return Err(PoolError::auth_failed());
        };
        let expected_digest = md5_password(user, pw, salt);
        if given != expected_digest {
            let _ = send_error_and_flush(client, "28P01", "password authentication failed");
            return Err(PoolError::auth_failed());
        }
        Ok(())
    }

    fn message_loop(
        &self,
        client: &mut dyn ClientStream,
        key: &PoolKey,
        bound_server: &mut Option<(Box<dyn ServerStream>, u8)>,
    ) -> Result<(), PoolError> {
        let max_bytes = self.inner.config.max_message_bytes;
        let mut pending_extended: Vec<Message> = Vec::new();

        loop {
            let msg = match read_message(client, max_bytes) {
                Ok(m) => m,
                Err(err) => match err.kind() {
                    ErrorKind::ServerClosed => return Ok(()),
                    ErrorKind::MessageTooLarge { .. } | ErrorKind::ProtocolViolation(_) => {
                        let _ = send_error_and_flush(client, "08P01", &err.to_string());
                        return Err(err);
                    }
                    _ => return Err(err),
                },
            };

            match msg.tag {
                b'X' => {
                    return Ok(());
                }
                b'P' => {
                    if is_named_parse(&msg.body) {
                        pending_extended.clear();
                        send_error_and_ready(
                            client,
                            "0A000",
                            "named prepared statements are not supported in transaction pooling mode",
                            bound_server.as_ref().map_or(b'I', |(_, s)| *s),
                        )?;
                        continue;
                    }
                    if bound_server.is_some() {
                        self.send_to_bound_server(bound_server, &msg)?;
                    } else {
                        pending_extended.push(msg);
                    }
                }
                b'B' | b'D' | b'E' | b'C' => {
                    if bound_server.is_some() {
                        self.send_to_bound_server(bound_server, &msg)?;
                    } else {
                        pending_extended.push(msg);
                    }
                }
                b'Q' | b'S' | b'H' | b'F' => {
                    if bound_server.is_none() {
                        match self.acquire_server(key) {
                            Ok(srv) => {
                                *bound_server = Some((srv, b'I'));
                            }
                            Err(err) => {
                                pending_extended.clear();
                                match err.kind() {
                                    ErrorKind::PoolTimeout => {
                                        send_error_and_ready(
                                            client,
                                            "53300",
                                            "pool timeout",
                                            b'I',
                                        )?;
                                        continue;
                                    }
                                    ErrorKind::UnsupportedAuth(_) => {
                                        let _ =
                                            send_error_and_flush(client, "28000", &err.to_string());
                                        return Err(err);
                                    }
                                    ErrorKind::AuthFailed => {
                                        let _ =
                                            send_error_and_flush(client, "28P01", &err.to_string());
                                        return Err(err);
                                    }
                                    _ => {
                                        let _ =
                                            send_error_and_flush(client, "08006", &err.to_string());
                                        return Err(err);
                                    }
                                }
                            }
                        }
                    }

                    for buffered in pending_extended.drain(..) {
                        self.send_to_bound_server(bound_server, &buffered)?;
                    }
                    self.send_to_bound_server(bound_server, &msg)?;
                    if let Some((srv, _)) = bound_server.as_mut() {
                        srv.flush().map_err(|e| PoolError::io(&e))?;
                    }

                    if msg.tag == b'H' {
                        self.relay_until_ready(client, key, bound_server, true)?;
                    } else {
                        self.relay_until_ready(client, key, bound_server, false)?;
                    }
                }
                _ => {
                    let _ = send_error_and_flush(client, "08P01", "unsupported frontend message");
                    return Err(PoolError::protocol_violation(
                        "unsupported frontend message",
                    ));
                }
            }
        }
    }

    fn send_to_bound_server(
        &self,
        bound_server: &mut Option<(Box<dyn ServerStream>, u8)>,
        msg: &Message,
    ) -> Result<(), PoolError> {
        let Some((srv, _)) = bound_server.as_mut() else {
            return Err(PoolError::server_closed());
        };
        if let Err(e) = write_message(srv, msg) {
            if let Some((dead, _)) = bound_server.take() {
                self.discard_server(dead);
            }
            return Err(PoolError::io(&e));
        }
        Ok(())
    }

    fn relay_until_ready(
        &self,
        client: &mut dyn ClientStream,
        key: &PoolKey,
        bound_server: &mut Option<(Box<dyn ServerStream>, u8)>,
        flush_only: bool,
    ) -> Result<(), PoolError> {
        let max_bytes = self.inner.config.max_message_bytes;
        loop {
            let msg = {
                let Some((srv, _)) = bound_server.as_mut() else {
                    return Err(PoolError::server_closed());
                };
                match read_message(srv, max_bytes) {
                    Ok(m) => m,
                    Err(err) => {
                        if let Some((dead, _)) = bound_server.take() {
                            self.discard_server(dead);
                        }
                        return Err(err);
                    }
                }
            };

            if msg.tag == b'S'
                && let Some((k, v)) = parse_parameter_status(&msg.body)
            {
                let mut guard = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !guard.cached_params.iter().any(|(ek, _)| ek == &k) {
                    guard.cached_params.push((k, v));
                }
            }

            if msg.tag == b'Z' {
                // Return an idle server to the pool before forwarding ReadyForQuery, so a
                // client that has observed 'I' always sees the server counted as idle.
                let status = msg.body.first().copied().unwrap_or(b'I');
                if let Some((_, st)) = bound_server.as_mut() {
                    *st = status;
                }
                if status == b'I'
                    && let Some((srv, _)) = bound_server.take()
                {
                    self.release_server(key, srv);
                }
                write_message(client, &msg).map_err(|e| PoolError::io(&e))?;
                client.flush().map_err(|e| PoolError::io(&e))?;
                return Ok(());
            }

            write_message(client, &msg).map_err(|e| PoolError::io(&e))?;

            if flush_only && matches!(msg.tag, b'1' | b'2' | b'n' | b's' | b'C' | b'E' | b'I') {
                client.flush().map_err(|e| PoolError::io(&e))?;
                return Ok(());
            }
        }
    }

    fn acquire_server(&self, key: &PoolKey) -> Result<Box<dyn ServerStream>, PoolError> {
        let deadline = Instant::now() + self.inner.config.pool_timeout;
        let mut guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        loop {
            if let Some(queue) = guard.idle.get_mut(key)
                && let Some(server) = queue.pop_front()
            {
                guard.idle_count -= 1;
                guard.active_count += 1;
                return Ok(server);
            }

            if guard.idle_count + guard.active_count < self.inner.config.pool_size {
                guard.active_count += 1;
                drop(guard);
                return self.connect_new_server(key);
            }

            if guard.idle_count > 0 {
                let victim_key = guard
                    .idle
                    .iter()
                    .find_map(|(k, q)| if q.is_empty() { None } else { Some(k.clone()) });
                if let Some(vk) = victim_key
                    && let Some(queue) = guard.idle.get_mut(&vk)
                    && let Some(mut old_srv) = queue.pop_front()
                {
                    guard.idle_count -= 1;
                    guard.active_count += 1;
                    drop(guard);
                    let _ = write_message(
                        &mut *old_srv,
                        &Message {
                            tag: b'X',
                            body: Vec::new(),
                        },
                    );
                    drop(old_srv);
                    return self.connect_new_server(key);
                }
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(PoolError::pool_timeout());
            }
            let remaining = deadline.saturating_duration_since(now);
            guard.waiting += 1;
            let (new_guard, timeout_res) = self
                .inner
                .cvar
                .wait_timeout(guard, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = new_guard;
            guard.waiting = guard.waiting.saturating_sub(1);
            if timeout_res.timed_out()
                && guard.idle.get(key).is_none_or(VecDeque::is_empty)
                && guard.idle_count + guard.active_count >= self.inner.config.pool_size
                && guard.idle_count == 0
            {
                return Err(PoolError::pool_timeout());
            }
        }
    }

    fn connect_new_server(&self, key: &PoolKey) -> Result<Box<dyn ServerStream>, PoolError> {
        self.inner.connects.fetch_add(1, Ordering::SeqCst);
        match self.inner.connector.connect(&key.user, &key.database) {
            Ok(srv) => {
                let init_params = srv.initial_parameters();
                if !init_params.is_empty() {
                    let mut guard = self
                        .inner
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if guard.cached_params.is_empty() {
                        guard.cached_params = init_params;
                    }
                }
                Ok(srv)
            }
            Err(io_err) => {
                let mut guard = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.active_count = guard.active_count.saturating_sub(1);
                self.inner.cvar.notify_one();
                drop(guard);

                if let Some(inner_box) = io_err.get_ref()
                    && let Some(pool_err) = inner_box.downcast_ref::<PoolError>()
                {
                    return Err(pool_err.clone());
                }
                Err(PoolError::io(&io_err))
            }
        }
    }

    fn release_server(&self, key: &PoolKey, server: Box<dyn ServerStream>) {
        let mut guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.active_count = guard.active_count.saturating_sub(1);
        guard.idle.entry(key.clone()).or_default().push_back(server);
        guard.idle_count += 1;
        self.inner.cvar.notify_one();
    }

    fn discard_server(&self, mut server: Box<dyn ServerStream>) {
        let _ = write_message(
            &mut *server,
            &Message {
                tag: b'X',
                body: Vec::new(),
            },
        );
        drop(server);
        let mut guard = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.active_count = guard.active_count.saturating_sub(1);
        self.inner.cvar.notify_one();
    }
}

fn is_named_parse(body: &[u8]) -> bool {
    !body.is_empty() && body[0] != 0
}

fn send_error_and_flush(
    client: &mut dyn ClientStream,
    sqlstate: &str,
    message: &str,
) -> Result<(), PoolError> {
    let err_msg = error_response(sqlstate, message);
    write_message(client, &err_msg).map_err(|e| PoolError::io(&e))?;
    client.flush().map_err(|e| PoolError::io(&e))
}

fn send_error_and_ready(
    client: &mut dyn ClientStream,
    sqlstate: &str,
    message: &str,
    status: u8,
) -> Result<(), PoolError> {
    let err_msg = error_response(sqlstate, message);
    write_message(client, &err_msg).map_err(|e| PoolError::io(&e))?;
    write_message(client, &ready_for_query(status)).map_err(|e| PoolError::io(&e))?;
    client.flush().map_err(|e| PoolError::io(&e))
}
