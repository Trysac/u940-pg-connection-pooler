//! Integration and property tests for `pg_connection_pooler`.

use std::collections::VecDeque;
use std::io::{self, Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use proptest::prelude::*;

use pg_connection_pooler::cli;
use pg_connection_pooler::codec::{
    CANCEL_REQUEST_CODE, GSS_REQUEST_CODE, Message, PROTOCOL_V3, SSL_REQUEST_CODE, Startup,
    error_response, read_message, read_startup, ready_for_query, write_message, write_startup,
};
use pg_connection_pooler::connector::handshake_server;
use pg_connection_pooler::md5::{md5_bytes, md5_hex};
use pg_connection_pooler::{
    ClientStream, Config, Connector, ErrorKind, PoolError, Pooler, ServerStream, TcpConnector,
    md5_password,
};

// --- In-memory bidirectional pipe for deterministic client & server streams ---

type ChannelState = Arc<(Mutex<(VecDeque<u8>, bool)>, Condvar)>;

#[derive(Clone)]
struct MemChannel {
    inner: ChannelState,
}

impl MemChannel {
    fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new((VecDeque::new(), false)), Condvar::new())),
        }
    }

    fn close(&self) {
        let (lock, cvar) = &*self.inner;
        let mut guard = lock.lock().unwrap();
        guard.1 = true;
        cvar.notify_all();
    }
}

impl Read for MemChannel {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let (lock, cvar) = &*self.inner;
        let mut guard = lock.lock().unwrap();
        while guard.0.is_empty() && !guard.1 {
            guard = cvar.wait(guard).unwrap();
        }
        if guard.0.is_empty() && guard.1 {
            return Ok(0);
        }
        let n = buf.len().min(guard.0.len());
        for b in buf.iter_mut().take(n) {
            *b = guard.0.pop_front().unwrap();
        }
        Ok(n)
    }
}

impl Write for MemChannel {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (lock, cvar) = &*self.inner;
        let mut guard = lock.lock().unwrap();
        if guard.1 {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"));
        }
        guard.0.extend(buf);
        cvar.notify_all();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct DuplexStream {
    rx: MemChannel,
    tx: MemChannel,
}

impl DuplexStream {
    fn pair() -> (Self, Self) {
        let c1 = MemChannel::new();
        let c2 = MemChannel::new();
        (
            Self {
                rx: c1.clone(),
                tx: c2.clone(),
            },
            Self { rx: c2, tx: c1 },
        )
    }
}

impl Read for DuplexStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.rx.read(buf)
    }
}

impl Write for DuplexStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tx.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.tx.flush()
    }
}

impl Drop for DuplexStream {
    fn drop(&mut self) {
        self.tx.close();
        self.rx.close();
    }
}

impl ServerStream for DuplexStream {}

// --- Scripted fake backend server ---

type ScriptFn = Arc<dyn Fn(&Message) -> Vec<Message> + Send + Sync>;

struct ScriptedServerStream {
    rx: Cursor<Vec<u8>>,
    tx_buf: Vec<u8>,
    script: ScriptFn,
    params: Vec<(String, String)>,
}

impl ScriptedServerStream {
    fn new(script: ScriptFn, params: Vec<(String, String)>) -> Self {
        Self {
            rx: Cursor::new(Vec::new()),
            tx_buf: Vec::new(),
            script,
            params,
        }
    }

    #[allow(clippy::cast_sign_loss)]
    fn process_pending(&mut self) {
        while self.tx_buf.len() >= 5 {
            let tag = self.tx_buf[0];
            let len = i32::from_be_bytes([
                self.tx_buf[1],
                self.tx_buf[2],
                self.tx_buf[3],
                self.tx_buf[4],
            ]) as usize;
            if self.tx_buf.len() < 1 + len {
                break;
            }
            let body = self.tx_buf[5..=len].to_vec();
            self.tx_buf.drain(..=len);
            if tag == b'X' {
                continue;
            }
            let msg = Message { tag, body };
            let replies = (self.script)(&msg);
            let pos = usize::try_from(self.rx.position()).unwrap_or(0);
            let existing = self.rx.get_ref()[pos..].to_vec();
            let mut new_rx = existing;
            for r in replies {
                write_message(&mut new_rx, &r).unwrap();
            }
            self.rx = Cursor::new(new_rx);
        }
    }
}

impl Read for ScriptedServerStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.rx.read(buf)
    }
}

impl Write for ScriptedServerStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tx_buf.extend_from_slice(buf);
        self.process_pending();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.process_pending();
        Ok(())
    }
}

impl ServerStream for ScriptedServerStream {
    fn initial_parameters(&self) -> Vec<(String, String)> {
        self.params.clone()
    }
}

struct ScriptedConnector {
    connects: AtomicUsize,
    script: ScriptFn,
    fail_with: Option<PoolError>,
}

impl ScriptedConnector {
    fn new(script: impl Fn(&Message) -> Vec<Message> + Send + Sync + 'static) -> Self {
        Self {
            connects: AtomicUsize::new(0),
            script: Arc::new(script),
            fail_with: None,
        }
    }

    fn failing(err: PoolError) -> Self {
        Self {
            connects: AtomicUsize::new(0),
            script: Arc::new(|_| Vec::new()),
            fail_with: Some(err),
        }
    }

    fn default_tx_aware() -> Self {
        Self::new(|msg: &Message| match msg.tag {
            b'Q' => {
                let q = String::from_utf8_lossy(&msg.body);
                let q_trimmed = q.trim_end_matches('\0').trim().to_uppercase();
                let status = if q_trimmed == "BEGIN" {
                    b'T'
                } else if q_trimmed == "COMMIT" || q_trimmed == "ROLLBACK" {
                    b'I'
                } else if q_trimmed == "FAIL_IN_TX" {
                    b'E'
                } else {
                    b'I'
                };
                let mut cmd_body = b"OK\0".to_vec();
                if q_trimmed == "FAIL_IN_TX" {
                    return vec![
                        error_response("42601", "syntax error"),
                        ready_for_query(status),
                    ];
                }
                let cmd = Message {
                    tag: b'C',
                    body: std::mem::take(&mut cmd_body),
                };
                vec![cmd, ready_for_query(status)]
            }
            b'P' => vec![Message {
                tag: b'1',
                body: Vec::new(),
            }],
            b'B' => vec![Message {
                tag: b'2',
                body: Vec::new(),
            }],
            b'D' => vec![Message {
                tag: b'n',
                body: Vec::new(),
            }],
            b'E' => vec![Message {
                tag: b'C',
                body: b"SELECT 1\0".to_vec(),
            }],
            b'H' => Vec::new(),
            _ => vec![ready_for_query(b'I')],
        })
    }
}

impl Connector for ScriptedConnector {
    fn connect(&self, _user: &str, _database: &str) -> io::Result<Box<dyn ServerStream>> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        if let Some(err) = &self.fail_with {
            return Err(io::Error::other(err.clone()));
        }
        Ok(Box::new(ScriptedServerStream::new(
            Arc::clone(&self.script),
            vec![("server_version".to_string(), "17.0".to_string())],
        )))
    }
}

fn make_query(sql: &str) -> Message {
    let mut body = Vec::with_capacity(sql.len() + 1);
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    Message { tag: b'Q', body }
}

fn extract_error_fields(msg: &Message) -> (String, String) {
    assert_eq!(msg.tag, b'E');
    let mut code = String::new();
    let mut text = String::new();
    let mut idx = 0;
    while idx < msg.body.len() {
        let field_type = msg.body[idx];
        idx += 1;
        if field_type == 0 {
            break;
        }
        let end = msg.body[idx..]
            .iter()
            .position(|&b| b == 0)
            .map_or(msg.body.len(), |p| idx + p);
        let val = String::from_utf8_lossy(&msg.body[idx..end]).to_string();
        if field_type == b'C' {
            code = val;
        } else if field_type == b'M' {
            text = val;
        }
        idx = end + 1;
    }
    (code, text)
}

fn client_handshake_trust(stream: &mut DuplexStream, user: &str, database: &str) {
    write_startup(stream, user, database).unwrap();
    stream.flush().unwrap();
    loop {
        let m = read_message(stream, 16_777_216).unwrap();
        if m.tag == b'Z' {
            assert_eq!(m.body, vec![b'I']);
            break;
        }
    }
}

// --- Tests ---

#[test]
fn rfc1321_abc_vector_and_pg_md5_password() {
    assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
    assert_eq!(md5_bytes(b"").len(), 16);
    assert_eq!(
        md5_password("bench", "pw", [1, 2, 3, 4]),
        "md5aac0038df22f496d2ca86cd9867ec66c"
    );
}

#[test]
fn ssl_request_then_startup_and_oversized_frame_rejected() {
    let connector = Arc::new(ScriptedConnector::default_tx_aware());
    let config = Config {
        max_message_bytes: 16_777_216,
        users: Vec::new(),
        ..Config::default()
    };
    let pooler = Pooler::new(config, connector);

    let (mut client_end, pooler_end) = DuplexStream::pair();
    let p = pooler.clone();
    let handle = thread::spawn(move || p.handle_client(Box::new(pooler_end)));

    // Send SSLRequest bytes [0, 0, 0, 8, 4, 210, 22, 47]
    let ssl_request: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];
    client_end.write_all(&ssl_request).unwrap();
    client_end.flush().unwrap();

    let mut ssl_reply = [0u8; 1];
    client_end.read_exact(&mut ssl_reply).unwrap();
    assert_eq!(ssl_reply[0], b'N');

    // Now send StartupMessage with user "bench" and database "app"
    write_startup(&mut client_end, "bench", "app").unwrap();
    client_end.flush().unwrap();

    loop {
        let m = read_message(&mut client_end, 16_777_216).unwrap();
        if m.tag == b'Z' {
            assert_eq!(m.body, vec![b'I']);
            break;
        }
    }

    // Send a frame with length 16_777_217 (> max_message_bytes 16_777_216)
    let mut oversized_header = [0u8; 5];
    oversized_header[0] = b'Q';
    oversized_header[1..5].copy_from_slice(&16_777_217i32.to_be_bytes());
    client_end.write_all(&oversized_header).unwrap();
    client_end.flush().unwrap();

    let err_msg = read_message(&mut client_end, 16_777_216).unwrap();
    let (sqlstate, _) = extract_error_fields(&err_msg);
    assert_eq!(sqlstate, "08P01");

    let res = handle.join().unwrap();
    let err = res.unwrap_err();
    assert_eq!(err.code(), "message_too_large");
}

#[test]
fn transaction_binding_and_reuse_across_clients() {
    let connector = Arc::new(ScriptedConnector::default_tx_aware());
    let config = Config {
        pool_size: 1,
        pool_timeout: Duration::from_secs(2),
        ..Config::default()
    };
    let pooler = Pooler::new(config, Arc::clone(&connector) as Arc<dyn Connector>);

    let (mut client_a, pooler_a) = DuplexStream::pair();
    let (mut client_b, pooler_b) = DuplexStream::pair();

    let pa = pooler.clone();
    let ta = thread::spawn(move || pa.handle_client(Box::new(pooler_a)));
    let pb = pooler.clone();
    let tb = thread::spawn(move || pb.handle_client(Box::new(pooler_b)));

    client_handshake_trust(&mut client_a, "bench", "app");
    client_handshake_trust(&mut client_b, "bench", "app");

    // Client A sends "BEGIN" -> server answers ReadyForQuery 'T'
    write_message(&mut client_a, &make_query("BEGIN")).unwrap();
    let msg1 = read_message(&mut client_a, 16_777_216).unwrap();
    assert_eq!(msg1.tag, b'C');
    let rfq1 = read_message(&mut client_a, 16_777_216).unwrap();
    assert_eq!(rfq1, ready_for_query(b'T'));

    // Client B sends a query on a background thread; it must wait while A holds the transaction
    let b_done = Arc::new(AtomicUsize::new(0));
    let b_done_clone = Arc::clone(&b_done);
    let b_worker = thread::spawn(move || {
        write_message(&mut client_b, &make_query("SELECT 1")).unwrap();
        let m1 = read_message(&mut client_b, 16_777_216).unwrap();
        assert_eq!(m1.tag, b'C');
        let m2 = read_message(&mut client_b, 16_777_216).unwrap();
        assert_eq!(m2, ready_for_query(b'I'));
        b_done_clone.store(1, Ordering::SeqCst);
        write_message(
            &mut client_b,
            &Message {
                tag: b'X',
                body: Vec::new(),
            },
        )
        .unwrap();
    });

    // Wait until B is observed waiting in pooler stats
    let start = Instant::now();
    while pooler.stats().waiting == 0 && start.elapsed() < Duration::from_millis(500) {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(b_done.load(Ordering::SeqCst), 0);

    // Client A sends "COMMIT" -> server answers 'I', releasing connection to B
    write_message(&mut client_a, &make_query("COMMIT")).unwrap();
    let msg2 = read_message(&mut client_a, 16_777_216).unwrap();
    assert_eq!(msg2.tag, b'C');
    let rfq2 = read_message(&mut client_a, 16_777_216).unwrap();
    assert_eq!(rfq2, ready_for_query(b'I'));

    b_worker.join().unwrap();
    assert_eq!(b_done.load(Ordering::SeqCst), 1);
    assert_eq!(connector.connects.load(Ordering::SeqCst), 1);
    assert_eq!(pooler.stats().connects, 1);

    write_message(
        &mut client_a,
        &Message {
            tag: b'X',
            body: Vec::new(),
        },
    )
    .unwrap();
    ta.join().unwrap().unwrap();
    tb.join().unwrap().unwrap();
}

#[test]
fn pool_timeout_50ms_and_max_clients_100() {
    let connector = Arc::new(ScriptedConnector::default_tx_aware());
    let config = Config {
        pool_size: 1,
        max_clients: 100,
        pool_timeout: Duration::from_millis(50),
        ..Config::default()
    };
    let pooler = Pooler::new(config, Arc::clone(&connector) as Arc<dyn Connector>);

    let (mut client_a, pooler_a) = DuplexStream::pair();
    let (mut client_b, pooler_b) = DuplexStream::pair();

    let pa = pooler.clone();
    let ta = thread::spawn(move || pa.handle_client(Box::new(pooler_a)));
    let pb = pooler.clone();
    let tb = thread::spawn(move || pb.handle_client(Box::new(pooler_b)));

    client_handshake_trust(&mut client_a, "bench", "app");
    client_handshake_trust(&mut client_b, "bench", "app");

    // A holds a transaction
    write_message(&mut client_a, &make_query("BEGIN")).unwrap();
    let _ = read_message(&mut client_a, 16_777_216).unwrap();
    let rfq_a = read_message(&mut client_a, 16_777_216).unwrap();
    assert_eq!(rfq_a, ready_for_query(b'T'));

    // B waits longer than 50 ms and gets 53300 "pool timeout" within 1 s
    let t0 = Instant::now();
    write_message(&mut client_b, &make_query("SELECT 1")).unwrap();
    let err_b = read_message(&mut client_b, 16_777_216).unwrap();
    let rfq_b = read_message(&mut client_b, 16_777_216).unwrap();
    assert!(t0.elapsed() < Duration::from_secs(1));

    let (code, text) = extract_error_fields(&err_b);
    assert_eq!(code, "53300");
    assert_eq!(text, "pool timeout");
    assert_eq!(rfq_b, ready_for_query(b'I'));

    // A commits, and B's session stays usable!
    write_message(&mut client_a, &make_query("COMMIT")).unwrap();
    let _ = read_message(&mut client_a, 16_777_216).unwrap();
    let _ = read_message(&mut client_a, 16_777_216).unwrap();

    write_message(&mut client_b, &make_query("SELECT 1")).unwrap();
    let ok_b = read_message(&mut client_b, 16_777_216).unwrap();
    assert_eq!(ok_b.tag, b'C');
    let rfq_b2 = read_message(&mut client_b, 16_777_216).unwrap();
    assert_eq!(rfq_b2, ready_for_query(b'I'));

    // Now open 98 more clients so total active clients == 100
    let mut extra_clients = Vec::with_capacity(98);
    let mut extra_threads = Vec::with_capacity(98);
    for _ in 0..98 {
        let (mut c, s) = DuplexStream::pair();
        let p = pooler.clone();
        extra_threads.push(thread::spawn(move || p.handle_client(Box::new(s))));
        client_handshake_trust(&mut c, "bench", "app");
        extra_clients.push(c);
    }
    assert_eq!(pooler.stats().clients, 100);

    // 101st client receives SQLSTATE "53300" and message "too many clients" and is closed
    let (mut c101, s101) = DuplexStream::pair();
    let p101 = pooler.clone();
    let t101 = thread::spawn(move || p101.handle_client(Box::new(s101)));
    let err101 = read_message(&mut c101, 16_777_216).unwrap();
    let (code101, text101) = extract_error_fields(&err101);
    assert_eq!(code101, "53300");
    assert_eq!(text101, "too many clients");
    let res101 = t101.join().unwrap();
    assert_eq!(res101.unwrap_err().code(), "too_many_clients");

    // Clean up all 100 clients
    for mut c in extra_clients {
        let _ = write_message(
            &mut c,
            &Message {
                tag: b'X',
                body: Vec::new(),
            },
        );
    }
    for t in extra_threads {
        t.join().unwrap().unwrap();
    }
    write_message(
        &mut client_a,
        &Message {
            tag: b'X',
            body: Vec::new(),
        },
    )
    .unwrap();
    write_message(
        &mut client_b,
        &Message {
            tag: b'X',
            body: Vec::new(),
        },
    )
    .unwrap();
    ta.join().unwrap().unwrap();
    tb.join().unwrap().unwrap();
}

#[test]
fn dirty_disconnect_discards_server_and_failed_tx_stays_bound_until_rollback() {
    let connector = Arc::new(ScriptedConnector::default_tx_aware());
    let config = Config {
        pool_size: 1,
        ..Config::default()
    };
    let pooler = Pooler::new(config, Arc::clone(&connector) as Arc<dyn Connector>);

    // Client 1 starts a transaction ('T') and disconnects without COMMIT/ROLLBACK
    let (mut c1, s1) = DuplexStream::pair();
    let p1 = pooler.clone();
    let t1 = thread::spawn(move || p1.handle_client(Box::new(s1)));
    client_handshake_trust(&mut c1, "bench", "app");

    write_message(&mut c1, &make_query("BEGIN")).unwrap();
    let _ = read_message(&mut c1, 16_777_216).unwrap();
    let rfq1 = read_message(&mut c1, 16_777_216).unwrap();
    assert_eq!(rfq1, ready_for_query(b'T'));
    assert_eq!(connector.connects.load(Ordering::SeqCst), 1);

    // Terminate while in 'T'
    write_message(
        &mut c1,
        &Message {
            tag: b'X',
            body: Vec::new(),
        },
    )
    .unwrap();
    drop(c1);
    t1.join().unwrap().unwrap();
    assert_eq!(pooler.stats().servers_idle, 0);

    // Client 2 gets a fresh server connection (connect count 2)
    let (mut c2, s2) = DuplexStream::pair();
    let p2 = pooler.clone();
    let t2 = thread::spawn(move || p2.handle_client(Box::new(s2)));
    client_handshake_trust(&mut c2, "bench", "app");

    // Trigger 'E' (failed transaction state)
    write_message(&mut c2, &make_query("FAIL_IN_TX")).unwrap();
    let err_in_tx = read_message(&mut c2, 16_777_216).unwrap();
    assert_eq!(err_in_tx.tag, b'E');
    let rfq_e = read_message(&mut c2, 16_777_216).unwrap();
    assert_eq!(rfq_e, ready_for_query(b'E'));
    assert_eq!(connector.connects.load(Ordering::SeqCst), 2);
    assert_eq!(pooler.stats().servers_active, 1);
    assert_eq!(pooler.stats().servers_idle, 0);

    // Send "ROLLBACK" -> returns 'I' and releases connection back to idle pool
    write_message(&mut c2, &make_query("ROLLBACK")).unwrap();
    let ok_rb = read_message(&mut c2, 16_777_216).unwrap();
    assert_eq!(ok_rb.tag, b'C');
    let rfq_i = read_message(&mut c2, 16_777_216).unwrap();
    assert_eq!(rfq_i, ready_for_query(b'I'));
    assert_eq!(pooler.stats().servers_idle, 1);
    assert_eq!(pooler.stats().servers_active, 0);

    write_message(
        &mut c2,
        &Message {
            tag: b'X',
            body: Vec::new(),
        },
    )
    .unwrap();
    t2.join().unwrap().unwrap();
    assert_eq!(pooler.stats().servers_idle, 1);
    assert_eq!(pooler.stats().servers_active, 0);
}

#[test]
fn copy_from_stdin_relays_client_data_until_copy_done_or_fail() {
    // Scripted server: "COPY" answers CopyInResponse, CopyData is recorded,
    // CopyDone completes the command and CopyFail reports an error.
    let received = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let rec = Arc::clone(&received);
    let connector = Arc::new(ScriptedConnector::new(move |msg: &Message| match msg.tag {
        b'Q' => vec![Message {
            tag: b'G',
            body: vec![0, 0, 0],
        }],
        b'd' => {
            rec.lock().unwrap().push(msg.body.clone());
            Vec::new()
        }
        b'c' => vec![
            Message {
                tag: b'C',
                body: b"COPY 2\0".to_vec(),
            },
            ready_for_query(b'I'),
        ],
        b'f' => vec![
            error_response("57014", "COPY from stdin failed"),
            ready_for_query(b'I'),
        ],
        _ => Vec::new(),
    }));
    let pooler = Pooler::new(
        Config {
            pool_size: 1,
            ..Config::default()
        },
        Arc::clone(&connector) as Arc<dyn Connector>,
    );

    let (mut client, server_end) = DuplexStream::pair();
    let p = pooler.clone();
    let handle = thread::spawn(move || p.handle_client(Box::new(server_end)));
    client_handshake_trust(&mut client, "bench", "app");

    // COPY ... FROM STDIN finished with CopyDone
    write_message(&mut client, &make_query("COPY t FROM STDIN")).unwrap();
    assert_eq!(read_message(&mut client, 16_777_216).unwrap().tag, b'G');
    for row in [b"1\ta\n".to_vec(), b"2\tb\n".to_vec()] {
        write_message(
            &mut client,
            &Message {
                tag: b'd',
                body: row,
            },
        )
        .unwrap();
    }
    write_message(
        &mut client,
        &Message {
            tag: b'c',
            body: Vec::new(),
        },
    )
    .unwrap();
    let done = read_message(&mut client, 16_777_216).unwrap();
    assert_eq!(done.tag, b'C');
    assert_eq!(done.body, b"COPY 2\0".to_vec());
    assert_eq!(
        read_message(&mut client, 16_777_216).unwrap(),
        ready_for_query(b'I')
    );
    assert_eq!(
        *received.lock().unwrap(),
        vec![b"1\ta\n".to_vec(), b"2\tb\n".to_vec()]
    );
    assert_eq!(pooler.stats().servers_idle, 1);

    // COPY ... FROM STDIN aborted with CopyFail reuses the same server
    write_message(&mut client, &make_query("COPY t FROM STDIN")).unwrap();
    assert_eq!(read_message(&mut client, 16_777_216).unwrap().tag, b'G');
    write_message(
        &mut client,
        &Message {
            tag: b'f',
            body: b"aborted\0".to_vec(),
        },
    )
    .unwrap();
    let err = read_message(&mut client, 16_777_216).unwrap();
    assert_eq!(extract_error_fields(&err).0, "57014");
    assert_eq!(
        read_message(&mut client, 16_777_216).unwrap(),
        ready_for_query(b'I')
    );
    assert_eq!(connector.connects.load(Ordering::SeqCst), 1);
    assert_eq!(pooler.stats().servers_idle, 1);

    write_message(
        &mut client,
        &Message {
            tag: b'X',
            body: Vec::new(),
        },
    )
    .unwrap();
    handle.join().unwrap().unwrap();
}

#[test]
fn copy_from_stdin_passes_flush_and_sync_and_discards_server_on_unexpected_message() {
    // Scripted server records every message it receives after CopyInResponse.
    let received = Arc::new(Mutex::new(Vec::<u8>::new()));
    let rec = Arc::clone(&received);
    let connector = Arc::new(ScriptedConnector::new(move |msg: &Message| {
        if msg.tag == b'Q' {
            return vec![Message {
                tag: b'G',
                body: vec![0, 0, 0],
            }];
        }
        rec.lock().unwrap().push(msg.tag);
        Vec::new()
    }));
    let pooler = Pooler::new(
        Config {
            pool_size: 1,
            ..Config::default()
        },
        Arc::clone(&connector) as Arc<dyn Connector>,
    );

    let (mut client, server_end) = DuplexStream::pair();
    let p = pooler.clone();
    let handle = thread::spawn(move || p.handle_client(Box::new(server_end)));
    client_handshake_trust(&mut client, "bench", "app");

    write_message(&mut client, &make_query("COPY t FROM STDIN")).unwrap();
    assert_eq!(read_message(&mut client, 16_777_216).unwrap().tag, b'G');
    for tag in *b"dHS" {
        write_message(
            &mut client,
            &Message {
                tag,
                body: Vec::new(),
            },
        )
        .unwrap();
    }
    // A simple Query is not allowed while COPY is in progress.
    write_message(&mut client, &make_query("SELECT 1")).unwrap();
    let err = read_message(&mut client, 16_777_216).unwrap();
    let (code, text) = extract_error_fields(&err);
    assert_eq!(code, "08P01");
    assert_eq!(text, "unexpected message during COPY");

    let res = handle.join().unwrap();
    assert_eq!(res.unwrap_err().code(), "protocol_violation");
    assert_eq!(*received.lock().unwrap(), b"dHS".to_vec());
    // The server was left mid-COPY, so it is discarded instead of returned to the pool.
    assert_eq!(pooler.stats().servers_idle, 0);
    assert_eq!(pooler.stats().servers_active, 0);
}

#[test]
fn full_pool_evicts_idle_server_of_another_user_database_pair() {
    let connector = Arc::new(ScriptedConnector::default_tx_aware());
    let pooler = Pooler::new(
        Config {
            pool_size: 1,
            ..Config::default()
        },
        Arc::clone(&connector) as Arc<dyn Connector>,
    );

    // Client for ("bench", "app") runs a query; its server goes back to the idle pool.
    let (mut c1, s1) = DuplexStream::pair();
    let p1 = pooler.clone();
    let t1 = thread::spawn(move || p1.handle_client(Box::new(s1)));
    client_handshake_trust(&mut c1, "bench", "app");
    write_message(&mut c1, &make_query("SELECT 1")).unwrap();
    assert_eq!(read_message(&mut c1, 16_777_216).unwrap().tag, b'C');
    assert_eq!(
        read_message(&mut c1, 16_777_216).unwrap(),
        ready_for_query(b'I')
    );
    assert_eq!(pooler.stats().servers_idle, 1);

    // Client for ("bench", "other") cannot reuse it, so the idle server is closed and replaced.
    let (mut c2, s2) = DuplexStream::pair();
    let p2 = pooler.clone();
    let t2 = thread::spawn(move || p2.handle_client(Box::new(s2)));
    client_handshake_trust(&mut c2, "bench", "other");
    write_message(&mut c2, &make_query("SELECT 1")).unwrap();
    assert_eq!(read_message(&mut c2, 16_777_216).unwrap().tag, b'C');
    assert_eq!(
        read_message(&mut c2, 16_777_216).unwrap(),
        ready_for_query(b'I')
    );
    assert_eq!(connector.connects.load(Ordering::SeqCst), 2);
    assert_eq!(pooler.stats().servers_idle, 1);
    assert_eq!(pooler.stats().servers_active, 0);

    for (mut c, t) in [(c1, t1), (c2, t2)] {
        write_message(
            &mut c,
            &Message {
                tag: b'X',
                body: Vec::new(),
            },
        )
        .unwrap();
        t.join().unwrap().unwrap();
    }
}

#[test]
fn config_debug_reports_user_count_without_passwords() {
    let config = Config {
        pool_size: 7,
        users: vec![("bench".to_string(), "s3cret-pw".to_string())],
        ..Config::default()
    };
    let shown = format!("{config:?}");
    assert!(shown.contains("pool_size: 7"), "{shown}");
    assert!(shown.contains("users_count: 1"), "{shown}");
    assert!(!shown.contains("s3cret-pw"), "{shown}");
}

#[test]
#[allow(clippy::too_many_lines)]
fn md5_auth_wrong_password_and_sasl_unsupported_and_named_parse_refused() {
    // 1. Client with wrong md5 password receives SQLSTATE "28P01" and is closed.
    //    Client with right md5 password succeeds, then named Parse returns "0A000".
    let connector = Arc::new(ScriptedConnector::default_tx_aware());
    let config = Config {
        users: vec![("bench".to_string(), "pw".to_string())],
        salt_source: Box::new(|| [1, 2, 3, 4]),
        ..Config::default()
    };
    let pooler = Pooler::new(config, connector);

    // Wrong password attempt
    {
        let (mut c_bad, s_bad) = DuplexStream::pair();
        let p = pooler.clone();
        let t_bad = thread::spawn(move || p.handle_client(Box::new(s_bad)));

        write_startup(&mut c_bad, "bench", "app").unwrap();
        let challenge = read_message(&mut c_bad, 16_777_216).unwrap();
        assert_eq!(challenge.tag, b'R');
        assert_eq!(&challenge.body[4..8], &[1, 2, 3, 4]);

        let wrong = md5_password("bench", "wrong_pw", [1, 2, 3, 4]);
        let mut pw_body = wrong.into_bytes();
        pw_body.push(0);
        write_message(
            &mut c_bad,
            &Message {
                tag: b'p',
                body: pw_body,
            },
        )
        .unwrap();

        let err_msg = read_message(&mut c_bad, 16_777_216).unwrap();
        let (sqlstate, _) = extract_error_fields(&err_msg);
        assert_eq!(sqlstate, "28P01");
        let res = t_bad.join().unwrap();
        assert_eq!(res.unwrap_err().code(), "auth_failed");
    }

    // Correct password + named Parse -> "0A000" + unnamed Parse works
    {
        let (mut c_ok, s_ok) = DuplexStream::pair();
        let p = pooler.clone();
        let t_ok = thread::spawn(move || p.handle_client(Box::new(s_ok)));

        write_startup(&mut c_ok, "bench", "app").unwrap();
        let challenge = read_message(&mut c_ok, 16_777_216).unwrap();
        assert_eq!(challenge.tag, b'R');

        let digest = md5_password("bench", "pw", [1, 2, 3, 4]);
        let mut pw_body = digest.into_bytes();
        pw_body.push(0);
        write_message(
            &mut c_ok,
            &Message {
                tag: b'p',
                body: pw_body,
            },
        )
        .unwrap();

        loop {
            let m = read_message(&mut c_ok, 16_777_216).unwrap();
            if m.tag == b'Z' {
                break;
            }
        }

        // Named Parse (statement name "stmt1\0SELECT 1\0\0\0") -> refused with 0A000
        let named_parse = Message {
            tag: b'P',
            body: b"stmt1\0SELECT 1\0\0\0".to_vec(),
        };
        write_message(&mut c_ok, &named_parse).unwrap();
        let err_parse = read_message(&mut c_ok, 16_777_216).unwrap();
        let (code, _) = extract_error_fields(&err_parse);
        assert_eq!(code, "0A000");
        let rfq = read_message(&mut c_ok, 16_777_216).unwrap();
        assert_eq!(rfq, ready_for_query(b'I'));

        // Unnamed Parse ("\0SELECT 1\0\0\0") + Bind + Execute + Sync works
        write_message(
            &mut c_ok,
            &Message {
                tag: b'P',
                body: b"\0SELECT 1\0\0\0".to_vec(),
            },
        )
        .unwrap();
        write_message(
            &mut c_ok,
            &Message {
                tag: b'B',
                body: b"\0\0\0\0\0\0\0\0".to_vec(),
            },
        )
        .unwrap();
        write_message(
            &mut c_ok,
            &Message {
                tag: b'E',
                body: b"\0\0\0\0\0".to_vec(),
            },
        )
        .unwrap();
        write_message(
            &mut c_ok,
            &Message {
                tag: b'S',
                body: Vec::new(),
            },
        )
        .unwrap();

        assert_eq!(read_message(&mut c_ok, 16_777_216).unwrap().tag, b'1');
        assert_eq!(read_message(&mut c_ok, 16_777_216).unwrap().tag, b'2');
        assert_eq!(read_message(&mut c_ok, 16_777_216).unwrap().tag, b'C');
        assert_eq!(
            read_message(&mut c_ok, 16_777_216).unwrap(),
            ready_for_query(b'I')
        );

        write_message(
            &mut c_ok,
            &Message {
                tag: b'X',
                body: Vec::new(),
            },
        )
        .unwrap();
        t_ok.join().unwrap().unwrap();
    }

    // 2. Server requesting SASL (authentication code 10) -> client gets 28000 & PoolError "unsupported_auth"
    {
        let sasl_connector = Arc::new(ScriptedConnector::failing(PoolError::unsupported_auth(10)));
        let sasl_pooler = Pooler::new(Config::default(), sasl_connector);
        let (mut c_sasl, s_sasl) = DuplexStream::pair();
        let t_sasl = thread::spawn(move || sasl_pooler.handle_client(Box::new(s_sasl)));

        client_handshake_trust(&mut c_sasl, "bench", "app");
        write_message(&mut c_sasl, &make_query("SELECT 1")).unwrap();

        let err_msg = read_message(&mut c_sasl, 16_777_216).unwrap();
        let (sqlstate, _) = extract_error_fields(&err_msg);
        assert_eq!(sqlstate, "28000");

        let res = t_sasl.join().unwrap();
        let err = res.unwrap_err();
        assert_eq!(err.code(), "unsupported_auth");
    }
}

#[test]
fn tcp_connector_handshakes_ok_cleartext_md5_and_sasl_error() {
    // Test handshake_server against scripted server messages for Cleartext (3), MD5 (5), SASL (10)
    for auth_mode in [0i32, 3, 5, 10] {
        let (mut client_side, mut server_side) = DuplexStream::pair();
        let srv_thread = thread::spawn(move || {
            let st = read_startup(&mut server_side, 16_777_216).unwrap();
            assert!(matches!(st, Startup::Params { .. }));
            match auth_mode {
                0 => {
                    write_message(
                        &mut server_side,
                        &Message {
                            tag: b'R',
                            body: 0i32.to_be_bytes().to_vec(),
                        },
                    )
                    .unwrap();
                }
                3 => {
                    write_message(
                        &mut server_side,
                        &Message {
                            tag: b'R',
                            body: 3i32.to_be_bytes().to_vec(),
                        },
                    )
                    .unwrap();
                    let pw_msg = read_message(&mut server_side, 16_777_216).unwrap();
                    assert_eq!(pw_msg.tag, b'p');
                    assert_eq!(pw_msg.body, b"secret\0");
                    write_message(
                        &mut server_side,
                        &Message {
                            tag: b'R',
                            body: 0i32.to_be_bytes().to_vec(),
                        },
                    )
                    .unwrap();
                }
                5 => {
                    let mut body = 5i32.to_be_bytes().to_vec();
                    body.extend_from_slice(&[9, 8, 7, 6]);
                    write_message(&mut server_side, &Message { tag: b'R', body }).unwrap();
                    let pw_msg = read_message(&mut server_side, 16_777_216).unwrap();
                    assert_eq!(pw_msg.tag, b'p');
                    let expected = md5_password("bench", "secret", [9, 8, 7, 6]);
                    assert_eq!(&pw_msg.body[..expected.len()], expected.as_bytes());
                    write_message(
                        &mut server_side,
                        &Message {
                            tag: b'R',
                            body: 0i32.to_be_bytes().to_vec(),
                        },
                    )
                    .unwrap();
                }
                10 => {
                    let mut body = 10i32.to_be_bytes().to_vec();
                    body.extend_from_slice(b"SCRAM-SHA-256\0\0");
                    write_message(&mut server_side, &Message { tag: b'R', body }).unwrap();
                    return;
                }
                _ => unreachable!(),
            }
            write_message(
                &mut server_side,
                &Message {
                    tag: b'S',
                    body: b"client_encoding\0UTF8\0".to_vec(),
                },
            )
            .unwrap();
            write_message(
                &mut server_side,
                &Message {
                    tag: b'K',
                    body: [0u8; 8].to_vec(),
                },
            )
            .unwrap();
            write_message(&mut server_side, &ready_for_query(b'I')).unwrap();
        });

        let res = handshake_server(&mut client_side, "bench", "app", Some("secret"), 16_777_216);
        if auth_mode == 10 {
            assert_eq!(res.unwrap_err().code(), "unsupported_auth");
        } else {
            let params = res.unwrap();
            assert_eq!(
                params,
                vec![("client_encoding".to_string(), "UTF8".to_string())]
            );
        }
        srv_thread.join().unwrap();
    }
}

#[test]
fn tcp_connector_and_pooler_serve_over_loopback() {
    // Spin up a fake TCP PostgreSQL backend
    let backend_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let backend_addr = backend_listener.local_addr().unwrap().to_string();

    thread::spawn(move || {
        if let Ok((mut s, _)) = backend_listener.accept() {
            let _ = read_startup(&mut s, 16_777_216).unwrap();
            write_message(
                &mut s,
                &Message {
                    tag: b'R',
                    body: 0i32.to_be_bytes().to_vec(),
                },
            )
            .unwrap();
            write_message(
                &mut s,
                &Message {
                    tag: b'S',
                    body: b"TimeZone\0UTC\0".to_vec(),
                },
            )
            .unwrap();
            write_message(&mut s, &ready_for_query(b'I')).unwrap();
            if let Ok(q) = read_message(&mut s, 16_777_216)
                && q.tag == b'Q'
            {
                write_message(
                    &mut s,
                    &Message {
                        tag: b'C',
                        body: b"SELECT 1\0".to_vec(),
                    },
                )
                .unwrap();
                write_message(&mut s, &ready_for_query(b'I')).unwrap();
            }
        }
    });

    let tcp_conn = Arc::new(TcpConnector::new(backend_addr, None));
    let pooler = Pooler::new(
        Config::default(),
        Arc::clone(&tcp_conn) as Arc<dyn Connector>,
    );

    let pool_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let pool_addr = pool_listener.local_addr().unwrap();
    let p_clone = pooler.clone();
    thread::spawn(move || {
        let _ = p_clone.serve(pool_listener);
    });

    let mut client = TcpStream::connect(pool_addr).unwrap();
    write_startup(&mut client, "bench", "app").unwrap();
    loop {
        let m = read_message(&mut client, 16_777_216).unwrap();
        if m.tag == b'Z' {
            break;
        }
    }
    write_message(&mut client, &make_query("SELECT 1")).unwrap();
    let reply = read_message(&mut client, 16_777_216).unwrap();
    assert_eq!(reply.tag, b'C');
    let rfq = read_message(&mut client, 16_777_216).unwrap();
    assert_eq!(rfq, ready_for_query(b'I'));
    assert_eq!(tcp_conn.cached_parameters().len(), 1);
    write_message(
        &mut client,
        &Message {
            tag: b'X',
            body: Vec::new(),
        },
    )
    .unwrap();
}

#[test]
fn codec_edge_cases_and_cancel_and_gss_and_errors() {
    // GSSENCRequest
    let mut gss = Vec::new();
    gss.extend_from_slice(&8i32.to_be_bytes());
    gss.extend_from_slice(&GSS_REQUEST_CODE.to_be_bytes());
    assert_eq!(
        read_startup(&mut Cursor::new(gss), 1024).unwrap(),
        Startup::Gss
    );

    // CancelRequest
    let mut cancel = Vec::new();
    cancel.extend_from_slice(&16i32.to_be_bytes());
    cancel.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    cancel.extend_from_slice(&42i32.to_be_bytes());
    cancel.extend_from_slice(&99i32.to_be_bytes());
    assert_eq!(
        read_startup(&mut Cursor::new(cancel.clone()), 1024).unwrap(),
        Startup::Cancel { pid: 42, key: 99 }
    );

    // Pooler accepts and drops CancelRequest cleanly
    let pooler = Pooler::new(
        Config::default(),
        Arc::new(ScriptedConnector::default_tx_aware()),
    );
    let (mut c, s) = DuplexStream::pair();
    c.write_all(&cancel).unwrap();
    drop(c);
    pooler.handle_client(Box::new(s)).unwrap();

    // Invalid protocol version
    let mut bad_ver = Vec::new();
    bad_ver.extend_from_slice(&8i32.to_be_bytes());
    bad_ver.extend_from_slice(&123_456i32.to_be_bytes());
    let err = read_startup(&mut Cursor::new(bad_ver.clone()), 1024).unwrap_err();
    assert_eq!(err.code(), "protocol_violation");

    // Pooler answers invalid protocol version with 08P01
    let (mut c2, s2) = DuplexStream::pair();
    c2.write_all(&bad_ver).unwrap();
    let r2 = pooler.handle_client(Box::new(s2));
    assert_eq!(r2.unwrap_err().code(), "protocol_violation");
    let em = read_message(&mut c2, 1024).unwrap();
    assert_eq!(extract_error_fields(&em).0, "08P01");

    // Error formatting never leaks secrets and covers all codes
    let errors = [
        (PoolError::io(&io::Error::other("secret")), "io"),
        (PoolError::message_too_large(20, 10), "message_too_large"),
        (PoolError::protocol_violation("bad"), "protocol_violation"),
        (PoolError::auth_failed(), "auth_failed"),
        (PoolError::unsupported_auth(10), "unsupported_auth"),
        (PoolError::pool_timeout(), "pool_timeout"),
        (PoolError::too_many_clients(), "too_many_clients"),
        (PoolError::server_closed(), "server_closed"),
    ];
    for (e, code) in errors {
        assert_eq!(e.code(), code);
        assert!(!e.to_string().contains("secret"));
        assert!(matches!(
            e.kind(),
            ErrorKind::Io(_)
                | ErrorKind::MessageTooLarge { .. }
                | ErrorKind::ProtocolViolation(_)
                | ErrorKind::AuthFailed
                | ErrorKind::UnsupportedAuth(_)
                | ErrorKind::PoolTimeout
                | ErrorKind::TooManyClients
                | ErrorKind::ServerClosed
        ));
    }
}

#[test]
fn cli_parsing_and_execution_paths() {
    let mut out = Vec::new();
    let mut err = Vec::new();

    // --help exits 0
    assert_eq!(cli::run(&["--help".to_string()], &mut out, &mut err), 0);

    // Empty args exits 2
    assert_eq!(cli::run(&[], &mut out, &mut err), 2);

    // Unknown subcommand exits 2
    assert_eq!(cli::run(&["unknown".to_string()], &mut out, &mut err), 2);

    // Valid serve args parsing
    let args = vec![
        "serve".to_string(),
        "--listen".to_string(),
        "127.0.0.1:6432".to_string(),
        "--server".to_string(),
        "127.0.0.1:5432".to_string(),
        "--pool-size".to_string(),
        "10".to_string(),
        "--users".to_string(),
        "users.txt".to_string(),
    ];
    let parsed = cli::parse_args(&args).unwrap();
    assert_eq!(parsed.listen, "127.0.0.1:6432");
    assert_eq!(parsed.server, "127.0.0.1:5432");
    assert_eq!(parsed.pool_size, 10);
    assert_eq!(parsed.users_file.as_deref(), Some("users.txt"));

    // Users content parser
    let users = cli::parse_users_content("# comment\n\nbench:pw\nalice:s3cr3t\n").unwrap();
    assert_eq!(
        users,
        vec![
            ("bench".to_string(), "pw".to_string()),
            ("alice".to_string(), "s3cr3t".to_string())
        ]
    );
    assert!(cli::parse_users_content("bad_line_without_colon").is_err());
    assert!(cli::parse_users_content(":empty_user").is_err());

    // Missing users file exits 2
    assert_eq!(cli::run(&args, &mut out, &mut err), 2);
}

proptest! {
    #[test]
    fn message_roundtrip_preserves_tag_and_body(tag in any::<u8>(), body in proptest::collection::vec(any::<u8>(), 0..256)) {
        let msg = Message { tag, body };
        let mut wire = Vec::new();
        write_message(&mut wire, &msg).unwrap();
        let decoded = read_message(&mut Cursor::new(wire), 1024).unwrap();
        prop_assert_eq!(decoded, msg);
    }

    #[test]
    fn startup_roundtrip_preserves_user_and_database(
        user in "[a-zA-Z0-9_]{1,16}",
        db in "[a-zA-Z0-9_]{1,16}"
    ) {
        let mut wire = Vec::new();
        write_startup(&mut wire, &user, &db).unwrap();
        let decoded = read_startup(&mut Cursor::new(wire), 1024).unwrap();
        prop_assert_eq!(
            decoded,
            Startup::Params {
                user: user.clone(),
                database: db.clone(),
                params: vec![("user".to_string(), user), ("database".to_string(), db)],
            }
        );
    }

    #[test]
    fn md5_password_always_has_md5_prefix_and_35_chars(
        user in "\\PC{0,32}",
        pw in "\\PC{0,32}",
        salt in any::<[u8; 4]>()
    ) {
        let hash = md5_password(&user, &pw, salt);
        prop_assert_eq!(hash.len(), 35);
        prop_assert!(hash.starts_with("md5"));
        prop_assert!(hash[3..].chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }
}

#[test]
fn protocol_constants_match_pg_spec() {
    assert_eq!(PROTOCOL_V3, 196_608);
    assert_eq!(SSL_REQUEST_CODE, 80_877_103);
    assert_eq!(GSS_REQUEST_CODE, 80_877_104);
    assert_eq!(CANCEL_REQUEST_CODE, 80_877_102);
    let _ = Box::new(Cursor::new(Vec::<u8>::new())) as Box<dyn ClientStream>;
}
