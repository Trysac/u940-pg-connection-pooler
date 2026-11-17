# Standard-Library Thread-Per-Client and Condvar Pool for PostgreSQL v3 Transaction Pooling

Status: accepted (generated, pending human review)

## Context

Applications with many concurrent workers can exhaust PostgreSQL backend connections. A transaction-mode pooler multiplexes client sessions onto a small set of backend connections, but must parse the v3 wire protocol accurately, enforce frame size and queue bounds, and never release a backend while a transaction ('T' or 'E') is open.

## Decision

Implement the PostgreSQL v3.0 startup and message framing, RFC 1321 MD5 authentication, and transaction state machine using only the Rust standard library, with one thread per client and a Mutex + Condvar pool keyed by (user, database). Inspect every backend ReadyForQuery ('Z') status byte to return connections on 'I', retain them on 'T' and 'E', and close them if a client disconnects mid-transaction.

## Consequences

Zero external runtime dependencies, deterministic in-memory testing via scripted streams, strict bounds on message sizes and client/server counts, and PgBouncer-compatible transaction pooling semantics (named prepared statements rejected with 0A000, SET session state shared across pooled transactions).
