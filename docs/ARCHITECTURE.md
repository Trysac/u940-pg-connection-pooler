# Architecture

## Overview

`trysac-pg-connection-pooler` (library `pg_connection_pooler`) implements a PostgreSQL v3.0 wire-protocol transaction-mode connection pooler using only the Rust standard library.

## Modules

- `src/md5.rs`: Pure-Rust implementation of RFC 1321 MD5 and PostgreSQL's `md5_password(user, password, salt)` (`"md5" + md5(md5(password + user) + salt)`).
- `src/codec.rs`: Wire-protocol framing for `Startup` (`SSLRequest`, `GSSENCRequest`, `CancelRequest`, v3.0 `StartupMessage`) and tagged `Message` frames, with explicit `max_message_bytes` enforcement.
- `src/connector.rs`: `ServerStream` and `Connector` traits plus `TcpConnector`, which completes backend startup (`AuthenticationOk`, `AuthenticationCleartextPassword`, `AuthenticationMD5Password`, `ParameterStatus`, `BackendKeyData`, `ReadyForQuery`).
- `src/pool.rs`: `Pooler`, `Config`, and `Stats`. Coordinates client admission (`max_clients`), server checkout/checkin (`Mutex` + `Condvar` bounded by `pool_size` and `pool_timeout`), and transaction-state tracking on every `ReadyForQuery` (`'I'`, `'T'`, `'E'`).
- `src/cli.rs`: Command-line interface (`serve --listen ... --server ... --pool-size ... --users ...`).
