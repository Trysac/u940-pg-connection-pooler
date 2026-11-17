# Testing

## Local Test Tiers

- **Unit & Integration (`tests/api.rs`)**: Exercises RFC 1321 MD5 (`"abc"`), PostgreSQL `md5_password`, `SSLRequest` negotiation, `max_message_bytes` enforcement, transaction binding (`'T'`, `'E'`, `'I'`), dirty client disconnects, 50 ms `pool_timeout`, 101st client rejection at `max_clients = 100`, MD5 auth failures (`28P01`), SASL rejection (`28000`), named `Parse` refusal (`0A000`), and `TcpConnector` over loopback.
- **Property tests (`proptest!`)**: Verifies wire framing roundtrips and `md5_password` invariants.

## Conformance Tier (`tests/pgbench.rs`)

Behind `#![cfg(feature = "conformance")]`, runs `pgbench -i -s 1` and `pgbench -c 8 -j 2 -t 200 -M simple` through `Pooler` with `pool_size = 4` against a local PostgreSQL instance configured by `ci/conformance-setup.sh`.
