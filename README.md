# trysac-pg-connection-pooler

PostgreSQL transaction-mode connection pooler speaking the v3.0 wire protocol, with MD5 client authentication, bounded wait queues, and server reuse at idle `ReadyForQuery`.

## Usage

```rust
use pg_connection_pooler::{Config, Pooler, TcpConnector, md5_password};
use std::sync::Arc;

let hash = md5_password("bench", "pw", [1, 2, 3, 4]);
assert_eq!(hash, "md5aac0038df22f496d2ca86cd9867ec66c");
let connector = Arc::new(TcpConnector::new("127.0.0.1:5432", None));
let pooler = Pooler::new(Config::default(), connector);
assert_eq!(pooler.stats().clients, 0);
```

## CLI

```text
pg-connection-pooler serve --listen 127.0.0.1:6432 --server 127.0.0.1:5432 --pool-size 10 --users users.txt
```

## Protocol and Pooling Notes

- **Transaction pooling**: A backend connection is checked out when a client sends a query or extended-query message and returned to the idle pool only when the server emits `ReadyForQuery` with status `'I'` (idle). While the server reports `'T'` (in transaction) or `'E'` (failed transaction), the backend stays bound to that client. If a client disconnects while bound in `'T'` or `'E'`, the backend connection is closed and discarded.
- **Prepared statements**: Unnamed prepared statements in the extended query protocol work across pooled connections. Named `Parse` (`'P'`) statements are rejected with SQLSTATE `0A000`.
- **Cancel requests**: `CancelRequest` packets are accepted and dropped.
- **Session state**: Session parameters changed via `SET` remain on the backend connection and are visible to subsequent clients in transaction mode, as in `PgBouncer`.
