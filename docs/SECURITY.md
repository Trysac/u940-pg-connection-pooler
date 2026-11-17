# Security Policy and Threat Model

## Threat Model

- **Oversized frame denial-of-service**: Every `read_startup` and `read_message` call validates the 4-byte wire length prefix against `Config::max_message_bytes` (default 16 MiB) before allocating the payload buffer, rejecting oversized frames with `PoolError` (`message_too_large`) and SQLSTATE `08P01`.
- **Connection and queue exhaustion**: Concurrent client sessions are bounded by `Config::max_clients` (rejecting excess clients with SQLSTATE `53300`), and backend connections are bounded by `Config::pool_size` with `Config::pool_timeout` wait limits.
- **Transaction state isolation**: A backend connection in state `'T'` or `'E'` is never returned to the idle pool; if the client disconnects mid-transaction, the backend connection is terminated and discarded.
- **Credential redaction**: `PoolError`'s `Display` implementation never includes passwords or SQL query payloads.
