# Architecture

## Purpose

`trysac-pg-connection-pooler` is a small, dependency-free Rust library: PostgreSQL transaction-mode connection pooler speaking the v3 wire protocol, with md5 client auth, bounded wait queues and server reuse at idle ReadyForQuery

## Shape

```text
caller code --> crate pg_connection_pooler (src/lib.rs)
                    |
                    +-- pure functions, no I/O, no global mutable state
                    +-- private modules under src/ (optional)
```

- **Public API**: every `pub` item reachable from `src/lib.rs`. Private modules and `pub(crate)` items
  may change without a major version.
- **Errors**: invalid input returns `Err` with the crate's `Error` enum (`#[non_exhaustive]`, so new
  variants are not breaking). The library never panics on input, prints or exits.
- **Side effects**: none. If a feature needs I/O, accept a trait (`std::io::Read` or a small custom
  one) instead of opening resources, and record the choice in an ADR.
- **Safety**: `unsafe` code is forbidden by the crate's lints.

## Quality contract

`scripts/check` is the single definition of the gates; `fabrica.json` tells the factory and CI
how to run it. See [TESTING.md](TESTING.md).

## Decisions

Architecture decisions are recorded in [decisions/](decisions/) as numbered ADRs.
