# pg-connection-pooler

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/Trysac/u940-pg-connection-pooler/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u940-pg-connection-pooler/actions/workflows/ci.yml)

PostgreSQL transaction-mode connection pooler speaking the v3 wire protocol, with md5 client auth, bounded wait queues and server reuse at idle ReadyForQuery

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
cargo add trysac-pg-connection-pooler --git https://github.com/Trysac/u940-pg-connection-pooler
```

Requires Rust 1.88 or newer. No dependencies outside the standard library.

## Usage

```rust
use pg_connection_pooler::{Error, normalize};

// normalize trims surrounding whitespace and lower-cases the text.
assert_eq!(normalize("  Hello World ").as_deref(), Ok("hello world"));

// Blank input is rejected explicitly instead of returning an empty string.
assert_eq!(pg_connection_pooler::normalize("   "), Err(Error::Blank));
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
