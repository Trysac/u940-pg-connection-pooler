// The README is the crate documentation, so its usage example runs as a doc test.
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub mod cli;
pub mod codec;
pub mod connector;
pub mod error;
pub mod md5;
pub mod pool;

pub use connector::{Connector, HandshakedTcpStream, ServerStream, TcpConnector};
pub use error::{ErrorKind, PoolError};
pub use md5::md5_password;
pub use pool::{ClientStream, Config, Pooler, Stats};
