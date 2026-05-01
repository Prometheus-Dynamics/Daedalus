pub const SCHEMA_VERSION: u32 = 1;
pub const WORKER_PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_CORRELATION_ID: &str = "fixture-0";

mod schema_package;
mod wire_protocol;

pub use schema_package::*;
pub use wire_protocol::*;

#[cfg(test)]
mod tests;
