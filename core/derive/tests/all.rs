//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

// A case that must compile, built here: tests/ui, through trybuild, holds only what must not
// compile, so trybuild checks it and never builds it. It declares items nothing uses.
#[allow(dead_code)]
#[path = "compiles/config.rs"]
mod compiles_config;
#[allow(dead_code)]
#[path = "compiles/enum_key.rs"]
mod compiles_enum_key;
#[allow(dead_code)]
#[path = "compiles/struct_key.rs"]
mod compiles_struct_key;
#[path = "derived.rs"]
mod derived;
#[path = "plain.rs"]
mod plain;
