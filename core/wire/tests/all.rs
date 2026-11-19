//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

#[path = "any_type.rs"]
mod any_type;
#[path = "by_reference.rs"]
mod by_reference;
#[path = "corpus.rs"]
mod corpus;
#[path = "service.rs"]
mod service;
#[path = "service_additive.rs"]
mod service_additive;
mod service_support;
#[path = "wire_form.rs"]
mod wire_form;
#[path = "worker.rs"]
mod worker;
