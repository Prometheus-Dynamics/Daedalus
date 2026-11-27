//! This crate's integration tests that need no process of their own, as one test binary:
//! one link instead of one per file. A file that needs a process to itself is a
//! `[[test]]` of its own in Cargo.toml, with the reason beside it. A new file goes in one
//! or the other; the boundary check refuses a test file in neither.

// A case that must compile and run, built here: tests/ui, through trybuild, holds only what must
// not compile, so trybuild checks it and never builds it.
#[path = "compiles/comment_binding.rs"]
mod compiles_comment_binding;
#[path = "element.rs"]
mod element;
#[path = "shader.rs"]
mod shader;
