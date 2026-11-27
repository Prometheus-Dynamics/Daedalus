//! What must compile, and run where it has a body, built with the crate's other tests: what
//! must not compile is tests/ui, through trybuild, which then only checks and never builds.
//! The cases declare items nothing else uses.
#![allow(dead_code)]

#[path = "compiles/id_expressions.rs"]
mod id_expressions;
#[path = "compiles/node.rs"]
mod node;
#[path = "compiles/three_ref_params.rs"]
mod three_ref_params;
