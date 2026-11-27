//! What must compile, and run where it has a body, built with the crate's other tests: what
//! must not compile is tests/ui, through trybuild, which then only checks and never builds.
//! The cases declare items nothing else uses.
#![allow(dead_code)]

#[path = "compiles/const_body.rs"]
mod const_body;
#[path = "compiles/fire_reusable.rs"]
mod fire_reusable;
#[path = "compiles/generics.rs"]
mod generics;
#[path = "compiles/nodes_list.rs"]
mod nodes_list;
#[path = "compiles/plain_fn.rs"]
mod plain_fn;
#[path = "compiles/result_error.rs"]
mod result_error;
#[path = "compiles/state.rs"]
mod state;
