//! The wrapper's typed errors and optional takes, against the surface library (no core linked
//! into the wrapper; the surface is built and loaded by path).

#![cfg(unix)]

#[path = "support/cdylib.rs"]
mod cdylib;

use daedalus_host::{HostError, SURFACE_ABI, Surface};
use lemnos_abi::Status;

const EMPTY_GRAPH: &str = r#"{"format":"daedalus.graph","schema_version":1,"requires":[],
    "graph":{"nodes":[],"edges":[],"metadata":{}}}"#;

#[test]
fn typed_errors_and_optional_takes() {
    let path = cdylib::build_cdylib("daedalus-surface", "daedalus_surface", "");
    let surface = Surface::load(&path).unwrap();
    assert_eq!(surface.version().accepts(&SURFACE_ABI), Status::OK);
    // FEATURE_GRAPHS (`daedalus-surface`'s `abi::FEATURE_GRAPHS`, bit 1).
    assert!(surface.features().contains(1 << 1));

    // A missing library is a load error, not a panic.
    let missing = Surface::load("/nonexistent/libdaedalus_surface.so");
    assert!(matches!(missing, Err(HostError::Load { .. })));

    let host = surface.create_host().unwrap();
    let error = host.install("/nonexistent/libnope.so").unwrap_err();
    assert_eq!(error.status(), Some(Status::NOT_FOUND));
    assert!(error.to_string().contains("no plugin library"), "{error}");

    let graph = host.compile(EMPTY_GRAPH).unwrap();
    assert_eq!(graph.ports().unwrap(), r#"{"inputs":[],"outputs":[]}"#);
    assert_eq!(graph.telemetry().unwrap(), "null");
    graph.tick().unwrap();
    assert!(graph.telemetry().unwrap().starts_with('{'));
    assert!(graph.explain().unwrap().starts_with('{'));
    assert_eq!(host.schemas().unwrap(), "[]");

    let unknown = graph.push_i64("nope", 1).unwrap_err();
    assert_eq!(unknown.status(), Some(Status::NOT_FOUND));
    assert_eq!(
        graph.take_i64("nope").unwrap_err().status(),
        Some(Status::NOT_FOUND)
    );

    let bad = match host.compile("{not json") {
        Ok(_) => panic!("a malformed document compiled"),
        Err(error) => error,
    };
    assert_eq!(bad.status(), Some(Status::INVALID_ARGUMENT));
    assert!(
        bad.to_string().contains("daedalus_surface_graph_compile"),
        "{bad}"
    );

    let mut out = Vec::new();
    let unknown = graph.take_bytes("nope", &mut out).unwrap_err();
    assert_eq!(unknown.status(), Some(Status::NOT_FOUND));
}
