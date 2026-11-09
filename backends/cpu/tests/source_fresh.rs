//! The checked-in module is what the generator writes today. `BLESS=1` rewrites it.

#![cfg(feature = "source")]

#[path = "shared/source_fixture.rs"]
mod nodes;

fn fresh(name: &str, graph: &daedalus_graph::GraphBody) {
    let path = format!("{}/tests/fixtures/{name}.rs", env!("CARGO_MANIFEST_DIR"));
    let text = nodes::generate(graph).unwrap();
    if std::env::var_os("BLESS").is_some() {
        std::fs::write(&path, &text).unwrap();
    }
    assert_eq!(std::fs::read(&path).unwrap(), text.as_bytes(), "{name}");
}

#[test]
fn the_checked_in_modules_are_fresh() {
    fresh("blink", &nodes::graph());
    fresh("mixed", &nodes::mixed());
    fresh("blink_tunable", &nodes::tunable());
}
