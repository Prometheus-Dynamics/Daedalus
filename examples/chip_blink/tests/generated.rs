//! The checked-in files are what the generator writes today.

#[path = "../nodes/examples/generate.rs"]
mod generate;

#[test]
fn generated_files_match_the_planner() {
    let folder = concat!(env!("CARGO_MANIFEST_DIR"), "/generated");
    for (name, bytes) in generate::files() {
        let on_disk = std::fs::read(format!("{folder}/{name}")).expect("read a generated file");
        assert_eq!(
            on_disk, bytes,
            "{name}: run `cargo run -p daedalus-chip-blink-nodes --example generate`"
        );
    }
}
