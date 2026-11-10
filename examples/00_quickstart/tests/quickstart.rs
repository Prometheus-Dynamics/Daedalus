//! The example's output, against the line it is meant to print.

use std::process::Command;

const GOLDEN: &str = "quickstart: 21 doubled is 42\n";

#[test]
fn quickstart_output_matches() {
    assert_eq!(daedalus_example_quickstart::run(), GOLDEN);
    // The binary prints the same, and exits 0.
    let output = Command::new(env!("CARGO_BIN_EXE_daedalus-example-quickstart"))
        .output()
        .expect("the example runs");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), GOLDEN);
}
