//! The example's output, against the lines it is meant to print.

use std::process::Command;

const GOLDEN: &str = "\
basics: x = 4 gives 26
basics: x = 20 gives 40
basics: a text limit is refused: no backend can run node `basics.clamp` (cpu: refused (the config doesn't fit its type))
";

#[test]
fn basics_fan_in_out() {
    // One input fans out to two nodes, and both results fan in to one: 4 * 3 + (4 + 10).
    let mut host = daedalus_example_basics::load();
    assert_eq!(daedalus_example_basics::through(&mut host, 4.0), 26.0);
    // The clamp's config holds the sum to its upper limit.
    assert_eq!(daedalus_example_basics::through(&mut host, 20.0), 40.0);
    assert_eq!(daedalus_example_basics::through(&mut host, -100.0), 0.0);

    assert_eq!(daedalus_example_basics::run(), GOLDEN);
    let output = Command::new(env!("CARGO_BIN_EXE_daedalus-example-basics"))
        .output()
        .expect("the example runs");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), GOLDEN);
}
