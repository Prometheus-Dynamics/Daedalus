//! Prints what the example computes. `scripts/ci.sh smoke` runs it.

#![allow(clippy::print_stdout)] // an example's output is what it is for

fn main() {
    print!("{}", daedalus_example_host::run());
}
