//! Prints what the example computes. The worker is the `daedalus-example-rust-worker-serve` binary
//! beside this one.

#![allow(clippy::print_stdout)] // an example's output is what it is for

fn main() {
    let worker = std::env::current_exe()
        .expect("the example knows where it runs")
        .with_file_name(format!(
            "daedalus-example-rust-worker-serve{}",
            std::env::consts::EXE_SUFFIX
        ));
    print!("{}", daedalus_example_rust_worker::run(&worker));
}
