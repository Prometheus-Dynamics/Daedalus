//! Prints what the example computes, or why it couldn't. The GPU lane runs it.

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example's output is what it is for

fn main() -> std::process::ExitCode {
    match daedalus_example_gpu::mixed::run() {
        Ok(line) => {
            print!("{line}");
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("mixed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
