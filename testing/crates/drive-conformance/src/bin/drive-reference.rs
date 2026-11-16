//! The Rust reference driver. See the crate's top-level doc for its contract.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match daedalus_drive_conformance::reference::cli(&args, false) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = std::io::Write::write_all(
                &mut std::io::stderr(),
                format!("drive-reference: {error}\n").as_bytes(),
            );
            ExitCode::FAILURE
        }
    }
}
