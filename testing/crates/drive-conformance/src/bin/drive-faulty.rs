//! The reference driver with wrong answers on purpose, so the suite can be shown to fail a wrong
//! driver. It is never a client of anything.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match daedalus_drive_conformance::reference::cli(&args, true) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = std::io::Write::write_all(
                &mut std::io::stderr(),
                format!("drive-faulty: {error}\n").as_bytes(),
            );
            ExitCode::FAILURE
        }
    }
}
