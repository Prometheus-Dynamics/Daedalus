//! Checking the files a worker runs under the host's integrity mode, before it starts.
//!
//! Under Off nothing is opened and the program runs by its name, as it always did. Under Locked
//! and Record the program is opened once, hashed, and started from that open file, so the bytes
//! checked are the bytes run. An `also_check` file is checked before each start: it is read by
//! path, by the interpreter, so this is check-then-use, not a binding.

// The integrity errors name the file and the digests, so they are large. They are returned at
// start time, not per run.
#![allow(clippy::result_large_err)]

use std::ffi::OsString;

use daedalus_integrity::{Checked, Integrity, IntegrityError, Mode, find_program};

use crate::spec::WorkerSpec;

/// The files a start uses: the program's checked file, and the `also_check` files.
#[derive(Default)]
pub(crate) struct Vetted {
    /// The program, opened and checked. `None` under Off, and for a `#!` program under Record,
    /// which runs by its name as it did before.
    pub program: Option<Checked>,
    /// The `also_check` files, checked against the lock.
    pub extras: Vec<Checked>,
}

impl Vetted {
    /// Under Record, writes the entries of the files this start used. Called once the worker
    /// has said hello, so a worker that did not start records nothing.
    pub(crate) fn record(&self) -> Result<(), IntegrityError> {
        if let Some(program) = &self.program {
            program.record()?;
        }
        for extra in &self.extras {
            extra.record()?;
        }
        Ok(())
    }
}

/// Opens and checks what a start of `spec` uses under `integrity`. A refusal is the error.
pub(crate) fn vet(integrity: &Integrity, spec: &WorkerSpec) -> Result<Vetted, IntegrityError> {
    if integrity.mode() == Mode::Off {
        return Ok(Vetted::default());
    }
    let search = search_path(spec);
    let found = find_program(spec.program.as_os_str(), search.as_deref()).ok_or_else(|| {
        IntegrityError::Open {
            path: spec.program.clone(),
            message: "no such program on the search path".into(),
        }
    })?;
    let checked = integrity.open(&found)?;
    let program = match checked.interpreter() {
        // The kernel would run the interpreter, not what was hashed. Under Record the program
        // runs by its name, as it did, and nothing is recorded for it.
        Some(interpreter) if integrity.mode() == Mode::Locked => {
            return Err(IntegrityError::Script {
                path: found,
                interpreter,
            });
        }
        Some(_) => None,
        None => Some(checked),
    };
    let extras = spec
        .checked_files
        .iter()
        .map(|path| integrity.open(path))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Vetted { program, extras })
}

/// The directories a bare program name is searched in: the spec's own `PATH` when it sets one,
/// otherwise the host's.
fn search_path(spec: &WorkerSpec) -> Option<OsString> {
    if let Some((_, value)) = spec.env.iter().rev().find(|(key, _)| key == "PATH") {
        return Some(value.into());
    }
    if spec.env_remove.iter().any(|key| key == "PATH") {
        return None;
    }
    std::env::var_os("PATH")
}
