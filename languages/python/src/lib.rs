//! The Python worker, as a worker spec. The SDK is in `sdk/` and the canonical fixture nodes in
//! `fixtures/`; the fixture worker is `fixtures/worker.py`.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use daedalus_backend_process::WorkerSpec;

/// The interpreter [`spec`] starts: `python` on Windows, the name the python.org installer gives
/// it (`python3` there is the Store's stub), and `python3` elsewhere. Nothing checks that it runs:
/// a worker whose interpreter is wrong or missing is refused at start-up, naming the program.
pub const DEFAULT_INTERPRETER: &str = if cfg!(windows) { "python" } else { "python3" };

/// The fixture worker script, in this crate.
pub fn fixture_worker() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("worker.py")
}

/// A spec that starts `script` with [`DEFAULT_INTERPRETER`]. The script finds its own SDK.
pub fn spec(script: &Path) -> WorkerSpec {
    spec_with(DEFAULT_INTERPRETER, script)
}

/// A spec that starts `script` with `interpreter`: a bare name, found on the path, or an absolute
/// path.
pub fn spec_with(interpreter: impl Into<PathBuf>, script: &Path) -> WorkerSpec {
    WorkerSpec::new(interpreter).arg(script.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn the_default_interpreter_on_windows_is_python() {
        assert_eq!(spec(Path::new("w.py")).program, Path::new("python"));
    }

    #[cfg(not(windows))]
    #[test]
    fn the_default_interpreter_off_windows_is_python3() {
        assert_eq!(spec(Path::new("w.py")).program, Path::new("python3"));
    }

    #[test]
    fn a_named_interpreter_replaces_the_default_and_the_script_follows_it() {
        let spec = spec_with("/opt/python/bin/python3.12", Path::new("w.py"));
        assert_eq!(spec.program, Path::new("/opt/python/bin/python3.12"));
        assert_eq!(spec.args, ["w.py"]);
    }
}
