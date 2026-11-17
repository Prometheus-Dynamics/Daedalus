//! The Java SDK, as Rust sees it: the worker, the service client and the drive driver, built with the
//! JDK alone. The SDK compiles into one jar, `daedalus.jar`, with `javac --release 21` and `jar`. The
//! fixture worker and the driver compile against that jar, so the jar is what the tests exercise. No
//! build tool and nothing from a registry is used; the sources are in `sdk/` (the jar) and `drive/` (the
//! driver).
//!
//! To build the jar by hand, run `javac --release 21 -d classes $(find sdk/src/main/java -name '*.java')`
//! and then `jar --create --file daedalus.jar -C classes .`, from this directory.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use daedalus_backend_process::WorkerSpec;

/// The Java release the SDK is compiled for. The SDK uses nothing newer: no FFM, no JNI, no native code.
pub const RELEASE: &str = "21";

/// The oldest JDK the tests accept: `javac --release 21` is refused by an older compiler.
pub const MIN_JDK: u32 = 21;

/// The JDK's tools, found and checked once for the process.
#[derive(Debug)]
pub struct Tools {
    /// `javac`, from `JAVA_HOME/bin` when it is set, else from `PATH`.
    pub javac: PathBuf,
    /// `java`, located the same way.
    pub java: PathBuf,
    /// `jar`, located the same way.
    pub jar: PathBuf,
    /// The major version `javac` reports.
    pub version: u32,
}

/// Unwraps a result that a test needs, and panics with the error's own sentence (not its `Debug` form).
pub fn or_fail<T>(result: io::Result<T>) -> T {
    result.unwrap_or_else(|error| panic!("{error}"))
}

static TOOLS: OnceLock<Result<Tools, String>> = OnceLock::new();

/// The JDK this crate builds and runs with. `JAVA_HOME` decides when it is set; otherwise the tools
/// come from `PATH`. The version of `javac` and `java` is checked once, and below `MIN_JDK` is an error
/// that names the version found.
pub fn tools() -> io::Result<&'static Tools> {
    TOOLS
        .get_or_init(find_tools)
        .as_ref()
        .map_err(|error| io::Error::other(error.clone()))
}

fn find_tools() -> Result<Tools, String> {
    let home = std::env::var_os("JAVA_HOME").map(PathBuf::from);
    let javac = locate("javac", home.as_deref())?;
    let java = locate("java", home.as_deref())?;
    let jar = locate("jar", home.as_deref())?;
    let javac_version = version_of(&javac)?;
    let java_version = version_of(&java)?;
    let found = javac_version.min(java_version);
    if found < MIN_JDK {
        return Err(format!(
            "the Java tests need JDK {MIN_JDK} or newer, but `{}` reports version {found}: set JAVA_HOME to a JDK {MIN_JDK} or newer",
            javac.display()
        ));
    }
    Ok(Tools {
        javac,
        java,
        jar,
        version: javac_version,
    })
}

/// A tool's path: inside `JAVA_HOME/bin` when that is set (and must hold it), else its bare name for `PATH`.
fn locate(name: &str, home: Option<&Path>) -> Result<PathBuf, String> {
    let exe = format!("{name}{}", std::env::consts::EXE_SUFFIX);
    match home {
        Some(home) => {
            let path = home.join("bin").join(&exe);
            if path.is_file() {
                Ok(path)
            } else {
                Err(format!(
                    "JAVA_HOME is `{}`, but it has no `bin/{exe}`",
                    home.display()
                ))
            }
        }
        None => Ok(PathBuf::from(exe)),
    }
}

/// The major version a tool reports for `-version`. A failure to run it is an error that says to install
/// a JDK or set `JAVA_HOME`.
fn version_of(tool: &Path) -> Result<u32, String> {
    let output = Command::new(tool).arg("-version").output().map_err(|error| {
        format!(
            "cannot run `{}` ({error}): install a JDK {MIN_JDK} or newer, or set JAVA_HOME to one",
            tool.display()
        )
    })?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    major_version(&text).ok_or_else(|| {
        format!(
            "cannot read the version `{}` reports: {}",
            tool.display(),
            text.trim()
        )
    })
}

/// The major version in a `-version` line: `javac 25.0.4.1` is 25, `javac 1.8.0_402` is 8 and
/// `openjdk version "21-ea"` is 21. The first token that starts with a digit is the version.
pub fn major_version(text: &str) -> Option<u32> {
    let token = text
        .split_whitespace()
        .map(|word| word.trim_matches('"'))
        .find(|word| word.starts_with(|c: char| c.is_ascii_digit()))?;
    let mut parts = token.split('.');
    let first: u32 = leading_digits(parts.next()?)?;
    if first == 1 {
        leading_digits(parts.next()?)
    } else {
        Some(first)
    }
}

fn leading_digits(text: &str) -> Option<u32> {
    let digits: String = text.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// The jar's file name.
pub const JAR: &str = "daedalus.jar";

/// The directory this crate's Java sources are in.
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The SDK's main sources: the wire codec, the worker, the service codec and the client.
pub fn sdk_sources() -> Vec<PathBuf> {
    java_files(&root().join("sdk/src/main/java"))
}

/// The SDK's own checks, compiled against the jar.
pub fn sdk_test_sources() -> Vec<PathBuf> {
    java_files(&root().join("sdk/src/test/java"))
}

/// The canonical fixture nodes and the fixture worker.
pub fn fixture_sources() -> Vec<PathBuf> {
    java_files(&root().join("fixtures/src/main/java"))
}

/// The drive driver: the Java client side of the drive conformance suite.
pub fn driver_sources() -> Vec<PathBuf> {
    java_files(&root().join("drive/src/main/java"))
}

fn java_files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    collect(dir, &mut found);
    found.sort();
    found
}

fn collect(dir: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "java") {
            found.push(path);
        }
    }
}

/// Builds the SDK into `out/daedalus.jar`, through `out/classes`. Returns the jar's path.
pub fn build_jar(out: &Path) -> io::Result<PathBuf> {
    let classes = out.join("classes");
    let _ = std::fs::remove_dir_all(&classes);
    compile(&classes, &[], &sdk_sources())?;
    let jar = out.join(JAR);
    run(Command::new(&tools()?.jar)
        .arg("--create")
        .arg("--file")
        .arg(&jar)
        .arg("-C")
        .arg(&classes)
        .arg("."))?;
    Ok(jar)
}

/// Compiles `sources` into `out` with `javac --release 21`, against `classpath`.
pub fn compile(out: &Path, classpath: &[PathBuf], sources: &[PathBuf]) -> io::Result<()> {
    std::fs::create_dir_all(out)?;
    let mut javac = Command::new(&tools()?.javac);
    javac
        .args([
            "--release",
            RELEASE,
            "-encoding",
            "UTF-8",
            "-Xlint:all",
            "-d",
        ])
        .arg(out);
    if !classpath.is_empty() {
        javac.arg("-cp").arg(class_path(classpath)?);
    }
    javac.args(sources);
    run(&mut javac)
}

/// A class path of the given entries, joined the way the platform joins them.
pub fn class_path(entries: &[PathBuf]) -> io::Result<OsString> {
    std::env::join_paths(entries).map_err(io::Error::other)
}

/// Builds the jar into `target`, then the fixture classes against it. Returns the class path: the jar,
/// then the fixtures. Each caller uses its own `target`, so two test binaries never build into one place.
pub fn fixture_class_path(target: &Path) -> io::Result<OsString> {
    let jar = build_jar(target)?;
    let fixtures = target.join("fixtures");
    compile(&fixtures, std::slice::from_ref(&jar), &fixture_sources())?;
    class_path(&[jar, fixtures])
}

/// Builds the jar into `target`, then the driver against it. Returns the class path: the jar, then the driver.
pub fn driver_class_path(target: &Path) -> io::Result<OsString> {
    let jar = build_jar(target)?;
    let driver_classes = target.join("drive");
    compile(
        &driver_classes,
        std::slice::from_ref(&jar),
        &driver_sources(),
    )?;
    class_path(&[jar, driver_classes])
}

/// A spec that starts the fixture worker, with `class_path` (the jar, then the fixture classes).
pub fn spec(class_path: &OsString) -> io::Result<WorkerSpec> {
    Ok(WorkerSpec::new(&tools()?.java)
        .arg("-Xlog:disable")
        .arg("-Xlog:all=warning:stderr")
        .arg("-cp")
        .arg(class_path.to_string_lossy().into_owned())
        .arg("fixtures.FixtureWorker"))
}

/// The command that runs the Java driver with `class_path`. The drive suite appends the endpoint, or
/// `--recode KIND FILE`. JVM log lines go to stderr, so none of them reaches the answers on stdout.
pub fn driver(class_path: &OsString) -> io::Result<Command> {
    let mut command = Command::new(&tools()?.java);
    command
        .args(["-Xlog:disable", "-Xlog:all=warning:stderr", "-cp"])
        .arg(class_path)
        .arg("dev.daedalus.drive.Drive");
    Ok(command)
}

/// Runs a command to success, or reports its exit and its standard error.
fn run(command: &mut Command) -> io::Result<()> {
    let output = command.output().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot run {:?}: {error}", command.get_program()),
        )
    })?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{:?} failed ({}): {}",
            command.get_program(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::major_version;

    #[test]
    fn the_major_version_is_read_from_each_jdk_format() {
        assert_eq!(major_version("javac 25.0.4.1\n"), Some(25));
        assert_eq!(major_version("javac 21"), Some(21));
        assert_eq!(major_version("javac 1.8.0_402"), Some(8));
        assert_eq!(
            major_version("openjdk version \"21-ea\" 2026-01-01"),
            Some(21)
        );
        assert_eq!(
            major_version("openjdk version \"17.0.2\" 2022-01-18"),
            Some(17)
        );
        assert_eq!(major_version("no version here"), None);
    }
}
