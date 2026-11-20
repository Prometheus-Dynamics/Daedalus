//! Runs a driver against each scenario through a live host, and checks what it printed against the
//! golden transcripts. Also the codec check, and the fixture build.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::scenario::{self, Proxy, Scenario};
use crate::serve::{Serving, Transport, library_under, tcp_addr};
use crate::{Failure, corpus, proxy};

/// Transcripts run side by side, at most this many at once. Each runs its own host, and a host
/// that loads plugins loads its own copy of the fixture library (`PrivateLibrary`), so the
/// library's statics (the gated node's poll count) are never shared between two transcripts.
const AT_ONCE: usize = 4;

/// How long a driver may take to answer one scenario before it is stopped.
const LIMIT: Duration = Duration::from_secs(30);

/// Runs `cmd` over every scenario and every transport. Each answer is compared with the golden
/// transcript of its scenario. `fixture` is the built plugin library (`build_fixture`).
pub fn run_driver(cmd: Command, fixture: &Path) -> Vec<Failure> {
    let scenarios = match scenario::load_all() {
        Ok(scenarios) => scenarios,
        Err(error) => return vec![Failure::plain("scenarios", error)],
    };
    let mut failures = Vec::new();
    let mut runs = Vec::new();
    for scenario in &scenarios {
        match golden(&scenario.name) {
            Ok(golden) => {
                let golden = std::sync::Arc::new(golden);
                for transport in transports_for(scenario) {
                    runs.push((scenario, transport, golden.clone()));
                }
            }
            Err(error) => failures.push(Failure::plain(&scenario.name, error)),
        }
    }
    // Each run's failures in its own place, so the list reads in scenario order however the
    // runs interleave.
    let found: Vec<std::sync::Mutex<Vec<Failure>>> = runs
        .iter()
        .map(|_| std::sync::Mutex::new(Vec::new()))
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..AT_ONCE.min(runs.len()) {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some((scenario, transport, golden)) = runs.get(index) else {
                        break;
                    };
                    let mut mine = Vec::new();
                    match transcript(&cmd, scenario, *transport, fixture) {
                        Ok(got) => compare(scenario, *transport, golden, &got, &mut mine),
                        Err(error) => mine.push(Failure {
                            scenario: scenario.name.clone(),
                            transport: label(scenario, *transport),
                            line: None,
                            expected: "a transcript".to_owned(),
                            got: error,
                        }),
                    }
                    *found[index]
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = mine;
                }
            });
        }
    });
    for slot in found {
        failures.extend(
            slot.into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }
    failures
}

/// What `run_driver` compares: the number of transcripts (a scenario over one transport) and of
/// answer lines in them.
pub fn coverage() -> Result<(usize, usize), String> {
    let scenarios = scenario::load_all()?;
    Ok(scenarios.iter().fold((0, 0), |(runs, lines), scenario| {
        let transports = transports_for(scenario).len();
        (
            runs + transports,
            lines + transports * scenario.commands.len(),
        )
    }))
}

/// The transports a scenario runs over. A scenario behind the proxy runs over TCP only.
fn transports_for(scenario: &Scenario) -> Vec<Transport> {
    if scenario.proxy != Proxy::None {
        vec![Transport::Tcp]
    } else {
        Transport::all()
    }
}

fn label(scenario: &Scenario, transport: Transport) -> String {
    match scenario.proxy {
        Proxy::None => transport.name().to_owned(),
        Proxy::Newer => format!("{}+newer", transport.name()),
        Proxy::Frames(cap) => format!("{}+frames={cap}", transport.name()),
    }
}

fn golden(name: &str) -> Result<String, String> {
    let path = scenario::golden_dir().join(format!("{name}.out"));
    std::fs::read_to_string(&path).map_err(|error| {
        format!(
            "{}: {error} (write it with BLESS=1 once the reference driver is right)",
            path.display()
        )
    })
}

fn compare(
    scenario: &Scenario,
    transport: Transport,
    golden: &str,
    got: &str,
    failures: &mut Vec<Failure>,
) {
    let expected: Vec<&str> = golden.lines().collect();
    let answers: Vec<&str> = got.lines().collect();
    for (index, (line, _)) in scenario.commands.iter().enumerate() {
        let want = expected.get(index).copied().unwrap_or("<no line>");
        let have = answers.get(index).copied().unwrap_or("<no line>");
        if want != have {
            failures.push(Failure {
                scenario: scenario.name.clone(),
                transport: label(scenario, transport),
                line: Some(*line),
                expected: want.to_owned(),
                got: have.to_owned(),
            });
        }
    }
    if answers.len() > scenario.commands.len() {
        failures.push(Failure {
            scenario: scenario.name.clone(),
            transport: label(scenario, transport),
            line: None,
            expected: format!("{} answer lines", scenario.commands.len()),
            got: format!("{} answer lines", answers.len()),
        });
    }
}

/// The transcript a driver gives for one scenario over one transport: one line per command.
pub fn transcript(
    cmd: &Command,
    scenario: &Scenario,
    transport: Transport,
    fixture: &Path,
) -> Result<String, String> {
    let library = PrivateLibrary::copy(fixture, scenario.plugins)?;
    let fixture = library.path();
    let mut lines = Vec::new();
    for (_, line) in &scenario.commands {
        lines.push(scenario::expand(line, fixture, &scenario::graphs_dir())?);
    }
    let mut input = lines.join("\n");
    input.push('\n');
    let serving = Serving::start(transport, scenario.plugins)?;
    let mode = match scenario.proxy {
        Proxy::None => None,
        Proxy::Newer => Some(proxy::Mode::Newer),
        Proxy::Frames(cap) => Some(proxy::Mode::SmallFrames(cap)),
    };
    let endpoint = match mode {
        Some(mode) => {
            let upstream = tcp_addr(&serving.address).ok_or("no TCP address to proxy")?;
            let front = proxy::start(upstream, mode).map_err(|error| format!("proxy: {error}"))?;
            format!("tcp:{front}")
        }
        None => serving.address.clone(),
    };
    let result = run_child(cmd, vec![endpoint], &input);
    serving.finish();
    // The answers name the fixture and the graphs by their paths, which differ per machine.
    result.map(|text| normalize(&text, fixture))
}

/// The fixture library a transcript's host loads: a copy of its own, in a directory of its own,
/// under the same file name. A library loaded from another path is another load, with statics of
/// its own. The directory is removed when the transcript ends. A host that loads no plugins uses
/// the fixture's path as it is, and nothing is copied.
struct PrivateLibrary {
    path: PathBuf,
    dir: Option<PathBuf>,
}

impl PrivateLibrary {
    fn copy(fixture: &Path, plugins: bool) -> Result<PrivateLibrary, String> {
        if !plugins {
            return Ok(PrivateLibrary {
                path: fixture.to_path_buf(),
                dir: None,
            });
        }
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "dd-drive-lib-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let name = fixture.file_name().ok_or("the fixture has no file name")?;
        std::fs::create_dir_all(&dir).map_err(|error| format!("{}: {error}", dir.display()))?;
        let library = PrivateLibrary {
            path: dir.join(name),
            dir: Some(dir),
        };
        std::fs::copy(fixture, &library.path)
            .map_err(|error| format!("copy {}: {error}", fixture.display()))?;
        Ok(library)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for PrivateLibrary {
    fn drop(&mut self) {
        if let Some(dir) = &self.dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// The answers with the fixture's path as `{plugin}` and the graph directory as `{graphs}`. A path
/// can appear as it is or inside a JSON string, where each `\` of a Windows path is written `\\`.
fn normalize(text: &str, fixture: &Path) -> String {
    let mut text = text.to_owned();
    for (path, name) in [
        (fixture.display().to_string(), "{plugin}"),
        (scenario::graphs_dir().display().to_string(), "{graphs}"),
    ] {
        text = text
            .replace(&path.replace('\\', "\\\\"), name)
            .replace(&path, name);
    }
    text
}

/// Runs the driver once with `input` on stdin and `args` after its own arguments. Its stdout is
/// returned when it exits cleanly. A driver that outlives `LIMIT` is stopped.
fn run_child(cmd: &Command, args: Vec<String>, input: &str) -> Result<String, String> {
    let mut child_cmd = fresh(cmd);
    child_cmd.args(args);
    child_cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = child_cmd
        .spawn()
        .map_err(|error| format!("cannot start the driver: {error}"))?;
    let mut stdin = child.stdin.take().ok_or("no stdin")?;
    let writer_input = input.to_owned();
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(writer_input.as_bytes());
    });
    let mut stdout = child.stdout.take().ok_or("no stdout")?;
    let mut stderr = child.stderr.take().ok_or("no stderr")?;
    let out = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stdout.read_to_string(&mut text);
        text
    });
    let err = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });
    let deadline = Instant::now() + LIMIT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            Ok(None) => {
                // This child is ours: it is the process started above, and only it is stopped.
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("no end within {} s", LIMIT.as_secs()));
            }
            Err(error) => return Err(format!("waiting for the driver: {error}")),
        }
    };
    let _ = writer.join();
    let text = out.join().unwrap_or_default();
    let stderr_text = err.join().unwrap_or_default();
    if !status.success() {
        return Err(format!(
            "exit {status}: {}",
            stderr_text.trim_end().replace('\n', " / ")
        ));
    }
    Ok(text)
}

/// A new command with the same program, arguments, environment and directory.
fn fresh(cmd: &Command) -> Command {
    let mut out = Command::new(cmd.get_program());
    out.args(cmd.get_args());
    for (key, value) in cmd.get_envs() {
        match value {
            Some(value) => out.env(key, value),
            None => out.env_remove(key),
        };
    }
    if let Some(dir) = cmd.get_current_dir() {
        out.current_dir(dir);
    }
    out
}

/// Writes the golden transcripts from `cmd`, over TCP. Used with `BLESS=1` once the reference
/// driver is right. A scenario that the driver fails is reported, and its file is left alone.
pub fn bless(cmd: &Command, fixture: &Path) -> Vec<Failure> {
    let mut failures = Vec::new();
    let scenarios = match scenario::load_all() {
        Ok(scenarios) => scenarios,
        Err(error) => return vec![Failure::plain("scenarios", error)],
    };
    for scenario in &scenarios {
        match transcript(cmd, scenario, Transport::Tcp, fixture) {
            Ok(text) => {
                let path = scenario::golden_dir().join(format!("{}.out", scenario.name));
                if let Err(error) = std::fs::write(&path, text) {
                    failures.push(Failure::plain(&scenario.name, error.to_string()));
                }
            }
            Err(error) => failures.push(Failure::plain(&scenario.name, error)),
        }
    }
    bless_codec(cmd, &mut failures);
    failures
}

/// The golden file of the codec check for `kind`: the reference driver's re-encoding of each body
/// of the corpus file, one hex line each.
pub fn codec_golden(kind: &str) -> PathBuf {
    crate::scenario::golden_dir().join(format!("codec-{kind}.out"))
}

/// The codec check. `cmd` is run in recode mode on each corpus file, and each line it prints must
/// equal the golden line for that body (the reference's re-encoding). A driver that prints too few
/// lines fails, and so does one that prints a line that is not the golden one.
///
/// The golden is not the input body. The corpus holds the bytes the first version sent, and a
/// field added since is encoded at its default, so a re-encoding can be longer than its body.
pub fn run_codec_check(cmd: &Command) -> Vec<Failure> {
    let mut failures = Vec::new();
    for kind in ["client", "server"] {
        let file = format!("{kind}.bin");
        let expected = match std::fs::read_to_string(codec_golden(kind)) {
            Ok(text) => text,
            Err(error) => {
                failures.push(Failure::plain(
                    &file,
                    format!("{error} (write it with BLESS=1)"),
                ));
                continue;
            }
        };
        let args = vec![
            "--recode".to_owned(),
            kind.to_owned(),
            corpus::path(kind).display().to_string(),
        ];
        let got = match run_child(cmd, args, "") {
            Ok(text) => text,
            Err(error) => {
                failures.push(Failure {
                    scenario: file,
                    transport: "codec".to_owned(),
                    line: None,
                    expected: format!("{} lines", expected.lines().count()),
                    got: error,
                });
                continue;
            }
        };
        let answers: Vec<&str> = got.lines().collect();
        for (index, want) in expected.lines().enumerate() {
            let have = answers.get(index).copied().unwrap_or("<no line>");
            if have != want {
                failures.push(Failure {
                    scenario: file.clone(),
                    transport: "codec".to_owned(),
                    line: Some(index + 1),
                    expected: want.to_owned(),
                    got: have.to_owned(),
                });
            }
        }
        if answers.len() > expected.lines().count() {
            failures.push(Failure {
                scenario: file,
                transport: "codec".to_owned(),
                line: None,
                expected: format!("{} lines", expected.lines().count()),
                got: format!("{} lines", answers.len()),
            });
        }
    }
    failures
}

/// Writes the codec goldens from `cmd`, with the same args as the codec check.
fn bless_codec(cmd: &Command, failures: &mut Vec<Failure>) {
    for kind in ["client", "server"] {
        let args = vec![
            "--recode".to_owned(),
            kind.to_owned(),
            corpus::path(kind).display().to_string(),
        ];
        match run_child(cmd, args, "") {
            Ok(text) => {
                if let Err(error) = std::fs::write(codec_golden(kind), text) {
                    failures.push(Failure::plain(kind, error.to_string()));
                }
            }
            Err(error) => failures.push(Failure::plain(kind, error)),
        }
    }
}

/// Builds the dylib fixture into `target`, and returns the library's path. The build is a
/// separate workspace, as the loader tests build it.
pub fn build_fixture(target: &Path) -> Result<PathBuf, String> {
    let manifest =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/dylib/Cargo.toml");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let status = Command::new(cargo)
        .args(["build", "-q", "--manifest-path"])
        .arg(&manifest)
        .arg("--target-dir")
        .arg(target)
        .status()
        .map_err(|error| format!("cannot run cargo: {error}"))?;
    if !status.success() {
        return Err("the dylib fixture does not build".to_owned());
    }
    Ok(library_under(target))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_windows_path_is_replaced_as_it_is_and_inside_a_json_string() {
        let fixture = Path::new(r"D:\a\target\fixture.dll");
        let text =
            r#"load D:\a\target\fixture.dll ["2|load_plugin|D:\\a\\target\\fixture.dll|none"]"#;
        assert_eq!(
            normalize(text, fixture),
            r#"load {plugin} ["2|load_plugin|{plugin}|none"]"#
        );
    }
}
