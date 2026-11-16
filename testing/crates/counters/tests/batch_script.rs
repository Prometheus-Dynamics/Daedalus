//! The board's batch script, run at home with only the board's own commands on PATH
//! (`testing/board-runs/cm5-commands.txt`) and stand-ins for the bundle's programs. A command the
//! board does not have fails here, not on the board; so does a GPU check that reads "could not
//! run" as a wedged GPU.
//!
//! A gap: the shell here is this machine's `sh`, not the board's busybox ash, so a construct only
//! ash refuses is not caught here. The script's preflight covers it on the board: it has the
//! board's shell parse the whole script (`sh -n`) before the first step.

#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;

const BUNDLE: [&str; 14] = [
    "counters",
    "daedalus-budget-harness",
    "daedalus-cpu-timings",
    "daedalus-gpu-budgets",
    "daedalus-process-timings",
    "daedalus-sweep-worker",
    "dma-sys.kernel",
    "dma-sys.lib",
    "gpu-budgets.lib",
    "ref-dylib_frames_overhead",
    "ref-frame_chain",
    "ticks-bench",
    "vk-sys.lib",
    "wgpu-conformance.lib",
];
const WGPU_TESTS: [&str; 5] = ["storage", "transfer", "region", "fragment", "lib"];

/// Programs the test stands in for instead of linking the host's: they would read this machine.
const STOOD_IN: [&str; 4] = ["dmesg", "findmnt", "systemctl", "vulkaninfo"];

struct Board {
    root: PathBuf,
}

impl Drop for Board {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn script(path: &Path, body: &str) {
    fs::write(path, format!("#!/bin/sh\n{body}\n")).expect("a stand-in is written");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("it is executable");
}

fn host_program(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(name))
        .find(|path| path.is_file())
}

/// A directory laid out as the bundle is on the board, and a PATH of the board's commands only.
/// `gpu` is the body of the stand-in for the GPU timing program.
fn board(name: &str, gpu: &str) -> Board {
    let root = std::env::temp_dir().join(format!("dd-batch-test-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let (path, bin) = (root.join("path"), root.join("run/bin"));
    fs::create_dir_all(&path).expect("the PATH directory");
    fs::create_dir_all(&bin).expect("the bundle directory");
    let list = fs::read_to_string(repo().join("testing/board-runs/cm5-commands.txt"))
        .expect("the board's command list");
    for command in list
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        if STOOD_IN.contains(&command) {
            script(&path.join(command), ":");
        } else if let Some(program) = host_program(command) {
            symlink(program, path.join(command)).expect("a command is linked");
        }
    }
    for program in BUNDLE {
        script(&bin.join(program), "echo \"ran $*\"");
    }
    for test in WGPU_TESTS {
        script(&bin.join(format!("wgpu.{test}")), "echo \"ran $*\"");
    }
    script(&bin.join("daedalus-gpu-budgets"), gpu);
    script(&root.join("run/cargo-shim"), ":");
    for library in [
        "libdaedalus_dylib_fixture.so",
        "libdaedalus_plugins_stable_abi.so",
    ] {
        fs::write(bin.join(library), b"").expect("a library stand-in");
    }
    fs::copy(env!("CARGO_BIN_EXE_bounded"), bin.join("bounded")).expect("bounded is copied");
    fs::copy(
        repo().join("scripts/board/batch-on-board.sh"),
        root.join("run/batch-on-board.sh"),
    )
    .expect("the script is copied");
    Board { root }
}

impl Board {
    fn run(&self) -> Option<i32> {
        Command::new(self.root.join("path/sh"))
            .arg(self.root.join("run/batch-on-board.sh"))
            .env_clear()
            .env("PATH", self.root.join("path"))
            .env("BATCH_I3_DIR", self.root.join("i3"))
            .env("BATCH_SECONDS", "20")
            .env("BATCH_HEALTH_SECONDS", "1")
            .output()
            .expect("the script runs")
            .status
            .code()
    }

    fn result(&self, name: &str) -> String {
        fs::read_to_string(self.root.join("run/results").join(name)).unwrap_or_default()
    }

    fn results(&self) -> Vec<(String, String)> {
        fs::read_dir(self.root.join("run/results"))
            .expect("the results directory")
            .map(|entry| {
                entry
                    .expect("an entry")
                    .file_name()
                    .into_string()
                    .expect("a name")
            })
            .map(|name| (self.result(&name), name))
            .map(|(text, name)| (name, text))
            .collect()
    }
}

const GPU_ANSWERS: &str = "echo \"dispatch-floor ran $*\"";

#[test]
fn every_step_runs_with_only_the_boards_commands() {
    let board = board("whole", GPU_ANSWERS);
    assert_eq!(board.run(), Some(0));
    assert_eq!(
        board.result("preflight.txt").lines().last(),
        Some("preflight ok")
    );
    assert!(!board.result("done").is_empty(), "the run reaches its end");
    assert_eq!(
        board.result("gpu-health.txt").lines().last(),
        Some("gpu ok")
    );
    let results = board.results();
    assert!(results.iter().any(|(name, _)| name == "c1-byte-chain.txt"));
    assert!(results.iter().any(|(name, _)| name == "e11-gate.txt"));
    for (name, text) in results {
        assert!(!text.contains("not found"), "{name}: {text}");
        for line in text.lines().filter(|line| line.starts_with("== exit ")) {
            assert_eq!(line, "== exit 0", "{name}: {text}");
        }
    }
}

#[test]
fn a_command_the_board_lacks_ends_the_run_at_the_preflight() {
    let board = board("lacks", GPU_ANSWERS);
    fs::remove_file(board.root.join("path/paste")).expect("paste is taken away");
    assert_eq!(board.run(), Some(3));
    let preflight = board.result("preflight.txt");
    assert!(preflight.contains("missing command: paste"), "{preflight}");
    assert_eq!(preflight.lines().last(), Some("preflight failed"));
    assert!(
        board.result("c1-byte-chain.txt").is_empty(),
        "nothing is measured"
    );
    assert!(
        board.result("gpu-health.txt").is_empty(),
        "the GPU is not judged"
    );
}

#[test]
fn a_program_that_does_not_start_ends_the_run_at_the_preflight() {
    let board = board("starts", GPU_ANSWERS);
    fs::write(
        board.root.join("run/bin/vk-sys.lib"),
        b"#!/no/such/loader\n",
    )
    .expect("rewritten");
    assert_eq!(board.run(), Some(3));
    let preflight = board.result("preflight.txt");
    assert!(
        preflight.contains("does not start: vk-sys.lib"),
        "{preflight}"
    );
}

#[test]
fn only_a_gpu_call_past_its_bound_or_a_reset_reads_as_wedged() {
    // Past its bound: wedged.
    let hangs = "[ \"$*\" = dispatch-floor ] && exec sleep 30; echo \"ran $*\"";
    let board_hangs = board("hangs", hangs);
    assert_eq!(board_hangs.run(), Some(0));
    assert_eq!(
        board_hangs.result("gpu-health.txt").lines().last(),
        Some("gpu wedged")
    );

    // A failure with no reset in the kernel log is a failed check, not a wedged GPU.
    let fails = "[ \"$*\" = dispatch-floor ] && exit 1; echo \"ran $*\"";
    let board_fails = board("fails", fails);
    assert_eq!(board_fails.run(), Some(0));
    assert_eq!(
        board_fails.result("gpu-health.txt").lines().last(),
        Some("gpu check failed")
    );

    // The same failure after the kernel logged a V3D reset during the run: wedged.
    let board_reset = board("reset", fails);
    let marker = board_reset.root.join("run/results/c1-byte-chain.txt");
    script(
        &board_reset.root.join("path/dmesg"),
        &format!(
            "[ -e {} ] && echo '[drm] Resetting GPU for hang.'; :",
            marker.display()
        ),
    );
    assert_eq!(board_reset.run(), Some(0));
    assert_eq!(
        board_reset.result("gpu-health.txt").lines().last(),
        Some("gpu wedged")
    );

    // A check that cannot run says so, and is not a wedged GPU.
    let board_gone = board("gone", "exit 127");
    assert_eq!(board_gone.run(), Some(3), "the preflight catches it first");
    let cannot = "[ \"$*\" = dispatch-floor ] && exit 127; echo \"ran $*\"";
    let board_cannot = board("cannot", cannot);
    assert_eq!(board_cannot.run(), Some(0));
    assert_eq!(
        board_cannot.result("gpu-health.txt").lines().last(),
        Some("gpu check could not run")
    );
}
