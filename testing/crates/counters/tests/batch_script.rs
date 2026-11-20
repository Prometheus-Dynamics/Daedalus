//! The board's batch script and its figure scripts, run at home with only the board's own commands
//! on PATH (`testing/board-runs/cm5-commands.txt`) and stand-ins for the bundle's programs. A
//! command the board does not have fails here, not on the board; so does a GPU check that reads
//! "could not run" as a wedged GPU, and a batch that measures anything after E13 did not open.
//!
//! A gap: the shell here is this machine's `sh`, not the board's busybox ash, so a construct only
//! ash refuses is not caught here. The script's preflight covers it on the board: it has the
//! board's shell parse the whole script (`sh -n`) before the first step.

#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The bundle's programs, as `batch.sh` makes it. Each stand-in notes its arguments in `calls`.
const BUNDLE: [&str; 5] = [
    "vk-sys.lib",
    "wgpu.all",
    "wgpu.region",
    "daedalus-budget-harness",
    "ticks-bench",
];

/// Programs the test stands in for instead of linking the host's: they would read this machine.
const STOOD_IN: [&str; 4] = ["dmesg", "findmnt", "systemctl", "vulkaninfo"];

/// The GPU figures after E13, each of which runs only when E13 opened. Ticks, the CPU's, runs
/// whatever E13 said.
const AFTER_E13: [&str; 4] = ["e9", "e14", "e6", "e11"];

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
/// `gpu` is the body of the stand-in for the GPU timing program. With `layer` the bundle has a
/// validation layer, and the planted call is named.
fn board(name: &str, gpu: &str, layer: bool) -> Board {
    let root = std::env::temp_dir().join(format!("dd-batch-test-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let (path, bin) = (root.join("path"), root.join("run/bin"));
    fs::create_dir_all(&path).expect("the PATH directory");
    fs::create_dir_all(&bin).expect("the bundle directory");
    fs::create_dir_all(root.join("run/figures")).expect("the figures directory");
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
    let calls = root.join("calls");
    let note = format!("echo \"$(basename \"$0\") $*\" >> {}", calls.display());
    for program in BUNDLE {
        script(&bin.join(program), &format!("{note}; echo \"ran $*\""));
    }
    // The planted call is named only by a layer.
    script(
        &bin.join("vk-sys.lib"),
        &format!(
            "{note}; [ -n \"$DAEDALUS_PLANT_VALIDATION_ERROR\" ] && [ -n \"$VK_LAYER_PATH\" ] && echo 'Validation Error: [ VUID-VkBufferCreateInfo-size-00912 ]'; echo \"ran $*\""
        ),
    );
    script(&bin.join("daedalus-gpu-budgets"), &format!("{note}; {gpu}"));
    if layer {
        fs::create_dir_all(root.join("run/vvl")).expect("the layer directory");
        fs::write(root.join("run/vvl/VkLayer_khronos_validation.json"), b"{}").expect("a layer");
    }
    fs::copy(env!("CARGO_BIN_EXE_bounded"), bin.join("bounded")).expect("bounded is copied");
    fs::copy(
        repo().join("scripts/board/batch-on-board.sh"),
        root.join("run/batch-on-board.sh"),
    )
    .expect("the script is copied");
    for entry in fs::read_dir(repo().join("scripts/board/figures")).expect("the figures") {
        let entry = entry.expect("a figure");
        fs::copy(
            entry.path(),
            root.join("run/figures").join(entry.file_name()),
        )
        .expect("a figure is copied");
    }
    Board { root }
}

impl Board {
    fn run(&self) -> Option<i32> {
        self.run_with(&[])
    }

    /// Runs the script with `extra` set on top of the board's own environment.
    fn run_with(&self, extra: &[(&str, &str)]) -> Option<i32> {
        Command::new(self.root.join("path/sh"))
            .arg(self.root.join("run/batch-on-board.sh"))
            .env_clear()
            .env("PATH", self.root.join("path"))
            .env("BATCH_SECONDS", "20")
            .env("BATCH_HEALTH_SECONDS", "1")
            .envs(extra.iter().copied())
            .output()
            .expect("the script runs")
            .status
            .code()
    }

    /// What the bundle's programs were asked to do after the preflight's start checks, one line
    /// per call.
    fn calls(&self) -> String {
        fs::read_to_string(self.root.join("calls"))
            .unwrap_or_default()
            .lines()
            .filter(|line| !line.ends_with(" --list") && !line.ends_with(" --help"))
            .map(|line| format!("{line}\n"))
            .collect()
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

const GPU_ANSWERS: &str = "echo \"ran $*\"";

#[test]
fn every_step_runs_with_only_the_boards_commands() {
    let board = board("whole", GPU_ANSWERS, true);
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
    assert!(board.result("e13-gate.txt").starts_with("open"));
    let results = board.results();
    for figure in ["e13", "e9", "e14", "e6", "ticks", "e11"] {
        let name = format!("{figure}.txt");
        assert!(results.iter().any(|(file, _)| *file == name), "{name}");
    }
    for (name, text) in results {
        assert!(!text.contains("not found"), "{name}: {text}");
        for line in text.lines().filter(|line| line.starts_with("== exit ")) {
            assert_eq!(line, "== exit 0", "{name}: {text}");
        }
    }
    // The order: the planted call first, E11 not at all with no armed watchdog.
    let calls = board.calls();
    assert!(
        calls.lines().next().unwrap_or("").contains("planted"),
        "{calls}"
    );
    let at = |what: &str| {
        calls
            .find(what)
            .unwrap_or_else(|| panic!("no {what}: {calls}"))
    };
    assert!(at("planted") < at("dma-fence"));
    assert!(at("dma-fence") < at("dma-steady"));
    assert!(at("dma-steady") < at("elements"));
    assert!(at("elements") < at("daedalus-budget-harness chain"));
    assert!(at("daedalus-budget-harness idle") < at("ticks-bench"));
    assert!(!calls.contains("grind"), "{calls}");
    assert!(
        board
            .result("e11-gate.txt")
            .starts_with("not run: no armed")
    );
}

#[test]
fn a_command_the_board_lacks_ends_the_run_at_the_preflight() {
    let board = board("lacks", GPU_ANSWERS, true);
    fs::remove_file(board.root.join("path/paste")).expect("paste is taken away");
    assert_eq!(board.run(), Some(3));
    let preflight = board.result("preflight.txt");
    assert!(preflight.contains("missing command: paste"), "{preflight}");
    assert_eq!(preflight.lines().last(), Some("preflight failed"));
    assert!(board.calls().is_empty(), "nothing is measured");
    assert!(
        board.result("gpu-health.txt").is_empty(),
        "the GPU is not judged"
    );
}

#[test]
fn a_program_that_does_not_start_ends_the_run_at_the_preflight() {
    let board = board("starts", GPU_ANSWERS, true);
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
fn an_unnamed_planted_call_stops_every_gpu_figure_after_e13() {
    let board = board("unnamed", GPU_ANSWERS, true);
    // A layer that names nothing.
    script(&board.root.join("run/bin/vk-sys.lib"), "echo \"ran $*\"");
    assert_eq!(board.run(), Some(0));
    assert!(
        board
            .result("e13-gate.txt")
            .starts_with("closed: the planted call went unnamed")
    );
    for figure in AFTER_E13 {
        let text = board.result(&format!("{figure}.txt"));
        assert!(
            text.contains("not run: E13 did not open"),
            "{figure}: {text}"
        );
    }
    let calls = board.calls();
    for section in ["dma-fence", "dma-steady", "elements", "grind", "raw-submit"] {
        assert!(!calls.contains(section), "{section} ran: {calls}");
    }
    assert!(board.result("ticks-gate.txt").starts_with("open"));
    assert!(calls.contains("ticks-bench"), "{calls}");
}

#[test]
fn a_board_without_a_layer_measures_nothing() {
    let board = board("nolayer", GPU_ANSWERS, false);
    assert_eq!(board.run(), Some(0));
    assert!(
        board
            .result("e13-gate.txt")
            .starts_with("closed: no validation layer")
    );
    assert!(board.result("e9.txt").contains("not run: E13 did not open"));
    assert!(!board.calls().contains("dma-fence"));
}

#[test]
fn only_a_gpu_call_past_its_bound_or_a_reset_reads_as_wedged() {
    // Past its bound: wedged.
    let hangs = "[ \"$*\" = dispatch-floor ] && exec sleep 30; echo \"ran $*\"";
    let board_hangs = board("hangs", hangs, true);
    assert_eq!(board_hangs.run(), Some(0));
    assert_eq!(
        board_hangs.result("gpu-health.txt").lines().last(),
        Some("gpu wedged")
    );

    // A failure with no reset in the kernel log is a failed check, not a wedged GPU.
    let fails = "[ \"$*\" = dispatch-floor ] && exit 1; echo \"ran $*\"";
    let board_fails = board("fails", fails, true);
    assert_eq!(board_fails.run(), Some(0));
    assert_eq!(
        board_fails.result("gpu-health.txt").lines().last(),
        Some("gpu check failed")
    );

    // The same failure after the kernel logged a V3D reset during the run: wedged.
    let board_reset = board("reset", fails, true);
    let marker = board_reset.root.join("run/results/e13.txt");
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
    let board_gone = board("gone", "exit 127", true);
    assert_eq!(board_gone.run(), Some(3), "the preflight catches it first");
    let cannot = "[ \"$*\" = dispatch-floor ] && exit 127; echo \"ran $*\"";
    let board_cannot = board("cannot", cannot, true);
    assert_eq!(board_cannot.run(), Some(0));
    assert_eq!(
        board_cannot.result("gpu-health.txt").lines().last(),
        Some("gpu check could not run")
    );
}

#[test]
fn a_rehearsal_without_a_validation_layer_records_it_and_runs_the_rest() {
    let rehearsal = board("layer", GPU_ANSWERS, false);
    assert_eq!(
        rehearsal.run_with(&[("BATCH_REHEARSAL", "1"), ("BATCH_SKIP_E11", "1")]),
        Some(0)
    );
    assert!(
        rehearsal
            .result("e13-gate.txt")
            .starts_with("skipped: rehearsal")
    );
    assert!(
        rehearsal.result("e9.txt").contains("== exit 0"),
        "the rest runs"
    );
    assert!(
        rehearsal.result("e6.txt").contains("== exit 0"),
        "the rest runs"
    );
    assert_eq!(
        rehearsal.result("gpu-health.txt").lines().last(),
        Some("gpu ok")
    );
}

#[test]
fn a_rehearsal_never_reaches_the_e11_block() {
    let board = board("e11", GPU_ANSWERS, true);
    assert_eq!(board.run_with(&[("BATCH_SKIP_E11", "1")]), Some(0));
    assert!(
        board
            .result("e11-gate.txt")
            .starts_with("not run: rehearsal")
    );
    assert!(!board.calls().contains("grind"), "no grind ran");
}
