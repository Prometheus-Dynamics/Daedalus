//! The surface from C: `tests/c/host.c`, compiled against `daedalus_surface.h` and linked to the
//! surface library, installs the `stable_abi` plugin, compiles a graph and runs it frame by frame
//! with ports by index and a batch push. Its printed outputs must equal the same graph run
//! statically in this process. So the C ABI is exercised by a C compiler and a C caller, not only
//! by the Rust wrapper.
//!
//! Linux only (the program links with `-Wl,-rpath`); skipped when there is no C compiler.

#![cfg(target_os = "linux")]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::path::Path;
use std::process::Command;

use daedalus::GraphDocument;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::engine::{Engine, EngineConfig};
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_plugins_stable_abi::StableAbiPlugin;

const FRAMES: i64 = 50;

/// `step` into a running total, with `period` fed as in the shared-mode tests.
fn document(registry: &PluginRegistry) -> String {
    let int = TypeExpr::Scalar(ValueType::Int);
    let graph = registry
        .graph_builder()
        .unwrap()
        .node_id("stable_abi:accumulate", "accumulate")
        .node_id("stable_abi:every", "every")
        .input_as("step", int.clone())
        .input_as("period", int)
        .connect("step", "accumulate.step")
        .connect("step", "every.tick")
        .connect("period", "every.every")
        .connect("accumulate.total", "total")
        .connect("every.out", "hits")
        .build();
    GraphDocument::new(graph).to_json().unwrap()
}

/// The `total` of each frame, statically, as the C program prints it.
fn static_totals(json: &str) -> Vec<String> {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let mut graph = engine
        .compile_document(&registry, GraphDocument::from_json(json).unwrap())
        .unwrap();
    graph.prepare().unwrap();
    (0..FRAMES)
        .map(|i| {
            graph.push("step", (i * 7) % 11 - 5);
            graph.push("period", 3_i64);
            graph.tick().unwrap();
            let _ = graph.take::<i64>("hits");
            graph
                .take::<i64>("total")
                .map_or_else(|| "none".to_string(), |total| total.to_string())
        })
        .collect()
}

#[test]
#[allow(clippy::print_stderr)] // The skip notice.
fn a_c_program_runs_a_graph_through_the_surface() {
    let compiler = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    if Command::new(&compiler).arg("--version").output().is_err() {
        eprintln!("skipped: no C compiler ({compiler})");
        return;
    }
    let surface = cdylib::build_cdylib("daedalus-surface", "daedalus_surface", "");
    let plugin = cdylib::build_cdylib(
        "daedalus-plugins-stable-abi",
        "daedalus_plugins_stable_abi",
        "dylib",
    );

    let mut registry = PluginRegistry::new();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    let json = document(&registry);

    let dir = std::env::temp_dir().join(format!("daedalus-c-host-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let graph_path = dir.join("graph.json");
    std::fs::write(&graph_path, &json).unwrap();
    let program = dir.join("host");

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let include = manifest.join("../surface/include");
    let lib_dir = surface.parent().unwrap();
    let build = Command::new(&compiler)
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic"])
        .arg("-I")
        .arg(&include)
        .arg(manifest.join("tests/c/host.c"))
        .arg("-o")
        .arg(&program)
        .arg(format!("-L{}", lib_dir.display()))
        .arg("-ldaedalus_surface")
        .arg(format!("-Wl,-rpath,{}", lib_dir.display()))
        .output()
        .unwrap();
    assert!(
        build.status.success(),
        "the C host does not build:\n{}",
        String::from_utf8_lossy(&build.stderr)
    );

    let run = Command::new(&program)
        .arg(&plugin)
        .arg(&graph_path)
        .arg(FRAMES.to_string())
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        run.status.success(),
        "the C host failed:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let printed: Vec<String> = String::from_utf8(run.stdout)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    let expected = static_totals(&json);
    assert_eq!(printed, expected, "C through the surface against static");
    assert!(expected.iter().any(|total| total != "none"));
}
