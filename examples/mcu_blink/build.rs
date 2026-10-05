//! Plans the blink graphs on the host with the Daedalus planner and writes, to `$OUT_DIR`:
//!
//! - `graph.rs`: `graph.json` compiled, its parameters frozen into literals (compiled mode);
//! - `tunable.rs` + `tunable.json`: the same graph with its parameters tunable, and the
//!   manifest naming them (compiled + tunable mode);
//! - `library.rs` + `library.json`: the node library of the loaded-mode firmware, and
//!   `plan_a.bin`/`plan_b.bin` (+ `.json` manifests): `graph.json` and `graph_b.json` compiled
//!   for it (loaded mode).
//!
//! For bare-metal targets it also provides the linker memory layout.

use std::{env, fs, path::PathBuf};

use daedalus_mcu_blink_nodes::{blink, lowpass, rising, scale, threshold};
use daedalus_mcu_build::{
    CompileOptions, compile_document, compile_loaded_document, plan_document, write_out_dir,
};

/// A generic 128 KiB flash / 32 KiB RAM part (an STM32F4- or G0-class layout): enough to link and
/// measure the firmware; adapt it to the board when flashing.
const MEMORY: &str = "MEMORY
{
  FLASH : ORIGIN = 0x08000000, LENGTH = 128K
  RAM : ORIGIN = 0x20000000, LENGTH = 32K
}
";

fn main() {
    let nodes = [
        scale::NODE,
        lowpass::NODE,
        threshold::NODE,
        rising::NODE,
        blink::NODE,
    ];
    let read = |path: &str| {
        println!("cargo:rerun-if-changed={path}");
        fs::read_to_string(path).unwrap_or_else(|err| panic!("{path}: {err}"))
    };
    let fail = |err: daedalus_mcu_build::CompileError| -> ! { panic!("{err}") };
    let out = |file: &str, contents: &[u8]| {
        write_out_dir(file, contents).unwrap_or_else(|err| fail(err));
    };
    let (graph, graph_b) = (read("graph.json"), read("graph_b.json"));

    let frozen = CompileOptions {
        freeze_params: true,
        ..Default::default()
    };
    out(
        "graph.rs",
        compile_document(&graph, &nodes, &frozen)
            .unwrap_or_else(|e| fail(e))
            .as_bytes(),
    );

    let tunable = CompileOptions {
        graph_name: "TunableGraph".into(),
        ..Default::default()
    };
    let plan = plan_document(&graph, &nodes, &tunable).unwrap_or_else(|e| fail(e));
    out("tunable.rs", plan.to_rust(&tunable).as_bytes());
    out("tunable.json", plan.manifest(None).to_json().as_bytes());

    let library = daedalus_mcu_build::library(&nodes).unwrap_or_else(|e| fail(e));
    out(
        "library.rs",
        library.to_rust().unwrap_or_else(|e| fail(e)).as_bytes(),
    );
    out("library.json", library.to_json().as_bytes());
    for (name, json) in [("plan_a", &graph), ("plan_b", &graph_b)] {
        let (blob, manifest) = compile_loaded_document(json, &library, &Default::default())
            .unwrap_or_else(|e| fail(e));
        out(&format!("{name}.bin"), &blob);
        out(&format!("{name}.json"), manifest.to_json().as_bytes());
    }

    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("none") {
        let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
        fs::write(out.join("memory.x"), MEMORY).expect("write memory.x");
        println!("cargo:rustc-link-search={}", out.display());
        println!("cargo:rustc-link-arg-bins=-Tlink.x");
    }
}
