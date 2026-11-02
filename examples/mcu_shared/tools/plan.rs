//! The shared graph planned on the host, as the files `generated/` holds. Shared by the generator
//! (`examples/generate.rs`, which writes them) and the freshness test (`tests/generated.rs`, which
//! compares them with the checked-in copies), so both always see the same planner output.

use std::path::Path;

use daedalus_mcu_build::{CompileOptions, compile_document, compile_loaded_document, library};
use daedalus_mcu_shared::{clamp, lowpass, rising, scale, threshold};

/// Plans `graph.json` under `root` for the device nodes and returns `(file name, contents)` for
/// each generated file, in `generated/`: the compiled module, the node library and its manifest,
/// and the plan blob with its manifest.
pub fn outputs(root: &Path) -> Vec<(String, Vec<u8>)> {
    let nodes = [
        scale::NODE,
        lowpass::NODE,
        clamp::NODE,
        threshold::NODE,
        rising::NODE,
    ];
    let graph = std::fs::read_to_string(root.join("graph.json")).expect("graph.json");
    let fail = |err: daedalus_mcu_build::CompileError| -> ! { panic!("{err}") };
    let mut files = Vec::new();

    let source = compile_document(&graph, &nodes, &[], &CompileOptions::default())
        .unwrap_or_else(|e| fail(e));
    files.push(("graph.rs".to_owned(), source.into_bytes()));

    let lib = library(&nodes, &[]).unwrap_or_else(|e| fail(e));
    files.push((
        "library.rs".to_owned(),
        lib.to_rust().unwrap_or_else(|e| fail(e)).into_bytes(),
    ));
    files.push(("library.json".to_owned(), lib.to_json().into_bytes()));
    let (blob, manifest) =
        compile_loaded_document(&graph, &lib, &Default::default()).unwrap_or_else(|e| fail(e));
    files.push(("plan.bin".to_owned(), blob));
    files.push(("plan.json".to_owned(), manifest.to_json().into_bytes()));
    files
}
