//! The blink graphs planned on the host, as the files `generated/` holds. Shared by the generator
//! (`examples/generate.rs`, which writes them) and the freshness test (`tests/generated.rs`, which
//! compares them with the checked-in copies), so both always see the same planner output.

use std::path::Path;

use daedalus_mcu_blink_nodes::{blink, lowpass, rising, scale, threshold};
use daedalus_mcu_build::{
    CompileOptions, compile_document, compile_loaded_document, library, plan_document,
};

/// Plans `graph.json` and `graph_b.json` under `root` and returns `(file name, contents)` for
/// each generated file, in `generated/`.
pub fn outputs(root: &Path) -> Vec<(String, Vec<u8>)> {
    let nodes = [
        scale::NODE,
        lowpass::NODE,
        threshold::NODE,
        rising::NODE,
        blink::NODE,
    ];
    let read = |name: &str| {
        std::fs::read_to_string(root.join(name)).unwrap_or_else(|err| panic!("{name}: {err}"))
    };
    let fail = |err: daedalus_mcu_build::CompileError| -> ! { panic!("{err}") };
    let (graph, graph_b) = (read("graph.json"), read("graph_b.json"));
    let mut files = Vec::new();

    let frozen = CompileOptions {
        freeze_params: true,
        ..Default::default()
    };
    let source = compile_document(&graph, &nodes, &frozen).unwrap_or_else(|e| fail(e));
    files.push(("graph.rs".to_owned(), source.into_bytes()));

    let tunable = CompileOptions {
        graph_name: "TunableGraph".into(),
        ..Default::default()
    };
    let plan = plan_document(&graph, &nodes, &tunable).unwrap_or_else(|e| fail(e));
    files.push(("tunable.rs".to_owned(), plan.to_rust(&tunable).into_bytes()));
    files.push((
        "tunable.json".to_owned(),
        plan.manifest(None).to_json().into_bytes(),
    ));

    let lib = library(&nodes).unwrap_or_else(|e| fail(e));
    files.push((
        "library.rs".to_owned(),
        lib.to_rust().unwrap_or_else(|e| fail(e)).into_bytes(),
    ));
    files.push(("library.json".to_owned(), lib.to_json().into_bytes()));
    for (name, json) in [("plan_a", &graph), ("plan_b", &graph_b)] {
        let (blob, manifest) =
            compile_loaded_document(json, &lib, &Default::default()).unwrap_or_else(|e| fail(e));
        files.push((format!("{name}.bin"), blob));
        files.push((format!("{name}.json"), manifest.to_json().into_bytes()));
    }
    files
}
