//! The prelude's names exist, and examples reach Daedalus through the facade alone.

use std::fs;
use std::path::{Path, PathBuf};

#[allow(unused_imports)]
use daedalus::prelude::*;

#[test]
fn the_prelude_names_the_host() {
    fn takes(_: fn(&mut Host<'static>) -> Result<TickReport, HostError>) {}
    takes(Host::step);
    let _: Option<FeedOutcome> = None;
    let _: Option<Value> = None;
}

#[cfg(feature = "std")]
#[test]
fn the_prelude_names_loading_and_planning() {
    fn load(_: fn(Planned, LoadOptions) -> Result<LoadedHost, daedalus::host::LoadError>) {}
    load(LoadedHost::load);
    let _ = Config::default();
    let _: Option<ReloadReport> = None;
    let _: Option<FaultReport> = None;
    let _: Option<StreamGraph<LoadedHost>> = None;
}

#[cfg(feature = "runner")]
#[test]
fn the_runner_is_reachable() {
    let _ = daedalus::runner::CancelWord::new();
}

#[test]
fn layered_crates_stay_reachable() {
    let _ = daedalus::layers::runner::CancelWord::new();
    let _: Option<daedalus::layers::types::TypeIndex> = None;
    let _: Option<daedalus::layers::value::Pressure> = None;
}

fn rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}

/// An example shows what an application writes, so it imports the facade and not the layers below.
/// The crates that exist to exercise another layer directly are exempt.
#[test]
fn examples_use_only_the_facade() {
    let root: PathBuf = [env!("CARGO_MANIFEST_DIR"), "..", "..", "examples"]
        .iter()
        .collect();
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut violations = Vec::new();
    for path in files {
        let exempt = path.components().any(|part| {
            let part = part.as_os_str().to_str().unwrap_or("");
            part == "08_ffi"
                || part.starts_with("nostd")
                || part.starts_with("chip_")
                || part.starts_with("wasm_")
        });
        if exempt {
            continue;
        }
        let content = fs::read_to_string(&path).expect("read example");
        for (at, line) in content.lines().enumerate() {
            let line = line.trim();
            let forbidden = line.starts_with("use daedalus_")
                && !line.starts_with("use daedalus_plugins_")
                && !line.starts_with("use daedalus_wasm_");
            if forbidden {
                violations.push(format!("{}:{}: {line}", path.display(), at + 1));
            }
        }
    }
    assert!(violations.is_empty(), "{violations:#?}");
}
