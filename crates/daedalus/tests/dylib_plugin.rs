//! Loads `examples/plugins/example_project` as a `cdylib` and checks it installs the same
//! nodes and boundary contracts as the statically linked plugin.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::{PluginLibrary, PluginLibraryError};
use daedalus_plugins_example_project::ExampleProjectPlugin;

const PACKAGE: &str = "daedalus-plugins-example-project";
const LIB_NAME: &str = "daedalus_plugins_example_project";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("facade crate should live under <workspace>/crates")
        .to_path_buf()
}

/// Build the example plugin as a cdylib (once per test binary) and return its path.
fn plugin_cdylib() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(build_plugin_cdylib)
}

/// `--features` for the plugin build: its `dylib` export plus this host's enabled boundary
/// facade features (e.g. GPU), which the build fingerprint requires the plugin to share, so a
/// host built with e.g. `--all-features` gets a plugin built the same way.
fn plugin_features() -> String {
    let (_, facade) = daedalus::dylib::boundary_features()
        .into_iter()
        .find(|(krate, _)| *krate == "daedalus")
        .expect("facade features are fingerprinted");
    std::iter::once("dylib".to_string())
        .chain(
            facade
                .into_iter()
                .map(|feature| format!("daedalus/{feature}")),
        )
        .collect::<Vec<_>>()
        .join(",")
}

fn build_plugin_cdylib() -> PathBuf {
    // Use the cargo (and therefore rustc) running this test so the rustc check matches.
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .args([
            "build",
            "-p",
            PACKAGE,
            "--lib",
            "--features",
            &plugin_features(),
        ])
        .arg("--message-format=json")
        .current_dir(workspace_root())
        .stderr(Stdio::inherit());
    if !cfg!(debug_assertions) {
        command.arg("--release");
    }
    let output = command
        .output()
        .expect("failed to spawn cargo build for dynamic plugin");
    assert!(output.status.success(), "dynamic plugin build failed");
    let stdout = String::from_utf8(output.stdout).expect("cargo output is UTF-8");
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|msg| msg["reason"] == "compiler-artifact" && msg["target"]["name"] == LIB_NAME)
        .flat_map(|msg| {
            msg["filenames"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_str().map(PathBuf::from))
        })
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(std::env::consts::DLL_SUFFIX))
        })
        .expect("cargo did not report a cdylib artifact for the example plugin")
}

fn node_ids(registry: &PluginRegistry) -> BTreeSet<String> {
    registry
        .transport_capabilities
        .nodes()
        .values()
        .map(|decl| decl.id.0.clone())
        .filter(|id| id.starts_with("example_rust:"))
        .collect()
}

fn boundary_keys(registry: &PluginRegistry) -> BTreeSet<String> {
    registry
        .boundary_contracts
        .keys()
        .map(ToString::to_string)
        .collect()
}

#[test]
fn static_and_dynamic_rust_plugin_install_the_same_nodes() {
    let plugin = ExampleProjectPlugin::default();
    let mut static_registry = PluginRegistry::new();
    static_registry.install_plugin(&plugin).unwrap();

    let library_path = plugin_cdylib();
    let library = match unsafe { PluginLibrary::load(library_path) } {
        Ok(library) => library,
        Err(err) => panic!("failed to load {}: {err}", library_path.display()),
    };
    assert_eq!(library.rust_abi(), Ok(()));
    let info = library.info();
    assert_eq!(info.plugin_name.as_str(), Some(PACKAGE));
    assert_eq!(info.plugin_version.as_str(), Some(daedalus::version()));

    // The stable schema describes the same nodes the plugin installs.
    let schema_nodes: BTreeSet<String> = library
        .schema()
        .nodes
        .iter()
        .map(|node| node.id.clone())
        .collect();
    assert_eq!(library.schema().plugin.name, "example_rust");
    assert_eq!(schema_nodes, node_ids(&static_registry));

    let mut dynamic_registry = PluginRegistry::new();
    library.install_into(&mut dynamic_registry).unwrap();

    assert!(!node_ids(&dynamic_registry).is_empty());
    assert_eq!(node_ids(&static_registry), node_ids(&dynamic_registry));
    assert_eq!(
        boundary_keys(&static_registry),
        boundary_keys(&dynamic_registry)
    );
    let scalar_int_key =
        daedalus::registry::typeexpr_transport_key(&TypeExpr::Scalar(ValueType::Int)).to_string();
    assert!(
        boundary_keys(&dynamic_registry).contains(scalar_int_key.as_str()),
        "node macros should auto-register primitive boundary contracts"
    );

    // Installing the same plugin twice surfaces the plugin's error message instead of a bare bool.
    let err = library.install_into(&mut dynamic_registry).unwrap_err();
    assert!(
        matches!(err, PluginLibraryError::RegisterFailed { ref message } if !message.is_empty()),
        "unexpected error: {err:?}"
    );
}

#[test]
fn discovery_finds_built_plugin() {
    let library_path = plugin_cdylib();
    let dir = library_path.parent().expect("artifact dir");
    let found = daedalus::discover_plugin_libraries([dir]).unwrap();
    assert!(found.iter().any(|path| path == library_path), "{found:?}");
}
