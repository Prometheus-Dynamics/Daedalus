use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_ffi::PluginLibrary;
use daedalus_plugins_example_project::ExampleProjectPlugin;

const PACKAGE: &str = "daedalus-plugins-example-project";
const LIB_STEM: &str = "daedalus_plugins_example_project";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("ffi crate should live under workspace root/crates")
        .to_path_buf()
}

fn plugin_library_path(profile: &str) -> PathBuf {
    workspace_root().join("target").join(profile).join(format!(
        "{}{}{}",
        std::env::consts::DLL_PREFIX,
        LIB_STEM,
        std::env::consts::DLL_SUFFIX
    ))
}

fn ensure_plugin_built(profile: &str) -> PathBuf {
    let path = plugin_library_path(profile);
    let mut command = Command::new("cargo");
    command
        .arg("build")
        .arg("-p")
        .arg(PACKAGE)
        .current_dir(workspace_root());
    if profile == "release" {
        command.arg("--release");
    }
    let status = command
        .status()
        .expect("failed to spawn cargo build for dynamic plugin");
    assert!(status.success(), "dynamic plugin build failed");
    path
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
fn static_and_dynamic_rust_plugin_use_the_same_source_syntax() {
    let plugin = ExampleProjectPlugin::default();
    let mut static_registry = PluginRegistry::new();
    static_registry.install_plugin(&plugin).unwrap();

    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let library_path = ensure_plugin_built(profile);
    let library = unsafe { PluginLibrary::load(&library_path).unwrap() };
    let mut dynamic_registry = PluginRegistry::new();
    library.install_into(&mut dynamic_registry).unwrap();

    assert_eq!(node_ids(&static_registry), node_ids(&dynamic_registry));
    assert_eq!(
        boundary_keys(&static_registry),
        boundary_keys(&dynamic_registry)
    );
    let scalar_int_key =
        daedalus_registry::typeexpr_transport_key(&TypeExpr::Scalar(ValueType::Int)).to_string();
    assert!(
        boundary_keys(&dynamic_registry).contains(scalar_int_key.as_str()),
        "node macros should auto-register primitive boundary contracts"
    );
}
