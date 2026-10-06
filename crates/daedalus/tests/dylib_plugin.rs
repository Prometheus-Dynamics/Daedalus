//! Loads `examples/plugins/example_project` as a `cdylib` (exported by `example_project_dylib`)
//! and checks it installs the same nodes and boundary contracts as the statically linked plugin,
//! and that a boundary type built differently on each side is refused; then loads
//! `examples/plugins/foreign_consumer`, built with a different copy of the example crate, which
//! reads host counters through a foreign interface instead; and loads
//! `examples/plugins/dependent`, which depends on (and links) the example plugin.

#[path = "support/cdylib.rs"]
mod cdylib;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use cdylib::{build_cdylib, host_matching_features};

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::engine::{Engine, EngineConfig};
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{ForeignInterface, Payload, RustTypeIdentity, TypeKey};
use daedalus::{PluginLibrary, PluginLibraryError};
use daedalus_plugins_example_project::{Counter, CounterInterface, ExampleProjectPlugin};

const PACKAGE: &str = "daedalus-plugins-example-project-dylib";
const LIB_NAME: &str = "daedalus_plugins_example_project_dylib";
const CONSUMER_PACKAGE: &str = "daedalus-plugins-foreign-consumer";
const CONSUMER_LIB_NAME: &str = "daedalus_plugins_foreign_consumer";
const DEPENDENT_PACKAGE: &str = "daedalus-plugins-dependent";
const DEPENDENT_LIB_NAME: &str = "daedalus_plugins_dependent";

/// Build the example plugin as a cdylib (once per test binary) and return its path.
fn plugin_cdylib() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| build_cdylib(PACKAGE, LIB_NAME, &host_matching_features()))
}

/// Build the foreign-interface consumer plugin as a cdylib (in its own cargo invocation).
fn consumer_cdylib() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        build_cdylib(
            CONSUMER_PACKAGE,
            CONSUMER_LIB_NAME,
            &host_matching_features(),
        )
    })
}

/// Build the plugin that depends on the example plugin as a cdylib.
fn dependent_cdylib() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        build_cdylib(
            DEPENDENT_PACKAGE,
            DEPENDENT_LIB_NAME,
            &host_matching_features(),
        )
    })
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
    // The example's nodes take `i32`, which has its own key.
    let scalar_i32_key =
        daedalus::registry::typeexpr_transport_key(&TypeExpr::Scalar(ValueType::I32)).to_string();
    assert!(
        boundary_keys(&dynamic_registry).contains(scalar_i32_key.as_str()),
        "node macros should auto-register primitive boundary contracts"
    );

    // Installing the same plugin twice surfaces the plugin's error message instead of a bare bool.
    let err = library.install_into(&mut dynamic_registry).unwrap_err();
    assert!(
        matches!(err, PluginLibraryError::RegisterFailed { ref message } if !message.is_empty()),
        "unexpected error: {err:?}"
    );

    // The plugin exports the Rust type behind every key it uses.
    let counter = TypeKey::new("example:counter");
    let (_, plugin_counter) = library
        .boundary_types()
        .iter()
        .find(|(key, _)| *key == counter)
        .expect("the plugin exports its owned type");
    let host_counter = static_registry.boundary_types()[&counter];
    assert_eq!(host_counter, RustTypeIdentity::of::<Counter>());
    assert_eq!(plugin_counter.type_name, host_counter.type_name);

    // The plugin's copy of the example crate is built with `separate-build` and this host's
    // without, so `Counter` is another Rust type with the same key and name (the failure a host
    // hits with a separately built plugin). Install is refused before anything is registered.
    assert!(!plugin_counter.same_type(&host_counter));
    // Both copies of the example crate register their build, so the host names the difference
    // even before installing.
    let diff = library.crate_build_diff(&static_registry);
    assert_eq!(diff.len(), 1, "{diff:?}");
    assert_eq!(diff[0].host.name, "daedalus_plugins_example_project");
    assert_eq!(diff[0].extra_in_plugin(), ["separate-build"]);
    assert!(diff[0].missing_in_plugin().is_empty());
    let err = library.install_into(&mut static_registry).unwrap_err();
    assert!(
        matches!(err, PluginLibraryError::BoundaryTypeConflict { ref conflicts, ref crate_builds, .. }
            if conflicts.iter().any(|conflict| conflict.key == counter) && *crate_builds == diff),
        "unexpected error: {err}"
    );
    let message = err.to_string();
    assert!(
        message.contains(&format!(
            "crate `daedalus_plugins_example_project` {}: host features `default,plugins`, plugin \
             features `default,plugins,separate-build` (only in plugin: separate-build) — key \
             `example:counter`: host `daedalus_plugins_example_project::Counter` (",
            daedalus::version()
        )),
        "{message}"
    );
}

#[test]
fn separately_built_plugin_consumes_host_types_through_a_foreign_interface() {
    // The host owns `Counter` and its `example:counter_view` provider.
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&ExampleProjectPlugin::default())
        .unwrap();

    let library = unsafe { PluginLibrary::load(consumer_cdylib()) }.unwrap();
    assert_eq!(library.rust_abi(), Ok(()));
    // The consumer only declares the interface, not the (differently built) Rust type.
    let counter_key = TypeKey::new("example:counter");
    assert!(
        library
            .boundary_types()
            .iter()
            .all(|(key, _)| *key != counter_key)
    );
    assert_eq!(library.foreign_interfaces(), [*CounterInterface::info()]);
    library.install_into(&mut registry).unwrap();

    let node = registry
        .transport_capabilities
        .nodes()
        .values()
        .find(|decl| decl.id.0.ends_with("read_counter"))
        .cloned()
        .expect("consumer node installed");
    assert_eq!(
        node.inputs[0].type_key,
        TypeKey::new("example:counter_view")
    );

    let graph = registry
        .graph_builder()
        .unwrap()
        .input_as("counter", TypeExpr::opaque("example:counter"))
        .node_id(node.id.0.as_str(), "read")
        .connect("counter", "read.counter")
        .connect("read.value", "value")
        .connect("read.address", "address")
        .build();
    let mut host = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(&registry, graph)
        .unwrap();
    let counter = Arc::new(Counter(7));
    host.push_payload(
        "counter",
        Payload::shared("example:counter", counter.clone()),
    );
    host.tick().unwrap();
    assert_eq!(host.take::<i32>("value"), Some(7));
    let address = Arc::as_ptr(&counter) as i64;
    assert_eq!(host.take::<i64>("address"), Some(address), "read in place");
    drop(host);
    assert_eq!(
        Arc::strong_count(&counter),
        1,
        "the plugin released every handle"
    );
}

#[test]
fn discovery_finds_built_plugin() {
    let library_path = plugin_cdylib();
    let dir = library_path.parent().expect("artifact dir");
    let found = daedalus::discover_plugin_libraries([dir]).unwrap();
    assert!(found.iter().any(|path| path == library_path), "{found:?}");
}

#[test]
fn dependent_plugin_links_its_dependency_and_requires_the_host_to_install_it() {
    let library = unsafe { PluginLibrary::load(dependent_cdylib()) }.unwrap();
    let schema = library.schema();
    assert_eq!(schema.plugin.name, "example_dependent");
    assert_eq!(schema.dependencies, ["example_rust"]);
    // `Lease` has no key of its own; the linked example plugin maps it in the library's private
    // introspection registry, so the schema carries the real key and no external types.
    let port_key = |node: &str, port: &str, outputs: bool| {
        let node = schema
            .nodes
            .iter()
            .find(|n| n.id.ends_with(node))
            .unwrap_or_else(|| panic!("node {node}"));
        let ports = if outputs { &node.outputs } else { &node.inputs };
        let port = ports.iter().find(|p| p.name == port).expect("port");
        port.type_key.clone().expect("port key").to_string()
    };
    assert_eq!(port_key("slot", "lease", false), "example:lease");
    assert_eq!(port_key("lease", "lease", true), "example:lease");
    assert_eq!(port_key("lease", "counter", false), "example:counter");
    assert!(!schema.plugin.metadata.contains_key("external_types"));
    let lease = TypeKey::new("example:lease");
    assert!(
        library
            .boundary_types()
            .iter()
            .any(|(key, _)| *key == lease)
    );

    // The host must install the dependency (its own build of it) first.
    let err = library
        .install_into(&mut PluginRegistry::new())
        .unwrap_err();
    assert!(
        matches!(err, PluginLibraryError::MissingDependencies { ref missing, .. }
            if missing == &["example_rust"]),
        "unexpected error: {err}"
    );
    assert!(err.to_string().contains("`example_rust`"), "{err}");

    // With it installed the dependency check passes. This host's example crate is built
    // differently from the plugin's (see `static_and_dynamic_rust_plugin_install_the_same_nodes`),
    // so the boundary check then refuses the dependency's keys, including the mapped one.
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&ExampleProjectPlugin::default())
        .unwrap();
    let err = library.install_into(&mut registry).unwrap_err();
    assert!(
        matches!(err, PluginLibraryError::BoundaryTypeConflict { ref conflicts, .. }
            if conflicts.iter().any(|conflict| conflict.key == lease)),
        "unexpected error: {err}"
    );
    // Both builds of the example crate have the same features: its types differ through its
    // dependencies (the plugin's build enables other features of the facade), which the
    // message says.
    assert!(
        err.to_string().contains(&format!(
            "crate `daedalus_plugins_example_project` {} has the same features on both sides \
             (`default,plugins`) but resolved differently in the plugin's build through its \
             dependency graph",
            daedalus::version()
        )),
        "{err}"
    );

    // `#[plugin(crate_build)]` registered the plugin crate's build; the linked dependency's
    // is exported too.
    let mut names: Vec<_> = library
        .crate_builds()
        .iter()
        .map(|info| info.name)
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "daedalus_plugins_dependent",
            "daedalus_plugins_example_project"
        ]
    );
    let dependent = library
        .crate_builds()
        .iter()
        .find(|info| info.name == "daedalus_plugins_dependent")
        .unwrap();
    assert_eq!(dependent.version, daedalus::version());
    assert!(dependent.feature_list().contains(&"dylib"), "{dependent:?}");
}
