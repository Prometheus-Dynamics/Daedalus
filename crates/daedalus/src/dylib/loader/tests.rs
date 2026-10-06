use super::*;
use crate::data::model::{TypeExpr, ValueType};
use crate::dylib::{RUSTC_VERSION, STABLE_ABI_VERSION, build_fingerprint};
use crate::registry::capability::{NodeDecl, PortDecl};
use crate::runtime::NodeError;
use crate::runtime::plugins::{CrateBuildInfo, Plugin, PluginInstallContext, PluginResult};
use crate::transport::{ForeignInterface, FrameInterface};

#[derive(Default)]
struct LoaderTestPlugin;

impl Plugin for LoaderTestPlugin {
    fn id(&self) -> &'static str {
        "loader_test"
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_boundary_type::<LoaderFrame>(FRAME_KEY)?;
        ctx.register_crate_build(PLUGIN_STYX)?;
        ctx.register_foreign_interface::<FrameInterface>()?;
        ctx.register_node_decl(
            NodeDecl::new("add")
                .input(PortDecl::new("a", "i64").schema(TypeExpr::Scalar(ValueType::Int)))
                .output(PortDecl::new("out", "i64").schema(TypeExpr::Scalar(ValueType::Int)))
                .output(PortDecl::new("calls", "u32").schema(TypeExpr::Scalar(ValueType::U32))),
        )?;
        // Adds one; panics on negative input; counts its calls in node state.
        ctx.handlers.on("add", |_node, ctx, io| {
            let a = io
                .take_owned::<i64>("a")
                .ok_or_else(|| NodeError::InvalidInput("missing a".into()))?;
            assert!(a >= 0, "negative input {a}");
            let calls = ctx.state.take_node_state::<u32>(&ctx.node_id).unwrap_or(0) + 1;
            ctx.state.set_node_state(&ctx.node_id, calls);
            io.push_as(Some("out"), TypeKey::new("i64"), a + 1);
            io.push_as(Some("calls"), TypeKey::new("u32"), calls);
            Ok(())
        });
        Ok(())
    }
}

crate::export_plugin!(LoaderTestPlugin);

const FRAME_KEY: &str = "loader:frame";

/// The plugin's boundary type; the host shares it, as when both come from one cargo build.
struct LoaderFrame;

/// Like `LoaderFrame`, but as another build would see it: same key and name, other `TypeId`.
unsafe extern "C" fn forged_boundary_types(table: *mut BoundaryTypeTable, _: StrSink) -> bool {
    let real = RustTypeIdentity::of::<LoaderFrame>();
    let entries = vec![super::super::BoundaryTypeEntry {
        type_key: StrView::from_static(FRAME_KEY),
        type_name: StrView::from_static(real.type_name),
        type_id_hash: real.type_id_hash ^ 1,
        size: real.size,
        align: real.align,
    }]
    .leak();
    // Safety: the host passes a writable table.
    unsafe {
        *table = BoundaryTypeTable {
            entries: entries.as_ptr(),
            len: entries.len(),
        }
    };
    true
}

crate::transport::foreign_interface! {
    /// `daedalus:frame` as a plugin built against a newer, incompatible copy would declare it.
    interface FrameV2("daedalus:frame", version = 2);
    struct FrameV2VTable {
        width: unsafe extern "C" fn(data: *const c_void) -> u64,
    }
}

unsafe extern "C" fn newer_foreign_interfaces(
    table: *mut ForeignInterfaceTable,
    _: StrSink,
) -> bool {
    let entries = vec![*FrameV2::info()].leak();
    // Safety: the host passes a writable table.
    unsafe {
        *table = ForeignInterfaceTable {
            entries: entries.as_ptr(),
            len: entries.len(),
        }
    };
    true
}

fn host() -> PluginInfo {
    PluginInfo::for_plugin("host", "0.0.0")
}

/// This test binary's own descriptor, adjusted as if a different build produced it.
fn load_with(
    adjust: impl FnOnce(&mut PluginDescriptor),
) -> Result<PluginLibrary, PluginLibraryError> {
    let mut descriptor = daedalus_plugin_descriptor();
    adjust(&mut descriptor);
    // Safety: the descriptor was produced by this crate's `export_plugin!`.
    unsafe { PluginLibrary::from_descriptor(PathBuf::from("in-process"), descriptor) }
}

#[test]
fn matching_plugin_loads_exposes_schema_and_installs() {
    assert_eq!(daedalus_plugin_abi_version(), PLUGIN_ABI_VERSION);
    let library = load_with(|_| {}).unwrap();
    assert_eq!(library.rust_abi(), Ok(()));
    assert_eq!(library.info().plugin_name.as_str(), Some("daedalus-rs"));
    let schema = library.schema();
    assert_eq!(schema.plugin.name, "loader_test");
    let node = &schema.nodes[0];
    assert!(node.id.ends_with("add"), "{node:?}");
    assert_eq!(node.inputs[0].name, "a");
    assert!(node.outputs.iter().any(|port| port.name == "out"));

    let mut registry = PluginRegistry::new();
    library.install_into(&mut registry).unwrap();
    assert!(registry.plugin_manifests.contains_key("loader_test"));
}

const OLD_RUSTC: &str = "rustc 1.0.0 (a59aba2b1 2015-05-13)";

fn old_toolchain(descriptor: &mut PluginDescriptor) {
    descriptor.info.rustc_version = StrView::from_static(OLD_RUSTC);
}

fn old_rustc() -> RustAbiMismatch {
    RustAbiMismatch::Rustc {
        expected: RUSTC_VERSION.to_string(),
        found: OLD_RUSTC.to_string(),
    }
}

/// Run `node` from `registry` once with input `a`, in `state`.
fn run_add(
    registry: &PluginRegistry,
    state: &crate::runtime::StateStore,
    a: i64,
) -> Result<(Option<i64>, Option<u32>), NodeError> {
    use crate::runtime::executor::{CorrelatedPayload, NodeHandler};
    use crate::runtime::io::NodeIo;
    let mut io = NodeIo::from_inputs([(
        "a".into(),
        CorrelatedPayload::from_edge(crate::transport::Payload::owned("i64", a)),
    )]);
    let ctx = crate::runtime::ExecutionContext::detached(state.clone(), "add-1".into());
    registry
        .handlers
        .run(&crate::runtime::RuntimeNode::new("add"), &ctx, &mut io)?;
    let outputs = io.take_outputs();
    let output = |port: &str| {
        outputs
            .iter()
            .find(|(name, _)| name.as_str() == port)
            .map(|(_, payload)| payload.inner.clone())
    };
    Ok((
        output("out").and_then(|p| p.get_ref::<i64>().copied()),
        output("calls").and_then(|p| p.get_ref::<u32>().copied()),
    ))
}

#[test]
fn mismatched_toolchain_installs_and_runs_through_the_stable_path() {
    let library = load_with(old_toolchain).unwrap();
    assert_eq!(library.schema().plugin.name, "loader_test");
    assert_eq!(library.schema().nodes.len(), 1);
    assert_eq!(library.rust_abi(), Err(&old_rustc()));
    assert_eq!(library.stable_abi_version(), STABLE_ABI_VERSION);
    assert_eq!(library.install_mode(), Some(InstallPath::Stable));

    // The Rust ABI path stays refused.
    let mut registry = PluginRegistry::new();
    let err = library
        .install_into_as(&mut registry, InstallPath::RustAbi)
        .unwrap_err();
    assert!(
        matches!(err, PluginLibraryError::Incompatible { ref plugin, ref mismatch }
            if plugin == "loader_test" && *mismatch == old_rustc()),
        "{err:?}"
    );
    assert!(!registry.plugin_manifests.contains_key("loader_test"));

    // The stable path registers the schema's node with a handler calling the plugin. It
    // compares no boundary types (no Rust type crosses), so a conflicting host type is fine.
    registry.register_boundary_type::<u8>(FRAME_KEY).unwrap();
    assert_eq!(
        library.install_into(&mut registry).unwrap(),
        InstallPath::Stable
    );
    let manifest = &registry.plugin_manifests["loader_test"];
    assert_eq!(manifest.provided_nodes.len(), 1);
    assert!(
        registry
            .foreign_interfaces()
            .contains_key(&TypeKey::new("daedalus:frame"))
    );

    // Outputs come back as the host's Rust types; node state stays in the plugin, one store
    // per host node instance.
    let state = crate::runtime::StateStore::default();
    assert_eq!(run_add(&registry, &state, 41), Ok((Some(42), Some(1))));
    assert_eq!(run_add(&registry, &state, 1), Ok((Some(2), Some(2))));
    let other = crate::runtime::StateStore::default();
    assert_eq!(run_add(&registry, &other, 1), Ok((Some(2), Some(1))));

    // A panic inside the plugin is caught there and reported as a node error.
    let err = run_add(&registry, &state, -1).unwrap_err();
    let message = err.to_string();
    assert!(
        matches!(err, NodeError::Handler(_))
            && message.contains("plugin `loader_test` node `add`")
            && message.contains("negative input -1")
            && message.contains("panicked"),
        "{message}"
    );
    assert_eq!(run_add(&registry, &state, 2), Ok((Some(3), Some(3))));
}

#[test]
fn stable_abi_mismatch_is_a_typed_refusal() {
    let library = load_with(|descriptor| {
        old_toolchain(descriptor);
        descriptor.stable.version = STABLE_ABI_VERSION + 1;
    })
    .unwrap();
    assert_eq!(library.install_mode(), None);
    let mut registry = PluginRegistry::new();
    let err = library.install_into(&mut registry).unwrap_err();
    assert!(
        err.to_string()
            .starts_with("plugin `loader_test` cannot be installed"),
        "{err}"
    );
    assert!(
        matches!(err, PluginLibraryError::StableAbiMismatch { ref plugin, expected, found, ref rust }
            if plugin == "loader_test" && expected == STABLE_ABI_VERSION
                && found == STABLE_ABI_VERSION + 1 && *rust == Some(old_rustc())),
        "{err:?}"
    );
    assert!(!registry.plugin_manifests.contains_key("loader_test"));

    // A plugin the Rust ABI accepts installs through it whatever its stable version...
    let library =
        load_with(|descriptor| descriptor.stable.version = STABLE_ABI_VERSION + 1).unwrap();
    assert_eq!(library.install_mode(), Some(InstallPath::RustAbi));
    // ...but cannot be forced through the stable path.
    let err = library
        .install_into_as(&mut registry, InstallPath::Stable)
        .unwrap_err();
    assert!(
        matches!(
            err,
            PluginLibraryError::StableAbiMismatch { rust: None, .. }
        ),
        "{err:?}"
    );
}

#[test]
fn compatible_plugins_can_be_forced_through_the_stable_path() {
    let library = load_with(|_| {}).unwrap();
    assert_eq!(library.install_mode(), Some(InstallPath::RustAbi));
    let mut registry = PluginRegistry::new();
    library
        .install_into_as(&mut registry, InstallPath::Stable)
        .unwrap();
    let state = crate::runtime::StateStore::default();
    assert_eq!(run_add(&registry, &state, 1), Ok((Some(2), Some(1))));
    // Not recorded: boundary types only matter when Rust types cross.
    assert!(
        !registry
            .boundary_types()
            .contains_key(&TypeKey::new(FRAME_KEY))
    );
}

#[test]
fn boundary_types_are_exported_and_checked_against_the_host() {
    let library = load_with(|_| {}).unwrap();
    let frame = TypeKey::new(FRAME_KEY);
    let exported = library
        .boundary_types()
        .iter()
        .find(|(key, _)| *key == frame)
        .map(|(_, identity)| *identity);
    assert_eq!(exported, Some(RustTypeIdentity::of::<LoaderFrame>()));

    // The host knows the key with the same Rust type: installs.
    let mut registry = PluginRegistry::new();
    registry
        .register_boundary_type::<LoaderFrame>(FRAME_KEY)
        .unwrap();
    library.install_into(&mut registry).unwrap();
}

#[test]
fn boundary_type_conflict_refuses_install_before_anything_is_installed() {
    let library =
        load_with(|descriptor| descriptor.boundary_types = forged_boundary_types).unwrap();
    // Unknown to the host, but the exported table is recorded after install and contradicts
    // the type the plugin registered itself.
    let err = library
        .install_into(&mut PluginRegistry::new())
        .unwrap_err();
    assert!(
        matches!(err, PluginLibraryError::BoundaryTypeConflict { ref conflicts, .. }
            if conflicts.len() == 1),
        "{err}"
    );

    let mut registry = PluginRegistry::new();
    registry
        .register_boundary_type::<LoaderFrame>(FRAME_KEY)
        .unwrap();
    let err = library.install_into(&mut registry).unwrap_err();
    let message = err.to_string();
    let PluginLibraryError::BoundaryTypeConflict {
        plugin,
        conflicts,
        crate_builds,
        same_crate_builds,
        stable_compatible,
    } = err
    else {
        panic!("unexpected error: {err:?}");
    };
    assert!(same_crate_builds.is_empty());
    assert_eq!(plugin, "loader_test");
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].key, TypeKey::new(FRAME_KEY));
    assert_eq!(
        conflicts[0].registered,
        RustTypeIdentity::of::<LoaderFrame>()
    );
    assert_ne!(conflicts[0].new, conflicts[0].registered);
    // The host registered no build of the defining crate (`daedalus`): named only.
    assert!(crate_builds.is_empty());
    assert!(
        message.contains(
            "crate `daedalus` resolved differently in the plugin's build (different features, \
             version or dependency graph) — key `loader:frame`: host `daedalus::"
        ),
        "{message}"
    );
    assert!(message.contains("one cargo invocation"), "{message}");
    // No node port uses `loader:frame`, so the stable path would not carry it.
    assert!(stable_compatible);
    assert!(
        message.contains("install_into_as(InstallPath::Stable)"),
        "{message}"
    );
    assert!(!registry.plugin_manifests.contains_key("loader_test"));
}

/// The plugin's build of `styx_core`, registered by the test plugin.
const PLUGIN_STYX: CrateBuildInfo = CrateBuildInfo {
    name: "styx_core",
    version: "0.4.0",
    features: "framelease",
};

/// Types of two third-party crates as a separately built plugin reports them.
const FORGED: [(&str, &str); 3] = [
    ("styx:framelease", "styx_core::frame::FrameLease"),
    ("styx:planes", "alloc::sync::Arc<styx_core::frame::Plane>"),
    ("other:thing", "other_crate::Thing"),
];

fn forged_identity(type_name: &'static str, type_id_hash: u64) -> RustTypeIdentity {
    RustTypeIdentity {
        type_name,
        type_id_hash,
        size: 8,
        align: 8,
    }
}

unsafe extern "C" fn third_party_boundary_types(table: *mut BoundaryTypeTable, _: StrSink) -> bool {
    let entries: Vec<_> = FORGED
        .iter()
        .map(|(key, name)| super::super::BoundaryTypeEntry {
            type_key: StrView::from_static(key),
            type_name: StrView::from_static(name),
            type_id_hash: 1,
            size: 8,
            align: 8,
        })
        .collect();
    let entries = entries.leak();
    // Safety: the host passes a writable table.
    unsafe {
        *table = BoundaryTypeTable {
            entries: entries.as_ptr(),
            len: entries.len(),
        }
    };
    true
}

#[test]
fn boundary_conflicts_name_crates_and_their_build_differences() {
    let library =
        load_with(|descriptor| descriptor.boundary_types = third_party_boundary_types).unwrap();
    assert_eq!(library.crate_builds(), [PLUGIN_STYX]);
    let mut registry = PluginRegistry::new();
    let host_styx = CrateBuildInfo {
        features: "v4l2,framelease",
        ..PLUGIN_STYX
    };
    registry.register_crate_build(host_styx).unwrap();
    // Proactive: the difference shows before any install attempt.
    let diff = library.crate_build_diff(&registry);
    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].missing_in_plugin(), ["v4l2"]);
    let host_types: Vec<_> = FORGED
        .iter()
        .map(|(key, name)| (TypeKey::new(*key), forged_identity(name, 2)))
        .collect();
    registry.register_boundary_identities(&host_types).unwrap();

    let err = library.install_into(&mut registry).unwrap_err();
    let message = err.to_string();
    let host = |name| forged_identity(name, 2);
    let plugin = |name| forged_identity(name, 1);
    let expected = format!(
        "plugin `loader_test` uses type keys for different Rust types than the host: crate \
         `styx_core` 0.4.0: host features `framelease,v4l2`, plugin features `framelease` \
         (missing in plugin: v4l2) — key `styx:framelease`: host {} vs plugin {}, key \
         `styx:planes`: host {} vs plugin {}; crate `other_crate` resolved differently in the \
         plugin's build (different features, version or dependency graph) — key `other:thing`: \
         host {} vs plugin {}. Rust-ABI plugins",
        host(FORGED[0].1),
        plugin(FORGED[0].1),
        host(FORGED[1].1),
        plugin(FORGED[1].1),
        host(FORGED[2].1),
        plugin(FORGED[2].1),
    );
    assert!(message.starts_with(&expected), "{message}\n{expected}");
    assert!(
        matches!(err, PluginLibraryError::BoundaryTypeConflict { ref crate_builds, .. }
            if *crate_builds == diff),
        "{err:?}"
    );
    assert!(!registry.plugin_manifests.contains_key("loader_test"));
    // As the error suggests: none of these types reaches a node port, so the stable path works.
    library
        .install_into_as(&mut registry, InstallPath::Stable)
        .unwrap();
}

/// The `add` node's input type as another build would see it.
unsafe extern "C" fn conflicting_port_boundary_types(
    table: *mut BoundaryTypeTable,
    _: StrSink,
) -> bool {
    let real = RustTypeIdentity::of::<i64>();
    let entries = vec![super::super::BoundaryTypeEntry {
        type_key: StrView::from_static("i64"),
        type_name: StrView::from_static(real.type_name),
        type_id_hash: real.type_id_hash ^ 1,
        size: real.size,
        align: real.align,
    }]
    .leak();
    // Safety: the host passes a writable table.
    unsafe {
        *table = BoundaryTypeTable {
            entries: entries.as_ptr(),
            len: entries.len(),
        }
    };
    true
}

#[test]
fn conflicts_on_node_ports_do_not_suggest_the_stable_path() {
    let library =
        load_with(|descriptor| descriptor.boundary_types = conflicting_port_boundary_types)
            .unwrap();
    let mut registry = PluginRegistry::new();
    registry.register_boundary_type::<i64>("i64").unwrap();
    let err = library.install_into(&mut registry).unwrap_err();
    assert!(
        matches!(
            err,
            PluginLibraryError::BoundaryTypeConflict {
                stable_compatible: false,
                ..
            }
        ),
        "{err:?}"
    );
    let message = err.to_string();
    // A standard library type names no crate.
    assert!(
        message.contains("than the host: key `i64`: host `i64` (") && !message.contains("Stable"),
        "{message}"
    );
}

#[test]
fn foreign_interfaces_are_exported_and_checked_against_the_host() {
    let library = load_with(|_| {}).unwrap();
    assert_eq!(library.foreign_interfaces(), [*FrameInterface::info()]);
    let mut registry = PluginRegistry::new();
    registry
        .register_foreign_interface::<FrameInterface>()
        .unwrap();
    library.install_into(&mut registry).unwrap();

    let newer =
        load_with(|descriptor| descriptor.foreign_interfaces = newer_foreign_interfaces).unwrap();
    // Unknown to the host: fine.
    newer.install_into(&mut PluginRegistry::new()).unwrap();
    let mut registry = PluginRegistry::new();
    registry
        .register_foreign_interface::<FrameInterface>()
        .unwrap();
    let err = newer.install_into(&mut registry).unwrap_err();
    let message = err.to_string();
    let PluginLibraryError::ForeignInterfaceMismatch { plugin, mismatches } = err else {
        panic!("unexpected error: {err:?}");
    };
    assert_eq!(plugin, "loader_test");
    assert_eq!(mismatches.len(), 1);
    assert_eq!(mismatches[0].host, *FrameInterface::info());
    assert_eq!(mismatches[0].plugin, *FrameV2::info());
    assert!(
        message.contains("`daedalus:frame`: host v1 (layout ")
            && message.contains("plugin v2 (layout "),
        "{message}"
    );
    assert!(!registry.plugin_manifests.contains_key("loader_test"));
}

#[test]
fn invalid_info_is_rejected() {
    let null = StrView {
        ptr: std::ptr::null(),
        len: 0,
    };
    assert!(matches!(
        load_with(|descriptor| descriptor.info.plugin_name = null),
        Err(PluginLibraryError::InvalidInfo {
            field: "plugin_name"
        })
    ));
    assert!(matches!(
        load_with(|descriptor| descriptor.info.build_fingerprint = null),
        Err(PluginLibraryError::InvalidInfo {
            field: "build_fingerprint"
        })
    ));
}

#[test]
fn abi_mismatch_is_rejected() {
    assert!(matches!(
        check_abi_version(4, PLUGIN_ABI_VERSION),
        Err(PluginLibraryError::AbiMismatch { expected, found: 4 }) if expected == PLUGIN_ABI_VERSION
    ));
}

#[test]
fn daedalus_version_mismatch_is_rejected() {
    let info = PluginInfo {
        daedalus_version: StrView::from_static("0.0.1"),
        ..host()
    };
    assert_eq!(
        check_rust_abi_against(&info, &host()),
        Err(RustAbiMismatch::DaedalusVersion {
            expected: crate::version().to_string(),
            found: "0.0.1".to_string(),
        })
    );
    check_rust_abi(&PluginInfo::for_plugin("plugin", "1.2.3")).unwrap();
}

#[test]
fn build_fingerprint_mismatch_is_rejected() {
    let plugin_fingerprint: &'static str = Box::leak(
        build_fingerprint()
            .replace("features.runtime=", "features.runtime=gpu,")
            .into_boxed_str(),
    );
    let info = PluginInfo {
        build_fingerprint: StrView::from_static(plugin_fingerprint),
        ..host()
    };
    let err = check_rust_abi_against(&info, &host()).unwrap_err();
    let message = err.to_string();
    match err {
        RustAbiMismatch::BuildFingerprint {
            expected,
            found,
            differences,
        } => {
            assert_eq!(expected, build_fingerprint());
            assert_eq!(found, plugin_fingerprint);
            assert!(
                differences.starts_with("features.runtime: host `")
                    && differences.contains("plugin `gpu,"),
                "{differences}"
            );
            assert!(message.contains(&differences));
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn missing_library_is_a_typed_error() {
    let err =
        unsafe { PluginLibrary::load("/nonexistent/libdaedalus_missing_plugin.so") }.unwrap_err();
    assert!(matches!(err, PluginLibraryError::Load { .. }));
}

#[test]
fn discovery_sorts_and_dedups_by_file_name() {
    let root =
        std::env::temp_dir().join(format!("daedalus-dylib-discovery-{}", std::process::id()));
    let first = root.join("first");
    let second = root.join("second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    for (dir, name) in [
        (&first, "libb.so"),
        (&first, "notes.txt"),
        (&second, "libb.so"),
        (&second, "liba.dylib"),
        (&second, "c.dll"),
    ] {
        std::fs::write(dir.join(name), b"").unwrap();
    }
    let found =
        discover_plugin_libraries([first.clone(), second.clone(), root.join("missing")]).unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    assert_eq!(
        found,
        vec![
            second.join("c.dll"),
            second.join("liba.dylib"),
            first.join("libb.so"),
        ]
    );
}
