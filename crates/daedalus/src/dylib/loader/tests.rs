use super::*;
use crate::dylib::{RUSTC_VERSION, build_fingerprint};
use crate::registry::capability::{NodeDecl, PortDecl};
use crate::runtime::plugins::{Plugin, PluginInstallContext, PluginResult};

#[derive(Default)]
struct LoaderTestPlugin;

impl Plugin for LoaderTestPlugin {
    fn id(&self) -> &'static str {
        "loader_test"
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_node_decl(
            NodeDecl::new("add")
                .input(PortDecl::new("a", "i64"))
                .output(PortDecl::new("out", "i64")),
        )
    }
}

crate::export_plugin!(LoaderTestPlugin);

fn host() -> PluginInfo {
    PluginInfo::for_plugin("host", "0.0.0")
}

/// This test binary's own descriptor with `info` adjusted, as if a different build produced it.
fn load_with(adjust: impl FnOnce(&mut PluginInfo)) -> Result<PluginLibrary, PluginLibraryError> {
    let mut descriptor = daedalus_plugin_descriptor();
    adjust(&mut descriptor.info);
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
    assert_eq!(node.outputs[0].name, "out");

    let mut registry = PluginRegistry::new();
    library.install_into(&mut registry).unwrap();
    assert!(registry.plugin_manifests.contains_key("loader_test"));
}

#[test]
fn mismatched_toolchain_is_introspectable_but_not_installable() {
    let library = load_with(|info| {
        info.rustc_version = StrView::from_static("rustc 1.0.0 (a59aba2b1 2015-05-13)");
    })
    .unwrap();
    assert_eq!(library.schema().plugin.name, "loader_test");
    assert_eq!(library.schema().nodes.len(), 1);
    let expected = RustAbiMismatch::Rustc {
        expected: RUSTC_VERSION.to_string(),
        found: "rustc 1.0.0 (a59aba2b1 2015-05-13)".to_string(),
    };
    assert_eq!(library.rust_abi(), Err(&expected));

    let mut registry = PluginRegistry::new();
    let err = library.install_into(&mut registry).unwrap_err();
    assert!(
        err.to_string()
            .starts_with("plugin `loader_test` cannot be installed")
    );
    assert!(
        matches!(err, PluginLibraryError::Incompatible { ref plugin, ref mismatch }
            if plugin == "loader_test" && *mismatch == expected),
        "{err:?}"
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
        load_with(|info| info.plugin_name = null),
        Err(PluginLibraryError::InvalidInfo {
            field: "plugin_name"
        })
    ));
    assert!(matches!(
        load_with(|info| info.build_fingerprint = null),
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
