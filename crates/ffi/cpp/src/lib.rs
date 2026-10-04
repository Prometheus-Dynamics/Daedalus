//! C and C++ FFI ABI and packaging integration.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use core::{
    BackendConfig, BackendKind, FfiContractError, LanguagePackageInput, LanguagePackager,
    PackageArtifactKind, PluginPackage, PluginSchema, WirePayloadHandle, package_artifacts,
};
use daedalus_transport::{AccessMode, Payload};
use thiserror::Error;

pub use daedalus_ffi_core as core;

pub const CPP_PACKAGER: LanguagePackager =
    LanguagePackager::new(BackendKind::CCpp, "c_cpp", "daedalus-ffi-cpp");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CppPointerLengthAbi {
    pub pointer_type: String,
    pub length_type: String,
    pub mutable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CppResolvedPointerView<'a> {
    pub ptr: *const u8,
    pub mut_ptr: Option<*mut u8>,
    pub len: usize,
    pub access: AccessMode,
    lifetime: PhantomData<&'a [u8]>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CppPayloadResolveError {
    #[error(
        "payload handle `{handle_id}` type `{handle_type}` does not match payload type `{payload_type}`"
    )]
    TypeMismatch {
        handle_id: String,
        handle_type: String,
        payload_type: String,
    },
    #[error(
        "payload handle `{handle_id}` access `{found}` does not satisfy requested `{required}`"
    )]
    AccessMismatch {
        handle_id: String,
        required: AccessMode,
        found: AccessMode,
    },
    #[error("payload handle `{0}` does not point at byte-addressable storage")]
    NotByteAddressable(String),
    #[error("mutable pointer access for `{0}` requires unique byte storage")]
    MutableRequiresUniqueStorage(String),
}

impl CppPointerLengthAbi {
    pub fn bytes_view() -> Self {
        Self {
            pointer_type: "const uint8_t*".into(),
            length_type: "size_t".into(),
            mutable: false,
        }
    }

    pub fn mutable_bytes() -> Self {
        Self {
            pointer_type: "uint8_t*".into(),
            length_type: "size_t".into(),
            mutable: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([(
            "pointer_length_abi".into(),
            serde_json::json!({
                "pointer_type": self.pointer_type,
                "length_type": self.length_type,
                "mutable": self.mutable,
            }),
        )])
    }
}

pub fn resolve_cpp_payload_handle<'a>(
    handle: &WirePayloadHandle,
    payload: &'a Payload,
    required_access: AccessMode,
) -> Result<CppResolvedPointerView<'a>, CppPayloadResolveError> {
    if &handle.type_key != payload.type_key() {
        return Err(CppPayloadResolveError::TypeMismatch {
            handle_id: handle.id.clone(),
            handle_type: handle.type_key.to_string(),
            payload_type: payload.type_key().to_string(),
        });
    }
    if !handle.access.satisfies(required_access) {
        return Err(CppPayloadResolveError::AccessMismatch {
            handle_id: handle.id.clone(),
            required: required_access,
            found: handle.access,
        });
    }
    let bytes = payload
        .value_any()
        .and_then(|value| value.downcast_ref::<Arc<[u8]>>())
        .ok_or_else(|| CppPayloadResolveError::NotByteAddressable(handle.id.clone()))?;
    let mut_ptr = if matches!(required_access, AccessMode::Modify | AccessMode::Move) {
        return Err(CppPayloadResolveError::MutableRequiresUniqueStorage(
            handle.id.clone(),
        ));
    } else {
        None
    };
    Ok(CppResolvedPointerView {
        ptr: bytes.as_ptr(),
        mut_ptr,
        len: bytes.len(),
        access: required_access,
        lifetime: PhantomData,
    })
}

pub fn resolve_cpp_payload_handle_mut<'a>(
    handle: &WirePayloadHandle,
    bytes: &'a mut [u8],
) -> Result<CppResolvedPointerView<'a>, CppPayloadResolveError> {
    if !handle.access.satisfies(AccessMode::Modify) {
        return Err(CppPayloadResolveError::AccessMismatch {
            handle_id: handle.id.clone(),
            required: AccessMode::Modify,
            found: handle.access,
        });
    }
    Ok(CppResolvedPointerView {
        ptr: bytes.as_ptr(),
        mut_ptr: Some(bytes.as_mut_ptr()),
        len: bytes.len(),
        access: AccessMode::Modify,
        lifetime: PhantomData,
    })
}

pub fn cpp_in_process_backend_config(
    library_path: impl Into<String>,
    symbol: impl Into<String>,
) -> BackendConfig {
    BackendConfig::in_process(BackendKind::CCpp, symbol).with_entry_module(library_path)
}

pub fn cpp_in_process_backend_config_with_pointer_abi(
    library_path: impl Into<String>,
    symbol: impl Into<String>,
    abi: CppPointerLengthAbi,
) -> BackendConfig {
    cpp_in_process_backend_config(library_path, symbol).with_options(abi.backend_options())
}

/// Package C/C++ shared libraries and sources with the default lockfile.
pub fn cpp_plugin_package(
    schema: PluginSchema,
    backends: BTreeMap<String, BackendConfig>,
    shared_libraries: Vec<String>,
    source_files: Vec<String>,
) -> Result<PluginPackage, FfiContractError> {
    let mut artifacts = package_artifacts(
        PackageArtifactKind::SharedLibrary,
        &BackendKind::CCpp,
        shared_libraries,
    )?;
    artifacts.extend(package_artifacts(
        PackageArtifactKind::SourceFile,
        &BackendKind::CCpp,
        source_files,
    )?);
    CPP_PACKAGER.build(LanguagePackageInput::new(schema, backends, artifacts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_data::model::{TypeExpr, ValueType};
    use daedalus_ffi_core::{
        FixtureLanguage, NodeSchema, WirePort, generate_language_fixture, scalar_add_fixture_spec,
        validate_language_backends,
    };

    fn port(name: &str) -> WirePort {
        WirePort::new(name, TypeExpr::scalar(ValueType::Int))
    }

    fn cpp_plugin_schema(
        name: &str,
        version: Option<String>,
        nodes: Vec<NodeSchema>,
    ) -> Result<PluginSchema, FfiContractError> {
        PluginSchema::for_backend(name, version, nodes, BackendKind::CCpp)
    }

    fn validate_cpp_schema(
        schema: &PluginSchema,
        backends: &BTreeMap<String, BackendConfig>,
    ) -> Result<(), FfiContractError> {
        validate_language_backends(schema, backends, BackendKind::CCpp)
    }

    #[test]
    fn builds_and_validates_cpp_schema_helpers() {
        let node = NodeSchema::new(
            "demo:add",
            BackendKind::CCpp,
            "add_i32",
            vec![port("a")],
            vec![port("out")],
        );
        let schema =
            cpp_plugin_schema("demo.cpp", Some("1.0.0".into()), vec![node]).expect("schema");
        let backends = BTreeMap::from([(
            "demo:add".into(),
            cpp_in_process_backend_config("libdemo.so", "add_i32"),
        )]);

        validate_cpp_schema(&schema, &backends).expect("valid cpp schema");
        assert!(matches!(
            cpp_plugin_schema(
                "bad",
                None,
                vec![NodeSchema::new(
                    "bad:add",
                    BackendKind::Python,
                    "add",
                    vec![],
                    vec![]
                )],
            ),
            Err(FfiContractError::UnexpectedBackendKind { .. })
        ));
    }

    #[test]
    fn sdk_builders_match_rust_baseline_schema_surface() {
        let spec = scalar_add_fixture_spec();
        let rust = generate_language_fixture(&spec, FixtureLanguage::Rust).expect("rust fixture");
        let cpp = generate_language_fixture(&spec, FixtureLanguage::CCpp).expect("cpp fixture");
        let baseline = &rust.schema.nodes[0];

        let node = NodeSchema::new(
            baseline.id.clone(),
            BackendKind::CCpp,
            cpp.schema.nodes[0].entrypoint.clone(),
            baseline.inputs.clone(),
            baseline.outputs.clone(),
        );
        let schema = cpp_plugin_schema(
            "ffi.conformance.c_cpp.scalar_add",
            Some("1.0.0".into()),
            vec![node],
        )
        .expect("schema");
        let backend = cpp_in_process_backend_config("libscalar_add.so", "add_i64");
        let backends = BTreeMap::from([(baseline.id.clone(), backend.clone())]);
        let package = cpp_plugin_package(
            schema.clone(),
            backends.clone(),
            vec!["libscalar_add.so".into()],
            Vec::new(),
        )
        .expect("package");

        assert_eq!(schema.nodes[0].id, baseline.id);
        assert_eq!(schema.nodes[0].inputs, baseline.inputs);
        assert_eq!(schema.nodes[0].outputs, baseline.outputs);
        assert_eq!(schema.nodes[0].stateful, baseline.stateful);
        assert_eq!(backend, cpp.backends[&baseline.id]);
        assert_eq!(package.schema.as_ref(), Some(&schema));
        assert_eq!(package.backends, backends);
        assert_eq!(
            package.artifacts[0].path,
            "_bundle/native/any/libscalar_add.so"
        );
        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        validate_cpp_schema(&schema, &package.backends).expect("valid package schema");
    }

    #[test]
    fn cpp_transport_options_describe_pointer_length_abi() {
        let backend = cpp_in_process_backend_config_with_pointer_abi(
            "libplugin.so",
            "run",
            CppPointerLengthAbi::bytes_view(),
        );
        assert_eq!(
            backend.options.get("pointer_length_abi"),
            Some(&serde_json::json!({
                "pointer_type": "const uint8_t*",
                "length_type": "size_t",
                "mutable": false
            }))
        );
    }

    #[test]
    fn cpp_resolves_payload_handles_to_pointer_length_views() {
        let bytes = Arc::<[u8]>::from(vec![1_u8, 2, 3, 4]);
        let payload = Payload::bytes_with_type_key("bytes", bytes.clone());
        let handle = WirePayloadHandle::from_payload("lease-1", &payload, AccessMode::Read);

        let view = resolve_cpp_payload_handle(&handle, &payload, AccessMode::Read)
            .expect("resolve read pointer");
        assert_eq!(view.ptr, bytes.as_ptr());
        assert_eq!(view.len, 4);
        assert_eq!(view.mut_ptr, None);
        assert_eq!(view.access, AccessMode::Read);

        assert!(matches!(
            resolve_cpp_payload_handle(&handle, &payload, AccessMode::Modify),
            Err(CppPayloadResolveError::AccessMismatch { .. })
        ));
    }

    #[test]
    fn cpp_resolves_mutable_payload_handles_to_mut_pointer_length_views() {
        let payload = Payload::bytes_with_type_key("bytes", Arc::<[u8]>::from(vec![1_u8]));
        let handle = WirePayloadHandle::from_payload("lease-2", &payload, AccessMode::Modify);
        let mut bytes = vec![1_u8, 2, 3];
        let ptr = bytes.as_ptr();
        let mut_ptr = bytes.as_mut_ptr();

        let view =
            resolve_cpp_payload_handle_mut(&handle, &mut bytes).expect("resolve mut pointer");
        assert_eq!(view.ptr, ptr);
        assert_eq!(view.mut_ptr, Some(mut_ptr));
        assert_eq!(view.len, 3);
        assert_eq!(view.access, AccessMode::Modify);
    }

    #[test]
    fn complete_cpp_package_emits_lockfile_hash_sources_and_language_metadata() {
        let spec = scalar_add_fixture_spec();
        let fixture = generate_language_fixture(&spec, FixtureLanguage::CCpp).expect("cpp fixture");
        let package = cpp_plugin_package(
            fixture.schema.clone(),
            fixture.backends.clone(),
            vec!["build/libffi_showcase.so".into()],
            vec!["src/showcase.cpp".into(), "build-package.cpp".into()],
        )
        .expect("complete package");
        let lock = package.generate_lockfile();

        assert_eq!(package.lockfile.as_deref(), Some("plugin.lock.json"));
        assert!(package.manifest_hash.is_some());
        assert_eq!(
            package.metadata.get("package_builder"),
            Some(&serde_json::json!("daedalus-ffi-cpp"))
        );
        assert_eq!(package.artifacts.len(), 3);
        assert_eq!(
            lock.plugin_name.as_deref(),
            Some("ffi.conformance.c_cpp.scalar_add")
        );
        assert_eq!(lock.artifacts.len(), 3);
    }

    /// Compiles `source` against the SDK header into a uniquely named binary; `None` when no C++20
    /// compiler is installed.
    fn compile_cpp(source: &str, name: &str) -> Option<std::process::Output> {
        let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".into());
        std::process::Command::new(&cxx)
            .arg("--version")
            .output()
            .ok()?;
        let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
        let binary =
            std::env::temp_dir().join(format!("daedalus-cpp-{name}-{}", std::process::id()));
        let compiled = std::process::Command::new(&cxx)
            .args([
                "-std=c++20",
                "-Wall",
                "-Wextra",
                "-Werror",
                "-Wno-unused-parameter",
                "-I",
            ])
            .arg(sdk.join("include"))
            .args(["-x", "c++", "-"])
            .arg("-o")
            .arg(&binary)
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin")
                    .write_all(source.as_bytes())?;
                child.wait_with_output()
            })
            .expect("run C++ compiler");
        if !compiled.status.success() {
            return Some(compiled);
        }
        let ran = std::process::Command::new(&binary)
            .output()
            .expect("run C++ test");
        let _ = std::fs::remove_file(&binary);
        Some(ran)
    }

    fn sdk_test_source(name: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("sdk/tests")
            .join(name);
        std::fs::read_to_string(path).expect("read C++ SDK test")
    }

    /// `sdk/tests/sdk_descriptor.cpp`: descriptor shape and port types deduced from each node's
    /// function. Skipped when no C++20 compiler is installed.
    #[test]
    fn cpp_sdk_descriptor_test_passes() {
        let Some(output) = compile_cpp(&sdk_test_source("sdk_descriptor.cpp"), "descriptor") else {
            return;
        };
        assert!(
            output.status.success(),
            "C++ SDK descriptor test failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// `sdk/tests/wire.cpp` round trips and prints its encoder output: `uint64_t` values must
    /// decode as `WireValue::UInt` in Rust.
    #[test]
    fn cpp_sdk_wire_values_decode_in_rust() {
        let Some(output) = compile_cpp(&sdk_test_source("wire.cpp"), "wire") else {
            return;
        };
        assert!(
            output.status.success(),
            "C++ SDK wire test failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let values: Vec<daedalus_ffi_core::WireValue> =
            serde_json::from_slice(&output.stdout).expect("wire values");
        assert_eq!(
            values[..3],
            [
                daedalus_ffi_core::WireValue::UInt(u64::MAX),
                daedalus_ffi_core::WireValue::UInt(5),
                daedalus_ffi_core::WireValue::Int(-3)
            ]
        );
        assert_eq!(
            values[3],
            daedalus_ffi_core::WireValue::String("a\"b\\c\n\t".into())
        );
    }

    /// Registrations whose port types or counts do not match the function fail to compile with
    /// the SDK's `static_assert` message.
    #[test]
    fn cpp_sdk_rejects_unmapped_types_and_port_counts_at_compile_time() {
        let cases = [
            (
                "struct Opaque {};\nint64_t run(Opaque value) { return 0; }\n\
                 DAEDALUS_NODE(run, inputs(value), outputs(out))",
                "has no Daedalus mapping",
            ),
            (
                "int64_t run(int64_t a) { return a; }\nDAEDALUS_NODE(run, inputs(a, b), outputs(out))",
                "more inputs than its function has parameters",
            ),
            (
                "int64_t run(int64_t a) { return a; }\nDAEDALUS_NODE(run, inputs(a), outputs(x, y))",
                "output count does not match",
            ),
        ];
        for (node, message) in cases {
            let source = format!("#include <daedalus.hpp>\n{node}\nint main() {{}}\n");
            let Some(output) = compile_cpp(&source, "static-assert") else {
                return;
            };
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                !output.status.success() && stderr.contains(message),
                "expected a compile error containing `{message}`:\n{stderr}"
            );
        }
    }
}
