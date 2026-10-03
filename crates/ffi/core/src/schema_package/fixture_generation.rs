use std::collections::BTreeMap;

use super::*;
use crate::{
    DEFAULT_CORRELATION_ID, InvokeRequest, InvokeResponse, SCHEMA_VERSION, WORKER_PROTOCOL_VERSION,
};

pub fn generate_scalar_add_fixtures() -> Result<Vec<GeneratedLanguageFixture>, FfiContractError> {
    let spec = scalar_add_fixture_spec();
    fixture_languages()
        .into_iter()
        .map(|language| generate_language_fixture(&spec, language))
        .collect()
}

pub fn generate_canonical_fixtures() -> Result<Vec<GeneratedLanguageFixture>, FfiContractError> {
    let mut fixtures = Vec::new();
    for spec in canonical_fixture_specs() {
        for language in fixture_languages() {
            fixtures.push(generate_language_fixture(&spec, language)?);
        }
    }
    Ok(fixtures)
}

pub fn generate_scalar_add_package_fixtures()
-> Result<Vec<GeneratedPackageFixture>, FfiContractError> {
    generate_scalar_add_fixtures()?
        .into_iter()
        .map(package_fixture_from_language_fixture)
        .collect()
}

pub fn generate_language_fixture(
    spec: &CanonicalFixtureSpec,
    language: FixtureLanguage,
) -> Result<GeneratedLanguageFixture, FfiContractError> {
    let backend_kind = language.backend();
    let backend_kind = spec.backend.clone().unwrap_or(backend_kind);
    let entrypoint = fixture_entrypoint(spec, language);
    let schema = PluginSchema {
        schema_version: SCHEMA_VERSION,
        plugin: PluginSchemaInfo {
            name: format!("ffi.conformance.{}.{}", language.as_str(), spec.name),
            version: Some("1.0.0".into()),
            description: Some(format!("Generated {} conformance fixture", spec.name)),
            metadata: BTreeMap::from([("fixture_kind".into(), serde_json::json!(spec.kind))]),
        },
        dependencies: Vec::new(),
        required_host_capabilities: spec.required_host_capabilities.clone(),
        feature_flags: Vec::new(),
        boundary_contracts: spec.boundary_contracts.clone(),
        nodes: vec![NodeSchema {
            id: spec.node_id.clone(),
            backend: backend_kind.clone(),
            entrypoint: entrypoint.clone(),
            label: Some(fixture_label(&spec.name)),
            stateful: spec.stateful,
            feature_flags: Vec::new(),
            inputs: spec.inputs.clone(),
            outputs: spec.outputs.clone(),
            metadata: BTreeMap::from([("generated_from".into(), serde_json::json!(spec.name))]),
        }],
    };
    schema.validate()?;
    let backend = fixture_backend_config(language, &backend_kind, &entrypoint);
    backend.validate_for_node(&spec.node_id)?;
    let backends = BTreeMap::from([(spec.node_id.clone(), backend)]);
    let request = InvokeRequest {
        protocol_version: WORKER_PROTOCOL_VERSION,
        node_id: spec.node_id.clone(),
        correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
        args: spec.request_inputs.clone(),
        state: spec.request_state.clone(),
        context: BTreeMap::new(),
    };
    let expected_response = InvokeResponse {
        protocol_version: WORKER_PROTOCOL_VERSION,
        correlation_id: Some(DEFAULT_CORRELATION_ID.into()),
        outputs: spec.expected_outputs.clone(),
        state: spec.expected_state.clone(),
        events: spec.expected_events.clone(),
    };
    Ok(GeneratedLanguageFixture {
        language,
        schema,
        backends,
        request,
        expected_response,
        files: fixture_files(language),
    })
}

pub(crate) fn fixture_languages() -> [FixtureLanguage; 5] {
    [
        FixtureLanguage::Rust,
        FixtureLanguage::Python,
        FixtureLanguage::Node,
        FixtureLanguage::Java,
        FixtureLanguage::CCpp,
    ]
}

fn fixture_backend_config(
    language: FixtureLanguage,
    backend_kind: &BackendKind,
    entrypoint: &str,
) -> BackendConfig {
    let worker = |executable: &str| {
        BackendConfig::persistent_worker(backend_kind.clone(), executable, entrypoint)
            .with_entry_module(fixture_source_path(language))
    };
    match backend_kind {
        BackendKind::Rust => BackendConfig::in_process(BackendKind::Rust, entrypoint),
        BackendKind::Python => worker("python"),
        BackendKind::Node => worker("node"),
        BackendKind::Java => {
            BackendConfig::persistent_worker(BackendKind::Java, "java", entrypoint)
                .with_entry_class("ffi.conformance.ScalarAdd")
                .with_classpath(["classes"])
        }
        BackendKind::CCpp => BackendConfig::in_process(BackendKind::CCpp, entrypoint)
            .with_entry_module("libscalar_add.so"),
        BackendKind::Shader => BackendConfig::in_process(BackendKind::Shader, entrypoint)
            .with_entry_module("shaders/write_u32.wgsl"),
        BackendKind::Other(_) => worker("fixture-worker"),
    }
}

fn fixture_entrypoint(spec: &CanonicalFixtureSpec, language: FixtureLanguage) -> String {
    if spec.kind == CanonicalFixtureKind::Scalar {
        match language {
            FixtureLanguage::CCpp => "add_i64".into(),
            _ => "add".into(),
        }
    } else if language == FixtureLanguage::CCpp {
        format!("{}_ffi", spec.name)
    } else {
        "run".into()
    }
}

fn fixture_label(name: &str) -> String {
    name.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn fixture_source_path(language: FixtureLanguage) -> &'static str {
    match language {
        FixtureLanguage::Rust => "src/lib.rs",
        FixtureLanguage::Python => "scalar_add.py",
        FixtureLanguage::Node => "scalar_add.mjs",
        FixtureLanguage::Java => "ffi/conformance/ScalarAdd.java",
        FixtureLanguage::CCpp => "scalar_add.cpp",
    }
}

fn fixture_files(language: FixtureLanguage) -> Vec<GeneratedFixtureFile> {
    match language {
        FixtureLanguage::Rust => vec![GeneratedFixtureFile {
            path: "src/lib.rs".into(),
            contents: "#[daedalus::node(id = \"ffi.conformance.scalar_add:add\")]\npub fn add(a: i64, b: i64) -> i64 { a + b }\n".into(),
        }],
        FixtureLanguage::Python => vec![GeneratedFixtureFile {
            path: "scalar_add.py".into(),
            contents: "def add(a: int, b: int) -> int:\n    return a + b\n".into(),
        }],
        FixtureLanguage::Node => vec![GeneratedFixtureFile {
            path: "scalar_add.mjs".into(),
            contents: "export function add(a, b) {\n  return a + b;\n}\n".into(),
        }],
        FixtureLanguage::Java => vec![GeneratedFixtureFile {
            path: "ffi/conformance/ScalarAdd.java".into(),
            contents: "package ffi.conformance;\n\npublic final class ScalarAdd {\n  public static long add(long a, long b) { return a + b; }\n}\n".into(),
        }],
        FixtureLanguage::CCpp => vec![GeneratedFixtureFile {
            path: "scalar_add.cpp".into(),
            contents: "#include <cstdint>\nextern \"C\" int64_t add_i64(int64_t a, int64_t b) { return a + b; }\n".into(),
        }],
    }
}

fn package_fixture_from_language_fixture(
    fixture: GeneratedLanguageFixture,
) -> Result<GeneratedPackageFixture, FfiContractError> {
    let metadata = BTreeMap::from([
        (
            "language".into(),
            serde_json::json!(fixture.language.as_str()),
        ),
        ("fixture".into(), serde_json::json!("scalar_add")),
    ]);
    let mut files = Vec::with_capacity(fixture.files.len());
    let mut artifacts = Vec::with_capacity(fixture.files.len());
    for file in fixture.files {
        let mut artifact = PackageArtifact::bundled(
            PackageArtifactKind::SourceFile,
            Some(fixture.language.backend()),
            &file.path,
            None,
        )?;
        artifact.metadata = metadata.clone();
        files.push(GeneratedFixtureFile {
            path: artifact.path.clone(),
            contents: file.contents,
        });
        artifacts.push(artifact);
    }
    let package = PluginPackage {
        artifacts,
        metadata,
        ..PluginPackage::new(fixture.schema, fixture.backends)
    };
    package.validate()?;
    Ok(GeneratedPackageFixture {
        language: fixture.language,
        package,
        files,
    })
}
