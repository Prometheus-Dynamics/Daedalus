#![cfg(feature = "plugins")]

use daedalus_engine::{Engine, EngineConfig, EngineError};
use daedalus_planner::{Graph, GraphDocument, PluginRequirement};
use daedalus_registry::capability::{NodeDecl, PluginManifest};
use daedalus_runtime::plugins::{
    Plugin, PluginInstallContext, PluginRegistry, PluginResult, RegistryPluginExt,
};

struct DemoPlugin;

impl Plugin for DemoPlugin {
    fn id(&self) -> &'static str {
        "engine_doc.demo"
    }

    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(self.id()).version("0.3.0")
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_node_decl(NodeDecl::new("engine_doc.demo:noop"))
    }
}

fn plugins() -> PluginRegistry {
    let mut plugins = PluginRegistry::new();
    plugins.install_plugin(&DemoPlugin).unwrap();
    plugins
}

#[test]
fn prepare_document_checks_requirements_first() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let plugins = plugins();

    let missing = GraphDocument::new(Graph::default())
        .require(PluginRequirement::new("engine_doc.demo").with_version(">=0.2"))
        .require(PluginRequirement::new("engine_doc.absent"));
    match engine.prepare_document(&plugins, missing) {
        Err(EngineError::MissingPlugins(err)) => assert_eq!(err.ids(), vec!["engine_doc.absent"]),
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected missing plugin error"),
    }

    let ok = GraphDocument::new(Graph::default())
        .require(PluginRequirement::new("engine_doc.demo").with_version(">=0.2"));
    engine.check_document(&plugins, &ok).unwrap();
    engine.prepare_document(&plugins, ok).unwrap();
}

#[test]
fn compile_document_rejects_missing_plugins() {
    let engine = Engine::new(EngineConfig::default()).unwrap();
    let doc = GraphDocument::new(Graph::default())
        .require(PluginRequirement::new("engine_doc.demo").with_version("=9.9.9"));
    let err = engine.compile_document(&plugins(), doc).err().unwrap();
    assert!(matches!(err, EngineError::MissingPlugins(_)));
    assert!(err.to_string().contains("engine_doc.demo"));
}
