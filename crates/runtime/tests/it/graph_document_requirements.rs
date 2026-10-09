#![cfg(feature = "plugins")]

use daedalus_data::model::TypeExpr;
use daedalus_planner::{Graph, GraphDocument, NodeInstance, PluginRequirement, UnmetReason};
use daedalus_registry::capability::{NodeDecl, PluginManifest, PortDecl};
use daedalus_runtime::plugins::{
    Plugin, PluginInstallContext, PluginRegistry, PluginResult, RegistryPluginExt,
};

struct VersionedPlugin;

impl Plugin for VersionedPlugin {
    fn id(&self) -> &'static str {
        "docreq.math"
    }

    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(self.id()).version("1.4.0")
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_node_decl(
            NodeDecl::new("docreq.math:source")
                .output(PortDecl::new("out", "docreq:i32").schema(TypeExpr::opaque("docreq:i32"))),
        )
    }
}

struct UnversionedPlugin;

impl Plugin for UnversionedPlugin {
    fn id(&self) -> &'static str {
        "docreq.io"
    }

    fn install(&self, ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        ctx.register_node_decl(NodeDecl::new("docreq.io:unused"))
    }
}

fn registry() -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&VersionedPlugin).unwrap();
    registry.install_plugin(&UnversionedPlugin).unwrap();
    registry
}

#[test]
fn check_requirements_passes_for_installed_plugins() {
    let registry = registry();
    let doc = GraphDocument::new(Graph::default())
        .require(PluginRequirement::new("docreq.math").with_version(">=1.2.0"))
        .require(PluginRequirement::new("docreq.io"));
    registry.check_document(&doc).unwrap();
    assert_eq!(
        registry.installed_plugin_version("docreq.math"),
        Some(Some("1.4.0"))
    );
    assert_eq!(registry.installed_plugin_version("docreq.io"), Some(None));
    assert_eq!(registry.installed_plugin_version("nope"), None);
}

#[test]
fn check_requirements_reports_missing_and_mismatched_plugins() {
    let registry = registry();
    let err = registry
        .check_requirements(&[
            PluginRequirement::new("docreq.math").with_version(">=2.0.0"),
            PluginRequirement::new("docreq.io").with_version("=1.0.0"),
            PluginRequirement::new("docreq.missing"),
        ])
        .unwrap_err();
    assert_eq!(
        err.ids(),
        vec!["docreq.math", "docreq.io", "docreq.missing"]
    );
    assert_eq!(
        err.unmet[0].reason,
        UnmetReason::VersionMismatch {
            installed: "1.4.0".into()
        }
    );
    assert_eq!(err.unmet[1].reason, UnmetReason::VersionUnknown);
    assert_eq!(err.unmet[2].reason, UnmetReason::NotInstalled);
}

#[test]
fn graph_requirements_are_derived_from_node_providers() {
    let registry = registry();
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("docreq.math:source"));
    graph.nodes.push(NodeInstance::new("docreq.math:source"));
    graph.nodes.push(NodeInstance::new("unknown:node"));
    assert_eq!(
        registry.graph_requirements(&graph),
        vec![PluginRequirement::new("docreq.math")]
    );
    let doc = registry.graph_document(graph);
    assert_eq!(doc.requires, vec![PluginRequirement::new("docreq.math")]);
    registry.check_document(&doc).unwrap();
}
