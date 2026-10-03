use daedalus::prelude::*;

#[test]
fn graph_document_is_reexported_through_facade() {
    let doc = GraphDocument::new(daedalus::planner::Graph::default())
        .require(PluginRequirement::new("facade.demo").with_version(">=1.0.0"));
    let json = doc.to_json_pretty().unwrap();
    assert!(json.contains(daedalus::GRAPH_DOCUMENT_FORMAT));
    let parsed = GraphDocument::from_json(&json).unwrap();
    assert_eq!(
        parsed.schema_version,
        daedalus::GRAPH_DOCUMENT_SCHEMA_VERSION
    );
    assert_eq!(parsed, doc);
}

#[cfg(feature = "plugins")]
#[test]
fn plugin_registry_checks_document_requirements() {
    let registry = PluginRegistry::new();
    let doc = GraphDocument::new(daedalus::planner::Graph::default())
        .require(PluginRequirement::new("facade.absent"));
    let err: MissingPlugins = registry.check_document(&doc).unwrap_err();
    assert_eq!(err.ids(), vec!["facade.absent"]);
}

#[cfg(feature = "plugins")]
mod versioned_plugin {
    use daedalus::{
        macros::{node, plugin},
        runtime::NodeError,
    };

    #[node(id = "facade.versioned.echo", inputs("value"), outputs("out"))]
    fn echo(value: i64) -> Result<i64, NodeError> {
        Ok(value)
    }

    #[plugin(id = "facade.versioned", nodes(echo))]
    pub struct VersionedPlugin;
}

#[cfg(feature = "plugins")]
#[test]
fn plugins_record_their_crate_version_for_requirements() {
    let mut registry = PluginRegistry::new();
    registry
        .install(&versioned_plugin::VersionedPlugin::new())
        .unwrap();
    assert_eq!(
        registry.installed_plugin_version("facade.versioned"),
        Some(Some(env!("CARGO_PKG_VERSION")))
    );
    let doc = GraphDocument::new(daedalus::planner::Graph::default()).require(
        PluginRequirement::new("facade.versioned")
            .with_version(format!(">={}", env!("CARGO_PKG_VERSION"))),
    );
    registry.check_document(&doc).unwrap();
}
