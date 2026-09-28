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
