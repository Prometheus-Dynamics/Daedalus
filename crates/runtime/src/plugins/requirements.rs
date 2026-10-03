use super::*;
use daedalus_planner::{Graph, GraphDocument, MissingPlugins, PluginRequirement};

impl PluginRegistry {
    /// Whether a plugin (or plugin group) with this id is installed, and its declared version.
    ///
    /// Returns `None` when not installed, `Some(None)` when installed without a version.
    pub fn installed_plugin_version(&self, id: &str) -> Option<Option<&str>> {
        self.plugin_manifests
            .get(id)
            .map(|manifest| manifest.version.as_deref())
    }

    /// Check graph-document plugin requirements against the installed plugins.
    pub fn check_requirements(&self, requires: &[PluginRequirement]) -> Result<(), MissingPlugins> {
        daedalus_planner::check_plugin_requirements(requires, |id| {
            self.installed_plugin_version(id)
        })
    }

    /// Check a graph document's `requires` against the installed plugins.
    pub fn check_document(&self, document: &GraphDocument) -> Result<(), MissingPlugins> {
        self.check_requirements(&document.requires)
    }

    /// Derive plugin requirements for a graph: every installed non-built-in plugin whose manifest
    /// provides one of the graph's node ids, sorted by id and deduplicated.
    ///
    /// Requirements carry no version constraint; node ids with no plugin provider (manual or
    /// built-in declarations) contribute nothing. When plugins are nested, every plugin whose
    /// manifest lists the node is included.
    pub fn graph_requirements(&self, graph: &Graph) -> Vec<PluginRequirement> {
        let node_ids: BTreeSet<&NodeId> = graph.nodes.iter().map(|node| &node.id).collect();
        self.plugin_manifests
            .values()
            .filter(|manifest| {
                self.provider_source_kind(&manifest.id) != CapabilitySourceKind::BuiltIn
            })
            .filter(|manifest| {
                manifest
                    .provided_nodes
                    .iter()
                    .any(|id| node_ids.contains(id))
            })
            .map(|manifest| PluginRequirement::new(manifest.id.clone()))
            .collect()
    }

    /// Wrap a graph in a [`GraphDocument`] whose `requires` are derived via
    /// [`PluginRegistry::graph_requirements`].
    pub fn graph_document(&self, graph: Graph) -> GraphDocument {
        let requires = self.graph_requirements(&graph);
        GraphDocument::new(graph).with_requires(requires)
    }
}
