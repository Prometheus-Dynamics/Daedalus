//! [`GraphDocument`] entry points: check plugin requirements, then delegate to the regular
//! plugin-registry compile/prepare paths.

use crate::prelude::*;
use daedalus_planner::GraphDocument;
use daedalus_runtime::HostBridgeManager;
use daedalus_runtime::executor::NodeHandler;
use daedalus_runtime::handler_registry::HandlerRegistry;
use daedalus_runtime::plugins::PluginRegistry;

use crate::engine::Engine;
use crate::error::EngineError;
use crate::host_graph::HostGraph;
use crate::prepared_plan::PreparedPlan;

impl Engine {
    /// Verify that every plugin a document `requires` is installed in `plugins`.
    pub fn check_document(
        &self,
        plugins: &PluginRegistry,
        document: &GraphDocument,
    ) -> Result<(), EngineError> {
        plugins.check_document(document)?;
        Ok(())
    }

    /// Check document requirements, then [`Engine::prepare_plugin_registry`].
    pub fn prepare_document(
        &self,
        plugins: &PluginRegistry,
        document: GraphDocument,
    ) -> Result<PreparedPlan, EngineError> {
        self.check_document(plugins, &document)?;
        self.prepare_plugin_registry(plugins, document.graph)
    }

    /// Check document requirements, then [`Engine::compile_registry`].
    pub fn compile_document(
        &self,
        plugins: &PluginRegistry,
        document: GraphDocument,
    ) -> Result<HostGraph<HandlerRegistry>, EngineError> {
        self.check_document(plugins, &document)?;
        self.compile_registry(plugins, document.graph)
    }

    /// Check document requirements, then [`Engine::compile_host_graph_plugin_registry`].
    pub fn compile_document_host_graph<H: NodeHandler + Send + Sync + 'static>(
        &self,
        plugins: &PluginRegistry,
        document: GraphDocument,
        handler: H,
        bridges: HostBridgeManager,
        host_alias: impl Into<String>,
    ) -> Result<HostGraph<H>, EngineError> {
        self.check_document(plugins, &document)?;
        self.compile_host_graph_plugin_registry(
            plugins,
            document.graph,
            handler,
            bridges,
            host_alias,
        )
    }
}
