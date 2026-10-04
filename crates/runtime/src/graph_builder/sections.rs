//! Scoped graph sections (`inputs`/`outputs`/`nodes`/`edges`) and subgraph embedding.

use crate::prelude::*;
use daedalus_planner::Graph;

use super::{GraphBuildError, GraphBuilder, GraphScope, NestedGraph, NestedGraphHandle};

impl GraphBuilder {
    /// Inline a graph using its first host bridge and return a nested graph handle.
    ///
    /// # Errors
    ///
    /// Returns an error when `graph` does not contain a host bridge.
    ///
    /// # Panics
    ///
    /// Panics when `alias` is already used by a node or nested graph in this
    /// builder.
    pub fn subgraph(
        self,
        alias: impl Into<String>,
        graph: Graph,
    ) -> Result<(Self, NestedGraphHandle), GraphBuildError> {
        let nested = NestedGraph::first_host(graph)?;
        self.try_nest(&nested, alias)
    }

    pub fn try_subgraph(
        self,
        alias: impl Into<String>,
        graph: Graph,
    ) -> Result<(Self, NestedGraphHandle), GraphBuildError> {
        let nested = NestedGraph::first_host(graph)?;
        self.try_nest(&nested, alias)
    }

    /// Run a scoped convenience closure for examples/tests; use `try_*` scoped helpers to return
    /// graph construction errors.
    pub(super) fn scoped(self, define: impl FnOnce(&mut GraphScope)) -> Self {
        let mut scope = GraphScope::new(self);
        define(&mut scope);
        scope.into_builder()
    }

    pub(super) fn try_scoped(
        self,
        define: impl FnOnce(&mut GraphScope) -> Result<(), GraphBuildError>,
    ) -> Result<Self, GraphBuildError> {
        let mut scope = GraphScope::new(self);
        define(&mut scope)?;
        Ok(scope.into_builder())
    }

    /// Define graph inputs with a scoped convenience closure.
    ///
    /// Prefer [`Self::try_inputs`] for release-facing code that should return construction errors.
    ///
    /// # Panics
    ///
    /// Panics if the closure calls non-fallible `GraphScope` helpers with invalid wiring.
    pub fn inputs(self, define: impl FnOnce(&mut GraphScope)) -> Self {
        self.scoped(define)
    }

    /// Fallible scoped graph input definition helper.
    pub fn try_inputs(
        self,
        define: impl FnOnce(&mut GraphScope) -> Result<(), GraphBuildError>,
    ) -> Result<Self, GraphBuildError> {
        self.try_scoped(define)
    }

    /// Define graph outputs with a scoped convenience closure.
    ///
    /// Prefer [`Self::try_outputs`] for release-facing code that should return construction errors.
    ///
    /// # Panics
    ///
    /// Panics if the closure calls non-fallible `GraphScope` helpers with invalid wiring.
    pub fn outputs(self, define: impl FnOnce(&mut GraphScope)) -> Self {
        self.scoped(define)
    }

    /// Fallible scoped graph output definition helper.
    pub fn try_outputs(
        self,
        define: impl FnOnce(&mut GraphScope) -> Result<(), GraphBuildError>,
    ) -> Result<Self, GraphBuildError> {
        self.try_scoped(define)
    }

    /// Define graph nodes with a scoped convenience closure.
    ///
    /// Prefer [`Self::try_nodes`] for release-facing code that should return construction errors.
    ///
    /// # Panics
    ///
    /// Panics if the closure calls non-fallible `GraphScope` helpers with invalid wiring.
    pub fn nodes(self, define: impl FnOnce(&mut GraphScope)) -> Self {
        self.scoped(define)
    }

    /// Fallible scoped graph node definition helper.
    pub fn try_nodes(
        self,
        define: impl FnOnce(&mut GraphScope) -> Result<(), GraphBuildError>,
    ) -> Result<Self, GraphBuildError> {
        self.try_scoped(define)
    }

    /// Define graph edges with a scoped convenience closure.
    ///
    /// Prefer [`Self::try_edges`] for release-facing code that should return construction errors.
    ///
    /// # Panics
    ///
    /// Panics on invalid non-fallible `GraphScope` helper calls.
    pub fn edges(self, define: impl FnOnce(&mut GraphScope)) -> Self {
        self.scoped(define)
    }

    /// Fallible scoped edge definition helper for release-facing graph construction.
    pub fn try_edges(
        self,
        define: impl FnOnce(&mut GraphScope) -> Result<(), GraphBuildError>,
    ) -> Result<Self, GraphBuildError> {
        self.try_scoped(define)
    }
}
