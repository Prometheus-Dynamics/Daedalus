use super::{ExecutorMaskError, MetricsLevel};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ExecutorRunConfig {
    pub(crate) active_nodes: Option<Arc<Vec<bool>>>,
    pub(crate) active_edges: Option<Arc<Vec<bool>>>,
    pub(crate) active_direct_edges: Option<Arc<Vec<bool>>>,
    pub(crate) selected_host_output_ports: Option<Arc<HashSet<String>>>,
    pub(crate) fail_fast: bool,
    pub(crate) metrics_level: MetricsLevel,
    pub(crate) debug_config: crate::config::RuntimeDebugConfig,
    pub(crate) pool_size: Option<usize>,
}

impl Default for ExecutorRunConfig {
    fn default() -> Self {
        let debug_config = *crate::config::runtime_debug_config();
        Self {
            active_nodes: None,
            active_edges: None,
            active_direct_edges: None,
            selected_host_output_ports: None,
            fail_fast: true,
            metrics_level: MetricsLevel::default(),
            debug_config,
            pool_size: debug_config.pool_size,
        }
    }
}

impl ExecutorRunConfig {
    pub(crate) fn set_active_nodes_mask(
        &mut self,
        active_nodes: Option<Arc<Vec<bool>>>,
        expected_nodes: usize,
    ) -> Result<(), ExecutorMaskError> {
        validate_mask_len("active_nodes", active_nodes.as_deref(), expected_nodes)?;
        self.active_nodes = active_nodes;
        Ok(())
    }

    pub(crate) fn set_active_edges_mask(
        &mut self,
        active_edges: Option<Arc<Vec<bool>>>,
        expected_edges: usize,
    ) -> Result<(), ExecutorMaskError> {
        validate_mask_len("active_edges", active_edges.as_deref(), expected_edges)?;
        self.active_edges = active_edges;
        Ok(())
    }

    pub(crate) fn set_active_direct_edges_mask(
        &mut self,
        active_direct_edges: Option<Arc<Vec<bool>>>,
        expected_edges: usize,
    ) -> Result<(), ExecutorMaskError> {
        validate_mask_len(
            "active_direct_edges",
            active_direct_edges.as_deref(),
            expected_edges,
        )?;
        self.active_direct_edges = active_direct_edges;
        Ok(())
    }

    pub(crate) fn set_selected_host_output_ports(&mut self, ports: Option<Arc<HashSet<String>>>) {
        self.selected_host_output_ports = ports;
    }

    pub(crate) fn set_fail_fast(&mut self, enabled: bool) {
        self.fail_fast = enabled;
    }

    pub(crate) fn set_pool_size(&mut self, size: Option<usize>) {
        self.pool_size = size;
    }

    pub(crate) fn set_runtime_debug_config(
        &mut self,
        config: crate::config::RuntimeDebugConfig,
    ) -> Option<usize> {
        self.debug_config = config;
        self.pool_size = config.pool_size;
        config.pool_size
    }
}

fn validate_mask_len(
    mask: &'static str,
    values: Option<&Vec<bool>>,
    expected: usize,
) -> Result<(), ExecutorMaskError> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.len() == expected {
        return Ok(());
    }
    Err(ExecutorMaskError::LengthMismatch {
        mask,
        expected,
        actual: values.len(),
    })
}
