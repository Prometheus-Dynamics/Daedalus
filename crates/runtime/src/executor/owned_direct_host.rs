use super::owned::OwnedExecutor;
use super::{
    CorrelatedPayload, DirectHostRoute, DirectHostSingleNodeRoute, DirectSlotAccess, ExecuteError,
    ExecutionTelemetry, NodeError, NodeHandler, is_host_bridge_node, queue, serial,
};
use crate::io::NodeIo;
use crate::state::ExecutionContext;
use daedalus_transport::Payload;
use std::sync::Arc;

impl<H: NodeHandler> OwnedExecutor<H> {
    pub fn run_direct_host_payload(
        &mut self,
        input_port: &str,
        payload: Payload,
        output_port: &str,
    ) -> Result<Option<(ExecutionTelemetry, Option<Payload>)>, ExecuteError> {
        let Some(route) = self.direct_host_route(input_port, output_port) else {
            return Ok(None);
        };
        self.run_direct_host_route(&route, payload).map(Some)
    }

    pub fn direct_host_route(
        &self,
        input_port: &str,
        output_port: &str,
    ) -> Option<DirectHostRoute> {
        let input_edge = self.direct_host_input_edge(input_port)?;
        let output_edge = self.direct_host_output_edge(output_port)?;
        let mut direct_edges = vec![false; self.edges.len()];
        for edge_idx in self.core.direct_edges.iter().copied() {
            if let Some(slot) = direct_edges.get_mut(edge_idx) {
                *slot = true;
            }
        }
        if let Some(slot) = direct_edges.get_mut(input_edge) {
            *slot = true;
        }
        if let Some(slot) = direct_edges.get_mut(output_edge) {
            *slot = true;
        }
        Some(DirectHostRoute {
            input_edge,
            output_edge,
            active_direct_edges: Arc::new(direct_edges),
            single_node: self.direct_host_single_node_route(input_edge, output_edge),
        })
    }

    pub fn run_direct_host_route(
        &mut self,
        route: &DirectHostRoute,
        payload: Payload,
    ) -> Result<(ExecutionTelemetry, Option<Payload>), ExecuteError> {
        if let Some(single_node) = route.single_node.as_ref() {
            return self.run_direct_host_single_node(single_node, payload);
        }
        self.reset_for_run();
        let payload = CorrelatedPayload::from_edge(payload);
        if route
            .active_direct_edges
            .get(route.input_edge)
            .copied()
            .unwrap_or(false)
        {
            self.core.direct_slots[route.input_edge]
                .serial()
                .put(payload);
        } else if let Some(edge) = self.edges.get(route.input_edge) {
            let policy = edge.policy().clone();
            let queues = self.core.queues.clone();
            let warnings_seen = self.core.warnings_seen.clone();
            let data_size_inspectors = self.core.data_size_inspectors.clone();
            let backpressure = self.backpressure.clone();
            queue::apply_policy_owned(queue::ApplyPolicyOwnedArgs {
                edge_idx: route.input_edge,
                policy: &policy,
                payload,
                queues: &queues,
                warnings_seen: &warnings_seen,
                telem: &mut self.core.telemetry,
                warning_label: None,
                backpressure,
                data_size_inspectors: &data_size_inspectors,
            })
            .map_err(|error| ExecuteError::HandlerFailed {
                node: "host".into(),
                error,
            })?;
        } else {
            return Err(ExecuteError::HandlerFailed {
                node: "host".into(),
                error: NodeError::InvalidInput(
                    "direct host route input edge is out of bounds".into(),
                ),
            });
        }
        let mut exec = self.snapshot(DirectSlotAccess::Serial);
        exec.core.run_config.active_direct_edges = Some(route.active_direct_edges.clone());
        let telemetry = serial::run_order(exec, self.schedule_order.as_slice());
        if telemetry.is_err() {
            self.storage_needs_reset = true;
        }
        let telemetry = telemetry?;
        let output = if route
            .active_direct_edges
            .get(route.output_edge)
            .copied()
            .unwrap_or(false)
        {
            self.core.direct_slots[route.output_edge]
                .serial()
                .take()
                .map(|payload| payload.inner)
        } else {
            queue::pop_edge(
                route.output_edge,
                &self.core.queues,
                &self.core.data_size_inspectors,
            )
            .map(|payload| payload.inner)
        };
        Ok((telemetry, output))
    }

    pub fn run_direct_host_route_payload(
        &mut self,
        route: &DirectHostRoute,
        payload: Payload,
    ) -> Result<Option<Payload>, ExecuteError> {
        if let Some(single_node) = route.single_node.as_ref() {
            return self.run_direct_host_single_node_payload(single_node, payload);
        }
        self.run_direct_host_route(route, payload)
            .map(|(_, output)| output)
    }

    fn run_direct_host_single_node(
        &mut self,
        route: &DirectHostSingleNodeRoute,
        payload: Payload,
    ) -> Result<(ExecutionTelemetry, Option<Payload>), ExecuteError> {
        if let Some(handler) = &route.direct_payload {
            let _ = self.core.state.clear_node_custom_metrics(&route.node.id);
            let output = handler(&route.node, &route.ctx, payload).map_err(|error| {
                ExecuteError::HandlerFailed {
                    node: route.node.id.clone(),
                    error,
                }
            })?;
            let mut telemetry = ExecutionTelemetry::with_level(self.core.run_config.metrics_level);
            telemetry.nodes_executed = 1;
            if let Ok(metrics) = self.core.state.drain_node_custom_metrics(&route.node.id) {
                telemetry.record_node_custom_metrics(route.node_idx, metrics);
            }
            return Ok((telemetry, output));
        }
        if self.storage_needs_reset {
            self.reset_for_run();
        }
        let _ = self.core.state.clear_node_custom_metrics(&route.node.id);
        let mut io = NodeIo::from_single_input(
            route.input_port.clone(),
            CorrelatedPayload::from_edge(payload),
        )
        .with_const_coercers(self.core.const_coercers.clone());
        self.handler
            .run(&route.node, &route.ctx, &mut io)
            .map_err(|error| ExecuteError::HandlerFailed {
                node: route.node.id.clone(),
                error,
            })?;
        io.flush().map_err(|error| ExecuteError::HandlerFailed {
            node: route.node.id.clone(),
            error,
        })?;
        let output = io
            .take_outputs_small()
            .into_iter()
            .find_map(|(port, payload)| (port == route.output_port).then_some(payload.inner));
        let mut telemetry = ExecutionTelemetry::with_level(self.core.run_config.metrics_level);
        telemetry.nodes_executed = 1;
        if let Ok(metrics) = self.core.state.drain_node_custom_metrics(&route.node.id) {
            telemetry.record_node_custom_metrics(route.node_idx, metrics);
        }
        Ok((telemetry, output))
    }

    fn run_direct_host_single_node_payload(
        &mut self,
        route: &DirectHostSingleNodeRoute,
        payload: Payload,
    ) -> Result<Option<Payload>, ExecuteError> {
        if let Some(handler) = &route.direct_payload {
            let _ = self.core.state.clear_node_custom_metrics(&route.node.id);
            let output = handler(&route.node, &route.ctx, payload).map_err(|error| {
                ExecuteError::HandlerFailed {
                    node: route.node.id.clone(),
                    error,
                }
            })?;
            let _ = self.core.state.drain_node_custom_metrics(&route.node.id);
            return Ok(output);
        }
        if self.storage_needs_reset {
            self.reset_for_run();
        }
        let _ = self.core.state.clear_node_custom_metrics(&route.node.id);
        let mut io = NodeIo::from_single_input(
            route.input_port.clone(),
            CorrelatedPayload::from_edge(payload),
        )
        .with_const_coercers(self.core.const_coercers.clone());
        self.handler
            .run(&route.node, &route.ctx, &mut io)
            .map_err(|error| ExecuteError::HandlerFailed {
                node: route.node.id.clone(),
                error,
            })?;
        io.flush().map_err(|error| ExecuteError::HandlerFailed {
            node: route.node.id.clone(),
            error,
        })?;
        let _ = self.core.state.drain_node_custom_metrics(&route.node.id);
        Ok(io
            .take_outputs_small()
            .into_iter()
            .find_map(|(port, payload)| (port == route.output_port).then_some(payload.inner)))
    }

    fn direct_host_single_node_route(
        &self,
        input_edge: usize,
        output_edge: usize,
    ) -> Option<DirectHostSingleNodeRoute> {
        let input_edge = self.edges.get(input_edge)?;
        let output_edge = self.edges.get(output_edge)?;
        let input_node = input_edge.to();
        let input_port = input_edge.target_port();
        let output_node = output_edge.from();
        let output_port = output_edge.source_port();
        if input_node != output_node {
            return None;
        }
        let node = self.nodes.get(input_node.0)?;
        if is_host_bridge_node(node) {
            return None;
        }
        let ctx = ExecutionContext {
            state: self.core.state.clone(),
            node_id: node.id.clone().into(),
            metadata: self.core.node_metadata[input_node.0].clone(),
            graph_metadata: self.core.graph_metadata.clone(),
            capabilities: self.core.capabilities.clone(),
            #[cfg(feature = "gpu")]
            gpu: self.core.gpu.clone(),
        };
        Some(DirectHostSingleNodeRoute {
            node: node.clone(),
            node_idx: input_node.0,
            ctx,
            input_port: input_port.to_string(),
            output_port: output_port.to_string(),
            direct_payload: self.handler.direct_payload_handler(node.stable_id),
        })
    }

    fn direct_host_input_edge(&self, input_port: &str) -> Option<usize> {
        let mut matched = None;
        for node_ref in self.schedule.host_nodes.iter().copied() {
            for edge_idx in self.outgoing_edges.get(node_ref.0)?.iter().copied() {
                let edge = self.edges.get(edge_idx)?;
                if edge.source_port() == input_port
                    && !is_host_bridge_node(self.nodes.get(edge.to().0)?)
                    && matched.replace(edge_idx).is_some()
                {
                    return None;
                }
            }
        }
        matched
    }

    fn direct_host_output_edge(&self, output_port: &str) -> Option<usize> {
        let mut matched = None;
        for node_ref in self.schedule.host_nodes.iter().copied() {
            for edge_idx in self.incoming_edges.get(node_ref.0)?.iter().copied() {
                let edge = self.edges.get(edge_idx)?;
                if edge.target_port() == output_port
                    && !is_host_bridge_node(self.nodes.get(edge.from().0)?)
                    && matched.replace(edge_idx).is_some()
                {
                    return None;
                }
            }
        }
        matched
    }
}
