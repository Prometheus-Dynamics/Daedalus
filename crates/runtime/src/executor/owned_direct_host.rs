use super::owned::OwnedExecutor;
use super::{
    CorrelatedPayload, CustomMetricValue, DirectHostRoute, DirectHostSingleNodeRoute,
    DirectSlotAccess, ExecuteError, ExecutionTelemetry, NodeError, NodeHandler, ProbeCount,
    ProbeTime, is_host_bridge_node, push_const_inputs, queue, serial,
};
use crate::portable::Arc;
use crate::prelude::*;
use alloc::collections::BTreeMap;
use daedalus_transport::Payload;

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
        let mut direct_edges = self.core.direct_edges.to_vec();
        direct_edges.resize(self.edges.len(), false);
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
        let _scope = super::runtime_alloc_scope();
        if let Some(single_node) = route.single_node.as_ref() {
            return self.run_direct_host_single_node(single_node, payload);
        }
        let tick = self.begin_probe_tick();
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
                stamp_enqueue: self.core.run_config.frame_probe.is_some(),
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
        let telemetry = serial::run_order(&mut exec, self.schedule_order.as_slice());
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
        self.end_probe_tick(tick);
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
        let (output, metrics) = self.run_single_node(route, payload)?;
        let mut telemetry = ExecutionTelemetry::with_level(self.core.run_config.metrics_level)
            .with_clock(&self.core.clock);
        telemetry.nodes_executed = 1;
        telemetry.record_node_custom_metrics(route.node_idx, metrics);
        Ok((telemetry, output))
    }

    fn run_direct_host_single_node_payload(
        &mut self,
        route: &DirectHostSingleNodeRoute,
        payload: Payload,
    ) -> Result<Option<Payload>, ExecuteError> {
        self.run_single_node(route, payload)
            .map(|(output, _)| output)
    }

    /// Run the route's node on `payload` plus its const inputs (graph constants, port defaults
    /// and config fields, as a scheduled tick delivers them) and return its output and the
    /// custom metrics it recorded.
    fn run_single_node(
        &mut self,
        route: &DirectHostSingleNodeRoute,
        payload: Payload,
    ) -> Result<(Option<Payload>, BTreeMap<String, CustomMetricValue>), ExecuteError> {
        let _scope = super::runtime_alloc_scope();
        let failed = |error| ExecuteError::HandlerFailed {
            node: route.node.id.clone(),
            error,
        };
        let tick = self.begin_probe_tick();
        let probe = self.core.run_config.frame_probe.clone();
        let clock = self.core.clock.clone();
        let node_start = probe.is_some().then(|| clock.now());
        self.core.state.clear_node_custom_metrics(&route.node.id);
        // Direct payload handlers exist only for nodes with a single input, which the route's
        // edge feeds, so they have no const input to deliver.
        let output = if let Some(handler) = &route.direct_payload {
            let _scope = super::node_alloc_scope();
            let output = handler(&route.node, &route.ctx, payload).map_err(failed)?;
            if let (Some(probe), Some(start)) = (&probe, node_start) {
                probe.add_time(ProbeTime::Handlers, clock.elapsed(start));
            }
            output
        } else {
            if self.storage_needs_reset {
                self.reset_for_run();
            }
            let mut inputs = crate::io::port_buffer();
            inputs.push((
                route.input_port.clone(),
                CorrelatedPayload::from_edge(payload),
            ));
            push_const_inputs(&self.const_inputs, route.node_idx, &mut inputs);
            let mut io = self.core.node_io(route.node_idx, inputs);
            let handler_start = probe.is_some().then(|| clock.now());
            {
                let _scope = super::node_alloc_scope();
                self.handler
                    .run(&route.node, &route.ctx, &mut io)
                    .map_err(failed)?;
            }
            if let (Some(probe), Some(start)) = (&probe, handler_start) {
                probe.add_time(ProbeTime::Handlers, clock.elapsed(start));
            }
            io.flush().map_err(failed)?;
            io.take_output(&route.output_port)
        };
        let metrics = self.core.state.drain_node_custom_metrics(&route.node.id);
        if let (Some(probe), Some(start)) = (&probe, node_start) {
            probe.add_time(ProbeTime::NodeRuns, clock.elapsed(start));
            probe.add_count(ProbeCount::Nodes, 1);
        }
        self.end_probe_tick(tick);
        Ok((output, metrics))
    }

    fn direct_host_single_node_route(
        &self,
        input_edge: usize,
        output_edge: usize,
    ) -> Option<DirectHostSingleNodeRoute> {
        let input_edge = self.edges.get(input_edge)?;
        let output_edge = self.edges.get(output_edge)?;
        let input_node = input_edge.to();
        let output_node = output_edge.from();
        if input_node != output_node {
            return None;
        }
        let node = self.nodes.get(input_node.0)?;
        if is_host_bridge_node(node) {
            return None;
        }
        let ctx = self.core.contexts.get(input_node.0)?.clone();
        Some(DirectHostSingleNodeRoute {
            node: node.clone(),
            node_idx: input_node.0,
            ctx,
            input_port: input_edge.target_port_id().clone(),
            output_port: output_edge.source_port_id().clone(),
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
