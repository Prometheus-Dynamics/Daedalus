use std::collections::HashMap;
use std::io::{self, BufRead, Write};

#[cfg(feature = "tcp")]
use std::io::BufReader;

#[cfg(feature = "tcp")]
use std::net::{TcpListener, TcpStream};

use daedalus_engine::{CacheStatus, Engine, EngineCacheMetrics, EngineError};
use daedalus_planner::{Graph, GraphPatch};
use daedalus_registry::capability::CapabilityRegistry;
use serde::{Deserialize, Serialize};

fn default_session() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRequest,
    MissingGraph,
    MissingRegistry,
    RegistryError,
    EngineError,
    UnsupportedExecution,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceEnvelope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default = "default_session")]
    pub session: String,
    #[serde(flatten)]
    pub request: ServiceRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub session: String,
    #[serde(flatten)]
    pub response: ServiceResponse,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ServiceRequest {
    Ping,
    PutRegistry {
        name: String,
        registry: CapabilityRegistry,
    },
    PutGraph {
        name: String,
        graph: Graph,
    },
    GetGraph {
        name: String,
    },
    PatchGraph {
        name: String,
        patch: GraphPatch,
    },
    Plan {
        registry: String,
        graph: String,
    },
    Build {
        registry: String,
        graph: String,
    },
    InspectPlan {
        registry: String,
        graph: String,
    },
    InspectLatest {
        graph: String,
    },
    InspectCache,
    InspectState,
    ClearCache,
    ExportTrace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanSummary {
    pub graph: String,
    pub registry: String,
    pub plan_hash: u64,
    pub planner_cache: CacheStatus,
    pub nodes: usize,
    pub edges: usize,
    pub diagnostics: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildSummary {
    pub graph: String,
    pub registry: String,
    pub plan_hash: u64,
    pub planner_cache: CacheStatus,
    pub runtime_plan_cache: CacheStatus,
    pub nodes: usize,
    pub edges: usize,
    pub segments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceSummary {
    pub session: String,
    pub graphs: Vec<String>,
    pub registries: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceTraceEvent {
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub session: String,
    pub op: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub ok: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServiceResponse {
    Pong,
    Ack,
    Graph {
        name: String,
        graph: Graph,
    },
    Plan {
        summary: PlanSummary,
    },
    PlanInspection {
        plan: PlanSummary,
        build: BuildSummary,
    },
    Build {
        summary: BuildSummary,
    },
    Latest {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        plan: Option<PlanSummary>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        build: Option<BuildSummary>,
        cache_metrics: EngineCacheMetrics,
    },
    Cache {
        metrics: EngineCacheMetrics,
    },
    State {
        sessions: Vec<String>,
        graphs: Vec<String>,
        registries: Vec<String>,
        namespaces: Vec<NamespaceSummary>,
    },
    Trace {
        events: Vec<ServiceTraceEvent>,
    },
    Error {
        code: ErrorCode,
        message: String,
    },
}

#[derive(Default)]
struct ServiceNamespace {
    registries: HashMap<String, CapabilityRegistry>,
    graphs: HashMap<String, Graph>,
    latest_plan: HashMap<String, PlanSummary>,
    latest_build: HashMap<String, BuildSummary>,
}

#[derive(Default)]
pub struct ServiceState {
    namespaces: HashMap<String, ServiceNamespace>,
    trace: Vec<ServiceTraceEvent>,
    next_seq: u64,
}

impl ServiceState {
    pub fn handle(&mut self, engine: &Engine, envelope: ServiceEnvelope) -> ResponseEnvelope {
        let request_id = envelope.request_id.clone();
        let session = envelope.session.clone();
        let op_name = op_name(&envelope.request).to_string();
        let target = op_target(&envelope.request);
        let response = match self.try_handle(engine, envelope) {
            Ok(response) => response,
            Err(err) => ServiceResponse::Error {
                code: err.code(),
                message: err.to_string(),
            },
        };
        let ok = !matches!(response, ServiceResponse::Error { .. });
        self.next_seq = self.next_seq.saturating_add(1);
        self.trace.push(ServiceTraceEvent {
            seq: self.next_seq,
            request_id: request_id.clone(),
            session: session.clone(),
            op: op_name,
            target,
            ok,
        });
        ResponseEnvelope {
            request_id,
            session,
            response,
        }
    }

    fn try_handle(
        &mut self,
        engine: &Engine,
        envelope: ServiceEnvelope,
    ) -> Result<ServiceResponse, ServiceError> {
        let session = envelope.session;
        let request = envelope.request;
        if matches!(request, ServiceRequest::InspectState) {
            let mut sessions: Vec<String> = self.namespaces.keys().cloned().collect();
            sessions.sort();
            let mut namespaces = self
                .namespaces
                .iter()
                .map(|(session, namespace)| {
                    let mut graphs = namespace.graphs.keys().cloned().collect::<Vec<_>>();
                    let mut registries = namespace.registries.keys().cloned().collect::<Vec<_>>();
                    graphs.sort();
                    registries.sort();
                    NamespaceSummary {
                        session: session.clone(),
                        graphs,
                        registries,
                    }
                })
                .collect::<Vec<_>>();
            namespaces.sort_by(|left, right| left.session.cmp(&right.session));
            let (mut graphs, mut registries) = self
                .namespaces
                .get(&session)
                .map(|namespace| {
                    (
                        namespace.graphs.keys().cloned().collect::<Vec<_>>(),
                        namespace.registries.keys().cloned().collect::<Vec<_>>(),
                    )
                })
                .unwrap_or_default();
            graphs.sort();
            registries.sort();
            return Ok(ServiceResponse::State {
                sessions,
                graphs,
                registries,
                namespaces,
            });
        }

        let namespace = self.namespaces.entry(session).or_default();
        match request {
            ServiceRequest::Ping => Ok(ServiceResponse::Pong),
            ServiceRequest::PutRegistry { name, registry } => {
                namespace.registries.insert(name, registry);
                Ok(ServiceResponse::Ack)
            }
            ServiceRequest::PutGraph { name, graph } => {
                namespace.graphs.insert(name, graph);
                Ok(ServiceResponse::Ack)
            }
            ServiceRequest::GetGraph { name } => {
                let graph = namespace
                    .graphs
                    .get(&name)
                    .cloned()
                    .ok_or_else(|| ServiceError::missing_graph(&name))?;
                Ok(ServiceResponse::Graph { name, graph })
            }
            ServiceRequest::PatchGraph { name, patch } => {
                let graph = namespace
                    .graphs
                    .get_mut(&name)
                    .ok_or_else(|| ServiceError::missing_graph(&name))?;
                patch.apply_to_graph(graph);
                Ok(ServiceResponse::Ack)
            }
            ServiceRequest::Plan { registry, graph } => {
                let capabilities = namespace
                    .registries
                    .get(&registry)
                    .ok_or_else(|| ServiceError::missing_registry(&registry))?;
                let graph_ref = namespace
                    .graphs
                    .get(&graph)
                    .cloned()
                    .ok_or_else(|| ServiceError::missing_graph(&graph))?;
                let mut planner_config = engine.planner_config()?;
                planner_config.transport_capabilities = Some(capabilities.clone());
                let prepared = engine.prepare_plan_with_config(graph_ref, planner_config)?;
                let summary = PlanSummary {
                    graph: graph.clone(),
                    registry: registry.clone(),
                    plan_hash: prepared.plan().hash.0,
                    planner_cache: prepared.cache_status(),
                    nodes: prepared.plan().graph.nodes.len(),
                    edges: prepared.plan().graph.edges.len(),
                    diagnostics: prepared.planner_output().diagnostics.len(),
                };
                namespace.latest_plan.insert(graph, summary.clone());
                Ok(ServiceResponse::Plan { summary })
            }
            ServiceRequest::InspectPlan { registry, graph } => {
                let capabilities = namespace
                    .registries
                    .get(&registry)
                    .ok_or_else(|| ServiceError::missing_registry(&registry))?;
                let graph_ref = namespace
                    .graphs
                    .get(&graph)
                    .cloned()
                    .ok_or_else(|| ServiceError::missing_graph(&graph))?;
                let mut planner_config = engine.planner_config()?;
                planner_config.transport_capabilities = Some(capabilities.clone());
                let prepared = engine.prepare_plan_with_config(graph_ref, planner_config)?;
                let plan = PlanSummary {
                    graph: graph.clone(),
                    registry: registry.clone(),
                    plan_hash: prepared.plan().hash.0,
                    planner_cache: prepared.cache_status(),
                    nodes: prepared.plan().graph.nodes.len(),
                    edges: prepared.plan().graph.edges.len(),
                    diagnostics: prepared.planner_output().diagnostics.len(),
                };
                namespace.latest_plan.insert(graph.clone(), plan.clone());
                let built = prepared.build()?;
                let runtime_plan = built.runtime_plan();
                let build = BuildSummary {
                    graph: graph.clone(),
                    registry,
                    plan_hash: plan.plan_hash,
                    planner_cache: plan.planner_cache,
                    runtime_plan_cache: built.cache_status(),
                    nodes: runtime_plan.nodes.len(),
                    edges: runtime_plan.edges.len(),
                    segments: runtime_plan.segments.len(),
                };
                namespace.latest_build.insert(graph, build.clone());
                Ok(ServiceResponse::PlanInspection { plan, build })
            }
            ServiceRequest::Build { registry, graph } => {
                let capabilities = namespace
                    .registries
                    .get(&registry)
                    .ok_or_else(|| ServiceError::missing_registry(&registry))?;
                let graph_ref = namespace
                    .graphs
                    .get(&graph)
                    .cloned()
                    .ok_or_else(|| ServiceError::missing_graph(&graph))?;
                let mut planner_config = engine.planner_config()?;
                planner_config.transport_capabilities = Some(capabilities.clone());
                let prepared = engine.prepare_plan_with_config(graph_ref, planner_config)?;
                let planner_summary = PlanSummary {
                    graph: graph.clone(),
                    registry: registry.clone(),
                    plan_hash: prepared.plan().hash.0,
                    planner_cache: prepared.cache_status(),
                    nodes: prepared.plan().graph.nodes.len(),
                    edges: prepared.plan().graph.edges.len(),
                    diagnostics: prepared.planner_output().diagnostics.len(),
                };
                namespace
                    .latest_plan
                    .insert(graph.clone(), planner_summary.clone());
                let built = prepared.build()?;
                let runtime_plan = built.runtime_plan();
                let summary = BuildSummary {
                    graph: graph.clone(),
                    registry,
                    plan_hash: planner_summary.plan_hash,
                    planner_cache: planner_summary.planner_cache,
                    runtime_plan_cache: built.cache_status(),
                    nodes: runtime_plan.nodes.len(),
                    edges: runtime_plan.edges.len(),
                    segments: runtime_plan.segments.len(),
                };
                namespace.latest_build.insert(graph, summary.clone());
                Ok(ServiceResponse::Build { summary })
            }
            ServiceRequest::InspectLatest { graph } => Ok(ServiceResponse::Latest {
                plan: namespace.latest_plan.get(&graph).cloned(),
                build: namespace.latest_build.get(&graph).cloned(),
                cache_metrics: engine.cache_metrics(),
            }),
            ServiceRequest::InspectCache => Ok(ServiceResponse::Cache {
                metrics: engine.cache_metrics(),
            }),
            ServiceRequest::InspectState => unreachable!("handled before namespace borrow"),
            ServiceRequest::ClearCache => Ok(ServiceResponse::Cache {
                metrics: engine.clear_caches(),
            }),
            ServiceRequest::ExportTrace => Ok(ServiceResponse::Trace {
                events: self.trace.clone(),
            }),
        }
    }
}

fn op_name(request: &ServiceRequest) -> &'static str {
    match request {
        ServiceRequest::Ping => "ping",
        ServiceRequest::PutRegistry { .. } => "put_registry",
        ServiceRequest::PutGraph { .. } => "put_graph",
        ServiceRequest::GetGraph { .. } => "get_graph",
        ServiceRequest::PatchGraph { .. } => "patch_graph",
        ServiceRequest::Plan { .. } => "plan",
        ServiceRequest::Build { .. } => "build",
        ServiceRequest::InspectPlan { .. } => "inspect_plan",
        ServiceRequest::InspectLatest { .. } => "inspect_latest",
        ServiceRequest::InspectCache => "inspect_cache",
        ServiceRequest::InspectState => "inspect_state",
        ServiceRequest::ClearCache => "clear_cache",
        ServiceRequest::ExportTrace => "export_trace",
    }
}

fn op_target(request: &ServiceRequest) -> Option<String> {
    match request {
        ServiceRequest::PutRegistry { name, .. }
        | ServiceRequest::PutGraph { name, .. }
        | ServiceRequest::GetGraph { name }
        | ServiceRequest::PatchGraph { name, .. }
        | ServiceRequest::InspectLatest { graph: name } => Some(name.clone()),
        ServiceRequest::Plan { graph, .. }
        | ServiceRequest::Build { graph, .. }
        | ServiceRequest::InspectPlan { graph, .. } => Some(graph.clone()),
        _ => None,
    }
}

#[derive(Debug)]
enum ServiceError {
    MissingGraph(String),
    MissingRegistry(String),
    Engine(EngineError),
}

impl ServiceError {
    fn missing_graph(name: &str) -> Self {
        Self::MissingGraph(name.to_string())
    }

    fn missing_registry(name: &str) -> Self {
        Self::MissingRegistry(name.to_string())
    }

    fn code(&self) -> ErrorCode {
        match self {
            ServiceError::MissingGraph(_) => ErrorCode::MissingGraph,
            ServiceError::MissingRegistry(_) => ErrorCode::MissingRegistry,
            ServiceError::Engine(_) => ErrorCode::EngineError,
        }
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceError::MissingGraph(name) => write!(f, "missing graph: {name}"),
            ServiceError::MissingRegistry(name) => write!(f, "missing registry: {name}"),
            ServiceError::Engine(err) => write!(f, "{err}"),
        }
    }
}

impl From<EngineError> for ServiceError {
    fn from(value: EngineError) -> Self {
        Self::Engine(value)
    }
}

fn process_line(engine: &Engine, state: &mut ServiceState, line: &str) -> ResponseEnvelope {
    match serde_json::from_str::<ServiceEnvelope>(line) {
        Ok(envelope) => state.handle(engine, envelope),
        Err(err) => ResponseEnvelope {
            request_id: None,
            session: default_session(),
            response: ServiceResponse::Error {
                code: ErrorCode::InvalidRequest,
                message: format!("invalid request: {err}"),
            },
        },
    }
}

pub fn run_stdio_service(engine: Engine) -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut state = ServiceState::default();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = process_line(&engine, &mut state, &line);
        serde_json::to_writer(&mut stdout, &response)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}

#[cfg(feature = "tcp")]
fn handle_tcp_client(
    engine: &Engine,
    state: &mut ServiceState,
    stream: &mut TcpStream,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    while reader.read_line(&mut line)? != 0 {
        if !line.trim().is_empty() {
            let response = process_line(engine, state, line.trim_end());
            serde_json::to_writer(&mut *stream, &response)?;
            stream.write_all(b"\n")?;
            stream.flush()?;
        }
        line.clear();
    }
    Ok(())
}

#[cfg(feature = "tcp")]
pub fn run_tcp_service(engine: Engine, addr: &str) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    let mut state = ServiceState::default();
    for stream in listener.incoming() {
        let mut stream = stream?;
        handle_tcp_client(&engine, &mut state, &mut stream)?;
    }
    Ok(())
}

#[cfg(feature = "tcp")]
pub fn send_tcp_request(addr: &str, request: &ServiceEnvelope) -> io::Result<ResponseEnvelope> {
    let mut stream = TcpStream::connect(addr)?;
    serde_json::to_writer(&mut stream, request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    serde_json::from_str::<ResponseEnvelope>(&line)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}
