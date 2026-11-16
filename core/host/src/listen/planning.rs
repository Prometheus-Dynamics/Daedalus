//! Planning on the listener: the ops that store a graph, plan it and make a plan from it. Built
//! only with the planner; a host without it answers these ops with `unsupported` (see `made.rs`).

use std::string::{String, ToString};
use std::vec::Vec;

use daedalus_graph::{GraphBody, GraphDoc, GraphPatch};
use daedalus_made::encode;
use daedalus_planner::cache::PlanKey;
use daedalus_planner::edit::patch::apply_patch;
use daedalus_planner::explain::{explain_plan, json::to_json};
use daedalus_planner::hash::plan_hash;
use daedalus_planner::lint::lint;
use daedalus_planner::{Config, Form, Planned, plan, solve};
use daedalus_wire::service::{
    CacheInfo, Diagnostic, ErrorCode, Inspection, PlanForm, PlanOptions, PlanSummary, Reply,
    ServiceError, Severity, StartConfig,
};

use super::{Engine, ServiceHost, lock};

type Answer = Result<Reply, ServiceError>;

fn refuse(code: ErrorCode, message: impl Into<String>) -> ServiceError {
    ServiceError::new(code, message)
}

fn diagnostics(found: &[daedalus_planner::diagnostic::Diagnostic]) -> Vec<Diagnostic> {
    found
        .iter()
        .map(|item| Diagnostic {
            severity: match item.severity {
                daedalus_planner::diagnostic::Severity::Error => Severity::Error,
                daedalus_planner::diagnostic::Severity::Warning => Severity::Warning,
            },
            // The planner's code as it names it in JSON: snake case.
            code: serde_json::to_value(item.code)
                .ok()
                .and_then(|code| code.as_str().map(String::from))
                .unwrap_or_default(),
            message: item.message.clone(),
            node: item.node.clone(),
            port: item.port.clone(),
        })
        .collect()
}

fn form(form: PlanForm) -> Form {
    match form {
        PlanForm::Steps => Form::Steps,
        PlanForm::Blob => Form::Blob,
        PlanForm::Source => Form::Source,
    }
}

impl Engine {
    pub(super) fn graph(&self, session: &str, name: &str) -> Result<&GraphBody, ServiceError> {
        self.graphs
            .get(session)
            .and_then(|graphs| graphs.get(name))
            .ok_or_else(|| {
                refuse(
                    ErrorCode::UnknownGraph,
                    std::format!("session `{session}` has no graph `{name}`"),
                )
            })
    }

    /// What solving and linting `graph` finds, without planning it.
    pub(super) fn check(&self, graph: &GraphBody) -> Vec<Diagnostic> {
        let config = Config {
            lints: true,
            ..Config::default()
        };
        match solve(graph, &self.registry, &config) {
            Ok(solved) => diagnostics(&lint(&solved, &self.registry)),
            Err(errors) => {
                let found: Vec<_> = errors.iter().map(Into::into).collect();
                diagnostics(&found)
            }
        }
    }

    /// Plans a stored graph in the steps form, ready to load.
    pub(super) fn plan_steps(
        &self,
        session: &str,
        name: &str,
        config: &StartConfig,
    ) -> Result<Planned, ServiceError> {
        let planning = Config {
            priority: config.priority.clone(),
            merge: !config.unmerged,
            ..Config::default()
        };
        let graph = self.graph(session, name)?;
        plan(graph, &self.registry, &self.backends(), &planning).map_err(|error| {
            ServiceError::planning(
                self.planning_text(error.to_string()),
                diagnostics(&error.diagnostics()),
            )
        })
    }
}

impl ServiceHost {
    pub(super) fn put_graph(&self, session: &str, name: &str, json: &str) -> Answer {
        if name.is_empty() {
            return Err(refuse(ErrorCode::InvalidRequest, "a graph needs a name"));
        }
        let doc = GraphDoc::parse(json)
            .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?;
        let mut engine = lock(&self.engine);
        let diagnostics = engine.check(&doc.graph);
        engine
            .graphs
            .entry(String::from(session))
            .or_default()
            .insert(String::from(name), doc.graph);
        if let Some(plans) = engine.plans.get_mut(session) {
            plans.remove(name);
        }
        Ok(Reply::Graph {
            name: String::from(name),
            diagnostics,
        })
    }

    pub(super) fn patch_graph(&self, session: &str, name: &str, patch: &str) -> Answer {
        let patch = GraphPatch::parse(patch)
            .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?;
        let mut engine = lock(&self.engine);
        let mut graph = engine.graph(session, name)?.clone();
        let report = apply_patch(&mut graph, &patch)
            .map_err(|error| refuse(ErrorCode::InvalidRequest, std::format!("{error:?}")))?;
        let mut diagnostics: Vec<Diagnostic> = report
            .not_found
            .iter()
            .map(|missing| Diagnostic {
                severity: Severity::Warning,
                code: String::from("selector_not_found"),
                message: std::format!(
                    "op {} selects a node the graph does not have: {:?}",
                    missing.op,
                    missing.selector
                ),
                node: missing.selector.id.clone(),
                port: None,
            })
            .collect();
        diagnostics.extend(engine.check(&graph));
        if let Some(graphs) = engine.graphs.get_mut(session) {
            graphs.insert(String::from(name), graph);
        }
        Ok(Reply::Graph {
            name: String::from(name),
            diagnostics,
        })
    }

    pub(super) fn plan(&self, session: &str, name: &str, options: &PlanOptions) -> Answer {
        let mut engine = lock(&self.engine);
        let config = Config {
            priority: options.priority.clone(),
            lints: true,
            form: form(options.form),
            ..Config::default()
        };
        let key = {
            let graph = engine.graph(session, name)?;
            PlanKey::new(graph, &engine.registry, &engine.backends(), &config)
        };
        // An inspection is made from the plan itself, so it is planned again.
        if !options.inspect
            && let Some(summary) = engine.cache.get(&key)
        {
            let mut summary = summary.clone();
            summary.graph = String::from(name);
            summary.cached = true;
            return Ok(Reply::Plan(summary));
        }
        let graph = engine.graph(session, name)?;
        let backends = engine.backends();
        let planned = plan(graph, &engine.registry, &backends, &config).map_err(|error| {
            ServiceError::planning(
                engine.planning_text(error.to_string()),
                diagnostics(&error.diagnostics()),
            )
        })?;
        let summary = PlanSummary {
            graph: String::from(name),
            // A plan with no stored encoding is named by what it was made from.
            hash: plan_hash(&planned, &config, &backends)
                .unwrap_or(key.graph_hash() ^ key.settings_hash().rotate_left(1)),
            form: options.form,
            nodes: planned.graph.nodes.len() as u32,
            edges: planned.graph.edges.len() as u32,
            regions: planned.regions.regions.len() as u32,
            steps: planned.stitched.layout.len() as u32,
            cached: false,
            diagnostics: diagnostics(&planned.warnings),
            explain: match options.inspect {
                true => Some(
                    to_json(&explain_plan(&planned))
                        .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?,
                ),
                false => None,
            },
        };
        drop(planned);
        let mut stored = summary.clone();
        stored.explain = None;
        engine.cache.insert(key, stored);
        Ok(Reply::Plan(summary))
    }

    pub(super) fn make_plan(&self, session: &str, name: &str, config: &StartConfig) -> Answer {
        let planned = lock(&self.engine).plan_steps(session, name, config)?;
        let made = planned.made().map_err(|error| {
            ServiceError::planning(
                lock(&self.engine).planning_text(error.to_string()),
                diagnostics(&error.diagnostics()),
            )
        })?;
        let bytes =
            encode(&made).map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?;
        Ok(Reply::Made {
            hash: daedalus_types::fnv1a64(&bytes),
            bytes,
        })
    }

    pub(super) fn clear_cache(&self) -> Answer {
        let mut engine = lock(&self.engine);
        let capacity = self.options().plan_cache.max(1);
        engine.cache = daedalus_planner::cache::PlanCache::new(capacity);
        Ok(Reply::Ack)
    }

    pub(super) fn inspect_graph(
        &self,
        session: &str,
        name: &str,
    ) -> Result<Inspection, ServiceError> {
        let engine = lock(&self.engine);
        let doc = GraphDoc::new(engine.graph(session, name)?.clone());
        Ok(Inspection::Graph {
            name: String::from(name),
            json: doc
                .to_json()
                .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?,
        })
    }

    pub(super) fn inspect_cache(&self) -> Result<Inspection, ServiceError> {
        let engine = lock(&self.engine);
        Ok(Inspection::Cache(CacheInfo {
            entries: engine.cache.len() as u32,
            hits: engine.cache.hits(),
            misses: engine.cache.misses(),
        }))
    }
}
