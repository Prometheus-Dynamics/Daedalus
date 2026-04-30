use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock, RwLock};

use crate::diagnostics::Diagnostic;
use crate::graph::Graph;

use super::{
    AppliedPlannerLowering, PlannerCatalog, PlannerConfig, PlannerLoweringInfo,
    PlannerLoweringPhase,
};

pub struct PlannerLoweringContext<'a> {
    pub config: &'a PlannerConfig,
    pub catalog: &'a PlannerCatalog,
}

type PlannerLoweringFn = Arc<
    dyn for<'a> Fn(
            &mut Graph,
            &PlannerLoweringContext<'a>,
            &mut Vec<Diagnostic>,
        ) -> Vec<AppliedPlannerLowering>
        + Send
        + Sync,
>;

#[derive(Clone)]
struct RegisteredPlannerLowering {
    info: PlannerLoweringInfo,
    apply: PlannerLoweringFn,
}

#[derive(Clone)]
pub struct PlannerLoweringRegistry {
    lowerings: Arc<RwLock<BTreeMap<String, RegisteredPlannerLowering>>>,
}

impl PlannerLoweringRegistry {
    pub fn new() -> Self {
        Self {
            lowerings: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn global() -> Self {
        static LOWERINGS: OnceLock<PlannerLoweringRegistry> = OnceLock::new();
        LOWERINGS.get_or_init(Self::new).clone()
    }

    pub fn register<F>(&self, id: impl Into<String>, phase: PlannerLoweringPhase, apply: F)
    where
        F: for<'a> Fn(
                &mut Graph,
                &PlannerLoweringContext<'a>,
                &mut Vec<Diagnostic>,
            ) -> Vec<AppliedPlannerLowering>
            + Send
            + Sync
            + 'static,
    {
        let id = id.into();
        let lowering = RegisteredPlannerLowering {
            info: PlannerLoweringInfo {
                id: id.clone(),
                phase,
            },
            apply: Arc::new(apply),
        };
        let mut guard = self
            .lowerings
            .write()
            .expect("planner lowerings lock poisoned");
        guard.insert(id, lowering);
    }

    pub fn registered(&self) -> Vec<PlannerLoweringInfo> {
        self.lowerings
            .read()
            .expect("planner lowerings lock poisoned")
            .values()
            .map(|entry| entry.info.clone())
            .collect()
    }

    fn entries_for_phase(
        &self,
        phase: PlannerLoweringPhase,
    ) -> Vec<(PlannerLoweringInfo, PlannerLoweringFn)> {
        self.lowerings
            .read()
            .expect("planner lowerings lock poisoned")
            .values()
            .filter(|entry| entry.info.phase == phase)
            .map(|entry| (entry.info.clone(), entry.apply.clone()))
            .collect()
    }
}

impl Default for PlannerLoweringRegistry {
    fn default() -> Self {
        Self::global()
    }
}

impl fmt::Debug for PlannerLoweringRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlannerLoweringRegistry")
            .field("registered", &self.registered())
            .finish()
    }
}

pub fn register_planner_lowering<F>(id: impl Into<String>, phase: PlannerLoweringPhase, apply: F)
where
    F: for<'a> Fn(
            &mut Graph,
            &PlannerLoweringContext<'a>,
            &mut Vec<Diagnostic>,
        ) -> Vec<AppliedPlannerLowering>
        + Send
        + Sync
        + 'static,
{
    PlannerLoweringRegistry::global().register(id, phase, apply);
}

pub fn registered_planner_lowerings() -> Vec<PlannerLoweringInfo> {
    PlannerLoweringRegistry::global().registered()
}

pub(super) fn apply_planner_lowerings(
    graph: &mut Graph,
    catalog: &PlannerCatalog,
    config: &PlannerConfig,
    diags: &mut Vec<Diagnostic>,
    phase: PlannerLoweringPhase,
) -> Vec<AppliedPlannerLowering> {
    let entries = config.lowerings.entries_for_phase(phase);

    let ctx = PlannerLoweringContext { config, catalog };
    let mut applied = Vec::new();
    for (info, apply) in entries {
        let mut results = apply(graph, &ctx, diags);
        for result in &mut results {
            if result.id.is_empty() {
                result.id = info.id.clone();
            }
            result.phase = info.phase;
        }
        applied.extend(results);
    }
    applied.sort_by(|a, b| {
        a.phase
            .cmp(&b.phase)
            .then_with(|| a.id.cmp(&b.id))
            .then_with(|| a.summary.cmp(&b.summary))
    });
    applied
}

#[cfg(test)]
mod tests {
    use daedalus_data::model::Value;

    use crate::graph::Graph;
    use crate::passes::{
        AppliedPlannerLowering, PlannerConfig, PlannerInput, PlannerLoweringPhase,
        PlannerLoweringRegistry, build_plan, register_planner_lowering,
    };

    #[test]
    fn scoped_planner_lowering_registry_isolates_planning_runs() {
        register_planner_lowering(
            "test.global.lowering",
            PlannerLoweringPhase::BeforeTypecheck,
            |graph, _, _| {
                graph
                    .metadata
                    .insert("test.global.lowering".into(), Value::Bool(true));
                Vec::new()
            },
        );

        let isolated = PlannerLoweringRegistry::new();
        let output = build_plan(
            PlannerInput {
                graph: Graph::default(),
            },
            PlannerConfig {
                lowerings: isolated,
                ..PlannerConfig::default()
            },
        );
        assert!(
            !output
                .plan
                .graph
                .metadata
                .contains_key("test.global.lowering")
        );

        let scoped = PlannerLoweringRegistry::new();
        scoped.register(
            "test.scoped.lowering",
            PlannerLoweringPhase::BeforeTypecheck,
            |graph, _, _| {
                graph
                    .metadata
                    .insert("test.scoped.lowering".into(), Value::Bool(true));
                vec![AppliedPlannerLowering {
                    id: String::new(),
                    phase: PlannerLoweringPhase::BeforeTypecheck,
                    summary: "scoped lowering ran".into(),
                    changed: true,
                    metadata: Default::default(),
                }]
            },
        );

        let output = build_plan(
            PlannerInput {
                graph: Graph::default(),
            },
            PlannerConfig {
                lowerings: scoped,
                ..PlannerConfig::default()
            },
        );
        assert_eq!(
            output.plan.graph.metadata.get("test.scoped.lowering"),
            Some(&Value::Bool(true))
        );
    }
}
