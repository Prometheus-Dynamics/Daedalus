//! Made plans on the listener: stored by name, bound when a run starts on one, and the answers a
//! host built without the planner gives to the ops that need it.

use std::collections::BTreeSet;
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

use daedalus_backend::Backend;
use daedalus_made::{Bound, bind, decode};
use daedalus_wire::service::{ErrorCode, ServiceError, StartConfig};

use super::{Engine, ServiceHost, lock};

fn refuse(code: ErrorCode, message: impl Into<String>) -> ServiceError {
    ServiceError::new(code, message)
}

impl Engine {
    pub(crate) fn backends(&self) -> Vec<&dyn Backend> {
        self.backends.iter().map(|backend| &**backend).collect()
    }

    /// The bound plan a run of `name` starts on. With the planner, a stored graph is planned now;
    /// otherwise a stored made plan is bound to this host's backends and registry. A made plan
    /// was planned with its merging and priority already, so a run can't ask for other ones.
    pub(crate) fn bound(
        &self,
        session: &str,
        name: &str,
        config: &StartConfig,
    ) -> Result<Bound, ServiceError> {
        #[cfg(feature = "planner")]
        if self
            .graphs
            .get(session)
            .is_some_and(|graphs| graphs.contains_key(name))
        {
            return self.plan_steps(session, name, config).map(Bound::from);
        }
        let Some(made) = self.plans.get(session).and_then(|plans| plans.get(name)) else {
            return Err(refuse(
                ErrorCode::UnknownGraph,
                std::format!("session `{session}` has no graph or plan `{name}`"),
            ));
        };
        if config.unmerged || !config.priority.is_empty() {
            return Err(refuse(
                ErrorCode::Unsupported,
                "a made plan has its merging and priority fixed when it is made; make it again \
                 with the options a run needs",
            ));
        }
        bind(made, &self.registry, &self.backends())
            .map_err(|error| ServiceError::planning(error.to_string(), Vec::new()))
    }

    /// The sessions that hold a graph or a plan.
    pub(crate) fn stored_sessions(&self) -> Vec<String> {
        let held = self.plans.keys().cloned();
        #[cfg(feature = "planner")]
        let held = held.chain(self.graphs.keys().cloned());
        let sessions: BTreeSet<String> = held.collect();
        sessions.into_iter().collect()
    }

    /// The names of the graphs and plans a session holds.
    pub(crate) fn stored_names(&self, session: &str) -> Vec<String> {
        let held = self
            .plans
            .get(session)
            .into_iter()
            .flat_map(|plans| plans.keys().cloned());
        #[cfg(feature = "planner")]
        let held = held.chain(
            self.graphs
                .get(session)
                .into_iter()
                .flat_map(|graphs| graphs.keys().cloned()),
        );
        let names: BTreeSet<String> = held.collect();
        names.into_iter().collect()
    }
}

impl ServiceHost {
    /// Stores a made plan under `name` in `session`, as the `put_plan` op does. A graph of the same
    /// name is replaced by it. The bytes are read again when a run starts on the plan.
    pub fn put_plan(&self, session: &str, name: &str, bytes: &[u8]) -> Result<(), ServiceError> {
        if name.is_empty() {
            return Err(refuse(ErrorCode::InvalidRequest, "a plan needs a name"));
        }
        let made =
            decode(bytes).map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?;
        let mut engine = lock(&self.engine);
        #[cfg(feature = "planner")]
        if let Some(graphs) = engine.graphs.get_mut(session) {
            graphs.remove(name);
        }
        engine
            .plans
            .entry(String::from(session))
            .or_default()
            .insert(String::from(name), Arc::new(made));
        Ok(())
    }
}

/// The answer a host built without the planner gives to an op that needs it.
#[cfg(not(feature = "planner"))]
fn no_planner() -> ServiceError {
    refuse(
        ErrorCode::Unsupported,
        "this host was built without the planner; store a made plan with put_plan",
    )
}

#[cfg(not(feature = "planner"))]
impl ServiceHost {
    pub(super) fn put_graph(
        &self,
        _session: &str,
        _name: &str,
        _json: &str,
    ) -> Result<daedalus_wire::service::Reply, ServiceError> {
        Err(no_planner())
    }

    pub(super) fn patch_graph(
        &self,
        _session: &str,
        _name: &str,
        _patch: &str,
    ) -> Result<daedalus_wire::service::Reply, ServiceError> {
        Err(no_planner())
    }

    pub(super) fn plan(
        &self,
        _session: &str,
        _name: &str,
        _options: &daedalus_wire::service::PlanOptions,
    ) -> Result<daedalus_wire::service::Reply, ServiceError> {
        Err(no_planner())
    }

    pub(super) fn make_plan(
        &self,
        _session: &str,
        _name: &str,
        _config: &StartConfig,
    ) -> Result<daedalus_wire::service::Reply, ServiceError> {
        Err(no_planner())
    }

    pub(super) fn clear_cache(&self) -> Result<daedalus_wire::service::Reply, ServiceError> {
        Err(no_planner())
    }

    pub(super) fn inspect_graph(
        &self,
        _session: &str,
        _name: &str,
    ) -> Result<daedalus_wire::service::Inspection, ServiceError> {
        Err(no_planner())
    }

    pub(super) fn inspect_cache(&self) -> Result<daedalus_wire::service::Inspection, ServiceError> {
        Err(no_planner())
    }
}
