use serde_json::{Value, json};

use daedalus_planner::ExecutionPlan;
use daedalus_runtime::RuntimePlan;

use crate::EngineError;

/// Render an `EngineError` into a machine-readable JSON value.
pub fn render_error(err: &EngineError) -> Value {
    match err {
        EngineError::InvalidConfig(err) => {
            json!({ "code": "config", "message": err.to_string() })
        }
        EngineError::Config(msg) => json!({ "code": "config", "message": msg }),
        EngineError::Io { path, source } => json!({
            "code": "io",
            "path": path,
            "message": source.to_string(),
        }),
        EngineError::Registry(e) => json!({
            "code": "registry",
            "registry_code": format!("{:?}", e.code()),
            "message": e.message(),
        }),
        EngineError::Planner(diags) => json!({
            "code": "planner",
            "diagnostics": diags,
        }),
        EngineError::Runtime(e) => json!({
            "code": "runtime",
            "message": e.to_string(),
        }),
        EngineError::BundleParse { path, error } => json!({
            "code": "bundle_parse",
            "path": path,
            "message": error,
        }),
        #[cfg(feature = "gpu")]
        EngineError::Gpu(e) => json!({
            "code": "gpu",
            "message": e.to_string(),
        }),
        EngineError::FeatureDisabled(flag) => json!({
            "code": "feature_disabled",
            "feature": flag,
        }),
    }
}

/// Render a planner execution plan to JSON for diagnostics.
pub fn render_plan(plan: &ExecutionPlan) -> Result<Value, EngineError> {
    serde_json::to_value(plan)
        .map_err(|e| EngineError::Config(format!("serialize plan failed: {e}")))
}

/// Render a runtime plan to JSON for diagnostics.
pub fn render_runtime(runtime: &RuntimePlan) -> Result<Value, EngineError> {
    serde_json::to_value(runtime)
        .map_err(|e| EngineError::Config(format!("serialize runtime failed: {e}")))
}
