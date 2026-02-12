#![crate_type = "cdylib"]
//! Minimal dynamic plugin that exposes an enum input to reproduce mode-binding issues.

use daedalus::{
    ComputeAffinity, declare_plugin, ffi::export_plugin, macros::node, runtime::NodeError,
};
use serde::{Deserialize, Serialize};

declare_plugin!(
    EnumModePlugin,
    "ffi.enum_mode",
    [enum_mode],
    install = |reg| {
        reg.register_enum::<ExecMode>(["auto", "cpu", "gpu"]);
    }
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecMode {
    #[default]
    Auto,
    Cpu,
    Gpu,
}

#[node(
    id = "enum_mode",
    compute(ComputeAffinity::CpuOnly),
    inputs("mode"),
    outputs("out")
)]
fn enum_mode(mode: ExecMode) -> Result<i32, NodeError> {
    // Return the discriminant for easy validation.
    let val = match mode {
        ExecMode::Auto => 0,
        ExecMode::Cpu => 1,
        ExecMode::Gpu => 2,
    };
    Ok(val)
}

export_plugin!(EnumModePlugin);
