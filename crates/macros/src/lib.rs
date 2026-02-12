#![allow(
    clippy::collapsible_if,
    clippy::redundant_pattern_matching,
    clippy::doc_overindented_list_items
)]

mod config_derive;
mod daedalus_type_derive;
mod gpu_state_derive;
mod helpers;
mod node_fn_impl;
mod node_handler_impl;
mod shader_bindings;
mod to_value_derive;

/// Define a node handler without generating registry metadata.
///
/// ```ignore
/// use daedalus_macros::node_handler;
///
/// #[node_handler]
/// fn handler(
///     _node: &daedalus_runtime::RuntimeNode,
///     _ctx: &daedalus_runtime::state::ExecutionContext,
///     _io: &mut daedalus_runtime::io::NodeIo,
/// ) -> Result<(), daedalus_runtime::executor::NodeError> {
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn node_handler(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    node_handler_impl::node_handler(args, item)
}

/// Define a node with descriptor + handler generation.
///
/// ```ignore
/// use daedalus_macros::node;
/// use daedalus_runtime::NodeError;
///
/// #[node(id = "demo:noop", inputs("in"), outputs("out"))]
/// fn noop(value: i64) -> Result<i64, NodeError> {
///     Ok(value)
/// }
/// ```
#[proc_macro_attribute]
pub fn node(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    node_fn_impl::node(args, item)
}

/// Derive `NodeConfig` for structured config inputs.
///
/// ```ignore
/// use daedalus_macros::NodeConfig;
///
/// #[derive(NodeConfig)]
/// struct BlurConfig {
///     #[port(default = 3, min = 1, max = 31)]
///     radius: i32,
/// }
/// ```
#[proc_macro_derive(NodeConfig, attributes(port, validate))]
pub fn node_config(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    config_derive::node_config(item)
}

/// Derive WGSL bindings for a GPU shader.
///
/// ```ignore
/// use daedalus_macros::GpuBindings;
///
/// #[derive(GpuBindings)]
/// #[gpu(spec(src = "shader.wgsl", entry = "main"))]
/// struct Params {
///     #[gpu(binding = 0)]
///     data: Vec<u32>,
/// }
/// ```
#[proc_macro_derive(GpuBindings, attributes(gpu))]
pub fn gpu_bindings(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    shader_bindings::gpu_bindings(item)
}

/// Derive GPU state buffer metadata for a POD type.
///
/// ```ignore
/// use daedalus_macros::GpuStateful;
///
/// #[derive(GpuStateful)]
/// struct State {
///     counter: u32,
/// }
/// ```
#[proc_macro_derive(GpuStateful, attributes(gpu_state))]
pub fn gpu_stateful(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    gpu_state_derive::gpu_stateful(item)
}

/// Derive `DaedalusTypeExpr` for a struct/enum to define a stable `TypeExpr` schema.
///
/// Use `#[daedalus(type_key = \"cv:camera_calibration\")]` to pin a portable key; otherwise
/// the default key is `rust:<module_path>::<TypeName>`.
#[proc_macro_derive(DaedalusTypeExpr, attributes(daedalus))]
pub fn daedalus_type_expr(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    daedalus_type_derive::daedalus_type_expr(item)
}

/// Derive `ToValue` for a struct/enum to enable JSON-friendly host export.
#[proc_macro_derive(DaedalusToValue)]
pub fn daedalus_to_value(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    to_value_derive::daedalus_to_value(item)
}
