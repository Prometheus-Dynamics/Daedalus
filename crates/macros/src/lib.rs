mod adapt_impl;
mod branch_payload_derive;
mod config_derive;
mod daedalus_type_derive;
mod device_impl;
mod gpu_state_derive;
mod helpers;
mod node_fn_impl;
mod node_handler_impl;
mod plugin_impl;
mod shader_bindings;
mod to_value_derive;
mod type_key_impl;

/// Define a node handler without generating registry metadata.
///
#[proc_macro_attribute]
pub fn node_handler(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    node_handler_impl::node_handler(args, item)
}

/// Define a node with descriptor + handler generation.
///
#[proc_macro_attribute]
pub fn node(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    node_fn_impl::node(args, item)
}

/// Define a transport-native plugin module.
///
/// This is the entry point for the new plugin DX. The current implementation is intentionally a
/// passthrough while transport-native registry/runtime codegen is introduced.
#[proc_macro_attribute]
pub fn plugin(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    plugin_impl::plugin(args, item)
}

/// Attach a stable transport type key to a type alias.
///
/// This is currently metadata-only and will be consumed by the transport-native plugin macro.
#[proc_macro_attribute]
pub fn type_key(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    type_key_impl::type_key(args, item)
}

/// Declare a transport adapter function.
///
/// This is currently metadata-only and will be consumed by the transport-native plugin macro.
#[proc_macro_attribute]
pub fn adapt(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    adapt_impl::adapt(args, item)
}

/// Declare a device upload/download adapter function.
///
/// This is currently metadata-only and will be consumed by the transport-native plugin macro.
#[proc_macro_attribute]
pub fn device(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    device_impl::device(args, item)
}

/// Derive named output metadata for transport-native node functions.
#[proc_macro_derive(Outputs)]
pub fn outputs(_item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    proc_macro::TokenStream::new()
}

/// Derive `NodeConfig` for structured config inputs.
///
#[proc_macro_derive(NodeConfig, attributes(port, validate))]
pub fn node_config(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    config_derive::node_config(item)
}

/// Derive WGSL bindings for a GPU shader.
///
#[proc_macro_derive(GpuBindings, attributes(gpu))]
pub fn gpu_bindings(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    shader_bindings::gpu_bindings(item)
}

/// Derive GPU state buffer metadata for a POD type.
///
#[proc_macro_derive(GpuStateful, attributes(gpu_state))]
pub fn gpu_stateful(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    gpu_state_derive::gpu_stateful(item)
}

/// Derive `BranchPayload` using `Clone` as the domain branch operation.
#[proc_macro_derive(BranchPayload)]
pub fn branch_payload(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    branch_payload_derive::branch_payload(item)
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
