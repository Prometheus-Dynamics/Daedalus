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
mod type_expr;
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

/// Turn a unit struct into a `daedalus_runtime::plugins::Plugin` that
/// installs the listed types, nodes, adapters and devices.
///
/// Arguments (only `id` is required):
///
/// - `id = "..."`: plugin id; node handles are prefixed with it.
/// - `deps("other.plugin", ...)`: plugin dependencies, recorded in the manifest and install
///   context.
/// - `install = path::to::fn`: extra `fn(&mut PluginInstallContext<'_>) -> PluginResult<()>`
///   hook, run before everything else.
/// - `parts(path, ...)`: values implementing `PluginPart`, installed in order.
/// - `types(Ty, ...)`: `DaedalusTypeExpr` types (`#[type_key]` or derived), registered as named
///   types with `HostExportPolicy::None`.
/// - `values(Ty, ...)`: `DaedalusTypeExpr + ToValue` types (e.g. `#[derive(DaedalusTypeExpr,
///   DaedalusToValue)]` descriptors), registered with `HostExportPolicy::Value` plus a value
///   serializer.
///
///   Both register the nested `DaedalusTypeExpr` field types of derived types first, so nested
///   descriptors need no separate entry.
/// - `nodes(fn_name, ...)`: `#[node]` functions; registers their boundary contracts, descriptors
///   and handlers.
/// - `adapters(fn_name, ...)`: `#[adapt]` functions; calls `register_<fn>_adapter`.
/// - `devices(fn_name, ...)`: `#[device]` functions; calls `register_<fn>_device`.
///
/// The struct is re-emitted with one public field per node (`<fn>: <Fn>NodeHandle`), plus
/// `new()`, `Default`, `node_<fn>()` handle accessors, an inherent `install(&self, ctx)` and an
/// implementation of `Plugin` whose manifest carries `CARGO_PKG_VERSION` and `deps`. Generic
/// structs are rejected.
///
/// ```ignore
/// use daedalus::{PluginRegistry, macros::node, plugin, runtime::NodeError};
/// use daedalus::runtime::plugins::RegistryPluginExt;
///
/// #[node(id = "scale", inputs("value"), outputs("out"))]
/// fn scale(value: i64) -> Result<i64, NodeError> {
///     Ok(value * 2)
/// }
///
/// #[plugin(id = "demo.math", nodes(scale))]
/// pub struct MathPlugin;
///
/// let mut registry = PluginRegistry::new();
/// registry.install_plugin(&MathPlugin::new()).expect("install plugin");
/// ```
#[proc_macro_attribute]
pub fn plugin(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    plugin_impl::plugin(args, item)
}

/// Give a struct or enum a stable transport type key.
///
/// Accepts a string literal or a path to a string constant: `#[type_key("ns:name")]`,
/// `#[type_key(FRAME_KEY)]`, `#[type_key(key = ...)]` or `#[type_key(type_key = ...)]`. The item
/// is kept as written and the macro adds an implementation of `DaedalusTypeExpr` with
/// `TYPE_KEY = "ns:name"` and `type_expr() == TypeExpr::Opaque("ns:name")`. Register it with
/// `#[plugin(types(...))]` or `PluginRegistry::register_daedalus_type`.
///
/// Generic items and non-struct/enum items are rejected.
///
/// ```ignore
/// use daedalus::type_key;
///
/// pub const FRAME_KEY: &str = "demo:frame";
///
/// #[type_key(FRAME_KEY)]
/// pub struct Frame(pub Vec<u8>);
/// ```
#[proc_macro_attribute]
pub fn type_key(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    type_key_impl::type_key(args, item)
}

/// Register a function as a transport adapter between two type keys.
///
/// The function must be non-generic, take exactly one `T`, `&T`, `&mut T` or `Arc<T>` argument
/// and return `Result<U, TransportError>`, `Result<Arc<U>, TransportError>` or (for `&mut T`
/// in-place adapters) `Result<(), TransportError>`. The function is kept as written and the
/// macro adds `fn register_<fn>_adapter(&mut PluginRegistry) -> PluginResult<()>` (same
/// visibility), which `#[plugin(adapters(...))]` calls.
///
/// Arguments (only `id` is required):
///
/// - `id = "..."`: adapter id.
/// - `from = ...`, `to = ...`: source/target type keys (string literals or string constants);
///   default to the transport keys of the input and output types.
/// - `kind = "..."`: `AdaptKind` (`identity`, `reinterpret`, `view`, `shared_view`, `cow`,
///   `cow_view`, `metadata_only`, `branch`, `mutate_in_place`, `materialize`,
///   `device_transfer`, `device_upload`, ...); defaults to `materialize`.
/// - `access = "..."`: `read`, `move`, `modify` or `view`; inferred from the signature when
///   omitted.
/// - `cost = <int>`: planner CPU cost (default `1`).
/// - `residency = "cpu" | "gpu" | "cpu_and_gpu" | "external"`, `layout = "..."`,
///   `requires_gpu = <bool>`: capability metadata for the planner.
/// - `feature = "..."` (repeatable) or `features = "a,b"`: feature flags the adapter requires.
///
/// ```ignore
/// use daedalus::{adapt, transport::TransportError};
///
/// #[adapt(id = "demo.frame_to_len", from = "demo:frame", to = "demo:len", cost = 4)]
/// fn frame_len(frame: &Frame) -> Result<u64, TransportError> {
///     Ok(frame.0.len() as u64)
/// }
/// ```
#[proc_macro_attribute]
pub fn adapt(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    adapt_impl::adapt(args, item)
}

/// Register a CPU-to-device upload function (and its download counterpart) as a typed device
/// transport.
///
/// Applied to a non-generic upload function `fn(&Cpu) -> Result<Device, TransportError>`.
/// Arguments (all required): `id = "..."`, `cpu = "<cpu type key>"`,
/// `device = "<device type key>"` and `download = path::to::download_fn` where the download
/// function is `fn(&Device) -> Result<Cpu, TransportError>`.
///
/// The function is kept as written and the macro adds
/// `fn register_<fn>_device(&mut PluginRegistry) -> PluginResult<()>` (same visibility), which
/// registers a `TypedDeviceTransport` with adapters `<id>.upload` and `<id>.download`.
/// `#[plugin(devices(...))]` calls it for you.
///
/// ```ignore
/// use daedalus::{device, transport::TransportError};
///
/// fn download_frame(frame: &GpuFrame) -> Result<Frame, TransportError> {
///     Ok(frame.0.clone())
/// }
///
/// #[device(id = "demo.frame", cpu = "demo:frame", device = "demo:frame@gpu", download = download_frame)]
/// fn upload_frame(frame: &Frame) -> Result<GpuFrame, TransportError> {
///     Ok(GpuFrame(frame.clone()))
/// }
/// ```
#[proc_macro_attribute]
pub fn device(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    device_impl::device(args, item)
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
/// Use `#[daedalus(type_key = "cv:camera_calibration")]` (or a string constant) to pin a portable
/// key; otherwise the default key is `rust:<module_path>::<TypeName>`. Field types that implement
/// `DaedalusTypeExpr` (also inside `Vec`/`Option`/`Box`/`Arc`/arrays/tuples) are reported by
/// `visit_dependencies`, so registering the type registers them first.
#[proc_macro_derive(DaedalusTypeExpr, attributes(daedalus))]
pub fn daedalus_type_expr(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    daedalus_type_derive::daedalus_type_expr(item)
}

/// Derive `ToValue` for a struct/enum to enable JSON-friendly host export.
#[proc_macro_derive(DaedalusToValue)]
pub fn daedalus_to_value(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    to_value_derive::daedalus_to_value(item)
}
