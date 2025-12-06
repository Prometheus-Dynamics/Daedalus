#![crate_type = "cdylib"]
//! Dynamic plugin exported over the FFI boundary with real nodes, capability
//! dispatch, and a custom conversion.
//!
//! Build: `cargo build -p daedalus-ffi --example plugin_lib`
//! Produces: `target/debug/examples/libplugin_lib.{so|dylib|dll}`

use daedalus::declare_plugin;
use daedalus::ffi::export_plugin;
use daedalus::macros::node;
use daedalus::runtime::NodeError;
// Bundle of nodes: source -> sum (capability) -> widen -> sink.
// IDs are auto-prefixed with the plugin id ("ffi.demo") when installed via PluginRegistry.
declare_plugin!(
    DemoPlugin,
    "ffi.demo",
    [source, sum, sum_as_float, sink, log_int, log_float],
    install = |reg| {
        // Capability: shared "Add" dispatch that `sum` uses.
        reg.register_capability_typed::<i32, _>("Add", |a, b| Ok(a + b));
        reg.register_capability_typed::<f64, _>("Add", |a, b| Ok(a + b));

        // Custom conversion to show FFI plugins can extend runtime conversions.
        reg.register_conversion::<i32, f64>(|v| Some(*v as f64));
    }
);

#[node(id = "source", outputs("left", "right"))]
fn source() -> Result<(i32, i32), NodeError> {
    Ok((2, 3))
}

#[node(id = "sum", capability = "Add", inputs("a", "b"), outputs("sum"))]
fn sum<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: std::ops::Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

#[node(id = "sum_as_float", inputs("value"), outputs("out"))]
fn sum_as_float(value: i32) -> Result<f64, NodeError> {
    Ok(value as f64)
}

#[node(id = "sink", inputs("int_sum", "float_sum"))]
fn sink(int_sum: i32, float_sum: f64) -> Result<(), NodeError> {
    println!("ffi.demo results -> int_sum: {int_sum}, float_sum: {float_sum}");
    Ok(())
}

#[node(id = "log_int", inputs("value"))]
fn log_int(value: i32) -> Result<(), NodeError> {
    println!("ffi.demo log_int -> {value}");
    Ok(())
}

#[node(id = "log_float", inputs("value"))]
fn log_float(value: f64) -> Result<(), NodeError> {
    println!("ffi.demo log_float -> {value}");
    Ok(())
}

// Export the plugin for dynamic loading.
export_plugin!(DemoPlugin);
