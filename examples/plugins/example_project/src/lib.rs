//! Minimal Rust plugin crate showing the “native” node authoring experience.
//!
//! This is the baseline that other languages try to match:
//! - `#[node(...)]` macro attaches IDs/ports/defaults/metadata.
//! - `#[derive(NodeConfig)]` makes config inputs first-class (defaults/validation).
//! - `state(T)` wires up persistent state for stateful nodes.
//! - Capability nodes can dispatch via the global capability registry.
//! - `#[type_key]` gives a plugin-owned payload type a stable key that other plugins (and
//!   dynamically loaded copies of this one) resolve without registering it first.
//! - A foreign interface (`CounterInterface`) lets separately built plugins read a `Counter`
//!   without sharing its Rust type (see `examples/plugins/foreign_consumer`).
//! - `Lease` declares no key; the plugin maps it (`register_foreign_type`), as a library's
//!   integration plugin keys a type it cannot annotate. Plugins using it depend on this plugin
//!   (see `examples/plugins/dependent`).
//! - The plugin registers the crate's build (`crate_build_info!()`, features exported by
//!   `build.rs`), so a host refusing a separately built copy names the feature difference.

use std::ffi::c_void;

use daedalus::macros::NodeConfig;
use daedalus::transport::{ForeignRef, ProvideForeign, foreign_interface};
use daedalus::{PluginRegistry, declare_plugin, macros::node, runtime::NodeError, type_key};

// --- Stateless typed nodes --------------------------------------------------

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn add(a: i32, b: i32) -> Result<i32, NodeError> {
    Ok(a + b)
}

#[node(id = "split", inputs("value"), outputs("out0", "out1"))]
fn split(value: i32) -> Result<(i32, i32), NodeError> {
    Ok((value, -value))
}

/// A payload type this crate owns, with its stable transport key.
#[type_key("example:counter")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counter(pub i32);

/// A type without a key of its own: [`ExampleProjectPlugin`] maps it to `example:lease`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease(pub u32);

/// Key [`ExampleProjectPlugin`] gives [`Lease`].
pub const LEASE_KEY: &str = "example:lease";

#[node(id = "count", inputs("value"), outputs("counter"))]
fn count(value: i32) -> Result<Counter, NodeError> {
    Ok(Counter(value))
}

// --- Foreign interface ------------------------------------------------------

foreign_interface! {
    /// `example:counter_view` v1: reads a [`Counter`] through C-safe accessors, so plugins built
    /// separately from this crate (with a different `Counter` type) can consume host counters.
    pub interface CounterInterface("example:counter_view", version = 1);
    /// Accessors of `example:counter_view` v1.
    pub struct CounterVTable {
        pub value: unsafe extern "C" fn(data: *const c_void) -> i32,
    }
}

unsafe extern "C" fn counter_value(data: *const c_void) -> i32 {
    // Safety: only used in `Counter`'s vtable, whose data pointer is a `Counter`.
    unsafe { (*data.cast::<Counter>()).0 }
}

// Safety: `counter_value` reads a `Counter` and cannot unwind.
unsafe impl ProvideForeign<CounterInterface> for Counter {
    fn vtable() -> &'static CounterVTable {
        &CounterVTable {
            value: counter_value,
        }
    }
}

/// Typed accessors for `ForeignRef<'_, CounterInterface>`.
pub trait CounterView {
    fn value(&self) -> i32;
}

impl CounterView for ForeignRef<'_, CounterInterface> {
    fn value(&self) -> i32 {
        // Safety: the view checked the interface; the handle keeps the counter alive.
        unsafe { (self.vtable().value)(self.data()) }
    }
}

// --- Config-backed input ----------------------------------------------------

#[derive(Clone, Debug, NodeConfig)]
struct ScaleConfig {
    // Config fields are also “ports”, with defaults + validation rules.
    #[port(default = 2, min = 1, max = 16, policy = "clamp")]
    factor: i32,
}

#[node(id = "scale_cfg", inputs("value", config = ScaleConfig), outputs("out"))]
fn scale_cfg(value: i32, cfg: ScaleConfig) -> Result<i32, NodeError> {
    Ok(value * cfg.factor)
}

// --- Stateful node ----------------------------------------------------------

#[derive(Default)]
struct AccumState {
    sum: i32,
}

#[node(id = "accum", inputs("value"), outputs("sum"), state(AccumState))]
fn accum(value: i32, state: &mut AccumState) -> Result<i32, NodeError> {
    state.sum += value;
    Ok(state.sum)
}

// --- Metadata-only ports (port source/default metadata) ---------------------

pub fn modes() -> Vec<&'static str> {
    vec!["fast", "balanced", "quality"]
}

#[node(
    id = "choose_mode_meta",
    inputs(port(name = "mode", source = "modes", default = "quality")),
    outputs("out")
)]
fn choose_mode_meta(mode: String) -> Result<String, NodeError> {
    Ok(format!("mode={mode}"))
}

// --- Capability-dispatch node ----------------------------------------------

#[node(id = "cap_add", capability = "Add", inputs("a", "b"), outputs("out"))]
fn cap_add<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: std::ops::Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

fn register_capabilities(registry: &mut PluginRegistry) {
    registry.register_capability_typed::<i32, _>("Add", |a, b| Ok(*a + *b));
}

declare_plugin!(
    ExampleProjectPlugin,
    "example_rust",
    [
        add,
        split,
        count,
        scale_cfg,
        accum,
        choose_mode_meta,
        cap_add
    ],
    install = |registry| {
        // How this crate was built (version, features; see `build.rs`), so a host names the
        // exact difference when a plugin's copy of `Counter` is another Rust type.
        registry.register_crate_build(daedalus::crate_build_info!())?;
        register_capabilities(registry);
        registry.register_foreign_type::<Lease>(LEASE_KEY)?;
        registry.register_foreign_provider::<Counter, CounterInterface>()?;
    }
);

// The dynamic-plugin export lives in `examples/plugins/example_project_dylib`: other plugins
// link this crate, so its library must not carry `export_plugin!`'s symbols.
