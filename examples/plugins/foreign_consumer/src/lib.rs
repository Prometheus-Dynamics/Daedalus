//! A plugin that consumes a type it does not share with the host.
//!
//! It depends on `daedalus-plugins-example-project` with a feature the host does not enable,
//! so when it is built separately (as a `cdylib` in its own cargo invocation) its `Counter` is
//! another Rust type than the host's. Its node therefore takes the counter through the
//! `example:counter_view` foreign interface, which the host's copy of the example plugin
//! provides; the plugin passes the boundary type check and reads host counters in place.

use daedalus::macros::{node, plugin};
use daedalus::runtime::NodeError;
use daedalus::transport::ForeignRef;
use daedalus_plugins_example_project::{CounterInterface, CounterView};

/// The counter's value and the address it was read from (to check nothing was copied).
#[node(id = "read_counter", inputs("counter"), outputs("value", "address"))]
fn read_counter(counter: ForeignRef<'_, CounterInterface>) -> Result<(i32, i64), NodeError> {
    Ok((counter.value(), counter.data() as i64))
}

#[plugin(
    id = "example_foreign_consumer",
    deps("example_rust"),
    nodes(read_counter)
)]
pub struct ForeignConsumerPlugin;

#[cfg(feature = "dylib")]
daedalus::export_plugin!(ForeignConsumerPlugin);
