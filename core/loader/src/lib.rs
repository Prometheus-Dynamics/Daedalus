//! Plugins as dynamic libraries. A plugin exports its shapes and an entry that declares its
//! nodes and backends. The loader checks that the plugin's shapes fit its own before it reads
//! anything else, reads the plugin's tables at the plugin's own sizes, copies the plugin's strings into host storage, joins the nodes into a registry, and runs
//! the plugin's steps. Each library is a generation: it stays loaded while any of its steps lives.

pub use daedalus_abi as abi;

mod adapter;
mod codec;
mod entry;
pub mod export;
mod generation;
mod open;
mod owned;
mod pod;
mod records;
mod retry;

pub use adapter::{PLUGIN_SPACE, PluginBackend};
pub use codec::{Codec, Direction};
pub use entry::{Backend, Loader, Plugin, RegionIn, plugin_tables_held};

/// Runs a step that a loaded plugin's backend compiled, on its cells.
///
/// The step must come from [`Backend::compile`] on a plugin still held by its caller, or on a
/// step the caller holds from one: the step keeps its library loaded, so this is safe to call.
pub fn run_step(
    step: &mut daedalus_abi::Step,
    slots: &mut daedalus_abi::Slots,
    cx: &mut daedalus_abi::StepCx,
) -> daedalus_abi::StepEnd {
    // SAFETY: `run` and `data` belong together, as `Step` requires, and a step from `compile`
    // holds its generation open through `Pinned`. `slots` and `cx` are exclusive for this call.
    unsafe { (step.run)(step.data, slots, cx) }
}
pub use daedalus_abi::SharedBytes;
pub use generation::Generation;
pub use open::LoadError;
pub use owned::{bytes_copied, bytes_released};
pub use pod::{PodBlock, PodError};
pub use retry::{Retry, load_with_retry};
