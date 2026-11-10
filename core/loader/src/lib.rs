//! Plugins as dynamic libraries. A plugin exports one symbol, `daedalus_plugin_entry`, which
//! declares its nodes and backends. The loader checks the layout hash before it reads anything
//! else, copies the plugin's strings into host storage, joins the nodes into a registry, and runs
//! the plugin's steps. Each library is a generation: it stays loaded while any of its steps lives.

pub use daedalus_abi as abi;

mod entry;
pub mod export;
mod generation;
mod open;
mod owned;
mod pod;
mod records;
mod retry;

pub use entry::{Backend, Loader, Plugin, RegionIn};
pub use generation::Generation;
pub use open::LoadError;
pub use owned::SharedBytes;
pub use pod::{PodBlock, PodError};
pub use retry::{Retry, load_with_retry};
