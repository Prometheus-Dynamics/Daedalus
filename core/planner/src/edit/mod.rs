//! Changing a stored graph: patches, and bringing its port lists up to date.

pub mod impact;
pub mod patch;
pub mod refresh;

pub use impact::{PatchImpact, analyze};
pub use patch::{AppliedOp, PatchError, PatchReport, UnknownSelector, apply_patch};
pub use refresh::{NodeRefresh, RefreshPolicy, RefreshReport, refresh_ports, refresh_ports_with};
