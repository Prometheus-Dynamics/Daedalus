//! A read-only look at a host's ports.

use crate::inbound::PortPolicy;
use crate::{Dir, Host};

/// One host port as it stands between ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortInfo<'a> {
    pub name: &'a str,
    /// The type's key.
    pub type_key: &'a str,
    pub dir: Dir,
    /// The input's admission policy. Outputs report the default.
    pub policy: PortPolicy,
    /// Whether the port's value is context, kept for later ticks and not work: a held or shared
    /// input.
    pub held: bool,
    /// How many values are waiting behind it. A value kept for several readers that is work
    /// counts until a tick has run since it came.
    pub queued: u32,
}

impl Host<'_> {
    /// Every host port, sorted by name.
    pub fn port_infos(&self) -> impl Iterator<Item = PortInfo<'_>> {
        (0..self.ports.len()).filter_map(|at| {
            let spec = self.ports.at(at)?;
            Some(PortInfo {
                name: spec.name,
                type_key: spec.key.as_str(),
                dir: spec.dir,
                policy: self.port_policy(spec.name).unwrap_or_default(),
                held: spec.dir == Dir::Input && self.is_context(spec.slot),
                queued: self.waiting(spec.slot),
            })
        })
    }
}
