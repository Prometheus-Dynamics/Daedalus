//! Transport adapter registration options and the declared [`SmartAdapter`] surface.

use super::*;

/// Planner and capability metadata for a registered transport adapter.
#[derive(Clone, Debug)]
pub struct TransportAdapterOptions {
    pub cost: AdaptCost,
    pub access: AccessMode,
    pub requires_gpu: bool,
    pub residency: Option<Residency>,
    pub layout: Option<Layout>,
    pub feature_flags: Vec<String>,
}

impl Default for TransportAdapterOptions {
    fn default() -> Self {
        Self {
            cost: AdaptCost::materialize(),
            access: AccessMode::Read,
            requires_gpu: false,
            residency: None,
            layout: None,
            feature_flags: Vec::new(),
        }
    }
}

impl TransportAdapterOptions {
    pub fn kind(mut self, kind: AdaptKind) -> Self {
        self.cost.kind = kind;
        self
    }

    pub fn cost(mut self, cost: AdaptCost) -> Self {
        self.cost = cost;
        self
    }

    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    pub fn requires_gpu(mut self, requires_gpu: bool) -> Self {
        self.requires_gpu = requires_gpu;
        self
    }

    pub fn residency(mut self, residency: Residency) -> Self {
        self.residency = Some(residency);
        self
    }

    pub fn layout(mut self, layout: impl Into<Layout>) -> Self {
        self.layout = Some(layout.into());
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub(super) fn normalized(mut self) -> Self {
        self.feature_flags.sort();
        self.feature_flags.dedup();
        self
    }
}

/// Declared complex adapter surface for plugins.
///
/// A smart adapter is still just a payload-to-payload runtime function, but it also carries the
/// compile-time facts the planner needs to decide where it can be inserted. This keeps adapter
/// selection declared and deterministic instead of probing payloads at runtime.
pub trait SmartAdapter: Send + Sync + 'static {
    const ID: &'static str;
    const FROM: &'static str;
    const TO: &'static str;

    fn kind() -> AdaptKind {
        AdaptKind::Materialize
    }

    fn access() -> AccessMode {
        AccessMode::Read
    }

    fn cost() -> AdaptCost {
        AdaptCost::new(Self::kind())
    }

    fn requires_gpu() -> bool {
        false
    }

    fn residency() -> Option<Residency> {
        None
    }

    fn layout() -> Option<Layout> {
        None
    }

    fn feature_flags() -> &'static [&'static str] {
        &[]
    }

    fn adapt(payload: Payload, request: &AdaptRequest) -> Result<Payload, TransportError>;

    fn options() -> TransportAdapterOptions {
        let mut options = TransportAdapterOptions::default()
            .cost(Self::cost())
            .access(Self::access())
            .requires_gpu(Self::requires_gpu());
        if let Some(residency) = Self::residency() {
            options = options.residency(residency);
        }
        if let Some(layout) = Self::layout() {
            options = options.layout(layout);
        }
        for flag in Self::feature_flags() {
            options = options.feature_flag(*flag);
        }
        options
    }
}
