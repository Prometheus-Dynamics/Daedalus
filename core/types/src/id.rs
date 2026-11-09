//! Names written in code, and the indexes the planner assigns.

use crate::key::valid_text;

/// A node's id. A bad literal is a compile error when used in a `const`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(&'static str);

impl NodeId {
    pub const fn new(text: &'static str) -> NodeId {
        assert!(valid_text(text), "invalid node id");
        NodeId(text)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// A port's name. A bad literal is a compile error when used in a `const`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortName(&'static str);

impl PortName {
    pub const fn new(text: &'static str) -> PortName {
        assert!(valid_text(text), "invalid port name");
        PortName(text)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

macro_rules! index {
    ($($(#[$doc:meta])* $name:ident($int:ty)),* $(,)?) => {$(
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub $int);
    )*};
}

index!(
    NodeIndex(u32),
    PortIndex(u16),
    EdgeIndex(u32),
    SlotIndex(u32),
    TypeIndex(u32),
    SpaceIndex(u16),
    BackendIndex(u8),
);
