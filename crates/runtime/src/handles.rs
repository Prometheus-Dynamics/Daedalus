use std::borrow::Borrow;
use std::fmt;
use std::sync::Arc;

macro_rules! define_text_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Arc<str>);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into().into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                Ok(Self::from(value))
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value.into())
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0.to_string()
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }
    };
}

define_text_id!(NodeAlias, "Runtime node alias used for graph wiring.");
define_text_id!(NodeHandleId, "Runtime node id used for graph wiring.");
define_text_id!(
    PortId,
    "Runtime port identifier used for node and host bridge wiring."
);
define_text_id!(HostAlias, "Runtime host bridge alias.");
define_text_id!(FeatureFlag, "Runtime feature flag identifier.");
define_text_id!(CapabilityId, "Runtime capability identifier.");

/// Handle to a node port (alias + port name).
///
/// ```
/// use daedalus_runtime::handles::{NodeAlias, PortHandle, PortId};
/// let port = PortHandle::new("node", "out");
/// assert_eq!(port.node_alias_id(), NodeAlias::from("node"));
/// assert_eq!(port.port_id(), PortId::from("out"));
/// ```
#[derive(Clone, Debug)]
pub struct PortHandle {
    node_alias: NodeAlias,
    port: PortId,
}

impl PortHandle {
    /// Build a new port handle.
    pub fn new(node_alias: impl Into<String>, port: impl Into<String>) -> Self {
        Self {
            node_alias: NodeAlias::new(node_alias),
            port: PortId::new(port),
        }
    }

    pub fn node_alias(&self) -> &str {
        self.node_alias.as_str()
    }

    pub fn port(&self) -> &str {
        self.port.as_str()
    }

    pub fn node_alias_id(&self) -> NodeAlias {
        self.node_alias.clone()
    }

    pub fn port_id(&self) -> PortId {
        self.port.clone()
    }
}

/// Handle to a node id + alias pair.
///
/// ```
/// use daedalus_runtime::handles::{NodeAlias, NodeHandle};
/// let node = NodeHandle::new("demo:node").alias("alias");
/// assert_eq!(node.alias_id(), NodeAlias::from("alias"));
/// ```
#[derive(Clone, Debug)]
pub struct NodeHandle {
    id: NodeHandleId,
    alias: NodeAlias,
}

impl NodeHandle {
    /// Create a handle that uses the id as the initial alias.
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            alias: NodeAlias::new(id.clone()),
            id: NodeHandleId::new(id),
        }
    }

    /// Return a cloned handle with a new alias.
    pub fn alias(&self, alias: impl Into<String>) -> Self {
        let mut cloned = self.clone();
        cloned.alias = NodeAlias::new(alias);
        cloned
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn alias_name(&self) -> &str {
        self.alias.as_str()
    }

    pub fn alias_id(&self) -> NodeAlias {
        self.alias.clone()
    }

    /// Build an input port handle.
    pub fn input(&self, name: impl Into<String>) -> PortHandle {
        PortHandle::new(self.alias.as_str(), name)
    }

    /// Build an output port handle.
    pub fn output(&self, name: impl Into<String>) -> PortHandle {
        PortHandle::new(self.alias.as_str(), name)
    }
}

/// Common interface for node handles.
///
/// ```
/// use daedalus_runtime::handles::{NodeHandle, NodeHandleLike};
/// let node = NodeHandle::new("demo");
/// assert_eq!(node.id(), "demo");
/// ```
pub trait NodeHandleLike {
    fn id(&self) -> &str;
    fn alias(&self) -> &str;
}

impl NodeHandleLike for NodeHandle {
    fn id(&self) -> &str {
        self.id()
    }

    fn alias(&self) -> &str {
        self.alias_name()
    }
}

impl<T> NodeHandleLike for &T
where
    T: NodeHandleLike + ?Sized,
{
    fn id(&self) -> &str {
        (*self).id()
    }

    fn alias(&self) -> &str {
        (*self).alias()
    }
}
