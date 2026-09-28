use daedalus_transport::IdStr;
use std::borrow::Borrow;
use std::fmt;

macro_rules! define_text_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        ///
        /// String literals convert without allocating (`"frame".into()`); borrowed text goes
        /// through [`Self::new`]. Clones never copy text.
        #[derive(
            Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(IdStr);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(IdStr::new(value))
            }

            /// Wrap a string literal without allocating.
            pub const fn from_static(value: &'static str) -> Self {
                Self(IdStr::from_static(value))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
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

        impl From<&'static str> for $name {
            fn from(value: &'static str) -> Self {
                Self::from_static(value)
            }
        }

        /// Cheap clone (reference-count bump at most); lets `&id` satisfy `impl Into<Id>`.
        impl From<&$name> for $name {
            fn from(value: &$name) -> Self {
                value.clone()
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value.into())
            }
        }

        impl From<&String> for $name {
            fn from(value: &String) -> Self {
                Self::new(value.as_str())
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.as_str().to_string()
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
