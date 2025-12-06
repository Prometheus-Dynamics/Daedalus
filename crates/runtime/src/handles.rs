/// Handle to a node port (alias + port name).
///
/// ```
/// use daedalus_runtime::handles::PortHandle;
/// let port = PortHandle::new("node", "out");
/// assert_eq!(port.node_alias, "node");
/// ```
#[derive(Clone, Debug)]
pub struct PortHandle {
    pub node_alias: String,
    pub port: String,
}

impl PortHandle {
    /// Build a new port handle.
    pub fn new(node_alias: impl Into<String>, port: impl Into<String>) -> Self {
        Self {
            node_alias: node_alias.into(),
            port: port.into(),
        }
    }
}

/// Handle to a node id + alias pair.
///
/// ```
/// use daedalus_runtime::handles::NodeHandle;
/// let node = NodeHandle::new("demo:node").alias("alias");
/// assert_eq!(node.alias, "alias");
/// ```
#[derive(Clone, Debug)]
pub struct NodeHandle {
    pub id: String,
    pub alias: String,
}

impl NodeHandle {
    /// Create a handle that uses the id as the initial alias.
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            alias: id.clone(),
            id,
        }
    }

    /// Return a cloned handle with a new alias.
    pub fn alias(&self, alias: impl Into<String>) -> Self {
        let mut cloned = self.clone();
        cloned.alias = alias.into();
        cloned
    }

    /// Build an input port handle.
    pub fn input(&self, name: impl Into<String>) -> PortHandle {
        PortHandle::new(self.alias.clone(), name)
    }

    /// Build an output port handle.
    pub fn output(&self, name: impl Into<String>) -> PortHandle {
        PortHandle::new(self.alias.clone(), name)
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
        &self.id
    }

    fn alias(&self) -> &str {
        &self.alias
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
