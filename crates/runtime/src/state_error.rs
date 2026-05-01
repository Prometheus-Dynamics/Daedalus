use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("{lock} lock poisoned")]
    LockPoisoned { lock: &'static str },
    #[error("serde error: {source}")]
    Serde {
        #[from]
        source: serde_json::Error,
    },
    #[error("state type mismatch for key '{key}'")]
    StateTypeMismatch { key: String },
    #[error("resource class mismatch for node '{node_id}' resource '{name}'")]
    ResourceClassMismatch { node_id: String, name: String },
    #[error("resource '{name}' on node '{node_id}' is tracked as usage-only")]
    ResourceUsageOnly { node_id: String, name: String },
    #[error("resource '{name}' on node '{node_id}' is already mutably borrowed")]
    ResourceAlreadyBorrowed { node_id: String, name: String },
    #[error("resource type mismatch for node '{node_id}' resource '{name}'")]
    ResourceTypeMismatch { node_id: String, name: String },
}

impl StateError {
    pub(crate) fn lock(lock: &'static str) -> Self {
        Self::LockPoisoned { lock }
    }

    pub(crate) fn state_type_mismatch(key: &str) -> Self {
        Self::StateTypeMismatch {
            key: key.to_string(),
        }
    }

    pub(crate) fn resource_class_mismatch(node_id: &str, name: &str) -> Self {
        Self::ResourceClassMismatch {
            node_id: node_id.to_string(),
            name: name.to_string(),
        }
    }

    pub(crate) fn resource_usage_only(node_id: &str, name: &str) -> Self {
        Self::ResourceUsageOnly {
            node_id: node_id.to_string(),
            name: name.to_string(),
        }
    }

    pub(crate) fn resource_already_borrowed(node_id: &str, name: &str) -> Self {
        Self::ResourceAlreadyBorrowed {
            node_id: node_id.to_string(),
            name: name.to_string(),
        }
    }

    pub(crate) fn resource_type_mismatch(node_id: &str, name: &str) -> Self {
        Self::ResourceTypeMismatch {
            node_id: node_id.to_string(),
            name: name.to_string(),
        }
    }
}
