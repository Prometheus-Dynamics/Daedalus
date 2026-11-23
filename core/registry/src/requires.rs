//! What a graph needs of a host: the packages its nodes come from. A host writes this into a
//! document it stores, and checks it when it loads one, so a graph that needs a package the host
//! lacks, or a newer one than it has, is refused by the package's name before anything is planned.

#![cfg(feature = "std")]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use daedalus_graph::{GraphBody, GraphDoc, GroupDef, NodeKindDoc, Requirement};

use crate::Registry;

/// Why a document's `requires` is not met by a registry.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequireError {
    /// No node here comes from the provider.
    Missing { id: String, version: Option<String> },
    /// The host has the provider at a version the requirement doesn't accept.
    Version {
        id: String,
        required: String,
        has: String,
    },
    /// The provider's nodes carry no version to compare: a plugin's do not.
    NoVersion { id: String, required: String },
    /// The requirement's version isn't a plain version such as `1.4.2`.
    Unreadable { id: String, version: String },
}

impl fmt::Display for RequireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequireError::Missing { id, version: None } => {
                write!(f, "requires `{id}`, and no node from it is registered")
            }
            RequireError::Missing {
                id,
                version: Some(version),
            } => write!(
                f,
                "requires `{id}` {version}, and no node from it is registered"
            ),
            RequireError::Version { id, required, has } => {
                write!(f, "requires `{id}` {required}, and the host has {has}")
            }
            RequireError::NoVersion { id, required } => write!(
                f,
                "requires `{id}` {required}, and the host's `{id}` has no version to compare"
            ),
            RequireError::Unreadable { id, version } => write!(
                f,
                "requires `{id}` at `{version}`, which is not a version such as 1.4.2"
            ),
        }
    }
}

impl std::error::Error for RequireError {}

/// `1.4.2` as its numbers. One, two or three parts; a missing part is zero.
fn version(text: &str) -> Option<[u64; 3]> {
    let mut parts = [0u64; 3];
    let mut count = 0;
    for part in text.split('.') {
        *parts.get_mut(count)? = part.parse().ok()?;
        count += 1;
    }
    (count > 0).then_some(parts)
}

/// Whether a host at `has` meets a requirement of `required`: the same major version (the same
/// minor, while the major is 0), and at least as new.
fn meets(has: [u64; 3], required: [u64; 3]) -> bool {
    let same_line = match required[0] {
        0 => has[0] == 0 && has[1] == required[1],
        major => has[0] == major,
    };
    same_line && has >= required
}

impl Registry {
    /// Each provider here with its version: the packages the registered nodes come from.
    fn providers(&self) -> BTreeMap<&'static str, &'static str> {
        self.entries()
            .map(|entry| (entry.decl.provider.name, entry.decl.provider.version))
            .filter(|(name, _)| !name.is_empty())
            .collect()
    }

    /// The node ids a registered group is made of, and the groups inside it.
    fn stage_ids(&self, group: &GroupDef, into: &mut BTreeSet<String>) {
        for (_, id) in group.stages {
            if into.insert(id.to_string())
                && let Some(inner) = self.entry(id).and_then(|entry| entry.decl.group)
            {
                self.stage_ids(inner, into);
            }
        }
    }

    /// What `graph` needs of a host: the provider of every node it uses, a stage of a group
    /// included, each once with its version. A host writes this into the document it stores.
    /// A node that isn't registered contributes nothing: planning the graph refuses it by id.
    pub fn requirements(&self, graph: &GraphBody) -> Vec<Requirement> {
        let mut ids = BTreeSet::new();
        for node in &graph.nodes {
            if node.kind != NodeKindDoc::HostBridge {
                ids.insert(node.id.clone());
            }
        }
        for group in &graph.groups {
            ids.extend(group.stages.values().cloned());
        }
        for id in ids.clone() {
            if let Some(group) = self.entry(&id).and_then(|entry| entry.decl.group) {
                self.stage_ids(group, &mut ids);
            }
        }
        let mut found: BTreeMap<&str, &str> = BTreeMap::new();
        for id in &ids {
            if let Some(entry) = self.entry(id) {
                let provider = entry.decl.provider;
                if !provider.name.is_empty() {
                    found.insert(provider.name, provider.version);
                }
            }
        }
        found
            .into_iter()
            .map(|(name, version)| Requirement {
                id: name.to_string(),
                version: (!version.is_empty()).then(|| version.to_string()),
            })
            .collect()
    }

    /// Checks a document's `requires` against what is registered, and gives every requirement
    /// that isn't met. A requirement names a provider and may give a version. With no version,
    /// any version of the provider will do. With one, the host's must be compatible and at least
    /// as new: the same major version, or the same minor while the major is 0. A host calls this
    /// when it loads a stored document, before it plans.
    pub fn check_requires(&self, doc: &GraphDoc) -> Result<(), Vec<RequireError>> {
        let providers = self.providers();
        let mut errors = Vec::new();
        for requirement in &doc.requires {
            let id = requirement.id.clone();
            let Some(&has) = providers.get(id.as_str()) else {
                errors.push(RequireError::Missing {
                    id,
                    version: requirement.version.clone(),
                });
                continue;
            };
            let Some(required) = &requirement.version else {
                continue;
            };
            let Some(wanted) = version(required) else {
                errors.push(RequireError::Unreadable {
                    id,
                    version: required.clone(),
                });
                continue;
            };
            match version(has) {
                Some(has) if meets(has, wanted) => {}
                Some(_) => errors.push(RequireError::Version {
                    id,
                    required: required.clone(),
                    has: has.to_string(),
                }),
                None => errors.push(RequireError::NoVersion {
                    id,
                    required: required.clone(),
                }),
            }
        }
        match errors.is_empty() {
            true => Ok(()),
            false => Err(errors),
        }
    }
}
