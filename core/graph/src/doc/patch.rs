//! Edits to a loaded graph. Applying them is the host's job.

#![cfg(feature = "alloc")]

use alloc::string::String;
use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

use super::{DocError, Lit, parse_json, to_json};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PatchOp {
    SetConst {
        node: NodeSelector,
        port: String,
        #[serde(default)]
        value: Option<Lit>,
    },
    ReplaceNodeId {
        node: NodeSelector,
        new_id: String,
    },
    DeleteNode {
        node: NodeSelector,
    },
}

fn first_version() -> u32 {
    1
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPatch {
    #[serde(default = "first_version")]
    pub version: u32,
    #[serde(default)]
    pub ops: Vec<PatchOp>,
}

impl GraphPatch {
    pub fn parse(text: &str) -> Result<GraphPatch, DocError> {
        parse_json(text)
    }

    pub fn to_json(&self) -> Result<String, DocError> {
        to_json(self)
    }
}
