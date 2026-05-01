use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// How a consumer intends to access an input payload.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// Shared immutable access.
    #[default]
    Read,
    /// Exclusive ownership if the planner can prove it, otherwise an explicit branch/materialize
    /// step must be planned.
    Move,
    /// Mutable access. Runtime can mutate in place when unique or use a planned COW/branch path.
    Modify,
    /// Borrowed projection/reinterpretation access.
    View,
}

impl AccessMode {
    pub fn as_str(self) -> &'static str {
        match self {
            AccessMode::Read => "read",
            AccessMode::Move => "move",
            AccessMode::Modify => "modify",
            AccessMode::View => "view",
        }
    }

    pub fn satisfies(self, required: AccessMode) -> bool {
        match required {
            AccessMode::Read => matches!(
                self,
                AccessMode::Read | AccessMode::Move | AccessMode::Modify | AccessMode::View
            ),
            AccessMode::View => matches!(self, AccessMode::View),
            AccessMode::Move => matches!(self, AccessMode::Move | AccessMode::Modify),
            AccessMode::Modify => matches!(self, AccessMode::Modify),
        }
    }
}

impl fmt::Display for AccessMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AccessMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "read" => Ok(AccessMode::Read),
            "move" => Ok(AccessMode::Move),
            "modify" => Ok(AccessMode::Modify),
            "view" => Ok(AccessMode::View),
            _ => Err(()),
        }
    }
}

/// Payload residency class.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Residency {
    #[default]
    Cpu,
    Gpu,
    CpuAndGpu,
    External,
}

impl Residency {
    pub fn as_str(self) -> &'static str {
        match self {
            Residency::Cpu => "cpu",
            Residency::Gpu => "gpu",
            Residency::CpuAndGpu => "cpu_and_gpu",
            Residency::External => "external",
        }
    }
}

impl fmt::Display for Residency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Residency {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "cpu" => Ok(Residency::Cpu),
            "gpu" => Ok(Residency::Gpu),
            "cpu_and_gpu" => Ok(Residency::CpuAndGpu),
            "external" => Ok(Residency::External),
            _ => Err(()),
        }
    }
}

/// Transport adapter operation class.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AdaptKind {
    #[default]
    Identity,
    Reinterpret,
    View,
    SharedView,
    Cow,
    CowView,
    MetadataOnly,
    Branch,
    MutateInPlace,
    Materialize,
    DeviceTransfer,
    DeviceUpload,
    DeviceDownload,
    Serialize,
    Deserialize,
    Custom,
}

pub type AdapterKind = AdaptKind;

impl AdaptKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AdaptKind::Identity => "identity",
            AdaptKind::Reinterpret => "reinterpret",
            AdaptKind::View => "view",
            AdaptKind::SharedView => "shared_view",
            AdaptKind::Cow => "cow",
            AdaptKind::CowView => "cow_view",
            AdaptKind::MetadataOnly => "metadata_only",
            AdaptKind::Branch => "branch",
            AdaptKind::MutateInPlace => "mutate_in_place",
            AdaptKind::Materialize => "materialize",
            AdaptKind::DeviceTransfer => "device_transfer",
            AdaptKind::DeviceUpload => "device_upload",
            AdaptKind::DeviceDownload => "device_download",
            AdaptKind::Serialize => "serialize",
            AdaptKind::Deserialize => "deserialize",
            AdaptKind::Custom => "custom",
        }
    }
}

impl fmt::Display for AdaptKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AdaptKind {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "identity" => Ok(AdaptKind::Identity),
            "reinterpret" => Ok(AdaptKind::Reinterpret),
            "view" => Ok(AdaptKind::View),
            "shared_view" => Ok(AdaptKind::SharedView),
            "cow" => Ok(AdaptKind::Cow),
            "cow_view" => Ok(AdaptKind::CowView),
            "metadata_only" => Ok(AdaptKind::MetadataOnly),
            "branch" => Ok(AdaptKind::Branch),
            "mutate_in_place" => Ok(AdaptKind::MutateInPlace),
            "materialize" => Ok(AdaptKind::Materialize),
            "device_transfer" => Ok(AdaptKind::DeviceTransfer),
            "device_upload" => Ok(AdaptKind::DeviceUpload),
            "device_download" => Ok(AdaptKind::DeviceDownload),
            "serialize" => Ok(AdaptKind::Serialize),
            "deserialize" => Ok(AdaptKind::Deserialize),
            "custom" => Ok(AdaptKind::Custom),
            _ => Err(()),
        }
    }
}
