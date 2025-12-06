//! Convenience re-exports for registry consumers.
pub use crate::diagnostics::{RegistryError, RegistryErrorCode, RegistryResult};
pub use crate::ids::NodeId;
pub use crate::store::{NodeDescriptor, Registry, RegistryView};
pub use daedalus_data::convert::{ConversionResolution, ConverterId};
pub use daedalus_data::descriptor::{DataDescriptor, DescriptorId, DescriptorVersion};
pub use daedalus_data::model::TypeExpr;
