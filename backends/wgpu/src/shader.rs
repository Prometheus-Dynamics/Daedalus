//! Whole-shader nodes: one compute pass each, from a [`ShaderSpec`] the authoring macro made.

mod dispatch;
mod pipeline;
mod spec;

pub use dispatch::{Bound, Shader, ShaderError};
pub use pipeline::ShaderStats;
pub use spec::{BindingKind, BindingSpec, Dispatch, SampleKind, ShaderSpec, TextureAccess};

pub(crate) use pipeline::ShaderCache;

#[cfg(test)]
mod tests;
