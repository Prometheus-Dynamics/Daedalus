//! Whole-shader nodes: one compute pass each, from a [`ShaderSpec`] the authoring macro made.

mod dispatch;
pub(crate) mod pipeline;
mod spec;

pub use dispatch::{Bound, Run, Shader, ShaderError};
pub use pipeline::ShaderStats;
pub use spec::{
    BindingKind, BindingSpec, Dispatch, SampleKind, ShaderSpec, TextureAccess, port_bindings,
};

pub(crate) use dispatch::OpenPass;
pub(crate) use pipeline::ShaderCache;

#[cfg(test)]
mod tests;
