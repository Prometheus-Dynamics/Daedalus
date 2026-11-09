//! The shapes a plan can be delivered in.

/// How a plan is delivered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Form {
    /// Runs now, in this process. Every unit is a step.
    #[default]
    Steps,
    /// The stored encoding, for a loader or a small runner.
    Blob,
    /// Generated Rust, for a graph sealed into a build.
    Source,
}
