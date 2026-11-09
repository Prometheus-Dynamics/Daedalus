//! Why a tick or a port call stopped.

/// Small enough to return from anywhere, and has no text to format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[non_exhaustive]
pub enum ChipError {
    /// A queue with the error policy was full.
    QueueFull {
        edge: u16,
    },
    /// A node failed. `code` is the code of its error.
    Node {
        node: u16,
        code: u32,
    },
    Port {
        port: u16,
    },
}
