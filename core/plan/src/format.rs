//! The records, and where they sit in a Blob.

/// The first bytes of a Blob.
pub const MAGIC: &[u8; 8] = b"DAEDPLAN";
/// The format this build reads and writes.
pub const VERSION: u16 = 1;

/// Magic, format, flags, a reserved word, the hash, then an offset and count per section.
pub(crate) const HEADER_LEN: usize = 24 + SECTION_COUNT * 8;
pub(crate) const SECTION_COUNT: usize = 6;
pub(crate) const STEP_LEN: usize = 32;
pub(crate) const WORD_LEN: usize = 4;

pub(crate) const STEPS: usize = 0;
pub(crate) const NODES: usize = 1;
pub(crate) const SLOTS: usize = 2;
pub(crate) const SPACES: usize = 3;
pub(crate) const TYPES: usize = 4;
pub(crate) const BODIES: usize = 5;

pub(crate) const SECTION_NAMES: [&str; SECTION_COUNT] =
    ["steps", "nodes", "slots", "spaces", "types", "bodies"];

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepKind {
    Call = 0,
    Transfer = 1,
    Wait = 2,
    If = 3,
    ForEach = 4,
    Until = 5,
}

impl StepKind {
    pub(crate) const fn from_byte(byte: u8) -> Option<StepKind> {
        Some(match byte {
            0 => StepKind::Call,
            1 => StepKind::Transfer,
            2 => StepKind::Wait,
            3 => StepKind::If,
            4 => StepKind::ForEach,
            5 => StepKind::Until,
            _ => return None,
        })
    }
}

/// An index range into the section a field names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Span {
    pub start: u32,
    pub len: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepRecord {
    pub kind: StepKind,
    /// Index into the body table.
    pub body: u32,
    /// Into the node section: every node this step runs, so a fault can always name them.
    pub covers: Span,
    /// Into the slot section.
    pub reads: Span,
    /// Into the slot section.
    pub writes: Span,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    Truncated,
    BadMagic,
    UnknownFormat(u16),
    Bounds { section: &'static str, at: u32 },
    UnknownKind(u8),
}
