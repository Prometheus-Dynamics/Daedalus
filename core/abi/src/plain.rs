//! Plain data: a type whose bytes are its value. A plugin declares one with
//! `#[derive(PlainData)]` from `daedalus-derive`, which gives it a type key and a layout hash. A
//! receiver checks both before it reads the bytes, so two builds that disagree on the layout are
//! refused rather than misread.

/// A `repr(C)` type with no padding, and with a value for every bit pattern. Its bytes are the
/// value, so it can be copied in and out as bytes.
///
/// # Safety
/// Implementors must be `repr(C)`, have no padding bytes, and be valid for every bit pattern. The
/// derive checks the padding and the `repr(C)` attribute; a field that is a `bool`, a reference, or
/// an enum is not plain data, and the derive cannot see that, so the caller must not write one.
// SAFETY: an implementor upholds the contract under `# Safety` above.
pub unsafe trait PlainData: Copy + 'static {
    /// The type's key, a name the two sides agree on.
    const KEY: &'static str;
    /// The layout hash of the type, from its size, alignment and field offsets.
    const LAYOUT: u64;
}
