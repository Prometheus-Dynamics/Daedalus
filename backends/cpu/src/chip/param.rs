//! Constants that can change while a graph runs, by the application or by a message from a host.

use super::scalar::{Scalar, ScalarType};
use super::wire::{Reader, SliceWriter, WireError, Write};
use crate::scalar_kind::ScalarKind;

/// Why a parameter wasn't changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[non_exhaustive]
pub enum ParamError {
    /// The value doesn't convert to the parameter's type.
    Type,
    /// The value is outside the parameter's range, or NaN.
    Range,
    /// There's no parameter with this id.
    Id,
    /// A message didn't decode.
    Malformed,
}

impl From<WireError> for ParamError {
    fn from(_: WireError) -> Self {
        ParamError::Malformed
    }
}

/// A parameter's type and inclusive range.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ParamSpec {
    pub kind: ScalarKind,
    pub min: Scalar,
    pub max: Scalar,
}

impl ParamSpec {
    /// A parameter that can take any value of `kind`.
    pub const fn full(kind: ScalarKind) -> ParamSpec {
        let (min, max) = Scalar::bounds(kind);
        ParamSpec { kind, min, max }
    }

    /// Converts `value` to the parameter's type and checks its range.
    pub fn check(&self, value: Scalar) -> Result<Scalar, ParamError> {
        let value = value.coerce(self.kind).ok_or(ParamError::Type)?;
        if self.min.le(value) && value.le(self.max) {
            Ok(value)
        } else {
            Err(ParamError::Range)
        }
    }
}

/// [`ParamSpec::check`], into the parameter's Rust type.
pub fn checked<T: ScalarType>(spec: &ParamSpec, value: Scalar) -> Result<T, ParamError> {
    T::from_scalar(spec.check(value)?).ok_or(ParamError::Type)
}

/// A graph with parameters. A setter takes `&mut self`, so it can't run inside a tick, and a new
/// value is used from the next tick on.
pub trait Tunable {
    fn set_param(&mut self, id: u16, value: Scalar) -> Result<(), ParamError>;

    /// The current value.
    fn param(&self, id: u16) -> Option<Scalar>;

    /// Decodes a [`ParamUpdate`] and applies it.
    fn apply_update(&mut self, message: &[u8]) -> Result<(), ParamError> {
        let update = ParamUpdate::decode(message)?;
        self.set_param(update.id, update.value)
    }
}

/// The message that sets one parameter: the id, then the [`ScalarKind`] tag, then the value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ParamUpdate {
    pub id: u16,
    pub value: Scalar,
}

impl ParamUpdate {
    /// The longest encoding.
    pub const MAX_LEN: usize = 14;

    pub fn decode(message: &[u8]) -> Result<ParamUpdate, ParamError> {
        let mut reader = Reader::new(message);
        let update = ParamUpdate {
            id: reader.u16()?,
            value: reader.scalar()?,
        };
        reader.finish()?;
        Ok(update)
    }

    pub fn encode(&self, out: &mut impl Write) {
        out.varint(self.id.into());
        out.scalar(self.value);
    }

    /// Writes the message into `buf` and says how long it is.
    pub fn to_slice(&self, buf: &mut [u8]) -> Result<usize, WireError> {
        let mut out = SliceWriter::new(buf);
        self.encode(&mut out);
        out.finish()
    }
}
