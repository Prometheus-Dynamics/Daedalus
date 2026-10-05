//! Tunable parameters: graph constants the application (or a host tool, through
//! [`ParamUpdate`] messages) changes while the graph runs. Compiled graphs with parameters and
//! the loaded-plan interpreter both implement [`Tunable`].

use crate::wire::{Reader, WireError, Write};
use crate::{Scalar, ScalarKind, ScalarType};

/// A rejected parameter update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ParamError {
    /// No parameter with this id.
    UnknownId,
    /// The value does not convert to the parameter's type (a fraction for an integer, ...).
    Type,
    /// The value is outside the parameter's range (or NaN).
    Range,
    /// A [`ParamUpdate`] message did not decode.
    Malformed,
}

impl From<WireError> for ParamError {
    fn from(_: WireError) -> Self {
        Self::Malformed
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
    /// Convert `value` to the parameter's type (the planner's rules for constants, see
    /// [`Scalar::coerce`]) and check the range.
    pub fn check(&self, value: Scalar) -> Result<Scalar, ParamError> {
        let value = value.coerce(self.kind).ok_or(ParamError::Type)?;
        if self.min.le(value) && value.le(self.max) {
            Ok(value)
        } else {
            Err(ParamError::Range)
        }
    }
}

/// [`ParamSpec::check`] into the parameter's Rust type (generated compiled graphs).
pub fn checked<T: ScalarType>(spec: &ParamSpec, value: Scalar) -> Result<T, ParamError> {
    T::from_scalar(spec.check(value)?).ok_or(ParamError::Type)
}

/// A graph with tunable parameters, by id (the index in the generated `PARAM_NAMES` or in the
/// plan manifest). A new value applies from the next tick on.
pub trait Tunable {
    fn set_param(&mut self, id: u16, value: Scalar) -> Result<(), ParamError>;

    /// The current value.
    fn param(&self, id: u16) -> Option<Scalar>;

    /// Decode and apply a [`ParamUpdate`] message.
    fn apply_update(&mut self, message: &[u8]) -> Result<(), ParamError> {
        let update = ParamUpdate::decode(message)?;
        self.set_param(update.id, update.value)
    }
}

/// The wire message setting one parameter: postcard of `(id: u16, value: Scalar)`, where a
/// `Scalar` is its [`ScalarKind`] tag then the value (3 to 14 bytes).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ParamUpdate {
    pub id: u16,
    pub value: Scalar,
}

impl ParamUpdate {
    /// The longest encoding.
    pub const MAX_LEN: usize = 14;

    pub fn decode(message: &[u8]) -> Result<Self, ParamError> {
        let mut r = Reader::new(message);
        let update = Self {
            id: r.u16()?,
            value: r.scalar()?,
        };
        r.finish()?;
        Ok(update)
    }

    pub fn encode(&self, out: &mut impl Write) {
        out.varint(self.id.into());
        out.scalar(self.value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::SliceWriter;

    #[test]
    fn spec_converts_and_bounds() {
        let spec = ParamSpec {
            kind: ScalarKind::F32,
            min: Scalar::F32(0.0),
            max: Scalar::F32(1.0),
        };
        assert_eq!(spec.check(Scalar::F64(0.5)), Ok(Scalar::F32(0.5)));
        assert_eq!(spec.check(Scalar::I64(1)), Ok(Scalar::F32(1.0)));
        assert_eq!(spec.check(Scalar::F32(1.5)), Err(ParamError::Range));
        assert_eq!(spec.check(Scalar::F32(f32::NAN)), Err(ParamError::Range));
        assert_eq!(spec.check(Scalar::Bool(true)), Err(ParamError::Type));
        assert_eq!(checked::<f32>(&spec, Scalar::F32(0.25)), Ok(0.25));
    }

    #[test]
    fn update_round_trip() {
        let mut buf = [0u8; ParamUpdate::MAX_LEN];
        for value in [
            Scalar::U64(u64::MAX),
            Scalar::I64(i64::MIN),
            Scalar::F64(-0.5),
        ] {
            let update = ParamUpdate { id: 400, value };
            let mut w = SliceWriter::new(&mut buf);
            update.encode(&mut w);
            let len = w.finish().unwrap();
            assert_eq!(ParamUpdate::decode(&buf[..len]), Ok(update));
            assert_eq!(
                ParamUpdate::decode(&buf[..len - 1]),
                Err(ParamError::Malformed)
            );
        }
    }
}
