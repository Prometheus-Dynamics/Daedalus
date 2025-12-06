//! Units and scaling helpers.

use serde::{Deserialize, Serialize};

/// Supported units for typed metadata.
///
/// ```
/// use daedalus_data::units::Unit;
/// let unit = Unit::Seconds;
/// assert_eq!(unit, Unit::Seconds);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Unit {
    Unitless,
    Seconds,
    Meters,
    Bytes,
    Celsius,
    Fahrenheit,
}

/// Unit + scale pair (e.g. milliseconds as `Seconds` with `0.001` scale).
///
/// ```
/// use daedalus_data::units::{Unit, UnitValue};
/// let value = UnitValue::new(Unit::Seconds, 0.5);
/// assert_eq!(value.scale, 0.5);
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnitValue {
    pub unit: Unit,
    pub scale: f64,
}

impl UnitValue {
    /// Construct a new unit value.
    pub fn new(unit: Unit, scale: f64) -> Self {
        Self { unit, scale }
    }
}
