//! Strongly-typed IDs for nodes, ports, edges, channels, runs, and ticks.
//! Serde encodes them as `"prefix:n"` strings for stability in planner/runtime
//! diagnostics and golden outputs.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::errors::{CoreError, CoreErrorCode};

macro_rules! define_id {
    ($name:ident, $prefix:literal) => {
        #[doc = concat!("Strongly-typed ID for `", stringify!($prefix), "` resources.")]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            pub const PREFIX: &'static str = $prefix;

            pub fn new(raw: NonZeroU64) -> Self {
                Self(raw)
            }

            pub fn get(self) -> NonZeroU64 {
                self.0
            }

            pub fn into_inner(self) -> NonZeroU64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}:{}", Self::PREFIX, self.0)
            }
        }

        impl From<NonZeroU64> for $name {
            fn from(value: NonZeroU64) -> Self {
                Self::new(value)
            }
        }

        impl TryFrom<u64> for $name {
            type Error = CoreError;

            fn try_from(value: u64) -> Result<Self, Self::Error> {
                let Some(nz) = NonZeroU64::new(value) else {
                    return Err(CoreError::new(
                        CoreErrorCode::InvalidId,
                        format!("{} must be non-zero", Self::PREFIX),
                    ));
                };
                Ok(Self::new(nz))
            }
        }

        impl FromStr for $name {
            type Err = CoreError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let (prefix, rest) = s.split_once(':').ok_or_else(|| {
                    CoreError::new(CoreErrorCode::InvalidId, format!("missing prefix in {}", s))
                })?;
                if prefix != Self::PREFIX {
                    return Err(CoreError::new(
                        CoreErrorCode::InvalidId,
                        format!("expected prefix {} but found {}", Self::PREFIX, prefix),
                    ));
                }
                let raw: u64 = rest.parse().map_err(|_| {
                    CoreError::new(
                        CoreErrorCode::InvalidId,
                        format!("invalid numeric id {}", s),
                    )
                })?;
                Self::try_from(raw)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = String::deserialize(deserializer)?;
                s.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

define_id!(NodeId, "node");
define_id!(PortId, "port");
define_id!(EdgeId, "edge");
define_id!(ChannelId, "chan");
define_id!(RunId, "run");
define_id!(TickId, "tick");

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn display_and_parse_round_trip() {
        let id = NodeId::try_from(42).expect("id");
        let rendered = id.to_string();
        assert_eq!(rendered, "node:42");
        let parsed = rendered.parse::<NodeId>().expect("parse");
        assert_eq!(parsed, id);
    }

    #[test]
    fn rejects_zero() {
        let err = NodeId::try_from(0).unwrap_err();
        assert_eq!(err.code(), CoreErrorCode::InvalidId);
    }

    #[test]
    fn serde_string_is_stable() {
        let id = EdgeId::try_from(7).expect("id");
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(json, "\"edge:7\"");
        let back: EdgeId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, id);
    }
}
