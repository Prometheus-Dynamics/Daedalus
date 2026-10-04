//! Cheap-clone identifier string shared by transport and runtime ids.

use crate::portable::Arc;
use alloc::string::String;
use core::borrow::Borrow;
use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::ops::Deref;

/// Immutable identifier text that is either a `&'static str` or a shared `Arc<str>`.
///
/// Building one from a string literal never allocates, and cloning never copies text. Equality,
/// ordering, hashing and serialization only look at the text, so the two representations are
/// interchangeable (and `Borrow<str>` map lookups stay consistent).
#[derive(Clone)]
pub struct IdStr(Repr);

#[derive(Clone)]
enum Repr {
    Static(&'static str),
    Shared(Arc<str>),
}

impl IdStr {
    /// Wrap a string literal without allocating.
    pub const fn from_static(value: &'static str) -> Self {
        Self(Repr::Static(value))
    }

    /// Copy borrowed text into a shared allocation.
    pub fn new(value: impl Into<String>) -> Self {
        Self(Repr::Shared(value.into().into()))
    }

    pub fn as_str(&self) -> &str {
        match &self.0 {
            Repr::Static(value) => value,
            Repr::Shared(value) => value,
        }
    }
}

impl Deref for IdStr {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for IdStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for IdStr {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for IdStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for IdStr {}

impl PartialOrd for IdStr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IdStr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for IdStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl fmt::Debug for IdStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for IdStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&'static str> for IdStr {
    fn from(value: &'static str) -> Self {
        Self::from_static(value)
    }
}

impl From<String> for IdStr {
    fn from(value: String) -> Self {
        Self(Repr::Shared(value.into()))
    }
}

impl From<&String> for IdStr {
    fn from(value: &String) -> Self {
        Self::new(value.as_str())
    }
}

impl From<Arc<str>> for IdStr {
    fn from(value: Arc<str>) -> Self {
        Self(Repr::Shared(value))
    }
}

impl serde::Serialize for IdStr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for IdStr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::from)
    }
}

/// Define a text id newtype over [`IdStr`](crate::IdStr) with the conversions every id shares.
///
/// The invoking crate must depend on `serde`.
#[doc(hidden)]
#[macro_export]
macro_rules! define_text_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        ///
        /// String literals convert without allocating (`"frame".into()`); borrowed text goes
        /// through [`Self::new`]. Clones never copy text.
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name($crate::IdStr);

        impl $name {
            pub fn new(value: impl Into<$crate::__private::String>) -> Self {
                Self($crate::IdStr::new(value))
            }

            /// Wrap a string literal without allocating.
            pub const fn from_static(value: &'static str) -> Self {
                Self($crate::IdStr::from_static(value))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl ::core::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl From<&'static str> for $name {
            fn from(value: &'static str) -> Self {
                Self::from_static(value)
            }
        }

        /// Cheap clone (reference-count bump at most); lets `&id` satisfy `impl Into<Id>`.
        impl From<&$name> for $name {
            fn from(value: &$name) -> Self {
                value.clone()
            }
        }

        impl From<$crate::__private::String> for $name {
            fn from(value: $crate::__private::String) -> Self {
                Self(value.into())
            }
        }

        impl From<&$crate::__private::String> for $name {
            fn from(value: &$crate::__private::String) -> Self {
                Self::new(value.as_str())
            }
        }

        impl From<$name> for $crate::__private::String {
            fn from(value: $name) -> Self {
                $crate::__private::String::from(value.as_str())
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.as_str() == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.as_str() == *other
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn static_and_shared_are_interchangeable() {
        let fixed = IdStr::from("frame");
        let shared = IdStr::new("frame");
        assert_eq!(fixed, shared);
        let mut map = HashMap::new();
        map.insert(shared, 1);
        assert_eq!(map.get("frame"), Some(&1));
        assert_eq!(map.get(&fixed), Some(&1));
    }
}
