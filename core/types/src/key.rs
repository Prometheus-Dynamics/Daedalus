//! A type's name, which is all Daedalus ever compares.

use core::cmp::Ordering;
use core::hash::{Hash, Hasher};

/// Lowercase letters, digits and `. _ - :`, and not empty.
pub(crate) const fn valid_text(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let ok = b.is_ascii_lowercase()
            || b.is_ascii_digit()
            || b == b'.'
            || b == b'_'
            || b == b'-'
            || b == b':';
        if !ok {
            return false;
        }
        i += 1;
    }
    true
}

#[derive(Clone, Debug)]
enum KeyText {
    Static(&'static str),
    #[cfg(feature = "alloc")]
    Shared(alloc::sync::Arc<str>),
}

/// Plain text naming a type. Equal text is an equal key, wherever the text is stored.
#[derive(Clone, Debug)]
pub struct TypeKey(KeyText);

impl TypeKey {
    /// A key from a literal. A bad literal is a compile error when used in a `const`.
    pub const fn new(text: &'static str) -> TypeKey {
        assert!(valid_text(text), "invalid type key");
        TypeKey(KeyText::Static(text))
    }

    /// A key from text that arrived at run time.
    #[cfg(feature = "alloc")]
    pub fn shared(text: &str) -> Result<TypeKey, crate::Error> {
        if valid_text(text) {
            Ok(TypeKey(KeyText::Shared(alloc::sync::Arc::from(text))))
        } else {
            Err(crate::Error::new(
                crate::Code::InvalidKey,
                "invalid type key",
            ))
        }
    }

    pub fn as_str(&self) -> &str {
        match &self.0 {
            KeyText::Static(text) => text,
            #[cfg(feature = "alloc")]
            KeyText::Shared(text) => text,
        }
    }
}

impl PartialEq for TypeKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for TypeKey {}

impl PartialOrd for TypeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TypeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for TypeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}
