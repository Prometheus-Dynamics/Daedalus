//! Config inputs. A const input is decoded once, at load. A dynamic one keeps its last decoded
//! value and the input version it came from. It decodes again only when that version changes and
//! the raw value differs, so an equal value re-sent on a new version is not decoded again.

/// The last raw config value of one input, and its decoded form.
pub struct ConfigCache<R, T> {
    version: Option<u32>,
    raw: Option<R>,
    value: Option<T>,
    decodes: u32,
}

impl<R: PartialEq, T> Default for ConfigCache<R, T> {
    fn default() -> Self {
        ConfigCache {
            version: None,
            raw: None,
            value: None,
            decodes: 0,
        }
    }
}

impl<R: PartialEq, T> ConfigCache<R, T> {
    pub fn new() -> ConfigCache<R, T> {
        ConfigCache::default()
    }

    /// The decoded value for `raw`, which came from input version `version`. Decodes only when
    /// there is no value yet, or the raw value differs and the version moved. A failed decode
    /// keeps the previous value.
    pub fn update<E>(
        &mut self,
        version: u32,
        raw: R,
        decode: impl FnOnce(&R) -> Result<T, E>,
    ) -> Result<&T, E> {
        let cached = self.value.is_some();
        let reuse = cached && (self.version == Some(version) || self.raw.as_ref() == Some(&raw));
        if !reuse {
            let value = decode(&raw)?;
            self.decodes += 1;
            self.raw = Some(raw);
            self.value = Some(value);
        }
        self.version = Some(version);
        match &self.value {
            Some(value) => Ok(value),
            None => unreachable!("a value is set above"),
        }
    }

    /// A const input: decoded once, on its first call. Later calls return that value.
    pub fn load<E>(&mut self, raw: R, decode: impl FnOnce(&R) -> Result<T, E>) -> Result<&T, E> {
        self.update(0, raw, decode)
    }

    /// How many times this input has been decoded.
    pub fn decodes(&self) -> u32 {
        self.decodes
    }
}
