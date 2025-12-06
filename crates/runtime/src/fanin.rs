use std::ops::Deref;

/// Indexed fan-in input collection.
///
/// This is built from ports named `{prefix}{index}` (e.g. `ins0`, `ins1`, ...) and is ordered by
/// the numeric suffix.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FanIn<T> {
    values: Vec<T>,
}

impl<T> FanIn<T> {
    pub fn new(values: Vec<T>) -> Self {
        Self { values }
    }

    pub fn from_indexed(mut values: Vec<(u32, T)>) -> Self {
        values.sort_by_key(|(i, _)| *i);
        Self {
            values: values.into_iter().map(|(_, v)| v).collect(),
        }
    }

    pub fn into_vec(self) -> Vec<T> {
        self.values
    }
}

impl<T> Deref for FanIn<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<T> IntoIterator for FanIn<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

pub(crate) fn parse_indexed_port(prefix: &str, port: &str) -> Option<u32> {
    let suffix = port.strip_prefix(prefix)?;
    if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    suffix.parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_indexed_port_rejects_non_numeric() {
        assert_eq!(parse_indexed_port("in", "in"), None);
        assert_eq!(parse_indexed_port("in", "inx"), None);
        assert_eq!(parse_indexed_port("in", "in-1"), None);
    }

    #[test]
    fn parse_indexed_port_parses_numeric_suffix() {
        assert_eq!(parse_indexed_port("in", "in0"), Some(0));
        assert_eq!(parse_indexed_port("in", "in10"), Some(10));
    }

    #[test]
    fn from_indexed_sorts_by_index() {
        let v = FanIn::from_indexed(vec![(10, "a"), (2, "b"), (1, "c")]);
        assert_eq!(v.into_vec(), vec!["c", "b", "a"]);
    }
}
