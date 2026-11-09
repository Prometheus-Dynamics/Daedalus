//! The explain record as JSON.

use super::Explain;

/// Indented, with a final newline.
pub fn to_json(explain: &Explain) -> Result<String, serde_json::Error> {
    let mut text = serde_json::to_string_pretty(explain)?;
    text.push('\n');
    Ok(text)
}

pub fn from_json(text: &str) -> Result<Explain, serde_json::Error> {
    serde_json::from_str(text)
}
