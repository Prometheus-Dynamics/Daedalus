//! Near matches for an id that wasn't found.

use super::catalog::Catalog;

const SUGGESTIONS: usize = 5;

fn edit_distance(a: &[u8], b: &[u8]) -> usize {
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0; b.len() + 1];
    for (i, x) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, y) in b.iter().enumerate() {
            let cost = usize::from(x != y);
            current[j + 1] = (previous[j + 1] + 1)
                .min(current[j] + 1)
                .min(previous[j] + cost);
        }
        previous.clone_from_slice(&current);
    }
    previous[b.len()]
}

pub(super) fn suggest_nodes(catalog: &Catalog<'_>, missing: &str) -> Vec<String> {
    let needle = missing.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return Vec::new();
    }
    let mut scored: Vec<(usize, &str)> = catalog
        .ids()
        .map(|id| {
            (
                edit_distance(needle.as_bytes(), id.to_ascii_lowercase().as_bytes()),
                id,
            )
        })
        .collect();
    scored.sort();
    scored
        .into_iter()
        .take(SUGGESTIONS)
        .map(|(_, id)| id.to_string())
        .collect()
}
