use super::PlannerCatalog;

pub(super) fn suggest_nodes(catalog: &PlannerCatalog, missing: &str) -> Vec<String> {
    fn edit_distance(a: &str, b: &str) -> usize {
        let (a, b) = (a.as_bytes(), b.as_bytes());
        if a.is_empty() {
            return b.len();
        }
        if b.is_empty() {
            return a.len();
        }
        let mut prev: Vec<usize> = (0..=b.len()).collect();
        let mut curr = vec![0; b.len() + 1];
        for (i, &ac) in a.iter().enumerate() {
            curr[0] = i + 1;
            for (j, &bc) in b.iter().enumerate() {
                let cost = if ac == bc { 0 } else { 1 };
                curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
            }
            prev.clone_from_slice(&curr);
        }
        prev[b.len()]
    }

    let needle = missing.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return Vec::new();
    }
    let mut scored: Vec<(usize, String)> = catalog
        .node_ids()
        .map(|id| {
            let id_str = id.0.clone();
            let score = edit_distance(&needle, &id_str.to_ascii_lowercase());
            (score, id_str)
        })
        .collect();
    scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().take(5).map(|(_, id)| id).collect()
}
