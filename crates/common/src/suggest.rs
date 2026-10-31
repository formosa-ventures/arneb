//! Nearest-name suggestions for "did you mean" error hints.

/// Returns up to `max` candidates closest to `target`.
///
/// Matching is case-insensitive Levenshtein distance; only candidates within
/// `max(2, target.len() / 3)` edits are kept. Results are ordered by distance,
/// then by name, and deduplicated.
pub fn nearest(
    target: &str,
    candidates: impl IntoIterator<Item = impl AsRef<str>>,
    max: usize,
) -> Vec<String> {
    let t = target.to_lowercase();
    let limit = std::cmp::max(2, target.len() / 3);
    let mut scored: Vec<(usize, String)> = candidates
        .into_iter()
        .filter_map(|c| {
            let c = c.as_ref();
            let d = levenshtein(&t, &c.to_lowercase());
            (d <= limit).then(|| (d, c.to_string()))
        })
        .collect();
    scored.sort();
    scored.dedup_by(|a, b| a.1 == b.1);
    scored.into_iter().take(max).map(|(_, n)| n).collect()
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let sub = prev[j] + usize::from(ca != *cb);
            cur.push(sub.min(prev[j + 1] + 1).min(cur[j] + 1));
        }
        prev = cur;
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_finds_typo_and_orders_by_distance() {
        assert_eq!(
            nearest("l_quantiy", ["l_quantity", "l_orderkey", "l_tax"], 3),
            ["l_quantity"]
        );
    }

    #[test]
    fn nearest_is_case_insensitive_and_bounded() {
        assert_eq!(
            nearest("NAME", ["n_name", "name", "nation"], 2),
            ["name", "n_name"]
        );
    }

    #[test]
    fn nearest_returns_empty_when_nothing_is_close() {
        assert!(nearest("zzzz", ["l_tax"], 3).is_empty());
    }
}
