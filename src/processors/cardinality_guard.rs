use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::signal::Labels;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    Drop,
}

/// Tracks unique label combinations per metric name and drops signals that
/// would push a metric over `max_series_per_metric`.
///
/// This protects the tenant's budget from label explosions (e.g. a
/// high-cardinality `user_id` label accidentally applied to a counter).
#[derive(Clone)]
pub struct CardinalityGuard {
    max:    usize,
    // metric_name → set of label fingerprints
    series: Arc<DashMap<String, HashSet<u64>>>,
}

impl CardinalityGuard {
    pub fn new(max_series_per_metric: usize) -> Self {
        Self {
            max:    max_series_per_metric,
            series: Arc::new(DashMap::new()),
        }
    }

    pub fn check_and_record(&self, metric_name: &str, labels: &Labels) -> Verdict {
        let fp = fingerprint(labels);
        let mut entry = self.series.entry(metric_name.to_string()).or_default();

        if entry.contains(&fp) {
            return Verdict::Allow; // known series
        }

        if entry.len() >= self.max {
            tracing::warn!(
                metric = metric_name,
                series = entry.len(),
                max    = self.max,
                "cardinality guard: dropping new series"
            );
            return Verdict::Drop;
        }

        entry.insert(fp);
        Verdict::Allow
    }

    /// Current series count for a metric (for inspector / debug UI)
    pub fn series_count(&self, metric_name: &str) -> usize {
        self.series.get(metric_name).map(|e| e.len()).unwrap_or(0)
    }
}

fn fingerprint(labels: &Labels) -> u64 {
    use std::hash::Hash;
    let mut pairs: Vec<(&String, &String)> = labels.iter().collect();
    pairs.sort_by_key(|(k, _)| k.as_str());

    let mut h = std::collections::hash_map::DefaultHasher::new();
    pairs.hash(&mut h);
    std::hash::Hasher::finish(&h)
}
