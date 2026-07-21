use std::path::PathBuf;
use std::time::Duration;

/// Time constants for pruning age cutoffs.
pub(super) const ONE_DAY: Duration = Duration::from_secs(86400);
pub(super) const THREE_DAYS: Duration = Duration::from_secs(3 * 86400);
pub(super) const SEVEN_DAYS: Duration = Duration::from_secs(7 * 86400);
pub(super) const THIRTY_DAYS: Duration = Duration::from_secs(30 * 86400);
pub(super) const ONE_MONTH: Duration = Duration::from_secs(30 * 86400);
pub(super) const THREE_MONTHS: Duration = Duration::from_secs(90 * 86400);
pub(super) const SIX_MONTHS: Duration = Duration::from_secs(180 * 86400);

/// Information needed to execute a prune pass without holding the cache lock.
pub(super) struct PrunePlan {
    pub(super) static_range: String,
    pub(super) range_dir: PathBuf,
    /// Files with `last_modified < cutoff` should be deleted.
    pub(super) cutoff: u64,
    pub(super) fast_delete: bool,
}

/// Result of executing a prune pass.
pub(super) struct PruneResult {
    pub(super) static_range: String,
    pub(super) range_dir: PathBuf,
    /// Number of files remaining in the directory after pruning.
    pub(super) file_count: usize,
    /// Oldest last-modified timestamp among remaining files.
    pub(super) oldest_last_modified: u64,
}

/// The recommended action after checking cache state.
pub(super) enum PruneAction {
    /// Cache is over limit - prune this range.
    Prune(PrunePlan),
    /// Cache is within limit - adjust check frequency.
    NoPrune { frequency: u32 },
}

/// Compute the recommended pruner check frequency based on free space.
pub(super) fn prune_frequency(
    cache_limit: u64,
    cache_size_with_overhead: u64,
    want_free: u64,
) -> u32 {
    let free = cache_limit.saturating_sub(cache_size_with_overhead);
    if free > want_free * 10 {
        600
    } else if free > want_free {
        60
    } else {
        10
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_frequency_matches_java_free_space_bands() {
        let want_free = 100;

        assert_eq!(prune_frequency(2000, 899, want_free), 600);
        assert_eq!(prune_frequency(2000, 1899, want_free), 60);
        assert_eq!(prune_frequency(2000, 1900, want_free), 10);
    }
}
