pub(super) const LRU_CACHE_SIZE: usize = 1_048_576;

/// Per-file LRU tracking. Protected by its own [`std::sync::Mutex`] because it is
/// accessed on every HTTP request (via [`LruState::mark_recently_accessed`]) and
/// periodically cycled (via [`LruState::cycle`]).
#[derive(Debug)]
pub(super) struct LruState {
    pub(super) lru_cache_table: Box<[u16]>,
    pub(super) lru_clear_pointer: usize,
}

impl Default for LruState {
    fn default() -> Self {
        Self::new()
    }
}

impl LruState {
    pub(super) fn new() -> Self {
        Self {
            lru_cache_table: vec![0u16; LRU_CACHE_SIZE].into_boxed_slice(),
            lru_clear_pointer: 0,
        }
    }

    /// Low-level LRU bit check/set. Returns `true` if the file's LRU bit was
    /// NOT previously set (i.e., the file hasn't been accessed recently).
    pub(super) fn mark_recently_accessed(&mut self, fileid: &str) -> bool {
        if fileid.len() < 10 {
            return false;
        }
        let array_index = usize::from_str_radix(&fileid[4..9], 16).unwrap_or(0);
        let bit_mask: u16 = 1u16 << u16::from_str_radix(&fileid[9..10], 16).unwrap_or(0);
        if (self.lru_cache_table[array_index] & bit_mask) != 0 {
            return false;
        }
        self.lru_cache_table[array_index] |= bit_mask;
        true
    }

    /// Java: `CacheHandler.cycleLRUCacheTable()`
    pub(super) fn cycle(&mut self) {
        let clear_until = (self.lru_clear_pointer + 17).min(LRU_CACHE_SIZE);
        self.lru_cache_table[self.lru_clear_pointer..clear_until].fill(0);
        self.lru_clear_pointer = if clear_until >= LRU_CACHE_SIZE {
            0
        } else {
            clear_until
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_recently_accessed_sets_bit_once() {
        let mut lru = LruState::new();

        assert!(lru.mark_recently_accessed("abcd00000f"));
        assert!(!lru.mark_recently_accessed("abcd00000f"));
    }

    #[test]
    fn mark_recently_accessed_ignores_short_fileids() {
        let mut lru = LruState::new();

        assert!(!lru.mark_recently_accessed("short"));
    }

    #[test]
    fn cycle_clears_seventeen_entries_and_advances_pointer() {
        let mut lru = LruState::new();
        lru.lru_cache_table[16] = 1;
        lru.lru_cache_table[17] = 1;

        lru.cycle();

        assert_eq!(lru.lru_cache_table[16], 0);
        assert_eq!(lru.lru_cache_table[17], 1);
        assert_eq!(lru.lru_clear_pointer, 17);
    }

    #[test]
    fn cycle_wraps_at_table_end() {
        let mut lru = LruState::new();
        lru.lru_clear_pointer = LRU_CACHE_SIZE - 1;
        lru.lru_cache_table[LRU_CACHE_SIZE - 1] = 1;

        lru.cycle();

        assert_eq!(lru.lru_cache_table[LRU_CACHE_SIZE - 1], 0);
        assert_eq!(lru.lru_clear_pointer, 0);
    }
}
