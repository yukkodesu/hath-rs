//! Test-only helpers for migration compatibility consumers.

use crate::protocol::LegacyCacheState;

pub fn encode_hpcache_v1(state: &LegacyCacheState) -> Vec<u8> {
    crate::protocol::encode_hpcache_v1_for_test(state)
}
