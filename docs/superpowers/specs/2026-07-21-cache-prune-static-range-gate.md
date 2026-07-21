# Cache Prune Static-Range Eligibility Gate

**Date:** 2026-07-21  
**Status:** Proposed  
**Priority:** P2

## Context

The decision to prune must follow the server's current static-range
assignment, not merely historical cache metadata.

Java's `CacheHandler.checkAndPruneCache()` begins a prune only when all of the
following are true:

1. cache space needs to be freed;
2. `cacheCount > 0`; and
3. `Settings.getStaticRangeCount() > 0`.

The Rust equivalent checks that `static_range_oldest` is non-empty instead of
checking `Config.static_range_count`.  The persistent map can outlive a server
assignment change.  Therefore, if the current login assigns zero static
ranges but the loaded cache state still contains range ages, Rust can delete
cache files where Java would leave the cache untouched.

Relevant sources:

- Java: `HentaiAtHome_1.6.5_src/src/hath/base/CacheHandler.java`, line 508.
- Rust: `src/cache/mod.rs`, `CacheHandler::check_prune_action`.

## Goal

Make prune eligibility match Java: no prune scan or deletion may begin when
the current server configuration has zero assigned static ranges.

## Non-goals

- Removing stale files or stale `static_range_oldest` entries when assignment
  is zero.
- Changing the P1 persistent-state recovery and shutdown work; that is
  specified separately in `2026-07-21-cache-prune-shutdown-recovery.md`.
- Changing how a non-zero assignment chooses the oldest cached range.
- Treating an empty `static_range_oldest` map as a valid reason to prune.

## Design

In `CacheHandler::check_prune_action`, retain the existing calculation of
cache pressure and recommended check frequency.  Before constructing a
`PrunePlan`, require all of these conditions:

```text
bytes_to_free > 0
cache_count > 0
config.static_range_count > 0
static_range_oldest is non-empty
```

When any condition is false, return `PruneAction::NoPrune` with the existing
Java-compatible frequency calculation.  Do not mutate counters, range ages,
or filesystem entries.

`Config.static_range_count` is the current server-provided assignment count;
it is intentionally distinct from the number of entries in
`static_range_oldest`, which describes historical local cache state.

## Acceptance tests

1. With cache pressure, `cache_count > 0`, and a non-empty age map, setting
   `static_range_count = 0` returns `NoPrune` and leaves the cache directory
   untouched.
2. With the same cache state and `static_range_count = 1`, the handler returns
   a prune plan for the oldest range.
3. With `static_range_count > 0` but an empty age map, the handler returns
   `NoPrune`.
4. Run `cargo test` for the complete regression suite.

