# Cache Prune Shutdown and Persistent-State Recovery

**Date:** 2026-07-21  
**Status:** Proposed  
**Priority:** P1

## Context

`CacheHandler` persists `cacheCount`, `cacheSize`, `staticRangeOldest`, and the
LRU table in `pcache_*` files so a normal restart can avoid a full cache scan.
These fields are an optimisation; the cache filesystem remains the source of
truth for serving a file.

The current Rust pruner first unlinks every selected file in
`CachePruner::execute_prune`, then applies all counter changes in one later
`CacheHandler::apply_prune_result` call.  The shutdown coordinator waits only
five seconds for background tasks, aborts tasks that have not stopped, and
then writes persistent cache state.

Consequently, a SIGTERM during a long, normal-speed prune (one second between
deletions) can leave files already removed from disk while their deletion has
not been recorded in `cacheCount` or `cacheSize`.  The stale state can then be
written at shutdown and trusted on the next startup.

There is a second, independent recovery difference: after it has decided
whether it can use persistent state, Java deletes all three `pcache_*` files
before continuing startup.  It also deletes `pcache_info` before deserializing
the remaining objects.  A process that subsequently dies therefore must
rescan on its next startup.  Rust currently leaves all persistent files in
place, so an OOM, SIGKILL, power loss, or aborted prune can reuse an older
state snapshot.  This also affects a `rescan_cache=true` startup: Java clears
the old snapshot even though it deliberately did not load it; Rust currently
does not.

### Java baseline

- `CacheHandler.loadPersistentData()` removes `pcache_info` before it reads
  `pcache_ages` and `pcache_lru`.
- The constructor then calls `deletePersistentData()` unconditionally, which
  removes `pcache_info`, `pcache_ages`, and `pcache_lru` whether startup used
  persistent state or performed a rescan.
- `CacheHandler.checkAndPruneCache()` calls `deleteFileFromCache()` for each
  successful candidate before sleeping.  That method immediately changes the
  counters and stats.
- The Java prune loop checks `client.isShuttingDown()` before every candidate
  and returns without publishing a partially recomputed range-age result.

Relevant sources:

- Java: `HentaiAtHome_1.6.5_src/src/hath/base/CacheHandler.java`, lines
  191-210 and 564-607.
- Rust: `src/cache/persistent.rs`, `src/cache/pruner.rs`, `src/cache/mod.rs`,
  and `src/client.rs`.

## Goals

1. After every successfully unlinked cache file, `cacheCount`, `cacheSize`,
   and reported cache stats reflect that deletion before the next await point.
2. A shutdown that interrupts a prune cannot persist a snapshot that claims
   deleted files still exist.
3. Any unclean process exit after persistent state has been loaded forces a
   full filesystem rescan on the next startup, matching Java's dirty-marker
   behaviour.
4. Preserve normal graceful-restart performance: a clean shutdown still saves
   a complete, validated state snapshot.

## Non-goals

- Making Rust read Java `ObjectOutputStream` cache metadata.  Cross-client
  migration may rescan safely.
- Eliminating the pre-existing Java-compatible race between a prune directory
  snapshot and a concurrent proxy import.
- Changing cache-hit lookup.  Serving continues to use the physical file and
  its expected length, not persistent counters.
- Changing delete-failure semantics: Rust must continue to update counters
  only after a successful filesystem unlink.

## Design

### 1. Treat the complete `pcache_*` set as clean-shutdown state

Split cache startup into two explicit phases:

1. Attempt to load the persistent snapshot unless `rescan_cache` is set.  If
   valid, retain the decoded state in memory for this process only.
2. Immediately call `persistent::clear(config)` before cache cleanup, rescan,
   server startup, or any background task.  It removes `pcache_info`,
   `pcache_ages`, and `pcache_lru`, ignoring `NotFound` and logging other I/O
   failures.

Do this regardless of whether the snapshot was successfully loaded, rejected,
or intentionally bypassed by `rescan_cache`.  A fresh `pcache_*` set is
published only after the final clean shutdown save succeeds.

This is the Java constructor's `deletePersistentData()` ordering.  It means a
crash between startup and shutdown cannot trust a previous snapshot, including
when the current process deliberately performed a full rescan.  `pcache_info`
remains the final publish marker: `save()` writes ages, then LRU, then info;
if any write fails, it must leave no `pcache_info` behind.

### 2. Commit prune counter changes per successful unlink

Refactor the prune execution boundary so that a successful `remove_file` is
immediately committed through a `CacheHandler` operation.  That operation
must:

1. decrement `cache_count` and `cache_size` by the validated `HVFile` size;
2. refresh `Stats.cache_count` and `Stats.cache_size`; and
3. execute only after `remove_file` returned `Ok`.

`PruneResult` must no longer own deferred counter deltas.  It may retain only
the directory/range-age outcome needed after a completed scan.

This preserves the existing Rust improvement over Java: a failed unlink must
not affect state.  For a correctly placed cache file, it otherwise has Java's
per-file accounting timing.

### 3. Make prune shutdown cooperative

Pass the cache shutdown `CancellationToken` into prune execution.  Before
examining each next file and while waiting between deletions, select on the
token.  On cancellation:

- stop deleting further files;
- return an explicit `Interrupted` outcome rather than a normal
  `PruneResult`;
- do not remove or recompute `static_range_oldest` from a partial directory
  snapshot; and
- let `CachePruner::run()` exit promptly.

Files deleted before cancellation are already fully reflected in counters by
the per-unlink commit.  Retaining the old range-age entry is Java-compatible:
Java returns from the loop before it publishes a new age or removes the range
entry.

### 4. Never save cache state after forced background-task abortion

Make `BackgroundTasks::join_for` report whether every tracked task exited
cleanly.  If its timeout path aborts any task, the shutdown flow must skip
`cache.save_persistent_data()`.  Since the clean marker was removed at
startup, the following startup will rescan.

This is a defensive fallback for uninterruptible filesystem operations or a
future cache task that does not respond to cancellation.  Under ordinary
pruning, cooperative cancellation should complete before the timeout.

## Error handling and invariants

| Event | Filesystem state | In-memory state | Next startup |
|---|---|---|---|
| Normal prune deletion | File absent | Counters already decremented | Clean save may be reused |
| Unlink fails | File remains | Counters unchanged | Clean save may be reused |
| Shutdown during prune | Some files may be absent | Every prior successful deletion is accounted | Clean save allowed only if all tasks ended |
| Background task timeout/abort | Unknown partial work | May be incomplete | Do not write marker; force rescan |
| SIGKILL/OOM/power loss | Unknown partial work | Process lost | Marker was consumed; force rescan |

Counter operations must be guarded against underflow in the implementation.
Underflow indicates an already-invalid state and must not wrap an unsigned
counter into a huge cache size; log the invariant failure and ensure no clean
snapshot is published for the next startup.

## Files expected to change

- `src/cache/persistent.rs` — add `clear()` for all three `pcache_*` files;
  ensure save publishes `pcache_info` last and clears it after a failed save.
- `src/cache/mod.rs` — call `persistent::clear()` immediately after deciding
  the startup state; add the per-successful-prune accounting operation; narrow
  `apply_prune_result` to completed range metadata.
- `src/cache/pruner.rs` — cooperative cancellation and explicit interrupted
  result; commit each successful unlink immediately.
- `src/client.rs` — propagate background-task clean/timeout status and skip
  cache persistence after an abort.

## Acceptance tests

1. A completed prune of valid old files removes them and decrements count/size
   exactly once per file.
2. A failed unlink leaves both the file and counters unchanged.
3. Start a slow prune, wait until at least one file has been removed, cancel
   it, and assert that the removed-file counter delta is already visible.
4. The cancellation case must leave the selected range entry intact rather
   than applying a partial age/empty-directory result.
5. A successful persistent load retains state only in memory and removes all
   three `pcache_*` files; constructing a new handler without a final save
   performs a full rescan.
6. `rescan_cache=true` also removes a pre-existing complete `pcache_*` set.
7. A normal clean shutdown writes a valid marker and a subsequent start uses
   it without a rescan.
8. Force the background-task timeout path and assert that no usable
   `pcache_info` remains.
9. Run `cargo test` for the complete regression suite.
