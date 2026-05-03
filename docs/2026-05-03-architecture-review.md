# hath-rs Architecture Review

Date: 2026-05-03

Scope:

- Target parity: Hentai@Home Java 1.6.5 Build 178 -> Rust + Tokio.
- Rust source: `src/`
- Java source: `../HentaiAtHome_1.6.5_src/src/hath/base/`
- Gallery downloader is out of scope.
- This review focuses on protocol semantics, remote-visible behavior, maintainability, module organization, async/Tokio structure, tests, naming, and style.
- No code changes were made as part of this review.

Current workspace notes:

- Source tree had no tracked source diff during the review.
- Existing untracked document: `docs/2026-05-02-java-port-misbehave-audit.md`.
- Verification command run during review: `cargo test`
- Result: 55 passed, 0 failed.
- Remaining warning: `CacheHandler::delete_persistent_data` is unused.

## P0: Protocol Semantics / Remote-Visible Behavior

### 1. HTTP transaction logic is concentrated in `server/mod.rs`

Files:

- `src/server/mod.rs`
- `src/server/request.rs`
- `src/server/response.rs`
- `src/server/body.rs`

Problem:

`HathService::call` is currently doing too much at once: Java-equivalent request execution, cache/proxy selection, servercmd dispatch, header mutation, header byte accounting, stats updates, and response construction. The existing `request`, `response`, `body`, `session`, `access_log`, and `tls` modules help, but the high-risk Java HTTPResponse semantics still flow back into `server/mod.rs`.

This is a shallow Module: deleting it would not remove complexity; it would scatter status/body/header/HEAD/cache/proxy decisions across several call sites. That makes future fixes to remote-visible behavior easy to miss.

Recommended direction:

Create a deeper HTTP transaction Module, for example `server/transaction.rs` or `server/handler.rs`. Keep `server/request.rs` as the parser, but move the Java-equivalent "parsed request -> response" behavior behind one Interface such as:

- request context: client IP classification, local/RPC flags, config snapshot, bandwidth monitor, state handles
- input: `RequestType`
- output: `Response<StreamingBody>`

The Interface should hide file/proxy selection, servercmd command handling, speedtest response selection, and header/body stats decisions.

Risk and migration order:

Start with the least coupled pieces:

1. Move `servercmd` and `threaded_proxy_test` helpers out of `server/mod.rs`.
2. Move `/t`, favicon, robots, and simple error response execution.
3. Move `/h/` file/proxy selection once proxy lifecycle is made less leaky.
4. Keep response header insertion and header-byte accounting unchanged until tests cover it.

Immediate recommendation:

Yes. This should be one of the first refactors, but only in small behavior-preserving steps.

### 2. Proxy lifecycle leaks implementation details across Modules

Files:

- `src/proxy_downloader.rs`
- `src/server/body.rs`
- `src/server/response.rs`
- `src/server/mod.rs`

Problem:

`ProxyFileDownloader` is a valuable deep Module internally: it now owns connect-before-body validation, body-stage retry, watch progress, temp file writes, SHA1 verification, and cache import. But its external Interface leaks too much of that implementation: `temp_file`, `watch_rx`, and `proxy_done_tx` flow through `server/mod.rs`, `server/response.rs`, and `server/body.rs`.

As a result, `server/mod.rs` must know proxy-specific rules:

- open the temp file for body reads
- drop channels for HEAD
- drop `proxy_done_tx` on temp open failure
- preserve Java-style finalize gating

This spreads P0-01 behavior across several Modules.

Recommended direction:

Introduce an opaque proxy transfer Module/Interface, for example `ProxyTransfer`. `ProxyFileDownloader::new` should return a value whose public operations express semantics rather than mechanics:

- create a HEAD response path that preserves "HEAD still initializes and caches"
- create a GET body path that supplies a `StreamingBody`
- expose content length and content type
- keep watch/oneshot/temp-file coordination private or `pub(crate)` only where unavoidable

`server/response.rs` should consume semantic proxy body parts, not raw watch and oneshot channels.

Risk and migration order:

This touches the most fragile recent fix. Before moving code, keep the existing proxy tests green and add at least one fake upstream test covering body-stage retry and one covering HEAD caching/finalization. Then migrate without changing retry or finalize timing.

Immediate recommendation:

Yes. High priority because this is where remote-visible truncation and cache import semantics have already been fragile.

### 3. Java `FileDownloader` semantics are reimplemented in multiple places

Files:

- `src/downloader.rs`
- `src/rpc_client.rs`
- `src/server/mod.rs`
- `src/server/tls.rs`

Problem:

Java `FileDownloader` semantics are currently split across several Modules:

- generic file/cert download in `downloader.rs`
- RPC retry/status/body handling in `rpc_client.rs`
- threaded proxy test download in `server/mod.rs`
- certificate download setup in `server/tls.rs`

Recent fixes aligned important behavior, but the seam is still shallow. Retry count, 404 abort behavior, per-read timeout, first-byte timing, content length requirements, and `Connection: Close` are easy to change in one path while forgetting another.

Recommended direction:

Create a deeper Java-equivalent downloader Module. Its Interface should make the Java semantics explicit:

- connect timeout
- read timeout
- retry count
- max download time stored or enforced according to the Java call site
- memory/file/discard mode
- 404 abort versus retryable HTTP status
- first-byte-to-finish timing for threaded proxy tests
- required `Content-Length`

RPC can use this Module through an Adapter that parses the body into `ServerResponse`.

Risk and migration order:

Start with `threaded_proxy_test` and certificate download because those are closer to Java `FileDownloader` shape. Move RPC last or behind a small Adapter, since KEY_EXPIRED and RPC host failure routing add extra behavior.

Immediate recommendation:

Yes for design and test scaffolding; implementation should follow after fake downloader/upstream tests exist.

### 4. Connection admission rules are embedded in the accept loop

Files:

- `src/server/mod.rs`
- `src/server/session.rs`

Problem:

The accept task currently contains Java-visible admission order:

- TLS handshake first
- normalize IP
- configure send buffer
- identify local/RPC
- startup rejection
- flood control
- max connection admission
- overload notification
- session guard creation

The order matters for Java parity and remote-visible behavior. Today it is embedded in `run_server`, which makes it hard to test as a unit and hard to review for future changes.

Recommended direction:

Extract `server/admission.rs` with a small Interface that returns semantic outcomes:

- accepted with `SessionAdmission`
- rejected during startup
- rejected by flood control
- rejected by max connection limit
- overload notification needed

This Module should keep the Java order explicit and tested.

Risk and migration order:

Low to medium. Move pure decision logic first, keep network/TLS handling in `run_server`. Then move flood control state updates. Do not alter the `SessionManager` RAII guard behavior during the first pass.

Immediate recommendation:

Yes, after proxy lifecycle or in parallel if kept purely local to admission.

## P1: Maintainability / Test Reliability / Future Fix Risk

### 5. `CacheHandler` is a broad facade with too much implementation inside one file

Files:

- `src/cache/mod.rs`
- `src/cache/persistent.rs`
- `src/cache/pruner.rs`

Problem:

`CacheHandler` is close to a useful facade, but its implementation currently includes startup cleanup, persistent load/save, full rescan, LRU, pruning plan calculation, blacklist deletion, stats updates, and cache import. Some of this is intentionally Java-shaped, but too many unrelated maintenance concerns share one file.

Recommended direction:

Keep `CacheHandler` as the public facade and split implementation Modules behind it:

- `cache/startup.rs`: startup cleanup, full rescan, startup safety checks
- `cache/persistent.rs`: persistent load/save/delete lifecycle
- `cache/lru.rs`: `LruState` and Java LRU table behavior
- `cache/prune_plan.rs`: `PrunePlan`, `PruneResult`, `PruneAction`, cutoff calculation
- `cache/blacklist.rs`: initial and periodic blacklist deletion

Use `pub(crate)` or private types where callers do not need the Interface. The current public `LruState`, `PrunePlan`, and `PruneResult` can probably be narrowed once tests are moved to the new modules.

Risk and migration order:

Medium. Start with persistence and LRU because they are easiest to test through stable Interfaces. Then split full rescan and pruning plan. Keep `CacheHandler` method names stable while moving implementation.

Immediate recommendation:

Yes, but after P0 proxy/server seams are protected by tests.

### 6. `Config` owns RPC routing behavior that belongs closer to `RpcClient`

Files:

- `src/config.rs`
- `src/rpc.rs`
- `src/rpc_client.rs`

Problem:

`Config` stores server settings, CLI/local paths, static ranges, image proxy settings, and RPC connection settings. It also implements `get_rpc_host`, which depends on `RpcState` from `rpc_client`. That points the dependency in an awkward direction and makes the RPC routing seam harder to reason about.

Recommended direction:

Move RPC host selection into `RpcClient` or an internal `RpcRouter` Module. Let `Config` provide only a settings snapshot: servers, port, path, client id/key, server time. `RpcClient` should own `rpc_current`, `rpc_last_failed`, periodic reset, and host validity checks.

Risk and migration order:

Low to medium. Preserve current tests for IPv4-mapped normalization and add tests for current-host invalidation after `rpc_server_ip` changes.

Immediate recommendation:

Yes, useful before expanding RPC tests.

### 7. Background task lifecycle lacks one organizing Module

Files:

- `src/client.rs`
- `src/cache/mod.rs`
- `src/rpc_client.rs`
- `src/server/mod.rs`
- `src/server/tls.rs`

Problem:

CancellationToken usage is broadly reasonable, and no obvious await-while-holding-lock issue stood out in the reviewed code. The larger issue is task lifecycle locality: pruner, stats shifter, blacklist fetcher, still_alive, flood pruner, session reaper, cert watcher, and signal handling are spawned from multiple Modules as fire-and-forget tasks.

Shutdown currently relies on cancellation plus selected explicit waits, but there is no single Module that can answer "which background tasks are running, when are they cancelled, and which are joined before cache save?"

Recommended direction:

Introduce a task lifecycle Module, for example `runtime_tasks.rs` or `client/tasks.rs`, that owns spawn registration and bounded shutdown joins. Individual Modules can still provide task functions, but `client.rs` should hold a `TaskSet` or similar value that cancels and joins tasks in a predictable order.

Risk and migration order:

Low. First wrap spawn handles without changing task bodies. Then add bounded joins during shutdown. Keep server stop and cache save order unchanged at first.

Immediate recommendation:

Yes, after higher-risk protocol refactors.

### 8. Tests need fake upstream and fake RPC support

Files:

- existing unit tests across `src/`
- future `tests/support` or crate-local test support Modules

Problem:

The current 55 unit tests lock down several valuable semantics:

- proxy body-stage retry
- proxy watch reset behavior
- proxy completion stats
- session timeout and admission
- RPC KEY_EXPIRED retry classification
- RPC HTTP status retry classification
- request parsing basics
- access log shape

The remaining gap is cross-Module, remote-visible behavior. Many of the highest-risk bugs require an upstream image server or RPC server that can behave badly on purpose.

Recommended direction:

Add test support for:

- fake upstream image server: wrong content length, missing content length, mid-body EOF, slow trickle, reconnect success
- fake RPC server: KEY_EXPIRED, TEMPORARILY_UNAVAILABLE, 404, 500, delayed body
- fixture cache directories: valid file, wrong-size file, invalid fileid, fully invalid static range
- golden response checks for status, headers, content length, and body completion where practical

Risk and migration order:

Low. Add the support before refactoring proxy/server transaction code so it catches behavior drift.

Immediate recommendation:

Yes. This is the best safety net for the P0 refactors.

## P2: Style / Naming / Local Readability

### 9. Public surface is wider than needed

Files:

- `src/main.rs`
- `src/server/mod.rs`
- `src/server/body.rs`
- `src/proxy_downloader.rs`
- `src/cache/mod.rs`
- `src/stats.rs`

Problem:

The crate exposes many Modules and types publicly even though this appears to be a binary crate. Examples:

- `main.rs` declares all top-level Modules as `pub mod`
- `AppState` fields are public
- `Stats` exposes atomics directly
- proxy `DownloadState` and `DownloadFailReason` leak into server body
- cache internals such as LRU and prune types are public

This increases the Interface callers must understand and makes refactors feel riskier than they are.

Recommended direction:

After Module moves settle, narrow visibility:

- use `mod` at crate root unless external visibility is needed
- prefer `pub(crate)` over `pub`
- give `AppState` a constructor or smaller context structs
- expose `Stats` methods/snapshots instead of public atomics where practical
- keep proxy coordination types private to the proxy/body seam

Risk and migration order:

Low. Best done after moving Modules so visibility can be tightened naturally.

Immediate recommendation:

Not urgent; do after P0/P1 refactors.

### 10. Some comments and names lag behind current implementation

Files:

- `src/server/body.rs`
- `src/downloader.rs`
- `src/server/mod.rs`
- `src/stats.rs`

Problem:

Some comments and names still reflect earlier designs or Java naming:

- `server/body.rs` still mentions an mpsc channel in a comment even though proxy now uses watch + temp file.
- Short names such as `bwm` are understandable locally but less helpful across seams.
- `FileDownloader` uses Atomic fields in a way that mirrors the Java threaded model, but Rust call sites mostly await it directly.
- `Scheduler` appears unused after background tasks moved to spawned intervals.

Recommended direction:

Clean stale comments, replace ambiguous abbreviations where they cross Module Interfaces, and eventually reshape downloader results into a report type instead of public mutable/atomic fields.

Risk and migration order:

Low. Do as opportunistic cleanup after behavior-preserving refactors.

Immediate recommendation:

Not urgent, except stale comments near proxy internals should be fixed when touching that area.

## Suggested Refactoring Plan

Phase 1: Add safety tests first.

- Add fake upstream support for proxy downloader and threaded proxy tests.
- Add fake RPC support for retry/status/KEY_EXPIRED behavior.
- Add fixture cache tests for startup rescan and pruning edge cases.
- Preserve the current unit tests as semantic locks.

Phase 2: Deepen the proxy transfer Module.

- Introduce an opaque proxy transfer Interface.
- Hide `watch_rx`, `proxy_done_tx`, and temp file mechanics.
- Keep HEAD caching and Java finalize gating unchanged.
- Run all proxy/body/server response tests after each step.

Phase 3: Extract server transaction and admission Modules.

- Move servercmd and threaded proxy test handling out of `server/mod.rs`.
- Move simple response execution for `/t`, favicon, robots, and errors.
- Move `/h/` file/proxy execution after proxy transfer is hidden.
- Extract admission/flood/max-connection logic into a testable Module.

Phase 4: Unify Java downloader semantics.

- Create a Java-equivalent downloader Module.
- Migrate certificate download and threaded proxy test first.
- Migrate or adapt RPC request body fetching after RPC fake-server tests are in place.

Phase 5: Improve locality and visibility.

- Split CacheHandler internals behind its facade.
- Move RPC routing state out of Config.
- Introduce a task lifecycle Module with bounded shutdown joins.
- Narrow `pub` to `pub(crate)` or private.
- Clean stale comments, unused Modules, and Java-era naming where it no longer helps parity.

## Notes on Java-Equivalent Comments

Keep Java reference comments where they guard remote-visible behavior:

- HTTP status/header/body semantics
- session timeout constants and admission order
- proxy retry/finalize/cache import behavior
- RPC retry/KEY_EXPIRED/status behavior
- cache LRU, persistent state, startup safety checks, and pruning age windows
- speedtest/threaded proxy timing

Avoid Java comments where they only restate obvious Rust mechanics. Prefer comments that name the exact Java semantic being preserved and, where useful, the Java class/method.
