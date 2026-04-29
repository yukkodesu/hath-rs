# Shared Server Stop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make final shutdown and certificate refresh share one local HTTP server stop flow while keeping RPC protocol actions in their callers.

**Architecture:** `server::start_server` becomes the public one-generation starter currently named `spawn_server`; the accept-loop body becomes private `run_server`. Add `server::stop_server(&AppState)` to reject new normal traffic, cancel the active listener token, drain active requests, and wait for the accept loop to terminate. `run_server` does not listen to the global shutdown token; final shutdown order is enforced by `client.rs` calling `client_stop` before `stop_server`.

**Tech Stack:** Rust 2024, Tokio, `tokio_util::sync::CancellationToken`, `arc_swap::ArcSwapOption`, existing `AppState` fields.

---

### Task 1: Normalize Server Start Naming

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/client.rs`

- [ ] **Step 1: Rename the public starter**

In `src/server/mod.rs`, rename the public `spawn_server` function to
`start_server`, and make it set `server_terminated=false` before spawning a new
generation:

```rust
/// Start one HTTP server generation. Returns a oneshot receiver that fires when
/// the server binds, and the generation shutdown token (to store in AppState).
pub fn start_server(
    state: AppState,
) -> (
    tokio::sync::oneshot::Receiver<std::result::Result<u16, String>>,
    tokio_util::sync::CancellationToken,
) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server_shutdown_token = tokio_util::sync::CancellationToken::new();
    let server_shutdown = server_shutdown_token.clone();
    state.server_terminated.store(false, Ordering::Release);
    tokio::spawn(async move {
        if let Err(e) = run_server(state, server_shutdown, Some(ready_tx)).await {
            tracing::error!("Server error: {}", e);
        }
    });
    (ready_rx, server_shutdown_token)
}
```

- [ ] **Step 2: Rename the accept-loop body**

In `src/server/mod.rs`, rename the existing async `pub async fn start_server(...)`
to private `async fn run_server(...)`. Change only the function name and
visibility; leave the parameter list and implementation statements exactly as
they are.

Before:

```rust
pub async fn start_server(
    state: AppState,
    server_shutdown: tokio_util::sync::CancellationToken,
    ready_tx: Option<tokio::sync::oneshot::Sender<std::result::Result<u16, String>>>,
) -> Result<()> {
```

After:

```rust
async fn run_server(
    state: AppState,
    server_shutdown: tokio_util::sync::CancellationToken,
    ready_tx: Option<tokio::sync::oneshot::Sender<std::result::Result<u16, String>>>,
) -> Result<()> {
```

- [ ] **Step 3: Update the initial startup call site**

In `src/client.rs`, replace:

```rust
let (ready_rx, server_shutdown_token) = server::spawn_server(app_state.clone(), shutdown.clone());
```

with:

```rust
let (ready_rx, server_shutdown_token) = server::start_server(app_state.clone());
```

- [ ] **Step 4: Update the certificate refresh restart call site**

In `src/server/mod.rs`, inside `spawn_cert_refresh_watcher`, replace the manual
`tokio::spawn(async move { run_server/start_server(...) })` block with a call to
the public starter:

```rust
if shutdown.is_cancelled() {
    break;
}

let (ready_rx, new_shutdown) = start_server(state.clone());
state
    .server_shutdown_token
    .store(Some(Arc::new(new_shutdown)));
```

- [ ] **Step 5: Verify naming compile pass**

Run:

```bash
cargo test
```

Expected: all existing tests pass. If compilation fails with references to
`spawn_server` or to the old async `start_server` signature, update those
references to `start_server` for the public starter or `run_server` for the
private accept loop.

- [ ] **Step 6: Commit**

```bash
git add src/server/mod.rs src/client.rs
git commit -m "refactor: normalize server start naming"
```

### Task 2: Add Shared Local Server Stop

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/client.rs`

- [ ] **Step 1: Add `stop_server`**

In `src/server/mod.rs`, near `start_server`, add:

```rust
/// Stop the current HTTP server generation and drain in-flight requests.
///
/// RPC lifecycle actions such as client_stop/client_suspend are intentionally
/// handled by callers. This helper only quiesces the local HTTP server.
pub async fn stop_server(state: &AppState) {
    state.allow_normal_connections.store(false, Ordering::SeqCst);

    if let Some(token) = state.server_shutdown_token.load_full() {
        token.cancel();
    }

    for close_wait_cycles in 1..25 {
        let active = state.active_connections.load(Ordering::Relaxed);
        if active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        if close_wait_cycles % 5 == 0 {
            let remaining = 25 - close_wait_cycles;
            tracing::info!(
                "Waiting for {} request(s) to finish; will wait for another {} seconds",
                active,
                remaining
            );
        }
    }

    let mut wait_cycles = 0u32;
    loop {
        if state.server_terminated.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        wait_cycles += 1;
        if wait_cycles >= 60 {
            tracing::warn!("Server did not terminate after 300s");
            break;
        }
        if wait_cycles > 1 {
            tracing::info!(
                "Waiting for HTTPServer to fully terminate... (waited {} seconds)",
                wait_cycles * 5
            );
        }
    }
}
```

- [ ] **Step 2: Replace cert refresh stop logic**

In `src/server/mod.rs`, inside `spawn_cert_refresh_watcher`, keep the existing
`client_suspend` RPC and Java-compatible 10 second delay. Replace the manual
blocks that set `allow_normal_connections=false`, cancel the token, drain active
connections, and wait for `server_terminated` with:

```rust
stop_server(&state).await;
```

Keep the existing one second delay after server stop and before starting the new
generation:

```rust
tokio::time::sleep(Duration::from_secs(1)).await;
```

- [ ] **Step 3: Replace final shutdown local flow**

In `src/client.rs`, replace the current shutdown sleep and `client_stop` block:

```rust
// Waiting connections to drain
tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
if report_shutdown.load(Ordering::Relaxed) {
    rpc_client.client_stop().await.ok();
}
```

with:

```rust
if report_shutdown.load(Ordering::Relaxed) {
    if let Err(e) = rpc_client.client_stop().await {
        tracing::warn!("Failed to notify server about shutdown: {}", e);
    }
}
server::stop_server(&app_state).await;
```

This preserves the desired final order: best-effort `client_stop`, then reject
new normal connections, stop listener, drain, wait for termination, save state.

- [ ] **Step 4: Verify no stale API references remain**

Run:

```bash
rg -n "spawn_server|run_server\\(|start_server\\(" src
```

Expected:

```text
src/client.rs:<line>:    let (ready_rx, server_shutdown_token) = server::start_server(app_state.clone());
src/server/mod.rs:<line>:pub fn start_server(
src/server/mod.rs:<line>:        if let Err(e) = run_server(state, server_shutdown, Some(ready_tx)).await {
src/server/mod.rs:<line>:let (ready_rx, new_shutdown) = start_server(state.clone());
src/server/mod.rs:<line>:async fn run_server(
```

Line numbers may differ. There should be no `spawn_server` references.

- [ ] **Step 5: Run full tests**

Run:

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 6: Inspect shutdown order in code**

Run:

```bash
sed -n '214,236p' src/client.rs
sed -n '620,710p' src/server/mod.rs
```

Expected in `src/client.rs`: `client_stop` is attempted before
`server::stop_server(&app_state).await`, and cache persistence remains after
`stop_server`.

Expected in `src/server/mod.rs`: cert refresh still calls `client_suspend`
before `stop_server(&state).await`, starts a new generation with `start_server`,
sets `allow_normal_connections=true`, and then calls `still_alive(true)`.

- [ ] **Step 7: Commit**

```bash
git add src/server/mod.rs src/client.rs
git commit -m "fix: share server stop flow for shutdown"
```

### Task 3: Final Verification

**Files:**
- Verify: `src/server/mod.rs`
- Verify: `src/client.rs`
- Verify: `docs/superpowers/specs/2026-04-29-shared-server-stop-design.md`

- [ ] **Step 1: Run tests from a clean working tree**

Run:

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 2: Check git status**

Run:

```bash
git status --short
```

Expected: no unstaged changes after the two implementation commits.

- [ ] **Step 3: Summarize protocol-sensitive behavior**

Confirm these facts in the final response:

```text
Final shutdown: client_stop -> stop_server -> save persistent data.
Cert refresh: client_suspend -> stop_server -> start_server if global shutdown has not started -> allow connections -> still_alive(true).
start_server is the old spawn_server wrapper; the accept loop is private run_server.
```
