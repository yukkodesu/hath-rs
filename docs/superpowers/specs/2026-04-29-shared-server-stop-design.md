# Shared Server Stop Design

## Context

`hath-rs` is a protocol-level Rust port of Hentai@Home Java 1.6.5. The Rust
client already has a careful certificate refresh path:

1. notify the RPC server that traffic is suspended
2. reject new normal peer connections
3. stop the listener
4. wait for active requests to drain
5. start a fresh listener
6. resume traffic with `still_alive(true)`

The final shutdown path is less strict. It waits for the global shutdown token,
sleeps briefly, sends `client_stop`, and saves state. That does not fully match
the intended Java-compatible sequence documented for graceful shutdown:

1. send `client_stop`
2. reject new connections
3. close the listener
4. drain existing requests
5. save persistent state and exit

## Goal

Use one local server stop implementation for both certificate refresh and final
shutdown, while keeping the RPC protocol actions in the calling lifecycle code.

The desired final shutdown order is:

1. best-effort `client_stop`
2. reject new normal peer connections
3. stop the current listener
4. drain active requests
5. wait for the accept loop to terminate
6. save cache and client login state
7. exit

If `client_stop` fails, shutdown still continues locally.

## API Shape

Keep the lifecycle API small:

```rust
pub fn start_server(
    state: AppState,
) -> (
    tokio::sync::oneshot::Receiver<std::result::Result<u16, String>>,
    tokio_util::sync::CancellationToken,
)

pub async fn stop_server(state: &AppState)
```

The current public `spawn_server` already has the desired `start_server`
behavior: it spawns one generation of the HTTP server, returns the readiness
receiver, and returns the token used to stop that generation. Rename or fold that
public wrapper into `start_server`.

The current async `start_server` function is the actual accept-loop body. Rename
it to a private implementation detail such as `run_server` so the public
lifecycle pair is simply `start_server` and `stop_server`.

`run_server` must not listen directly to the global shutdown token. Final
shutdown needs to preserve protocol order, so the signal token wakes `client.rs`,
`client.rs` sends `client_stop`, and only then `stop_server` cancels the current
server generation token. Listening to the global token inside `run_server` would
let the listener stop before `client_stop`.

The initial startup and the post-certificate-refresh startup both call the same
public `start_server` function. There is no separate `restart_server` action.

`stop_server` performs only local HTTP server shutdown:

1. set `allow_normal_connections` to `false`
2. cancel the current `server_shutdown_token`
3. wait for `active_connections` to reach zero, up to the Java-style drain window
4. wait for `server_terminated` so the accept loop is no longer running

`stop_server` does not take a reason enum. Callers log the reason before calling
it. This keeps the helper protocol-agnostic and avoids encoding lifecycle policy
inside the local server stop primitive.

## Calling Flows

### Initial Startup

`client.rs` calls `start_server`, stores the returned token in
`AppState.server_shutdown_token`, waits for readiness, then continues with
`client_start`.

### Certificate Refresh

The refresh watcher keeps the RPC behavior:

1. log that certificate refresh is starting
2. send `client_suspend`
3. wait the existing Java-compatible delay
4. call `stop_server(&state)`
5. if global shutdown has not started, call `start_server` and store the new token
6. wait for readiness
7. set `allow_normal_connections` to `true`
8. send `still_alive(true)`

### Final Shutdown

The global shutdown path does:

1. log that shutdown is starting
2. if `report_shutdown` is set, send best-effort `client_stop`
3. call `stop_server(&app_state)`
4. save persistent cache data
5. save client login data

## Concurrency

Final shutdown uses the existing global cancellation token. The cert refresh
watcher already selects on that token and exits when shutdown begins.

`stop_server` should be idempotent enough for shutdown races:

- setting `allow_normal_connections=false` repeatedly is harmless
- cancelling an already-cancelled token is harmless
- waiting for zero active connections is harmless
- waiting for an already-terminated server should return quickly

No new lifecycle state machine is required.

## Testing

Add focused tests where practical:

- a unit test or small async test for `stop_server` behavior with a synthetic
  `AppState` and pre-cancelled token, if the existing structure allows it
- regression coverage around final shutdown order if a test hook already exists
- otherwise rely on `cargo test` plus manual code inspection of the two call
  sites, because this behavior is mostly coordination around spawned tasks

Manual verification should confirm:

- cert refresh still performs suspend before stopping and still resumes after
  the new listener is ready
- final shutdown sends `client_stop` before stopping the listener
- final shutdown stops accepting new connections before saving persistent data
