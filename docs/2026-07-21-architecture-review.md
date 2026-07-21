# hath-rs Architecture Review

Date: 2026-07-21

## Scope and baseline

This is a follow-up architecture review for `hath-rs`, after approximately two
months of normal operation. Gallery downloading remains out of scope.

The review looks for **deepening opportunities**: changes that place more
behaviour behind a smaller, more coherent **Interface**, improving both
maintainer **locality** and caller/test **leverage**.

Repository observations:

- No `CONTEXT.md` or `docs/adr/` directory was present. The review therefore
  uses the established H@H terms already used in the source and design notes.
- The 2026-05-03 review's main recommendations have materially landed:
  request dispatch, admission, proxy transfer, cache internals, RPC routing,
  and background-task joining are now each represented by focused Modules.
- Verification run: `cargo test --all-targets` — 76 passed, 0 failed.
- No code changes were made as part of this review.

## Candidates

### 1. Unify peer identity

Priority: high

Files:

- `src/server/admission.rs`
- `src/server/service.rs`
- `src/server/request.rs`
- `src/server/handler/mod.rs`

Problem:

The same connection's local/RPC/normal-H@H identity is derived by multiple
Modules. Their Interfaces have already drifted: admission applies
`disable_ip_origin_check`, as does request parsing, but request execution's
RPC calculation does not. Consequently, admission, server-command handling,
statistics, and throttling rules cannot be reviewed as one coherent set of
semantics.

Solution direction:

Deepen a peer-identity Module so normalized address and local/RPC/normal-H@H
classification are calculated once after TLS and carried through request
execution. Its Interface should be the sole test surface for the identity
matrix.

Java-parity constraint discovered during follow-up:

- RPC authorization affects startup admission, flood-control/max-connection
  exemption, and server-command authorization.
- Only local-network access affects bandwidth throttling and byte accounting.
  A remote RPC Peer remains remote traffic for those policies.
- Therefore, the Module must expose the underlying identity facts to the
  policy that needs them; a single `normal` convenience classification is too
  shallow to drive every downstream decision.
- Decision (2026-07-21): Java 1.6.5 is the source of truth. Every accepted
  connection contributes to session/open-connection accounting, while only a
  connection that is neither local nor RPC-authorized is subject to the
  connection limit and flood control.
- Follow-up refinement: local-network access is a session-time fact, but Java
  rechecks RPC authorization at every `servercmd` request using current
  settings. The deepened Module should centralize these rules without
  incorrectly freezing request authorization at session admission.

Benefits:

- Better locality: one place owns origin-classification rules and their
  configuration-dependent exceptions.
- More leverage: downstream Modules consume one classification rather than
  replicating policy.
- Tests can cover local, configured RPC, `disable_ip_origin_check`, and
  IPv4-mapped IPv6 cases across admission, commands, throttling, and stats.

Deletion test:

Deleting this Module would reintroduce the same classification rules across at
least admission, request parsing, and request execution. It would therefore
hide real complexity rather than act as a pass-through.

### 2. Deepen HTTP response policy

Priority: high

Files:

- `src/server/response.rs`
- `src/server/service.rs`
- `src/server/handler/mod.rs`

Problem:

Remote-visible HTTP policy is currently split between response builders,
post-dispatch insertion of `Server` and `Date` headers, header-byte throttling
and statistics, and an error fallback built directly in request execution.
The fallback is reached after the normal header/accounting work and therefore
does not follow the same response policy. A maintainer must cross several
Modules to learn the full Interface of one response.

Solution direction:

Deepen an HTTP response-policy Module which turns semantic outcomes into a
fully assembled response and its associated header accounting. Preserve
existing behaviour first with golden response tests, then migrate callers.

Benefits:

- Locality: status, headers, cache semantics, error modes, and accounting are
  maintained together.
- Leverage: future request handlers do not have to remember hidden header or
  accounting steps.
- Tests can exercise the Interface directly for text, file, proxy, HEAD,
  redirect, and internal-error outcomes.

Deletion test:

Removing this Module would force protocol-header and accounting rules back
into every response and error path. Existing near-duplicate builders show that
the complexity is real.

### 3. Consolidate general download semantics without merging proxy lifecycle

Priority: medium

Files:

- `src/downloader.rs`
- `src/rpc_client.rs`
- `src/server/threaded_proxy.rs`
- `src/proxy_downloader.rs`

Problem:

General Java `FileDownloader` semantics — connect/read timeouts, retries, 404
abort behaviour, `Content-Length` validation, and first-byte timing — are
implemented in the certificate-download, RPC, and threaded-proxy Modules.
`FileDownloader` itself is now almost exclusively a certificate-download
adapter. This makes Java-parity behaviour an implementation detail spread
across several call sites rather than a testable Interface.

Solution direction:

Deepen a small Module around general download-attempt and retry policy, with
the certificate, RPC, and threaded-proxy paths as adapters. Keep the proxy
download Module's temp-file, watch progress, body completion, and cache
finalization lifecycle behind its existing seam; only common attempt policy
belongs in the new Module.

Benefits:

- Locality: retry classification changes happen in one place.
- Leverage: each caller selects its policy and consumes the result without
  reimplementing the attempt state machine.
- Fake-upstream tests can table-drive 404, 500, timeout, and premature-EOF
behaviour for every adapter.

Deletion test:

Without this Module, the complete retry and validation rules must continue to
be maintained in three independent paths, so it would earn its keep.

### 4. Narrow `AppState` and request context

Priority: medium-low

Files:

- `src/server/mod.rs`
- `src/client.rs`
- `src/server/handler/mod.rs`
- `src/server/handler/file.rs`

Problem:

`AppState` exposes a broad `pub(crate)` Interface containing TLS, flood
control, session management, certificate refresh, shutdown, proxy, cache,
configuration, and statistics state. `RequestContext` passes that whole
Module to handlers. For example, a cached-HEAD handler test must construct
unrelated TLS, flood-control, session, refresh, and shutdown values.

Solution direction:

First centralize runtime construction and its test builder, preserving current
behaviour. Then narrow the request-facing Module seen by each handler while
leaving listener and lifecycle state with the listener Module.

Benefits:

- Locality: runtime wiring and its invariants live at one assembly point.
- Leverage: handlers reveal their actual dependencies and tests avoid
  unrelated runtime machinery.
- Adding runtime state does not require every test and handler caller to learn
  a larger Interface.

Deletion test:

There are already two real construction paths (production and tests), both of
which must know every field and invariant. A construction Module would hide
real complexity rather than merely forwarding it.

## Intentionally not recommended now

- Do not further split `CacheHandler`: the cache startup, persistence, LRU,
  pruning, and blacklist Modules already provide useful depth.
- Do not redo admission or background-task coordination: admission is already
  extracted and `BackgroundTasks` owns shutdown joins.
- Do not enlarge `ProxyTransfer`: it has one adapter and the deletion test
  does not yet demonstrate a real additional seam.
- Do not introduce a new server lifecycle state machine while narrowing
  `AppState`; preserve the decisions in
  `docs/superpowers/specs/2026-04-29-shared-server-stop-design.md`.

## Next step

Choose one candidate for a design discussion. That discussion should settle
constraints and the deepened Module's Interface before implementation begins.
