# Peer Identity Java-Parity Plan

Date: 2026-07-21

## Goal

Concentrate inbound-peer rules in a deep Module while preserving Java 1.6.5
semantics. The resulting Interface must distinguish session-time locality from
request-time RPC authorization instead of exposing a misleading `normal`
shortcut.

## Decisions

- Java 1.6.5 is the source of truth.
- **Session Origin** is created once after TLS from the normalized remote IP;
  local-network status is fixed for that accepted session.
- **RPC Authorization** is evaluated from the current configuration for
  admission and again for every `servercmd` request.
- Every accepted **Peer** contributes to session and open-connection counts.
- Only a **Normal H@H Peer** is checked against the connection limit and flood
  control. That check uses the total accepted-session count, including local
  and RPC peers.
- Bandwidth throttling, byte accounting, and file-sent accounting exempt only
  a **Local Peer**. A remote **RPC Peer** remains remote traffic.

## Module shape

Add a crate-private peer-policy Module under `src/server/`.

Its Interface should contain only the two facts that callers need:

- A small immutable **Session Origin** value: normalized peer IP and whether
  it is local.
- One request/admission-time RPC-authorization operation using a peer IP and a
  `Config` snapshot.

The Module owns IPv4-mapped normalization, local-network matching, configured
RPC-server matching, and `disable_ip_origin_check`. There is one concrete
implementation, so no adapter or public trait is warranted.

Do not add a generic `is_normal_hath_connection()` operation. It is shallow:
different callers need different policy facts. Admission needs local and RPC
authorization; traffic policy needs only locality; server-command parsing needs
fresh RPC authorization.

## Migration order

1. Move peer-policy rules out of `admission.rs` and remove their copies in
   `service.rs` and `request.rs`.
2. Build **Session Origin** once in the accept task and carry it in
   `HathService`; request execution must stop recomputing locality.
3. At admission, evaluate RPC authorization against the accept-time config and
   apply Java's startup/flood/connection-limit policy.
4. At request parsing, evaluate RPC authorization against that request's
   config snapshot before accepting `servercmd`.
5. Remove RPC status from `RequestClientContext`; make traffic policy depend
   solely on the **Session Origin**'s locality.
6. Refactor `SessionManager` to track every accepted session. Its max-limit
   reservation must apply only when admission policy says the peer is normal,
   while overload and open-connection figures use the total session count.
7. Run the full test suite and compare the changed paths with the Java source:
   `HTTPServer.java`, `HTTPSession.java`, `HTTPResponse.java`, and
   `Settings.java`.

## Minimal regression tests

Keep tests at the policy and stateful-semantic seams. Do not add route-by-route
or duplicate boolean-combination tests.

1. **Peer policy table**: configured RPC server, non-RPC remote peer, and
   `disable_ip_origin_check`; also assert a remote RPC peer is not local.
2. **Request-time authorization**: parse the same valid `servercmd` with two
   configuration snapshots and prove authorization follows the current one.
   This locks the persistent-session/settings-refresh rule.
3. **Session accounting versus admission limit**: an exempt local or RPC peer
   is admitted and increments the total count; a subsequent normal peer sees
   that count for the Java max-connection boundary. Dropping the sessions
   restores the count.
4. **Traffic policy**: a remote RPC peer is eligible for traffic statistics
   and bandwidth throttling; only a local peer is exempt.

The existing flood-control and timeout tests remain useful and should be kept;
they do not need to be duplicated.

## Rust quality constraints

- Prefer value types, pure functions, and exhaustive enums over boolean
  parameter lists.
- Keep the peer-policy Module crate-private; do not introduce a trait or an
  adapter until a second implementation exists.
- Pass immutable `Arc<Config>` snapshots across async work; do not hold locks
  across `.await`.
- Preserve RAII cleanup in `SessionGuard` so every accepted session decrements
  accounting exactly once.
- Make each test demonstrate one Java-visible invariant; avoid tests of
  constructors, private field layout, or repeated combinations with no new
  behaviour.
