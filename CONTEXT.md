# hath-rs Context

This glossary names H@H concepts whose meaning affects remote-visible
behaviour. It exists so protocol and architecture discussions use the same
terms as the client.

## Language

**Peer**:
The remote party attached to one inbound H@H connection.
_Avoid_: client, origin

**Peer Identity**:
The address-derived facts of a **Peer**, evaluated at the session or request
scope required by the H@H rule.
_Avoid_: connection type, access class

**Session Origin**:
The normalized-address and local-network classification assigned to a **Peer**
when its inbound session is accepted.

**Local Peer**:
A **Peer** whose address is local-network or equals the configured client host.

**RPC Peer**:
A **Peer** authorized to issue H@H server commands because its address is a
configured RPC server or IP-origin checks are disabled.

**RPC Authorization**:
The request-time decision that a **Peer** is an **RPC Peer** under the current
H@H settings.

**Normal H@H Peer**:
A **Peer** that is neither a **Local Peer** nor an **RPC Peer**.

## Relationships

- Every accepted **Peer** has one **Session Origin**.
- An **RPC Authorization** is evaluated for each server-command request and
  can differ after settings change on a persistent session.
- A **Peer** may be both local and RPC-authorized.
- A **Normal H@H Peer** is neither local nor RPC-authorized.
- An **RPC Peer** may issue H@H server commands after command-key validation.
- Every accepted **Peer** contributes to the session and open-connection count.
- Only a **Normal H@H Peer** is subject to the connection limit and flood
  control; a **Local Peer** or **RPC Peer** is exempt from those checks.
- Traffic accounting and bandwidth policy exempt only a **Local Peer**; an
  **RPC Peer** that is not local remains remote traffic.

## Example dialogue

> **Dev:** "With IP-origin checks disabled, is this remote **Peer** an **RPC
> Peer** for the whole request?"
> **Domain expert:** "It is an **RPC Peer** for admission and command
> validation, but it still contributes to session counts and remains remote
> traffic unless it is also a **Local Peer**."

## Flagged ambiguities

- "RPC client" can mean the outbound client contacting H@H RPC servers or an
  inbound **RPC Peer**. This glossary uses **RPC Peer** exclusively for the
  inbound case.
- **Normal H@H Peer** is useful for admission policy, but must not be used as
  a shorthand for traffic accounting or bandwidth policy: those depend only
  on whether the **Peer** is local.
- **Session Origin** is stable for an accepted session, while **RPC
  Authorization** is intentionally request-time. Do not collapse them into a
  single immutable `is_rpc` fact.
- Java 1.6.5 is the source of truth for these relationships, including the
  distinction between session accounting and connection-limit eligibility.
