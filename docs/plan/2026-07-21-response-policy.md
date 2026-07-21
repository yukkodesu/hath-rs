# HTTP Response Policy Plan

Date: 2026-07-21

## Goal

Deepen the HTTP response-policy Module so one Interface turns semantic request
outcomes into a Hyper response with common headers, header flow control, and
traffic accounting. It must improve locality without owning file, proxy, or
access-log lifecycle implementation.

## Decision constraints

- ADR-0001 applies: Hyper owns final wire serialization. Header-byte accounting
  is exact for the final Hyper response model, not a claim about hidden Hyper
  serialization details.
- Every normal and fallback response receives the same common-header policy.
- Header flow control and remote traffic accounting use the finalized response,
  after common headers have been added.
- Remote body-byte accounting occurs for every body frame produced, including
  incomplete responses. A **Local Peer** remains exempt.
- Existing file and proxy lifecycle semantics remain inside their Modules;
  their file-sent counters are not repurposed as traffic-byte accounting.

## Module shape

Handlers should return a semantic response value: status, content type,
declared content length, cache behaviour, any exceptional headers, and a
`StreamingBody`. The response-policy Module is the sole place that translates
this value into `Response<StreamingBody>` and adds common headers.

`HathService` supplies the policy with request-scoped traffic facts: whether
the **Peer** is local, the bandwidth monitor, and statistics. The policy:

1. converts handler failure to one ordinary internal-error semantic response;
2. applies common and exceptional headers;
3. computes the HTTP/1.1 header size from the final status and `HeaderMap`;
4. waits for header quota and records header bytes for remote traffic; and
5. configures `StreamingBody` for body-frame flow control and to record each
   frame for remote traffic.

The status line estimate includes `HTTP/1.1 `, the status code, one space, the
reason phrase, and CRLF. It replaces the current undercount by two bytes.

## Migration order

1. Introduce the semantic response value and policy Module beside
   `response.rs`, without changing file/proxy body lifecycle.
2. Move common `Connection`, `Server`, `Date`, cache, and content-length rules
   out of response builders into the policy Module.
3. Replace the direct `HathService` fallback 500 with policy finalization.
4. Add an opt-in traffic meter to `StreamingBody`; record bytes immediately
   before each body frame is returned, and remove eager speedtest accounting.
5. Preserve file/proxy file-sent semantics while routing their traffic bytes
   through the new meter.
6. Remove duplicated header accounting from `HathService` and run the full
   regression suite.

## Minimal regression tests

1. A semantic text response finalizes with common headers and a header-size
   calculation that includes the complete HTTP/1.1 status line.
2. A handler failure finalizes as the same class of response and receives
   common headers; a local Peer remains exempt from traffic accounting.
3. A remote `StreamingBody` records only the bytes of frames produced before
   it is dropped; the local equivalent records none.
4. A HEAD semantic response declares its body length but produces no body
   frames or body-byte accounting.

Do not test Hyper's raw header order, reason phrase, or date string. Those are
implementation details deliberately owned by Hyper under ADR-0001.
