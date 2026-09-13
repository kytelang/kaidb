# Wire protocol versioning and compatibility policy (D10)

The binary wire protocol was previously unversioned ("naive"): a client and server had no way to agree
on the format they speak, so any change to a message layout was an undetectable, silent break. This
document defines the versioning scheme and the compatibility policy that governs changes to it.

## The version

- `protocol.PROTOCOL_VERSION: u16` is the single source of truth for the current on-the-wire format.
  The current format is **version 1**.
- `protocol.MIN_SUPPORTED_VERSION: u16` is the oldest peer version this build still speaks. The
  compatible range is `[MIN_SUPPORTED_VERSION, PROTOCOL_VERSION]`.
- `protocol.isCompatible(peer)` and `protocol.negotiate(peer)` implement the rule: a peer newer than us,
  or older than our minimum, is refused rather than mis-parsed; otherwise both sides speak
  `min(peer, PROTOCOL_VERSION)`.

## Negotiation (at connect, once per session)

Version is negotiated once, in the CONNECT handshake, not carried on every message:

1. The client sends its `PROTOCOL_VERSION` in the connect request.
2. The server calls `negotiate(client_version)`. If it returns null, the server replies with a
   version-mismatch error and closes the connection. Otherwise it records the agreed version on the
   session and replies with its own `PROTOCOL_VERSION`.
3. The client verifies the server's version with its own `isCompatible` and adopts the agreed version.

A peer that predates versioning (sends no version) is treated as version 0 and, because
`MIN_SUPPORTED_VERSION` is 1, is refused with a clear "upgrade required" error. This is the one-time
cost of introducing versioning; from version 1 onward, every change is governed by the rules below.

## What is a compatible vs a breaking change

**Compatible (does NOT require a version bump), only if all hold:**
- Adding a new `MessageType` that old peers never send and can ignore when unsolicited.
- Adding a new optional, length-prefixed field at the END of a payload that old readers stop before.
- Widening an enum with values negotiated behind a capability both sides already agreed.

**Breaking (REQUIRES a `PROTOCOL_VERSION` bump):**
- Changing the size, order, or meaning of any field in `MessageHeader` or an existing payload struct.
- Repurposing a `flags` bit or a `msg_type` value.
- Changing the semantics of an existing message or its error codes.
- Anything that would make an old peer mis-parse a new peer's bytes.

When in doubt, it is breaking. `MessageHeader` is an `extern struct` with a fixed layout; treat its
shape as frozen within a version.

## Bumping the version

1. Make the format change.
2. Increment `PROTOCOL_VERSION`.
3. Keep `MIN_SUPPORTED_VERSION` where it is so the new build still serves the previous version's peers
   (dual-speak) for the deprecation window.
4. Where the code path differs by agreed version, branch on the session's negotiated version.
5. Document the change and the new version in this file's history.

## Deprecation window

A protocol version is supported for at least **one minor release after** a newer version ships. Only
after that window may `MIN_SUPPORTED_VERSION` be raised to drop it, which is itself an announced,
breaking change (it refuses peers that used to connect). This mirrors the language/ABI deprecation
policy: announce, keep dual-speak for the window, then remove.

## Forward note: multi-model

If the engine gains additional data models (document, graph -- see multi-model-feasibility.md), the
model selector should ride the versioned handshake (a model/capability field negotiated once), so
adding a model is a compatible capability rather than a second breaking protocol change. Design that
field into the next version rather than bolting it on later.

## Coordinated rollout note

`PROTOCOL_VERSION` + `negotiate()` are in `src/proto/protocol.zig`. Wiring the version exchange into the
CONNECT request/response payloads is a coordinated change with the Nova driver
(`packages/nova-btreedb`, a separate repo): both sides must add the version field in the same release,
because it moves bytes on the wire. Until that coordinated bump ships, both sides are implicitly
version 1 and the negotiation helpers above are the contract they will use.
