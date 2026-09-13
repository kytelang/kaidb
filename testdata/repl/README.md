# Replication mutual-TLS test fixtures (P6 / R4)

THROWAWAY, NON-PRODUCTION test certificates for the replication mutual-TLS work. Do NOT use these
anywhere real -- the private keys are committed on purpose so the mTLS tests are hermetic.

A single test CA signs a server cert (the follower's `ReplServer`) and a client cert (the leader's
`DurableReplicator`). A separate `rogue` CA + cert exercises the negative path (a peer whose cert does
not chain to our CA must be refused).

| file | role |
|---|---|
| `ca.crt` / `ca.key` | the test CA both sides trust as `root_ca` |
| `server.crt` / `server.key` | follower/server identity; SAN `IP:127.0.0.1, DNS:localhost` |
| `client.crt` / `client.key` | leader/client identity (presented for mutual auth) |
| `rogue_ca.crt` / `rogue.crt` / `rogue.key` | a cert NOT signed by `ca.crt` -- must be rejected |

Regenerate (P-256, 100-year validity) with the openssl steps recorded in
`docs/replication-ha-design.md` (R4 STATUS).

## How they plug in (planned wiring)

- Server (`ReplServer.handleReplConnInner`): mirror `tcp_server.zig`'s `runLoop` split -- when a TLS config
  is present, `tls.serverFromStream(io, stream, opts)` with `auth = server CertKeyPair`,
  `client_auth = { root_ca = ca bundle, .require }`, then run the existing HMAC handshake + frame loop over
  the TLS connection's `reader()/writer()` instead of the raw stream.
- Client (`ReplClient` / `DurableReplicator.connect`): `tls.clientFromStream(io, stream, opts)` with
  `host`, `root_ca`, `auth = client CertKeyPair`. This requires the client to hold a PERSISTENT TLS
  connection (the current `shipFrames` makes a fresh reader/writer per call, which would bypass TLS), with
  the raw stream reader/writer + the tls.Connection + its reader/writer all kept at stable addresses.
- `rng`: seed `std.Random.DefaultCsprng` from `std.Io.random`; `now`: `Io.Clock.real.now(io)`;
  certs: `tls.config.CertKeyPair.fromFilePath` + `tls.config.cert.fromFilePath` (CA bundle).
