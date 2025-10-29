# Sequencer SDK (SBCP Integration)

This package provides a sequencer-side SDK that integrates the base 2PC consensus (x/consensus) with the Superblock
Construction Protocol (SBCP). It can be used in two modes:

- Standalone 2PC (no SBCP): use `x/consensus` directly.
- With SBCP: wrap the base coordinator via `NewSequencerCoordinator` and wire it to the Shared Publisher (SP) and peer
  sequencers.

## Quick Start

1) Create the base 2PC coordinator

```go
cfg := consensus.DefaultConfig("seq-<id>")
cfg.Role = consensus.Follower
base := consensus.New(log, cfg)

```

2) Connect to the Shared Publisher (SP)

```go
spClient := tcp.NewClient(tcp.DefaultClientConfig(), log)
spClient.SetHandler(func (ctx context.Context, msg *pb.Message) ([]common.Hash, error) {
return nil, sequencerCoordinator.HandleMessage(ctx, msg.SenderId, msg)
})
_ = spClient.Connect(ctx, "<sp-host>:8080")
```

3) Create the Sequencer Coordinator (SBCP)

```go
seqCfg := sequencer.DefaultConfig(chainIDBytes)
sequencerCoordinator := sequencer.NewSequencerCoordinator(base, seqCfg, spClient, log)
_ = sequencerCoordinator.Start(ctx)
```

4) P2P for CIRC (sequencer-to-sequencer)

- Start a TCP server to receive CIRC messages from peers (no SP involvement). Route messages to the coordinator:

```go
p2pServer := tcp.NewServer(tcp.DefaultServerConfig(), log)
p2pServer.SetHandler(func (ctx context.Context, from string, msg *pb.Message) error {
return sequencerCoordinator.HandleMessage(ctx, from, msg)
})
_ = p2pServer.Start(ctx)
```

- Maintain TCP clients to other sequencers (by chain ID) and send `pb.Message_CircMessage` when simulation emits a
  `mailbox.write(...)`.

## Using x/consensus Alone (no SBCP)

The base coordinator exposes a small API:

- `StartTransaction(from, *pb.XTRequest)` – initialize local state for an xT.
- `RecordCIRCMessage(*pb.CIRCMessage)` – record incoming CIRC from a peer.
- `ConsumeCIRCMessage(xtID, sourceChainKey string)` – pop a queued CIRC for a given source chain.
- `RecordVote(xtID, chainKey, vote)` and `SetDecisionCallback(...)` – 2PC integration.

You’re responsible for:

- Exchanging CIRC directly between sequencers (e.g., using `x/transport/tcp`).
- Computing `xtID` from `XTRequest` and maintaining a consistent chain ID representation.

## Chain Key Normalization (IMPORTANT)

Consensus indexes per-chain structures using a hex-encoded key derived from raw chain ID bytes. Always normalize via:

```go
key := consensus.ChainKeyBytes(chainIDBytes)
// or, when you only have uint64 (minimal big-endian encoding):
key := consensus.ChainKeyUint64(chainID)
```

Use the same key for:

- `ConsumeCIRCMessage(xtID, key)`
- Internally-created maps keyed by chain ID
- Any logs/diagnostics comparing chain IDs across components

Tip: Ensure `pb.XTRequest.Transactions[i].ChainId` and `pb.CIRCMessage.{Source,Destination}Chain` contain the exact same
byte representation that you’ll normalize with `ChainKeyBytes`.

## With SBCP (recommended path)

The SDK wires the 2PC coordinator and provides the SBCP state machine and block builder.

- SP → Sequencer
    - `StartSlot`: sequencer transitions to Building-Free and starts a draft block (adds `Mailbox.clean()` top-of-block
      tx).
    - `StartSC`: sequencer locks, initializes local 2PC state (SDK does this), simulates, sends CIRC to peers, and votes
      to SP.
    - `Decided`: sequencer updates the block builder (include xT and CIRC mailbox-population txs if decision=true) and
      unlocks.
    - `RequestSeal`: sequencer seals block and sends `L2Block` to SP.

- Sequencer ↔ Sequencer (P2P CIRC)
    - On `mailbox.write(...)`, build `pb.CIRCMessage` and send to the peer’s TCP endpoint.
    - On receive, the coordinator records it via `RecordCIRCMessage` (already handled through `HandleMessage`).
    - When simulation requires data (`mailbox.read(...)`), call
      `ConsumeCIRCMessage(xtID, consensus.ChainKeyBytes(sourceChainBytes))`.

### Bootstrap Helper

To quickly wire a sequencer with SP + P2P using sensible defaults:

```go
rt, err := bootstrap.Setup(ctx, bootstrap.Config{
    ChainID:   chainIDBytes,
    SPAddr:    "sp-host:8080",
    PeerAddrs: map[string]string{
        "11111":  "peer-a:9000",   // decimal or hex keys accepted
        "0x1a2b": "peer-b:9001",
    },
    Log: log,
})
if err != nil { panic(err) }

if err := rt.Start(ctx); err != nil { panic(err) }
defer rt.Stop(ctx)

// CIRC send example
_ = rt.SendCIRC(ctx, &pb.CIRCMessage{DestinationChain: someChainIDBytes /* ... */})
```

## Integration Notes

- The coordinator now ensures local 2PC state exists when `StartSC` arrives, so CIRC recording works immediately.
- Ensure CIRC `XtId` equals `xtReq.XtID()` computed by both sequencers.
- Ensure P2P connections are up before `StartSC` to avoid delays.

## Example Connection Topology (from a Node wrapper)

- One TCP client to SP for SBCP messages.
- One TCP server per sequencer to receive CIRC from peers.
- Clients to other sequencers (by configured chain IDs) for sending CIRC.

See your `node` wiring for a concrete example of SP and peer setup using `x/transport/tcp` and `ConnectWithRetry`.
