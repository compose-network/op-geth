# Sequencer Bootstrap Helper

Utility to quickly wire a sequencer with:

- Base 2PC coordinator (`x/consensus`)
- SBCP sequencer coordinator (`x/superblock/sequencer`)
- Transport client to the Shared Publisher (SP)
- P2P TCP server and peer clients for CIRC messaging

## Import

```go
import (
"context"
"github.com/rs/zerolog"
pb "github.com/ssvlabs/rollup-shared-publisher/proto/rollup/v1"
"github.com/ssvlabs/rollup-shared-publisher/x/superblock/sequencer/bootstrap"
)
```

## Quick Start

```go
ctx := context.Background()
log := zerolog.Nop() // or your configured logger

rt, err := bootstrap.Setup(ctx, bootstrap.Config{
ChainID: []byte{0xD9, 0x03}, // example chain-id bytes (55555)
SPAddr:  "sp-host:8080",
PeerAddrs: map[string]string{
"11111":  "peer-a:9000", // decimal OK
"0x1a2b": "peer-b:9001", // hex OK
},
Log: log,
})
if err != nil { panic(err) }

if err := rt.Start(ctx); err != nil { panic(err) }
defer rt.Stop(ctx)

// Send a CIRC to a peer (destination chain determines the peer)
_ = rt.SendCIRC(ctx, &pb.CIRCMessage{
SourceChain:      []byte{0xD9, 0x03},
DestinationChain: []byte{0x01, 0x04, 0x6A},
XtId:             &pb.XtID{Hash: make([]byte, 32)},
Label:            "mailbox_write",
Data:             [][]byte{[]byte("payload")},
})
```

## Config

- `ChainID []byte`: raw chain-id bytes for this sequencer (must match `XTRequest.Transactions[i].ChainId`).
- `SPAddr string`: address of the Shared Publisher (e.g., `host:8080`).
- `PeerAddrs map[string]string`: other sequencers by chain-id → `host:port`. Keys may be decimal (e.g., `"11111"`) or
  hex (e.g., `"0x1a2b"` or `"1a2b"`). Internally normalized to the canonical consensus key.
- `Log zerolog.Logger`: optional; defaults to a no-op logger when zero.
- `BaseConsensus consensus.Coordinator`: optional; provide your own 2PC coordinator if needed.
- `SPClientConfig *tcp.ClientConfig`: optional; override TCP client options to SP.
- `P2PServerConfig *transport.Config`: optional; override TCP server options for P2P.
- `P2PListenAddr string`: optional; overrides server `ListenAddr`.
- `SlotDuration time.Duration`: optional; defaults to 12s.
- `SlotSealCutover float64`: optional; defaults to 2/3.

## Lifecycle

- `Setup(ctx, Config) (*Runtime, error)`: builds coordinator, transports and peers; does not start them.
- `(*Runtime) Start(ctx) error`: starts coordinator, P2P server, connects SP and peers.
- `(*Runtime) Stop(ctx) error`: stops transports and coordinator.
- `(*Runtime) SendCIRC(ctx, *pb.CIRCMessage) error`: sends to the peer selected by `DestinationChain`.

## Notes

- Chain ID normalization: peers and consume keys are aligned with `x/consensus` via hex-encoded keys derived from the
  raw bytes. Use `consensus.ChainKeyBytes([]byte)` when you need the same key.
- StartSC: the sequencer coordinator initializes local 2PC state on `StartSC`, so CIRC recording/consumption works
  immediately.
