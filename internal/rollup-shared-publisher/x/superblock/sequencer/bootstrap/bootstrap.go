package bootstrap

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/consensus"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/superblock/sequencer"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/superblock/slot"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/transport"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/transport/tcp"
	"github.com/rs/zerolog"
)

// Config holds inputs to wire a sequencer with SBCP and P2P CIRC.
type Config struct {
	// ChainID is the ID of the chain the sequencer is running for.
	ChainID []byte
	// SPAddr is the address of the shared publisher in host:port format.
	SPAddr string
	// PeerAddrs is a map of chainID to host:port for other sequencers.
	// The chainID can be in decimal or hex format and will be normalized.
	PeerAddrs map[string]string

	// Log is the logger to use. If not provided, a no-op logger is used.
	Log zerolog.Logger

	// BaseConsensus is an optional existing 2PC coordinator. If nil, a default follower is created.
	BaseConsensus consensus.Coordinator

	// SPClientConfig is an optional override for the shared publisher client config.
	SPClientConfig *tcp.ClientConfig
	// P2PServerConfig is an optional override for the P2P server config.
	// If nil, tcp.DefaultServerConfig() is used.
	P2PServerConfig *transport.Config

	// P2PListenAddr is an optional P2P listen address, overriding P2PServerConfig.ListenAddr.
	P2PListenAddr string

	// SlotDuration is the duration of a slot. Defaults to 12 s.
	SlotDuration time.Duration
	// SlotSealCutover is the fraction of the slot after which it should be sealed. Defaults to 2/3.
	SlotSealCutover float64
}

// Runtime exposes the wired components and lifecycle.
type Runtime struct {
	// Coordinator is the sequencer coordinator.
	Coordinator sequencer.Coordinator
	// SPClient is the client for the shared publisher.
	SPClient transport.Client
	// P2PServer is the P2P server for CIRC.
	P2PServer transport.Server
	// Peers is a map of hex chainID key to peer client.
	Peers map[string]transport.Client

	log zerolog.Logger
	cfg Config
}

// Setup wires a sequencer coordinator, SP client, P2P server, and peer clients.
func Setup(ctx context.Context, cfg Config) (*Runtime, error) {
	log := cfg.Log
	if reflect.ValueOf(log).IsZero() {
		log = zerolog.Nop()
	}

	// Base consensus (2PC)
	base := cfg.BaseConsensus
	if base == nil {
		nodeID := fmt.Sprintf("sequencer-%d", time.Now().UnixNano())
		c := consensus.DefaultConfig(nodeID)
		c.Role = consensus.Follower
		c.IsLeader = false
		c.Timeout = time.Minute
		base = consensus.New(log, c)
	}

	// SP client
	spCfg := tcp.DefaultClientConfig()
	if cfg.SPClientConfig != nil {
		spCfg = *cfg.SPClientConfig
	}
	if cfg.SPAddr != "" {
		spCfg.ServerAddr = cfg.SPAddr
	}
	spClient := tcp.NewClient(spCfg, log)

	// Sequencer coordinator (SBCP)
	slotDuration := cfg.SlotDuration
	if slotDuration == 0 {
		slotDuration = 12 * time.Second
	}
	sealCutover := cfg.SlotSealCutover
	if sealCutover == 0 {
		sealCutover = 2.0 / 3.0
	}

	seqCfg := sequencer.Config{
		ChainID: cfg.ChainID,
		Slot: slot.Config{
			Duration:    slotDuration,
			SealCutover: sealCutover,
			GenesisTime: time.Unix(1760599941, 0), // Custom genesis time
		},
		BlockTimeout:         30 * time.Second,
		MaxLocalTxs:          1000,
		SCPTimeout:           10 * time.Second,
		EnableCIRCValidation: true,
	}
	coord := sequencer.NewSequencerCoordinator(base, seqCfg, spClient, log)

	// SP message handler routes to coordinator
	spClient.SetHandler(func(c context.Context, msg *pb.Message) ([]common.Hash, error) {
		return nil, coord.HandleMessage(c, msg.SenderId, msg)
	})

	// P2P server for CIRC
	var p2pSrv transport.Server
	if cfg.P2PServerConfig != nil {
		if cfg.P2PListenAddr != "" {
			cfg.P2PServerConfig.ListenAddr = cfg.P2PListenAddr
		}
		p2pSrv = tcp.NewServer(*cfg.P2PServerConfig, log)
	} else {
		s := tcp.DefaultServerConfig()
		if cfg.P2PListenAddr != "" {
			s.ListenAddr = cfg.P2PListenAddr
		}
		p2pSrv = tcp.NewServer(s, log)
	}
	p2pSrv.SetHandler(func(c context.Context, from string, msg *pb.Message) error {
		return coord.HandleMessage(c, from, msg)
	})

	log.Info().Interface("peer_addrs", cfg.PeerAddrs).Msg("Setting up peer clients")

	peers := make(map[string]transport.Client)
	for id, addr := range cfg.PeerAddrs {
		if strings.TrimSpace(addr) == "" {
			log.Warn().Str("chain_id", id).Msg("Skipping peer with empty address")
			continue
		}
		key := normalizeChainIDKey(id)
		if key == "" {
			log.Warn().Str("chain_id", id).Msg("Skipping peer with invalid chain ID format")
			continue
		}
		log.Info().Str("chain_id", id).Str("normalized_key", key).Str("addr", addr).Msg("Creating peer client")
		cc := tcp.DefaultClientConfig()
		cc.ServerAddr = addr
		cc.ClientID = fmt.Sprintf("peer-%s", key)
		peers[key] = tcp.NewClient(cc, log)
	}

	log.Info().Int("peer_count", len(peers)).Interface("peer_keys", getPeerKeys(peers)).Msg("Peer clients created")

	rt := &Runtime{
		Coordinator: coord,
		SPClient:    spClient,
		P2PServer:   p2pSrv,
		Peers:       peers,
		log:         log,
		cfg:         cfg,
	}
	return rt, nil
}

// Start brings up coordinator, connects to SP, starts P2P, and dials peers.
func (r *Runtime) Start(ctx context.Context) error {
	if err := r.Coordinator.Start(ctx); err != nil {
		return fmt.Errorf("start coordinator: %w", err)
	}

	go func() {
		if err := r.P2PServer.Start(ctx); err != nil {
			r.log.Error().Err(err).Msg("P2P server failed")
		}
	}()

	if err := r.SPClient.ConnectWithRetry(ctx, "", 10); err != nil {
		return fmt.Errorf("connect SP: %w", err)
	}

	r.log.Info().
		Int("peer_count", len(r.Peers)).
		Interface("peer_keys", getPeerKeys(r.Peers)).
		Msg("Starting peer connections")

	for key, c := range r.Peers {
		if addr, exists := r.cfg.PeerAddrs[key]; exists && strings.TrimSpace(addr) != "" {
			r.log.Info().
				Str("peer", key).
				Str("addr", addr).
				Msg("Attempting to connect to peer")
			if err := c.ConnectWithRetry(ctx, addr, 5); err != nil {
				r.log.Error().
					Str("peer", key).
					Str("addr", addr).Err(err).
					Msg("Failed to connect to peer after retries")
			} else {
				r.log.Info().
					Str("peer", key).
					Str("addr", addr).
					Msg("Successfully connected to peer")
			}
		} else {
			r.log.Error().
				Str("peer", key).
				Interface("configured_addrs", r.cfg.PeerAddrs).
				Msg("No valid address configured for peer")
		}
	}
	return nil
}

// Stop stops coordinator and transports.
func (r *Runtime) Stop(ctx context.Context) error {
	r.log.Info().Msg("Stopping sequencer runtime")

	// Stop coordinator first to prevent new message processing
	_ = r.Coordinator.Stop(ctx)

	// Disconnect from shared publisher
	if err := r.SPClient.Disconnect(ctx); err != nil {
		r.log.Debug().Err(err).Msg("SP client disconnect error")
	}

	// Stop P2P server
	_ = r.P2PServer.Stop(ctx)

	// Disconnect all peer clients
	for key, c := range r.Peers {
		if err := c.Disconnect(ctx); err != nil {
			r.log.Debug().Str("peer", key).Err(err).Msg("Peer disconnect error")
		}
	}

	r.log.Info().Msg("Sequencer runtime stopped")
	return nil
}

// SendCIRC sends a CIRC message to the peer indicated by DestinationChain.
func (r *Runtime) SendCIRC(ctx context.Context, circ *pb.CIRCMessage) error {
	destKey := consensus.ChainKeyBytes(circ.DestinationChain)
	r.log.Info().Str("dest_key", destKey).Str("xt_id", circ.XtId.Hex()).Msg("Sending CIRC message to peer")

	peer, ok := r.Peers[destKey]
	if !ok || peer == nil {
		r.log.Error().
			Str("dest_key", destKey).
			Str("xt_id", circ.XtId.Hex()).
			Interface("available_peers", getPeerKeys(r.Peers)).
			Msg("No peer client found for destination chain")
		return fmt.Errorf("no peer for destination chain %s", destKey)
	}

	msg := &pb.Message{Payload: &pb.Message_CircMessage{CircMessage: circ}}

	if err := peer.Send(ctx, msg); err != nil {
		r.log.Error().
			Err(err).
			Str("dest_key", destKey).
			Str("xt_id", circ.XtId.Hex()).
			Msg("Failed to send CIRC message to peer")
		return err
	}

	r.log.Info().Str("dest_key", destKey).Str("xt_id", circ.XtId.Hex()).Msg("CIRC message sent successfully")
	return nil
}

// normalizeChainIDKey accepts decimal or hex chainID strings and returns the
// canonical hex key used by consensus. Unknown formats return an empty string.
func normalizeChainIDKey(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Try decimal first
	if bi, ok := new(big.Int).SetString(s, 10); ok {
		return consensus.ChainKeyUint64(bi.Uint64())
	}

	// Remove optional 0x and validate hex-ish
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
	}

	// If still not valid hex characters, give up; consensus will not know this key
	for _, ch := range s {
		if ch < '0' || ch > '9' && ch < 'A' || ch > 'F' && ch < 'a' || ch > 'f' {
			return ""
		}
	}

	// Already hex string; lower-case normalize by decoding/encoding not needed for key
	return strings.ToLower(s)
}

// getPeerKeys returns a slice of available peer keys for logging
func getPeerKeys(peers map[string]transport.Client) []string {
	keys := make([]string, 0, len(peers))
	for key := range peers {
		keys = append(keys, key)
	}
	return keys
}
