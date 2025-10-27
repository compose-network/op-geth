package registry_utils

import (
	"fmt"
	"sort"
	"strings"

	reg "github.com/compose-network/registry/registry"
)

// RegistryUtils provides minimal helpers to resolve values from the embedded (or on-disk) registry.
type RegistryUtils struct {
    r     reg.Registry
    chain reg.Chain
    ccfg  reg.ChainConfig
}

// NewByL2ID constructs a RegistryUtils by resolving the L1 network slug via the given L2 chain ID.
// If dir is empty, it uses the embedded registry; otherwise it loads from the given directory.
func NewByL2ID(dir string, l2ChainID uint64) (RegistryUtils, error) {
    var rr reg.Registry
    if strings.TrimSpace(dir) != "" {
        r2, err := reg.NewFromDir(dir)
        if err != nil {
            return RegistryUtils{}, fmt.Errorf("registry from %s: %w", dir, err)
        }
        rr = r2
    } else {
        rr = reg.New()
    }
    ch, err := rr.GetChainById(l2ChainID)
    if err != nil {
        return RegistryUtils{}, fmt.Errorf("resolve network for l2=%d: %w", l2ChainID, err)
    }
    ccfg, err := ch.LoadConfig()
    if err != nil {
        return RegistryUtils{}, fmt.Errorf("load chain config for %s/%s: %w", ch.Network().Slug(), ch.Slug(), err)
    }
    return RegistryUtils{r: rr, chain: ch, ccfg: ccfg}, nil
}

// Chain returns the selected L2 chain handle.
func (u RegistryUtils) Chain() reg.Chain { return u.chain }

// ChainConfig returns the selected L2 chain config (cached at construction).
func (u RegistryUtils) ChainConfig() reg.ChainConfig { return u.ccfg }

// Mailboxes returns a map chainID -> mailbox hex address for all chains in the network.
func (u RegistryUtils) Mailboxes() (map[uint64]string, error) {
    chains, err := u.chain.Network().ListChains()
    if err != nil {
        return nil, err
    }
	out := make(map[uint64]string, len(chains))
	for _, ch := range chains {
		ccfg, err := ch.LoadConfig()
		if err != nil {
			return nil, err
		}
		if addr := strings.TrimSpace(ccfg.Addresses.Mailbox); addr != "" {
			out[ccfg.ChainID] = addr
		}
	}
	return out, nil
}

// SequencerAddrs returns a map chainID -> host:port for all chains in the network.
func (u RegistryUtils) SequencerAddrs() (map[uint64]string, error) {
    chains, err := u.chain.Network().ListChains()
    if err != nil {
        return nil, err
    }
	out := make(map[uint64]string, len(chains))
	for _, ch := range chains {
		ccfg, err := ch.LoadConfig()
		if err != nil {
			return nil, err
		}

		addr := strings.TrimSpace(ccfg.Sequencer.Host)
		if strings.TrimSpace(ccfg.Sequencer.Host) != "" && ccfg.Sequencer.Port != 0 {
			addr = fmt.Sprintf("%s:%d", ccfg.Sequencer.Host, ccfg.Sequencer.Port)
		}
		if addr != "" {
			out[ccfg.ChainID] = addr
		}
	}
	return out, nil
}

// JoinSequencerAddrs deterministically formats map[chainID]addr into "id:addr,id:addr".
func JoinSequencerAddrs(m map[uint64]string) string {
	if len(m) == 0 {
		return ""
	}
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d:%s", id, m[id]))
	}
	return strings.Join(parts, ",")
}
