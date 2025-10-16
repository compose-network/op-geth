package registry_utils

import (
	"fmt"
	"strings"

	reg "github.com/compose-network/registry/registry"
)

// RegistryUtils provides minimal helpers to resolve values from the embedded (or on-disk) registry.
type RegistryUtils struct {
	r       reg.Registry
	network string
}

// New creates a RegistryUtils backed by embedded data or a directory override.
// If dir is empty, the embedded registry is used. networkSlug selects the parent network (e.g., "hoodi").
func New(dir, networkSlug string) (RegistryUtils, error) {
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
	return RegistryUtils{r: rr, network: networkSlug}, nil
}

// Mailboxes returns a map chainID -> mailbox hex address for all chains in the network.
func (u RegistryUtils) Mailboxes() (map[uint64]string, error) {
	n, err := u.r.GetNetworkBySlug(u.network)
	if err != nil {
		return nil, err
	}
	chains, err := n.ListChains()
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
