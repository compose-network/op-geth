package eth

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/txpool"
)

// nonceManager provides atomic nonce reservation for sequencer putInbox transactions.
type nonceManager struct {
	mu sync.Mutex

	// baseNonce is the last known state nonce from the txpool
	baseNonce uint64

	// reservedCount tracks how many nonces have been reserved beyond baseNonce
	reservedCount uint64

	// txpool reference to refresh base nonce
	pool *txpool.TxPool

	// account address for nonce tracking
	addr common.Address
}

func newNonceManager(pool *txpool.TxPool, addr common.Address) *nonceManager {
	return &nonceManager{
		pool: pool,
		addr: addr,
	}
}

// reserveNonces atomically reserves n sequential nonces and returns the starting nonce.
func (nm *nonceManager) reserveNonces(count int) uint64 {
	if count <= 0 {
		return 0
	}

	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.reservedCount == 0 {
		nm.baseNonce = nm.pool.PoolNonce(nm.addr)
	}

	startNonce := nm.baseNonce + nm.reservedCount
	nm.reservedCount += uint64(count)

	return startNonce
}

// reset clears all reserved nonces and refreshes from the txpool.
// Called after transactions are committed or when clearing sequencer state.
func (nm *nonceManager) reset() {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.baseNonce = nm.pool.PoolNonce(nm.addr)
	nm.reservedCount = 0
}

// currentPoolNonce returns the current pool nonce.
func (nm *nonceManager) currentPoolNonce() uint64 {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	return nm.pool.PoolNonce(nm.addr)
}

// pendingNonce returns the next nonce that would be reserved (base + reserved count).
func (nm *nonceManager) pendingNonce() uint64 {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.reservedCount == 0 {
		return nm.pool.PoolNonce(nm.addr)
	}
	return nm.baseNonce + nm.reservedCount
}
