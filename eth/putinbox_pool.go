package eth

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// putInboxTxPool manages the pool of putInbox transactions for the SSV coordinator.
type putInboxTxPool struct {
	mu sync.RWMutex

	coordinatorAddr common.Address
	coordinatorKey  *ecdsa.PrivateKey
	mainPool        *txpool.TxPool
	chainID         uint64

	nextNonce uint64
	baseNonce uint64

	pending   []putInboxTxEntry
	committed []putInboxTxEntry
	byHash    map[common.Hash]*putInboxTxEntry
}

type putInboxTxEntry struct {
	tx             *types.Transaction
	xtID           string
	nonce          uint64
	originalTxHash common.Hash
	sequence       uint64
	committed      bool
	committedBlock uint64
	committedSlot  uint64
}

func newPutInboxTxPool(
	coordinatorAddr common.Address,
	coordinatorKey *ecdsa.PrivateKey,
	mainPool *txpool.TxPool,
	chainID uint64,
) *putInboxTxPool {
	pool := &putInboxTxPool{
		coordinatorAddr: coordinatorAddr,
		coordinatorKey:  coordinatorKey,
		mainPool:        mainPool,
		chainID:         chainID,
		byHash:          make(map[common.Hash]*putInboxTxEntry),
	}
	pool.mu.Lock()
	pool.refreshNonceLocked("init")
	pool.mu.Unlock()
	return pool
}

func (p *putInboxTxPool) refreshNonceLocked(reason string) {
	pendingCount := len(p.pending)
	committedCount := len(p.committed)
	poolNonce := p.mainPool.PoolNonce(p.coordinatorAddr)
	oldNext := p.nextNonce
	p.baseNonce = poolNonce

	if pendingCount == 0 && committedCount == 0 {
		// No outstanding txs, safe to snap nextNonce directly to pool nonce.
		p.nextNonce = poolNonce
	} else if poolNonce > p.nextNonce {
		// Only move forward; never shrink when outstanding txs exist.
		p.nextNonce = poolNonce
	}

	if oldNext != p.nextNonce {
		log.Info("[SSV] PutInbox pool refreshed nonce",
			"addr", p.coordinatorAddr.Hex(),
			"reason", reason,
			"poolNonce", poolNonce,
			"previousNextNonce", oldNext,
			"nextNonce", p.nextNonce,
			"pending", pendingCount,
			"committed", committedCount)
	} else {
		log.Debug("[SSV] PutInbox pool refresh unchanged",
			"addr", p.coordinatorAddr.Hex(),
			"reason", reason,
			"poolNonce", poolNonce,
			"nextNonce", p.nextNonce,
			"pending", pendingCount,
			"committed", committedCount)
	}
}

func (p *putInboxTxPool) add(unsignedTx *types.Transaction, xtID string, sequence uint64) (*types.Transaction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if unsignedTx.Type() != types.DynamicFeeTxType {
		return nil, fmt.Errorf("putInbox tx must be DynamicFeeTxType, got %d", unsignedTx.Type())
	}

	nonce := p.nextNonce
	p.nextNonce++

	inner := &types.DynamicFeeTx{
		ChainID:    unsignedTx.ChainId(),
		Nonce:      nonce,
		GasTipCap:  unsignedTx.GasTipCap(),
		GasFeeCap:  unsignedTx.GasFeeCap(),
		Gas:        unsignedTx.Gas(),
		To:         unsignedTx.To(),
		Value:      unsignedTx.Value(),
		Data:       unsignedTx.Data(),
		AccessList: unsignedTx.AccessList(),
	}

	newTx := types.NewTx(inner)
	signer := types.NewLondonSigner(unsignedTx.ChainId())
	signedTx, err := types.SignTx(newTx, signer, p.coordinatorKey)
	if err != nil {
		p.nextNonce--
		return nil, fmt.Errorf("failed to sign putInbox tx: %w", err)
	}

	originalHash := unsignedTx.Hash()
	entry := putInboxTxEntry{
		tx:             signedTx,
		xtID:           xtID,
		nonce:          nonce,
		originalTxHash: originalHash,
		sequence:       sequence,
		committed:      false,
	}

	p.pending = append(p.pending, entry)
	p.byHash[signedTx.Hash()] = &p.pending[len(p.pending)-1]

	log.Info("[SSV] PutInbox pool added tx",
		"xtID", xtID,
		"nonce", nonce,
		"hash", signedTx.Hash().Hex(),
		"originalHash", originalHash.Hex(),
		"pendingCount", len(p.pending))

	return signedTx, nil
}

func (p *putInboxTxPool) inject() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pending) == 0 {
		return nil
	}

	if err := p.realignPendingNoncesLocked(); err != nil {
		return err
	}

	txs := make([]*types.Transaction, len(p.pending))
	for i, entry := range p.pending {
		txs[i] = entry.tx
	}

	log.Info("[SSV] PutInbox pool injecting transactions",
		"count", len(txs),
		"startNonce", p.pending[0].nonce,
		"endNonce", p.pending[len(p.pending)-1].nonce)

	errs := p.mainPool.Add(txs, false)
	injectedCount := 0
	alreadyKnownCount := 0
	failedCount := 0

	for i, err := range errs {
		entry := &p.pending[i]
		if err != nil {
			if errors.Is(err, txpool.ErrAlreadyKnown) {
				alreadyKnownCount++
			} else {
				log.Warn("[SSV] PutInbox pool failed to inject tx",
					"hash", entry.tx.Hash().Hex(),
					"nonce", entry.nonce,
					"xtID", entry.xtID,
					"err", err)
				failedCount++
			}
		} else {
			injectedCount++
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to inject %d/%d putInbox transactions", failedCount, len(txs))
	}

	log.Info("[SSV] PutInbox pool injection complete",
		"injected", injectedCount,
		"alreadyKnown", alreadyKnownCount,
		"failed", failedCount)

	return nil
}

func (p *putInboxTxPool) realignPendingNoncesLocked() error {
	poolNonce := p.mainPool.PoolNonce(p.coordinatorAddr)
	pendingCount := uint64(len(p.pending))
	if pendingCount > poolNonce {
		pendingCount = poolNonce
	}
	targetNonce := poolNonce - pendingCount
	signer := types.NewLondonSigner(new(big.Int).SetUint64(p.chainID))

	realigned := 0
	for i := range p.pending {
		entry := &p.pending[i]
		if entry.nonce == targetNonce {
			targetNonce++
			continue
		}
		if err := p.resignEntryWithNonce(entry, targetNonce, signer); err != nil {
			return err
		}
		targetNonce++
		realigned++
	}

	if realigned > 0 {
		log.Info("[SSV] PutInbox pool realigned nonces",
			"updatedCount", realigned,
			"nextNonce", targetNonce)
	}

	p.nextNonce = targetNonce
	return nil
}

func (p *putInboxTxPool) resignEntryWithNonce(entry *putInboxTxEntry, nonce uint64, signer types.Signer) error {
	accessList := entry.tx.AccessList()
	clonedList := make(types.AccessList, len(accessList))
	for i, tuple := range accessList {
		clonedList[i] = types.AccessTuple{
			Address:     tuple.Address,
			StorageKeys: append([]common.Hash(nil), tuple.StorageKeys...),
		}
	}

	inner := &types.DynamicFeeTx{
		ChainID:    new(big.Int).Set(entry.tx.ChainId()),
		Nonce:      nonce,
		GasTipCap:  new(big.Int).Set(entry.tx.GasTipCap()),
		GasFeeCap:  new(big.Int).Set(entry.tx.GasFeeCap()),
		Gas:        entry.tx.Gas(),
		To:         entry.tx.To(),
		Value:      new(big.Int).Set(entry.tx.Value()),
		Data:       append([]byte(nil), entry.tx.Data()...),
		AccessList: clonedList,
	}
	oldHash := entry.tx.Hash()

	newTx := types.NewTx(inner)
	signedTx, err := types.SignTx(newTx, signer, p.coordinatorKey)
	if err != nil {
		return fmt.Errorf("failed to re-sign putInbox tx: %w", err)
	}

	delete(p.byHash, oldHash)
	entry.tx = signedTx
	entry.nonce = nonce
	p.byHash[signedTx.Hash()] = entry
	return nil
}

func (p *putInboxTxPool) getPending() []*types.Transaction {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.pending) == 0 {
		return nil
	}

	txs := make([]*types.Transaction, len(p.pending))
	for i, entry := range p.pending {
		txs[i] = entry.tx
	}
	return txs
}

func (p *putInboxTxPool) pendingEntries() []putInboxTxEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.pending) == 0 {
		return nil
	}

	entries := make([]putInboxTxEntry, len(p.pending))
	copy(entries, p.pending)
	return entries
}

func (p *putInboxTxPool) entryByHash(hash common.Hash) *putInboxTxEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if entry, ok := p.byHash[hash]; ok && entry != nil {
		cpy := *entry
		return &cpy
	}
	return nil
}

func (p *putInboxTxPool) markCommitted(slot, blockNumber uint64, hashes []common.Hash) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	marked := 0
	hashSet := make(map[common.Hash]struct{}, len(hashes))
	for _, h := range hashes {
		hashSet[h] = struct{}{}
	}

	newPending := make([]putInboxTxEntry, 0, len(p.pending))
	for _, entry := range p.pending {
		if _, found := hashSet[entry.tx.Hash()]; found {
			entry.committed = true
			entry.committedBlock = blockNumber
			entry.committedSlot = slot
			p.committed = append(p.committed, entry)
			marked++

			log.Info("[SSV] PutInbox pool marked committed",
				"hash", entry.tx.Hash().Hex(),
				"nonce", entry.nonce,
				"xtID", entry.xtID,
				"block", blockNumber,
				"slot", slot)
		} else {
			newPending = append(newPending, entry)
		}
	}

	p.pending = newPending
	return marked
}

func (p *putInboxTxPool) clearCommitted(hashes map[common.Hash]bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(hashes) == 0 {
		return 0
	}

	newCommitted := make([]putInboxTxEntry, 0, len(p.committed))
	removed := 0

	for _, entry := range p.committed {
		if hashes[entry.tx.Hash()] {
			delete(p.byHash, entry.tx.Hash())
			removed++
			log.Info("[SSV] PutInbox pool cleared committed tx",
				"hash", entry.tx.Hash().Hex(),
				"nonce", entry.nonce,
				"xtID", entry.xtID,
				"block", entry.committedBlock)
		} else {
			newCommitted = append(newCommitted, entry)
		}
	}

	p.committed = newCommitted

	if removed > 0 {
		p.refreshNonceLocked("clearCommitted")
	}

	return removed
}

func (p *putInboxTxPool) clearAll() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := len(p.pending) + len(p.committed)
	if total == 0 {
		return 0
	}

	log.Info("[SSV] PutInbox pool clearing all transactions",
		"pending", len(p.pending),
		"committed", len(p.committed))

	p.pending = nil
	p.committed = nil
	p.byHash = make(map[common.Hash]*putInboxTxEntry)
	p.refreshNonceLocked("clearAll")

	return total
}

func (p *putInboxTxPool) dropByXtID(xtID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if xtID == "" {
		return 0
	}

	newPending := make([]putInboxTxEntry, 0, len(p.pending))
	newCommitted := make([]putInboxTxEntry, 0, len(p.committed))
	dropped := 0

	for _, entry := range p.pending {
		if entry.xtID == xtID {
			delete(p.byHash, entry.tx.Hash())
			dropped++
			log.Info("[SSV] PutInbox pool dropped pending tx",
				"xtID", xtID,
				"hash", entry.tx.Hash().Hex(),
				"nonce", entry.nonce)
		} else {
			newPending = append(newPending, entry)
		}
	}

	for _, entry := range p.committed {
		if entry.xtID == xtID {
			delete(p.byHash, entry.tx.Hash())
			dropped++
			log.Info("[SSV] PutInbox pool dropped committed tx",
				"xtID", xtID,
				"hash", entry.tx.Hash().Hex(),
				"nonce", entry.nonce,
				"block", entry.committedBlock)
		} else {
			newCommitted = append(newCommitted, entry)
		}
	}

	p.pending = newPending
	p.committed = newCommitted

	if dropped > 0 {
		if err := p.realignPendingNoncesLocked(); err != nil {
			log.Warn("[SSV] PutInbox pool failed to realign after drop", "err", err)
		}
		p.refreshNonceLocked("dropByXtID")
	}

	return dropped
}

func (p *putInboxTxPool) stats() (pending, committed int, nextNonce uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.pending), len(p.committed), p.nextNonce
}

func (p *putInboxTxPool) lookup(hash common.Hash) *types.Transaction {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if entry, ok := p.byHash[hash]; ok {
		return entry.tx
	}
	return nil
}

func (p *putInboxTxPool) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.Info("[SSV] PutInbox pool reset",
		"pending", len(p.pending),
		"committed", len(p.committed))

	p.pending = nil
	p.committed = nil
	p.byHash = make(map[common.Hash]*putInboxTxEntry)
	p.refreshNonceLocked("reset")
}
