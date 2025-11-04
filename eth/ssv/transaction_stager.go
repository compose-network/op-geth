// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.

package ssv

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// TxState represents the lifecycle state of a sequencer transaction
type TxState int

const (
	TxStateStaged    TxState = iota // Transaction simulated and ready for inclusion
	TxStateInBlock                  // Transaction snapshot taken for block building (committed to builder)
	TxStateCommitted                // Transaction included in finalized block
	TxStateAborted                  // Transaction rejected (XT aborted or simulation failed)
)

func (s TxState) String() string {
	switch s {
	case TxStateStaged:
		return "staged"
	case TxStateInBlock:
		return "in_block"
	case TxStateCommitted:
		return "committed"
	case TxStateAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// TxEntry tracks a sequencer transaction through its lifecycle
type TxEntry struct {
	Tx      *types.Transaction
	XtID    string  // Cross-chain transaction ID (hex-encoded)
	Kind    TxKind  // Original or PutInbox
	State   TxState // Current lifecycle state
	Slot    uint64  // Slot in which this transaction was staged
	BlockNo *uint64 // Block number if committed (nil otherwise)
}

type TxKind int

const (
	TxKindOriginal TxKind = iota
	TxKindPutInbox
)

func (k TxKind) String() string {
	switch k {
	case TxKindOriginal:
		return "original"
	case TxKindPutInbox:
		return "putInbox"
	default:
		return "unknown"
	}
}

// TransactionStager manages sequencer transaction lifecycle with atomic state transitions.
// Single source of truth for all sequencer-managed transactions.
type TransactionStager struct {
	mu sync.RWMutex

	// Primary storage: maintains insertion order (critical for execution)
	entries []*TxEntry

	// Indexes for O(1) lookups
	byHash map[common.Hash]int // hash → index in entries
	byXtID map[string][]int    // xtID → indexes in entries (one XT can have multiple txs)

	currentSlot uint64 // Current SBCP slot for validation
}

// NewTransactionStager creates a new transaction stager
func NewTransactionStager() *TransactionStager {
	return &TransactionStager{
		entries: make([]*TxEntry, 0),
		byHash:  make(map[common.Hash]int),
		byXtID:  make(map[string][]int),
	}
}

// Stage adds a transaction in Staged state. Returns error if already exists.
func (s *TransactionStager) Stage(tx *types.Transaction, xtID string, kind TxKind, slot uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := tx.Hash()
	if _, exists := s.byHash[hash]; exists {
		return fmt.Errorf("transaction %s already staged", hash.Hex())
	}

	entry := &TxEntry{
		Tx:    tx,
		XtID:  xtID,
		Kind:  kind,
		State: TxStateStaged,
		Slot:  slot,
	}

	idx := len(s.entries)
	s.entries = append(s.entries, entry)
	s.byHash[hash] = idx
	s.byXtID[xtID] = append(s.byXtID[xtID], idx)

	log.Info("[SSV] Transaction staged",
		"hash", hash.Hex(),
		"xtID", xtID,
		"kind", kind.String(),
		"slot", slot,
		"index", idx)

	return nil
}

// SnapshotForBlock atomically transitions all Staged transactions to InBlock state
// and returns them in insertion order. This ensures block building gets a consistent view.
func (s *TransactionStager) SnapshotForBlock(slot uint64) ([]*TxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var snapshot []*TxEntry
	stagedCount := 0
	inBlockCount := 0

	for _, entry := range s.entries {
		if entry.Slot != slot {
			continue
		}

		switch entry.State {
		case TxStateStaged:
			entry.State = TxStateInBlock
			stagedCount++
			snapshot = append(snapshot, entry)
		case TxStateInBlock:
			inBlockCount++
			snapshot = append(snapshot, entry)
		}
	}

	if len(snapshot) > 0 {
		log.Info("[SSV] Snapshot for block building",
			"slot", slot,
			"staged", stagedCount,
			"in_block", inBlockCount,
			"count", len(snapshot))
	}

	return snapshot, nil
}

// GetTransactionsForBlock returns transactions ready for block inclusion
func (s *TransactionStager) GetTransactionsForBlock(slot uint64) types.Transactions {
	snapshot, _ := s.SnapshotForBlock(slot)
	txs := make(types.Transactions, 0, len(snapshot))
	for _, entry := range snapshot {
		txs = append(txs, entry.Tx)
	}
	return txs
}

// CommitBlock marks all InBlock transactions as Committed after successful block finalization
func (s *TransactionStager) CommitBlock(blockNo uint64) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	committedPutInbox := 0
	committedOriginal := 0

	for _, entry := range s.entries {
		if entry.State == TxStateInBlock {
			entry.State = TxStateCommitted
			entry.BlockNo = &blockNo

			if entry.Kind == TxKindPutInbox {
				committedPutInbox++
			} else {
				committedOriginal++
			}

			log.Info("[SSV] Transaction committed",
				"hash", entry.Tx.Hash().Hex(),
				"xtID", entry.XtID,
				"kind", entry.Kind.String(),
				"blockNo", blockNo)
		}
	}

	return committedPutInbox, committedOriginal
}

// AbortXT atomically aborts all transactions belonging to an XT.
// Returns hashes of aborted transactions for cleanup.
func (s *TransactionStager) AbortXT(xtID string) []common.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	indexes, exists := s.byXtID[xtID]
	if !exists {
		return nil
	}

	abortedHashes := make([]common.Hash, 0, len(indexes))

	for _, idx := range indexes {
		entry := s.entries[idx]
		if entry.State == TxStateStaged || entry.State == TxStateInBlock {
			entry.State = TxStateAborted
			abortedHashes = append(abortedHashes, entry.Tx.Hash())

			log.Info("[SSV] Transaction aborted",
				"hash", entry.Tx.Hash().Hex(),
				"xtID", xtID,
				"kind", entry.Kind.String(),
				"prevState", entry.State.String())
		}
	}

	return abortedHashes
}

// ClearSlot removes all transactions from a completed slot.
// Only removes Committed or Aborted transactions to prevent data loss.
func (s *TransactionStager) ClearSlot(slot uint64) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	removedPutInbox := 0
	removedOriginal := 0
	filtered := make([]*TxEntry, 0, len(s.entries))

	for _, entry := range s.entries {
		shouldRemove := entry.Slot == slot &&
			(entry.State == TxStateCommitted || entry.State == TxStateAborted)

		if shouldRemove {
			if entry.Kind == TxKindPutInbox {
				removedPutInbox++
			} else {
				removedOriginal++
			}
			log.Debug("[SSV] Cleared transaction from slot",
				"hash", entry.Tx.Hash().Hex(),
				"xtID", entry.XtID,
				"state", entry.State.String(),
				"slot", slot)
		} else {
			filtered = append(filtered, entry)
		}
	}

	s.entries = filtered
	s.rebuildIndexes()

	return removedPutInbox, removedOriginal
}

// GetPendingByKind returns transactions in Staged or InBlock state filtered by kind
func (s *TransactionStager) GetPendingByKind(kind TxKind) []*types.Transaction {
	s.mu.RLock()
	defer s.mu.RUnlock()

	txs := make([]*types.Transaction, 0)
	for _, entry := range s.entries {
		if entry.Kind == kind && (entry.State == TxStateStaged || entry.State == TxStateInBlock) {
			txs = append(txs, entry.Tx)
		}
	}
	return txs
}

// RequeueInBlock transitions transactions that were part of an in-progress block
// back to the Staged state so they can be snapshotted again. If slot is zero,
// all in-flight transactions are requeued.
func (s *TransactionStager) RequeueInBlock(slot uint64) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	putInbox := 0
	original := 0

	for _, entry := range s.entries {
		if slot != 0 && entry.Slot != slot {
			continue
		}
		if entry.State != TxStateInBlock {
			continue
		}

		entry.State = TxStateStaged
		if entry.Kind == TxKindPutInbox {
			putInbox++
		} else {
			original++
		}
	}

	return putInbox, original
}

// UpdateSlot sets the current slot for validation
func (s *TransactionStager) UpdateSlot(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentSlot = slot
	log.Debug("[SSV] Updated current slot", "slot", slot)
}

// rebuildIndexes reconstructs byHash and byXtID from entries (must hold mu)
func (s *TransactionStager) rebuildIndexes() {
	s.byHash = make(map[common.Hash]int, len(s.entries))
	s.byXtID = make(map[string][]int)

	for i, entry := range s.entries {
		s.byHash[entry.Tx.Hash()] = i
		s.byXtID[entry.XtID] = append(s.byXtID[entry.XtID], i)
	}
}

// GetStats returns current stager statistics
func (s *TransactionStager) GetStats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[string]int)
	for _, entry := range s.entries {
		key := fmt.Sprintf("%s_%s", entry.State.String(), entry.Kind.String())
		stats[key]++
	}
	return stats
}
