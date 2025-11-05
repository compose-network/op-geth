package eth

import (
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// sequencerTxStatus represents the lifecycle stage of a sequencer-managed transaction.
type sequencerTxStatus uint8

const (
	sequencerTxStatusStaged sequencerTxStatus = iota
	sequencerTxStatusCommitted
)

func (s sequencerTxStatus) String() string {
	switch s {
	case sequencerTxStatusStaged:
		return "staged"
	case sequencerTxStatusCommitted:
		return "committed"
	default:
		return "unknown"
	}
}

// sequencerTxRecord keeps bookkeeping metadata for a sequencer-managed transaction.
// The tracker assumes external synchronization from EthAPIBackend.sequencerTxMutex.
type sequencerTxRecord struct {
	tx              *types.Transaction
	xtID            string
	kind            sequencerTxKind
	status          sequencerTxStatus
	committedBlock  uint64
	committedSlot   uint64
	lastUpdatedSlot uint64
}

// sequencerTxTracker maintains the ordered lifecycle of staged/committed transactions
// while allowing restaging after payload rebuilds. It preserves insertion ordering for
// staged entries and commitment ordering for committed entries.
type sequencerTxTracker struct {
	staged    []common.Hash
	committed []common.Hash
	records   map[common.Hash]*sequencerTxRecord
}

type sequencerBundleEntry struct {
	tx     *types.Transaction
	xtID   string
	kind   sequencerTxKind
	status sequencerTxStatus
}

type sequencerPendingBundle struct {
	xtID  string
	items []sequencerPendingItem
}

type sequencerPendingItem struct {
	kind   sequencerTxKind
	status sequencerTxStatus
}

func (k sequencerTxKind) String() string {
	switch k {
	case sequencerTxOriginal:
		return "original"
	case sequencerTxPutInbox:
		return "putInbox"
	default:
		return "unknown"
	}
}

func newSequencerTxTracker() *sequencerTxTracker {
	return &sequencerTxTracker{
		staged:    make([]common.Hash, 0),
		committed: make([]common.Hash, 0),
		records:   make(map[common.Hash]*sequencerTxRecord),
	}
}

func (t *sequencerTxTracker) reset() {
	t.staged = t.staged[:0]
	t.committed = t.committed[:0]
	t.records = make(map[common.Hash]*sequencerTxRecord)
}

func (t *sequencerTxTracker) add(tx *types.Transaction, kind sequencerTxKind, currentSlot uint64) {
	if t.records == nil {
		t.records = make(map[common.Hash]*sequencerTxRecord)
	}
	hash := tx.Hash()
	if record, ok := t.records[hash]; ok {
		record.tx = tx
		if kind == sequencerTxPutInbox {
			record.kind = kind
		}
		// If the transaction was already committed for a previous payload, keep it there.
		// Otherwise ensure it is present in the staged queue so it can be ordered correctly.
		if record.status == sequencerTxStatusStaged && !containsHash(t.staged, hash) {
			t.staged = append(t.staged, hash)
		}
		record.lastUpdatedSlot = currentSlot
		return
	}

	t.records[hash] = &sequencerTxRecord{
		tx:              tx,
		kind:            kind,
		status:          sequencerTxStatusStaged,
		lastUpdatedSlot: currentSlot,
	}
	t.staged = append(t.staged, hash)
}

func (t *sequencerTxTracker) assignXtID(hash common.Hash, xtID string) bool {
	if record, ok := t.records[hash]; ok {
		record.xtID = xtID
		return true
	}
	return false
}

func (t *sequencerTxTracker) record(hash common.Hash) *sequencerTxRecord {
	if t == nil {
		return nil
	}
	if record, ok := t.records[hash]; ok {
		return record
	}
	return nil
}

func (t *sequencerTxTracker) countByKind(kind sequencerTxKind) int {
	total := 0
	for _, hash := range t.staged {
		if record := t.records[hash]; record != nil && record.kind == kind {
			total++
		}
	}
	for _, hash := range t.committed {
		if record := t.records[hash]; record != nil && record.kind == kind {
			total++
		}
	}
	return total
}

func (t *sequencerTxTracker) pendingTotal() int {
	return len(t.staged) + len(t.committed)
}

func (t *sequencerTxTracker) orderedTransactions() types.Transactions {
	total := len(t.staged) + len(t.committed)
	if total == 0 {
		return types.Transactions{}
	}
	ordered := make(types.Transactions, 0, total)
	for _, hash := range t.committed {
		if record := t.records[hash]; record != nil {
			ordered = append(ordered, record.tx)
		}
	}
	for _, hash := range t.staged {
		if record := t.records[hash]; record != nil {
			ordered = append(ordered, record.tx)
		}
	}
	return ordered
}

func (t *sequencerTxTracker) orderedRecords() []*sequencerTxRecord {
	total := len(t.staged) + len(t.committed)
	if total == 0 {
		return nil
	}
	ordered := make([]*sequencerTxRecord, 0, total)
	for _, hash := range t.committed {
		if record := t.records[hash]; record != nil {
			ordered = append(ordered, record)
		}
	}
	for _, hash := range t.staged {
		if record := t.records[hash]; record != nil {
			ordered = append(ordered, record)
		}
	}
	return ordered
}

func (t *sequencerTxTracker) transactionsByKind(kind sequencerTxKind) []*types.Transaction {
	total := t.countByKind(kind)
	if total == 0 {
		return []*types.Transaction{}
	}
	result := make([]*types.Transaction, 0, total)
	for _, hash := range t.committed {
		if record := t.records[hash]; record != nil && record.kind == kind {
			result = append(result, record.tx)
		}
	}
	for _, hash := range t.staged {
		if record := t.records[hash]; record != nil && record.kind == kind {
			result = append(result, record.tx)
		}
	}
	return result
}

func (t *sequencerTxTracker) buildBundles() ([]sequencerBundleEntry, []sequencerPendingBundle) {
	ordered := t.orderedRecords()
	if len(ordered) == 0 {
		return nil, nil
	}

	ready := make([]sequencerBundleEntry, 0, len(ordered))
	pending := make(map[string][]*sequencerTxRecord)

	for _, record := range ordered {
		if record == nil || record.tx == nil {
			continue
		}
		if record.xtID == "" {
			ready = append(ready, sequencerBundleEntry{
				tx:     record.tx,
				xtID:   record.xtID,
				kind:   record.kind,
				status: record.status,
			})
			continue
		}

		bucket := pending[record.xtID]
		bucket = append(bucket, record)
		pending[record.xtID] = bucket

		var put, original *sequencerTxRecord
		for _, item := range bucket {
			if item.kind == sequencerTxPutInbox && put == nil {
				put = item
			} else if item.kind == sequencerTxOriginal && original == nil {
				original = item
			}
		}
		if put != nil && original != nil {
			ready = append(ready,
				sequencerBundleEntry{tx: put.tx, xtID: put.xtID, kind: put.kind, status: put.status},
				sequencerBundleEntry{tx: original.tx, xtID: original.xtID, kind: original.kind, status: original.status},
			)

			newBucket := make([]*sequencerTxRecord, 0, len(bucket))
			for _, item := range bucket {
				if item == put || item == original {
					continue
				}
				newBucket = append(newBucket, item)
			}
			if len(newBucket) == 0 {
				delete(pending, record.xtID)
			} else {
				pending[record.xtID] = newBucket
			}
		}
	}

	if len(pending) == 0 {
		return ready, nil
	}

	xtIDs := make([]string, 0, len(pending))
	for xtID := range pending {
		xtIDs = append(xtIDs, xtID)
	}
	sort.Strings(xtIDs)

	pendingBundles := make([]sequencerPendingBundle, 0, len(xtIDs))
	for _, xtID := range xtIDs {
		bucket := pending[xtID]
		bundle := sequencerPendingBundle{
			xtID:  xtID,
			items: make([]sequencerPendingItem, 0, len(bucket)),
		}
		for _, item := range bucket {
			bundle.items = append(bundle.items, sequencerPendingItem{kind: item.kind, status: item.status})
		}
		pendingBundles = append(pendingBundles, bundle)
	}

	return ready, pendingBundles
}

func (t *sequencerTxTracker) markCommitted(slot, blockNumber uint64, hashes []common.Hash) (marked int) {
	if len(hashes) == 0 {
		return 0
	}
	for _, hash := range hashes {
		record, ok := t.records[hash]
		if !ok {
			continue
		}
		record.status = sequencerTxStatusCommitted
		record.committedBlock = blockNumber
		record.committedSlot = slot
		record.lastUpdatedSlot = slot
		if containsHash(t.staged, hash) {
			t.staged = removeHash(t.staged, hash)
		}
		if !containsHash(t.committed, hash) {
			t.committed = append(t.committed, hash)
		}
		marked++
	}
	return marked
}

func (t *sequencerTxTracker) committedHashSet() map[common.Hash]struct{} {
	result := make(map[common.Hash]struct{}, len(t.committed))
	for _, hash := range t.committed {
		result[hash] = struct{}{}
	}
	return result
}

func (t *sequencerTxTracker) dropByXtID(xtID string) (removedPut, removedOriginal int, removed []*sequencerTxRecord) {
	if xtID == "" {
		return 0, 0, nil
	}
	var toDrop []common.Hash
	for hash, record := range t.records {
		if record != nil && record.xtID == xtID {
			toDrop = append(toDrop, hash)
		}
	}
	if len(toDrop) == 0 {
		return 0, 0, nil
	}
	return t.dropHashes(toDrop)
}

func (t *sequencerTxTracker) dropHashes(hashes []common.Hash) (removedPut, removedOriginal int, removed []*sequencerTxRecord) {
	if len(hashes) == 0 {
		return 0, 0, nil
	}
	dropSet := make(map[common.Hash]struct{}, len(hashes))
	for _, hash := range hashes {
		dropSet[hash] = struct{}{}
	}
	filter := func(in []common.Hash) []common.Hash {
		if len(in) == 0 {
			return in
		}
		out := in[:0]
		for _, hash := range in {
			if _, drop := dropSet[hash]; drop {
				continue
			}
			out = append(out, hash)
		}
		return out
	}

	for hash := range dropSet {
		if record, ok := t.records[hash]; ok && record != nil {
			if record.kind == sequencerTxPutInbox {
				removedPut++
			} else {
				removedOriginal++
			}
			removed = append(removed, record)
			delete(t.records, hash)
		}
	}
	t.staged = filter(t.staged)
	t.committed = filter(t.committed)
	return removedPut, removedOriginal, removed
}

func (t *sequencerTxTracker) markDelivered(delivered map[common.Hash]bool) (removedPut, removedOriginal int, removed []*sequencerTxRecord) {
	if len(delivered) == 0 {
		return 0, 0, nil
	}
	hashes := make([]common.Hash, 0, len(delivered))
	for hash, ok := range delivered {
		if ok {
			hashes = append(hashes, hash)
		}
	}
	return t.dropHashes(hashes)
}

func (t *sequencerTxTracker) clearAll() (removedPut, removedOriginal int, removed []*sequencerTxRecord) {
	if len(t.records) == 0 {
		return 0, 0, nil
	}
	hashes := make([]common.Hash, 0, len(t.records))
	for hash := range t.records {
		hashes = append(hashes, hash)
	}
	return t.dropHashes(hashes)
}

func containsHash(items []common.Hash, target common.Hash) bool {
	for _, hash := range items {
		if hash == target {
			return true
		}
	}
	return false
}

func removeHash(items []common.Hash, target common.Hash) []common.Hash {
	for i, hash := range items {
		if hash == target {
			return append(items[:i], items[i+1:]...)
		}
	}
	return items
}
