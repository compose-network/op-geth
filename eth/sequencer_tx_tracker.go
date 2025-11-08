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
	sequence        uint64
}

// sequencerTxTracker maintains the ordered lifecycle of staged/committed transactions.
type sequencerTxTracker struct {
	staged    []common.Hash
	committed []common.Hash
	records   map[common.Hash]*sequencerTxRecord
	nextSeq   uint64
}

type sequencerBundleEntry struct {
	tx     *types.Transaction
	xtID   string
	kind   sequencerTxKind
	status sequencerTxStatus
}

type sequencerPendingItem struct {
	kind   sequencerTxKind
	status sequencerTxStatus
}

type sequencerPendingBundle struct {
	xtID  string
	items []sequencerPendingItem
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
		nextSeq:   1,
	}
}

func (t *sequencerTxTracker) reset() {
	t.staged = t.staged[:0]
	t.committed = t.committed[:0]
	t.records = make(map[common.Hash]*sequencerTxRecord)
	t.nextSeq = 1
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
		sequence:        t.nextSeq,
	}
	t.nextSeq++
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

func (t *sequencerTxTracker) buildBundles() ([]sequencerBundleEntry, []sequencerPendingBundle) {
	if len(t.records) == 0 {
		return nil, nil
	}

	records := make([]*sequencerTxRecord, 0, len(t.records))
	for _, rec := range t.records {
		if rec != nil && rec.tx != nil {
			records = append(records, rec)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].sequence < records[j].sequence
	})

	ready := make([]sequencerBundleEntry, 0, len(records))
	type group struct {
		firstSeq  uint64
		puts      []*sequencerTxRecord
		originals []*sequencerTxRecord
	}

	groups := make(map[string]*group)

	for _, record := range records {
		if record.xtID == "" {
			ready = append(ready, sequencerBundleEntry{tx: record.tx, xtID: record.xtID, kind: record.kind, status: record.status})
			continue
		}

		g, ok := groups[record.xtID]
		if !ok {
			g = &group{firstSeq: record.sequence}
			groups[record.xtID] = g
		}
		if record.sequence < g.firstSeq {
			g.firstSeq = record.sequence
		}

		if record.kind == sequencerTxPutInbox {
			g.puts = append(g.puts, record)
		} else {
			g.originals = append(g.originals, record)
		}
	}

	if len(groups) == 0 {
		return ready, nil
	}

	xtIDs := make([]string, 0, len(groups))
	for xtID := range groups {
		xtIDs = append(xtIDs, xtID)
	}

	sort.Slice(xtIDs, func(i, j int) bool {
		gi := groups[xtIDs[i]]
		gj := groups[xtIDs[j]]

		if len(gi.originals) > 0 && len(gj.originals) > 0 {
			return gi.originals[0].tx.Nonce() < gj.originals[0].tx.Nonce()
		}

		return gi.firstSeq < gj.firstSeq
	})

	for _, xtID := range xtIDs {
		g := groups[xtID]

		sort.Slice(g.puts, func(i, j int) bool { return g.puts[i].sequence < g.puts[j].sequence })
		sort.Slice(g.originals, func(i, j int) bool { return g.originals[i].sequence < g.originals[j].sequence })

		i, j := 0, 0
		for i < len(g.puts) && j < len(g.originals) {
			ready = append(ready,
				sequencerBundleEntry{tx: g.puts[i].tx, xtID: xtID, kind: g.puts[i].kind, status: g.puts[i].status},
				sequencerBundleEntry{tx: g.originals[j].tx, xtID: xtID, kind: g.originals[j].kind, status: g.originals[j].status},
			)
			i++
			j++
		}

		for ; j < len(g.originals); j++ {
			ready = append(ready, sequencerBundleEntry{tx: g.originals[j].tx, xtID: xtID, kind: g.originals[j].kind, status: g.originals[j].status})
		}
	}

	return ready, nil
}

func (t *sequencerTxTracker) allXTIDsLocked() map[string]struct{} {
	xts := make(map[string]struct{}, len(t.records))
	for _, rec := range t.records {
		if rec != nil && rec.xtID != "" {
			xts[rec.xtID] = struct{}{}
		}
	}
	return xts
}

func (t *sequencerTxTracker) committedHashSet() map[common.Hash]struct{} {
	set := make(map[common.Hash]struct{}, len(t.committed))
	for _, hash := range t.committed {
		set[hash] = struct{}{}
	}
	return set
}

func (t *sequencerTxTracker) readyPairCount() int {
	if len(t.records) == 0 {
		return 0
	}
	type counts struct {
		put  int
		orig int
	}
	groups := make(map[string]*counts)
	for _, rec := range t.records {
		if rec == nil || rec.xtID == "" {
			continue
		}
		g := groups[rec.xtID]
		if g == nil {
			g = &counts{}
			groups[rec.xtID] = g
		}
		if rec.kind == sequencerTxPutInbox {
			g.put++
		} else {
			g.orig++
		}
	}
	total := 0
	for _, g := range groups {
		if g.put < g.orig {
			total += g.put
		} else {
			total += g.orig
		}
	}
	return total
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
