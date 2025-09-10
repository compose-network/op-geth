package core

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/core/types"
)

type JovianConfig interface {
	IsJovian(timestamp uint64) bool
}

type HeaderGetter interface {
	GetHeaderByNumber(uint64) *types.Header
}

func CalculateGasUsed(header *types.Header, txs []*types.Transaction, gasUsed uint64, config JovianConfig, parentHeader *types.Header) uint64 {
	var previousBlockWasJovian bool
	if parentHeader != nil {
		previousBlockWasJovian = config.IsJovian(parentHeader.Time)
	}
	if config.IsJovian(header.Time) && previousBlockWasJovian {
		data := txs[0].Data()
		daFootprintGasScalar := binary.BigEndian.Uint16(data[176:178])
		var cumulativeDAFootprint uint64
		for _, tx := range txs {
			if tx.IsDepositTx() {
				continue
			}
			cumulativeDAFootprint += tx.RollupCostData().EstimatedDASize().Uint64()
		}
		daFootprint := uint64(daFootprintGasScalar) * cumulativeDAFootprint
		if gasUsed < daFootprint {
			return daFootprint
		}
	}
	return gasUsed
}
