package sequencer

import (
	"time"
)

// Config holds sequencer coordinator configuration
type Config struct {
	ChainID []byte `json:"chain_id"`

	// Sequencer-specific settings
	BlockTimeout         time.Duration `json:"block_timeout"`
	MaxLocalTxs          int           `json:"max_local_txs"`
	SCPTimeout           time.Duration `json:"scp_timeout"`
	EnableCIRCValidation bool          `json:"enable_circ_validation"`
}
