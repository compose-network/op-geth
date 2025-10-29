package protocol

import (
	"fmt"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
)

// basicValidator implements basic validation for SBCP messages
type basicValidator struct{}

// NewBasicValidator creates a new basic SBCP message validator
func NewBasicValidator() Validator {
	return &basicValidator{}
}

// validateXTRequest validates cross-chain transaction requests
func (v *basicValidator) validateXTRequest(xtReq *pb.XTRequest) error {
	if xtReq == nil {
		return fmt.Errorf("XTRequest is nil")
	}

	if len(xtReq.Transactions) == 0 {
		return fmt.Errorf("no transactions in XTRequest")
	}

	// Validate each transaction request
	for i, txReq := range xtReq.Transactions {
		if err := v.validateTransactionRequest(txReq); err != nil {
			return fmt.Errorf("invalid transaction request at index %d: %w", i, err)
		}
	}

	return nil
}

// validateTransactionRequest validates individual transaction requests
func (v *basicValidator) validateTransactionRequest(txReq *pb.TransactionRequest) error {
	if txReq == nil {
		return fmt.Errorf("TransactionRequest is nil")
	}

	if len(txReq.ChainId) == 0 {
		return fmt.Errorf("missing chain ID")
	}

	if len(txReq.Transaction) == 0 {
		return fmt.Errorf("no transactions provided")
	}

	return nil
}
