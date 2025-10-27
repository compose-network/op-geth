package eth

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

// composeUserOpsAPI implements the `custom.compose_buildSignedUserOpsTx` RPC.
// It purposely lives in package `eth` to access sequencer signing facilities
// available on the concrete API backend.
type composeUserOpsAPI struct {
	b *EthAPIBackend
}

// Request JSON types - supports both packed and unpacked formats
type userOperationV07 struct {
	Sender               common.Address `json:"sender"`
	Nonce                *hexutil.Big   `json:"nonce"`
	InitCode             hexutil.Bytes  `json:"initCode"`
	CallData             hexutil.Bytes  `json:"callData"`
	CallGasLimit         *hexutil.Big   `json:"callGasLimit"`
	VerificationGasLimit *hexutil.Big   `json:"verificationGasLimit"`
	PreVerificationGas   *hexutil.Big   `json:"preVerificationGas"`
	MaxFeePerGas         *hexutil.Big   `json:"maxFeePerGas"`
	MaxPriorityFeePerGas *hexutil.Big   `json:"maxPriorityFeePerGas"`

	// Packed format (EntryPoint v0.7 standard)
	PaymasterAndData hexutil.Bytes `json:"paymasterAndData,omitempty"`

	// Unpacked format
	Paymaster                     *common.Address `json:"paymaster,omitempty"`
	PaymasterVerificationGasLimit *hexutil.Big    `json:"paymasterVerificationGasLimit,omitempty"`
	PaymasterPostOpGasLimit       *hexutil.Big    `json:"paymasterPostOpGasLimit,omitempty"`
	PaymasterData                 hexutil.Bytes   `json:"paymasterData,omitempty"`

	Signature hexutil.Bytes `json:"signature"`
}

type composeOpts struct {
	ChainID uint64 `json:"chainId"`
}

// Response JSON type
type SignedTxResp struct {
	Raw                  string   `json:"raw"`
	Hash                 string   `json:"hash"`
	To                   string   `json:"to"`
	ChainID              uint64   `json:"chainId"`
	Gas                  string   `json:"gas"`
	MaxFeePerGas         string   `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string   `json:"maxPriorityFeePerGas"`
	UserOpHashes         []string `json:"userOpHashes"`
}

// Minimal ABI JSON for EntryPoint v0.7 used here.
// Includes: balanceOf(address), getUserOpHash(PackedUserOperation),
//
//	handleOps(PackedUserOperation[],address)
const entryPointV07ABI = `[
  {"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
  {"inputs":[{"components":[
      {"internalType":"address","name":"sender","type":"address"},
      {"internalType":"uint256","name":"nonce","type":"uint256"},
      {"internalType":"bytes","name":"initCode","type":"bytes"},
      {"internalType":"bytes","name":"callData","type":"bytes"},
      {"internalType":"bytes32","name":"accountGasLimits","type":"bytes32"},
      {"internalType":"uint256","name":"preVerificationGas","type":"uint256"},
      {"internalType":"bytes32","name":"gasFees","type":"bytes32"},
      {"internalType":"bytes","name":"paymasterAndData","type":"bytes"},
      {"internalType":"bytes","name":"signature","type":"bytes"}
  ],"internalType":"struct PackedUserOperation","name":"userOp","type":"tuple"}],
   "name":"getUserOpHash","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},
  {"inputs":[{"components":[
      {"internalType":"address","name":"sender","type":"address"},
      {"internalType":"uint256","name":"nonce","type":"uint256"},
      {"internalType":"bytes","name":"initCode","type":"bytes"},
      {"internalType":"bytes","name":"callData","type":"bytes"},
      {"internalType":"bytes32","name":"accountGasLimits","type":"bytes32"},
      {"internalType":"uint256","name":"preVerificationGas","type":"uint256"},
      {"internalType":"bytes32","name":"gasFees","type":"bytes32"},
      {"internalType":"bytes","name":"paymasterAndData","type":"bytes"},
      {"internalType":"bytes","name":"signature","type":"bytes"}
  ],"internalType":"struct PackedUserOperation[]","name":"ops","type":"tuple[]"},
  {"internalType":"address payable","name":"beneficiary","type":"address"}],
   "name":"handleOps","outputs":[],"stateMutability":"nonpayable","type":"function"},
  {"type":"error","name":"FailedOp","inputs":[{"internalType":"uint256","name":"opIndex","type":"uint256"},{"internalType":"string","name":"reason","type":"string"}]},
  {"type":"error","name":"FailedOpWithRevert","inputs":[{"internalType":"uint256","name":"opIndex","type":"uint256"},{"internalType":"string","name":"reason","type":"string"},{"internalType":"bytes","name":"inner","type":"bytes"}]},
  {"type":"error","name":"SignatureValidationFailed","inputs":[{"internalType":"address","name":"aggregator","type":"address"}]},
  {"type":"error","name":"PostOpReverted","inputs":[{"internalType":"bytes","name":"returnData","type":"bytes"}]},
  {"type":"error","name":"ExecutionResult","inputs":[{"internalType":"uint256","name":"preOpGas","type":"uint256"},{"internalType":"uint256","name":"paid","type":"uint256"},{"internalType":"uint48","name":"validAfter","type":"uint48"},{"internalType":"uint48","name":"validUntil","type":"uint48"},{"internalType":"bool","name":"targetSuccess","type":"bool"},{"internalType":"bytes","name":"targetResult","type":"bytes"}]},
  {"type":"error","name":"ValidationResult","inputs":[
      {"components":[
          {"internalType":"uint256","name":"preOpGas","type":"uint256"},
          {"internalType":"uint256","name":"prefund","type":"uint256"},
          {"internalType":"bool","name":"sigFailed","type":"bool"},
          {"internalType":"uint48","name":"validAfter","type":"uint48"},
          {"internalType":"uint48","name":"validUntil","type":"uint48"},
          {"internalType":"bytes","name":"paymasterContext","type":"bytes"}
      ],"internalType":"struct IEntryPoint.ReturnInfo","name":"returnInfo","type":"tuple"},
      {"components":[
          {"internalType":"uint256","name":"stake","type":"uint256"},
          {"internalType":"uint256","name":"unstakeDelaySec","type":"uint256"}
      ],"internalType":"struct IStakeManager.StakeInfo","name":"senderInfo","type":"tuple"},
      {"components":[
          {"internalType":"uint256","name":"stake","type":"uint256"},
          {"internalType":"uint256","name":"unstakeDelaySec","type":"uint256"}
      ],"internalType":"struct IStakeManager.StakeInfo","name":"factoryInfo","type":"tuple"},
      {"components":[
          {"internalType":"uint256","name":"stake","type":"uint256"},
          {"internalType":"uint256","name":"unstakeDelaySec","type":"uint256"}
      ],"internalType":"struct IStakeManager.StakeInfo","name":"paymasterInfo","type":"tuple"}
  ]},
  {"type":"error","name":"ValidationResultWithAggregation","inputs":[
      {"components":[
          {"internalType":"uint256","name":"preOpGas","type":"uint256"},
          {"internalType":"uint256","name":"prefund","type":"uint256"},
          {"internalType":"bool","name":"sigFailed","type":"bool"},
          {"internalType":"uint48","name":"validAfter","type":"uint48"},
          {"internalType":"uint48","name":"validUntil","type":"uint48"},
          {"internalType":"bytes","name":"paymasterContext","type":"bytes"}
      ],"internalType":"struct IEntryPoint.ReturnInfo","name":"returnInfo","type":"tuple"},
      {"components":[
          {"internalType":"uint256","name":"stake","type":"uint256"},
          {"internalType":"uint256","name":"unstakeDelaySec","type":"uint256"}
      ],"internalType":"struct IStakeManager.StakeInfo","name":"senderInfo","type":"tuple"},
      {"components":[
          {"internalType":"uint256","name":"stake","type":"uint256"},
          {"internalType":"uint256","name":"unstakeDelaySec","type":"uint256"}
      ],"internalType":"struct IStakeManager.StakeInfo","name":"factoryInfo","type":"tuple"},
      {"components":[
          {"internalType":"uint256","name":"stake","type":"uint256"},
          {"internalType":"uint256","name":"unstakeDelaySec","type":"uint256"}
      ],"internalType":"struct IStakeManager.StakeInfo","name":"paymasterInfo","type":"tuple"},
      {"components":[
          {"internalType":"address","name":"aggregator","type":"address"},
          {"components":[
              {"internalType":"uint256","name":"stake","type":"uint256"},
              {"internalType":"uint256","name":"unstakeDelaySec","type":"uint256"}
          ],"internalType":"struct IStakeManager.StakeInfo","name":"stakeInfo","type":"tuple"}
      ],"internalType":"struct IEntryPoint.AggregatorStakeInfo","name":"aggregatorInfo","type":"tuple"}
  ]},
  {"type":"error","name":"SenderAddressResult","inputs":[{"internalType":"address","name":"sender","type":"address"}]},
  {"type":"error","name":"DelegateAndRevert","inputs":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"ret","type":"bytes"}]}
]`

// Packed userop for ABI packing
type packedUserOp struct {
	Sender             common.Address `abi:"sender"`
	Nonce              *big.Int       `abi:"nonce"`
	InitCode           []byte         `abi:"initCode"`
	CallData           []byte         `abi:"callData"`
	AccountGasLimits   [32]byte       `abi:"accountGasLimits"`
	PreVerificationGas *big.Int       `abi:"preVerificationGas"`
	GasFees            [32]byte       `abi:"gasFees"`
	PaymasterAndData   []byte         `abi:"paymasterAndData"`
	Signature          []byte         `abi:"signature"`
}

// BuildSignedUserOpsTx is the RPC-exposed entry point.
// Final JSON-RPC method: compose_buildSignedUserOpsTx (namespace "compose").
func (api *composeUserOpsAPI) BuildSignedUserOpsTx(
	ctx context.Context,
	userOps []userOperationV07,
	opts composeOpts,
) (*SignedTxResp, error) {
	log.Info("[BuildSignedUserOpsTempDebug] BuildSignedUserOpsTx called",
		"userOpsCount", len(userOps),
		"requestedChainID", opts.ChainID)

	// Canonicalize & quick policy checks
	chainID := api.b.ChainConfig().ChainID.Uint64()
	if opts.ChainID == 0 || opts.ChainID != chainID {
		log.Warn("[BuildSignedUserOpsTempDebug] Chain ID mismatch",
			"requestedChainID", opts.ChainID,
			"expectedChainID", chainID)
		return nil, &rpc.JsonError{Code: -32001, Message: "wrongChainId", Data: map[string]any{"expected": chainID}}
	}

	// Always use the canonical v0.7 EntryPoint address.
	ep := common.HexToAddress("0x0000000071727de22e5e9d8baf0edac6f37da032")
	log.Info("[BuildSignedUserOpsTempDebug] Using EntryPoint", "address", ep.Hex(), "chainID", chainID)

	if len(userOps) == 0 {
		log.Warn("[BuildSignedUserOpsTempDebug] Empty userOps array")
		return nil, &rpc.JsonError{
			Code:    -32602,
			Message: "invalidUserOperation",
			Data:    map[string]any{"reason": "empty userOps"},
		}
	}
	if len(userOps) > 10 {
		log.Warn("[BuildSignedUserOpsTempDebug] Batch too large", "count", len(userOps), "max", 10)
		return nil, &rpc.JsonError{
			Code:    -32007,
			Message: "rateLimited",
			Data:    map[string]any{"reason": "batch too large"},
		}
	}

	// Pull network fee context (for checks only; we no longer mutate user fee fields)
	tipSuggestion, err := api.b.SuggestGasTipCap(ctx)
	if err != nil {
		log.Error("[BuildSignedUserOpsTempDebug] Failed to get suggested tip", "err", err)
		return nil, err
	}
	head := api.b.CurrentHeader()
	baseFee := new(big.Int)
	if head != nil && head.BaseFee != nil {
		baseFee = new(big.Int).Set(head.BaseFee)
	}
	log.Info("[BuildSignedUserOpsTempDebug] Network fee context",
		"tipSuggestion", tipSuggestion.String(),
		"baseFee", baseFee.String(),
		"blockNumber", func() string {
			if head != nil {
				return head.Number.String()
			}
			return "nil"
		}())

	// ABI for EntryPoint
	parsedABI, err := abi.JSON(strings.NewReader(entryPointV07ABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse EntryPoint ABI: %w", err)
	}

	// Build packed ops & basic deposit checks
	packedOps := make([]packedUserOp, 0, len(userOps))
	userOpHashes := make([]string, 0, len(userOps))

	// Pre-encode helper to compute balanceOf & getUserOpHash via eth_call
	callAt := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	// Track min user fee caps across ops to set the outer tx caps without exceeding reimbursement
	minUserTip := (*big.Int)(nil)
	minUserFeeCap := (*big.Int)(nil)

	for i, op := range userOps {
		log.Info("[BuildSignedUserOpsTempDebug] Processing userOp",
			"opIndex", i,
			"sender", op.Sender.Hex(),
			"nonce", toBig(op.Nonce).String(),
			"initCodeLen", len(op.InitCode),
			"callDataLen", len(op.CallData),
			"verificationGasLimit", toBig(op.VerificationGasLimit).String(),
			"callGasLimit", toBig(op.CallGasLimit).String(),
			"preVerificationGas", toBig(op.PreVerificationGas).String(),
			"maxFeePerGas", toBig(op.MaxFeePerGas).String(),
			"maxPriorityFeePerGas", toBig(op.MaxPriorityFeePerGas).String(),
			"paymasterAndDataLen", len(op.PaymasterAndData),
			"hasUnpackedPaymaster", op.Paymaster != nil && *op.Paymaster != (common.Address{}),
			"signatureLen", len(op.Signature))

		var paymasterAddr common.Address
		var paymasterData []byte
		var paymasterVerificationGas, paymasterPostOpGas *big.Int

		// Check if unpacked format is provided
		if op.Paymaster != nil && *op.Paymaster != (common.Address{}) {
			// Use unpacked format
			paymasterAddr = *op.Paymaster
			paymasterVerificationGas = toBig(op.PaymasterVerificationGasLimit)
			paymasterPostOpGas = toBig(op.PaymasterPostOpGasLimit)

			log.Info("[SSV] Using unpacked paymaster format",
				"opIndex", i,
				"paymaster", paymasterAddr.Hex(),
				"verificationGas", paymasterVerificationGas.String(),
				"postOpGas", paymasterPostOpGas.String(),
				"paymasterDataLen", len(op.PaymasterData))

			// Pack into EntryPoint v0.7 format: address(20) + verificationGas(16) + postOpGas(16) + data
			paymasterData = make([]byte, 0, 52+len(op.PaymasterData))
			paymasterData = append(paymasterData, paymasterAddr.Bytes()...)

			// Pack verification gas as 16-byte big-endian
			verGasBytes := make([]byte, 16)
			paymasterVerificationGas.FillBytes(verGasBytes)
			paymasterData = append(paymasterData, verGasBytes...)

			// Pack post-op gas as 16-byte big-endian
			postOpGasBytes := make([]byte, 16)
			paymasterPostOpGas.FillBytes(postOpGasBytes)
			paymasterData = append(paymasterData, postOpGasBytes...)

			// Append paymaster-specific data
			paymasterData = append(paymasterData, op.PaymasterData...)

		} else if len(op.PaymasterAndData) > 0 {
			// Use packed format
			if len(op.PaymasterAndData) < common.AddressLength {
				return nil, &rpc.JsonError{
					Code:    -32003,
					Message: "invalidUserOperation",
					Data:    map[string]any{"opIndex": i, "reason": "paymasterAndData too short"},
				}
			}

			// EntryPoint v0.7 paymaster format: address(20) + verificationGas(16) + postOpGas(16) + data
			const paymasterDataOffset = 52 // 20 + 16 + 16

			if len(op.PaymasterAndData) < paymasterDataOffset {
				return nil, &rpc.JsonError{
					Code:    -32003,
					Message: "invalidUserOperation",
					Data:    map[string]any{"opIndex": i, "reason": "paymasterAndData too short for v0.7 format"},
				}
			}

			paymasterAddr = common.BytesToAddress(op.PaymasterAndData[:20])
			if paymasterAddr == (common.Address{}) {
				return nil, &rpc.JsonError{
					Code:    -32003,
					Message: "invalidUserOperation",
					Data:    map[string]any{"opIndex": i, "reason": "paymaster address cannot be zero"},
				}
			}

			// Extract paymaster gas limits from bytes 20-35 and 36-51
			paymasterVerificationGas = new(big.Int).SetBytes(op.PaymasterAndData[20:36])
			paymasterPostOpGas = new(big.Int).SetBytes(op.PaymasterAndData[36:52])
			paymasterData = op.PaymasterAndData

			log.Info("[SSV] Using packed paymaster format",
				"opIndex", i,
				"paymaster", paymasterAddr.Hex(),
				"verificationGas", paymasterVerificationGas.String(),
				"postOpGas", paymasterPostOpGas.String(),
				"paymasterAndDataLen", len(op.PaymasterAndData))
		} else {
			// No paymaster, set gas limits to zero
			paymasterVerificationGas = big.NewInt(0)
			paymasterPostOpGas = big.NewInt(0)

			log.Info("[SSV] No paymaster specified", "opIndex", i)
		}
		// Merge fee fields with server policy
		vgl := toBig(op.VerificationGasLimit)
		cgl := toBig(op.CallGasLimit)
		pvg := toBig(op.PreVerificationGas)
		if vgl.Sign() < 0 || cgl.Sign() < 0 || pvg.Sign() < 0 {
			return nil, &rpc.JsonError{
				Code:    -32602,
				Message: "invalidUserOperation",
				Data:    map[string]any{"opIndex": i, "reason": "negative gas not allowed"},
			}
		}

		// Pack gas pairs into bytes32 as per v0.7: (verificationGasLimit, callGasLimit)
		agl, ok := packPairToBytes32(vgl, cgl)
		if !ok {
			log.Warn("[BuildSignedUserOpsTempDebug] Gas limits exceed uint128 bounds",
				"opIndex", i,
				"verificationGasLimit", vgl.String(),
				"callGasLimit", cgl.String())
			return nil, &rpc.JsonError{
				Code:    -32005,
				Message: "gasCapExceeded",
				Data:    map[string]any{"opIndex": i, "reason": "gas exceeds uint128 bounds"},
			}
		}
		log.Info("[BuildSignedUserOpsTempDebug] Packed accountGasLimits",
			"opIndex", i,
			"verificationGasLimit", vgl.String(),
			"callGasLimit", cgl.String(),
			"packedHex", hexutil.Encode(agl[:]))

		// Use user-provided fees; do not mutate to preserve signature validity
		uTip := toBig(op.MaxPriorityFeePerGas)
		uFeeCap := toBig(op.MaxFeePerGas)
		gfees, ok := packPairToBytes32(uTip, uFeeCap)
		if !ok {
			log.Warn("[BuildSignedUserOpsTempDebug] Fee values exceed uint128 bounds",
				"opIndex", i,
				"maxPriorityFeePerGas", uTip.String(),
				"maxFeePerGas", uFeeCap.String())
			return nil, &rpc.JsonError{
				Code:    -32005,
				Message: "gasCapExceeded",
				Data:    map[string]any{"opIndex": i, "reason": "fee exceeds uint128 bounds"},
			}
		}
		log.Info("[BuildSignedUserOpsTempDebug] Packed gasFees",
			"opIndex", i,
			"maxPriorityFeePerGas", uTip.String(),
			"maxFeePerGas", uFeeCap.String(),
			"packedHex", hexutil.Encode(gfees[:]))

		p := packedUserOp{
			Sender:             op.Sender,
			Nonce:              toBig(op.Nonce),
			InitCode:           op.InitCode,
			CallData:           op.CallData,
			AccountGasLimits:   agl,
			PreVerificationGas: pvg,
			GasFees:            gfees,
			PaymasterAndData:   paymasterData,
			Signature:          op.Signature,
		}

		log.Info("[SSV] Packed UserOperation",
			"opIndex", i,
			"sender", op.Sender.Hex(),
			"nonce", toBig(op.Nonce).String(),
			"callDataLen", len(op.CallData),
			"callData", hexutil.Encode(op.CallData),
			"initCodeLen", len(op.InitCode))

		// Compute a conservative prefund bound; ensure deposit covers it
		// bound = (callGas + verificationGas + preVerificationGas + paymasterVerificationGas + paymasterPostOpGas) * uFeeCap
		// This matches EntryPoint v0.7's _getRequiredPrefund calculation
		gasSum := new(big.Int).Add(cgl, vgl)
		gasSum.Add(gasSum, pvg)
		gasSum.Add(gasSum, paymasterVerificationGas)
		gasSum.Add(gasSum, paymasterPostOpGas)
		prefundBound := new(big.Int).Mul(gasSum, uFeeCap)

		log.Info("[BuildSignedUserOpsTempDebug] Calculated prefund bound",
			"opIndex", i,
			"callGas", cgl.String(),
			"verificationGas", vgl.String(),
			"preVerificationGas", pvg.String(),
			"paymasterVerificationGas", paymasterVerificationGas.String(),
			"paymasterPostOpGas", paymasterPostOpGas.String(),
			"gasSum", gasSum.String(),
			"maxFeePerGas", uFeeCap.String(),
			"prefundBound", prefundBound.String())

		// balanceOf(sender) or paymaster if sponsored
		balanceTarget := op.Sender
		if paymasterAddr != (common.Address{}) {
			balanceTarget = paymasterAddr
		}
		log.Info("[BuildSignedUserOpsTempDebug] Checking balance",
			"opIndex", i,
			"balanceTarget", balanceTarget.Hex(),
			"isPaymaster", paymasterAddr != (common.Address{}))

		data, err := parsedABI.Pack("balanceOf", balanceTarget)
		if err != nil {
			log.Error("[BuildSignedUserOpsTempDebug] Failed to pack balanceOf call", "opIndex", i, "err", err)
			return nil, fmt.Errorf("abi pack balanceOf: %w", err)
		}
		bal, err := api.callUint256(ctx, ep, data, callAt)
		if err != nil {
			log.Error("[BuildSignedUserOpsTempDebug] balanceOf call failed",
				"opIndex", i,
				"target", balanceTarget.Hex(),
				"entryPoint", ep.Hex(),
				"err", err)
			return nil, fmt.Errorf("balanceOf call failed: %w", err)
		}
		log.Info("[BuildSignedUserOpsTempDebug] Balance check result",
			"opIndex", i,
			"balanceTarget", balanceTarget.Hex(),
			"balance", bal.String(),
			"required", prefundBound.String(),
			"sufficient", bal.Cmp(prefundBound) >= 0)

		if bal.Cmp(prefundBound) < 0 {
			log.Warn("[BuildSignedUserOpsTempDebug] Insufficient deposit",
				"opIndex", i,
				"sponsor", balanceTarget.Hex(),
				"balance", bal.String(),
				"required", prefundBound.String(),
				"shortfall", new(big.Int).Sub(prefundBound, bal).String())
			return nil, &rpc.JsonError{
				Code:    -32004,
				Message: "insufficientDeposit",
				Data: map[string]any{
					"opIndex":  i,
					"required": prefundBound.String(),
					"deposit":  bal.String(),
					"sponsor":  balanceTarget.Hex(),
				},
			}
		}

		// getUserOpHash(op)
		data, err = parsedABI.Pack("getUserOpHash", p)
		if err != nil {
			log.Error("[BuildSignedUserOpsTempDebug] Failed to pack getUserOpHash call", "opIndex", i, "err", err)
			return nil, fmt.Errorf("abi pack getUserOpHash: %w", err)
		}
		hashBytes, err := api.callBytes32(ctx, ep, data, callAt)
		if err != nil {
			log.Error("[BuildSignedUserOpsTempDebug] getUserOpHash call failed",
				"opIndex", i,
				"entryPoint", ep.Hex(),
				"err", err)
			return nil, fmt.Errorf("getUserOpHash call failed: %w", err)
		}
		userOpHash := "0x" + hex.EncodeToString(hashBytes[:])
		log.Info("[BuildSignedUserOpsTempDebug] Computed userOpHash",
			"opIndex", i,
			"userOpHash", userOpHash)

		userOpHashes = append(userOpHashes, userOpHash)
		packedOps = append(packedOps, p)

		// Maintain minimum user fee caps across batch
		prevMinTip := minUserTip
		prevMinFeeCap := minUserFeeCap
		if minUserTip == nil || uTip.Cmp(minUserTip) < 0 {
			minUserTip = new(big.Int).Set(uTip)
		}
		if minUserFeeCap == nil || uFeeCap.Cmp(minUserFeeCap) < 0 {
			minUserFeeCap = new(big.Int).Set(uFeeCap)
		}
		if prevMinTip != minUserTip || prevMinFeeCap != minUserFeeCap {
			log.Info("[BuildSignedUserOpsTempDebug] Updated minimum fees",
				"opIndex", i,
				"minUserTip", minUserTip.String(),
				"minUserFeeCap", minUserFeeCap.String())
		}
	}

	log.Info("[BuildSignedUserOpsTempDebug] Completed processing all userOps",
		"count", len(packedOps),
		"minUserTip", minUserTip.String(),
		"minUserFeeCap", minUserFeeCap.String())

	// Encode handleOps(ops, beneficiary)
	beneficiary := api.b.sequencerAddress // enforce reimbursement to sequencer
	log.Info("[BuildSignedUserOpsTempDebug] Encoding handleOps call",
		"beneficiary", beneficiary.Hex(),
		"opsCount", len(packedOps))

	callData, err := parsedABI.Pack("handleOps", packedOps, beneficiary)
	if err != nil {
		log.Error("[BuildSignedUserOpsTempDebug] Failed to pack handleOps", "err", err)
		return nil, fmt.Errorf("abi pack handleOps: %w", err)
	}
	log.Info("[BuildSignedUserOpsTempDebug] handleOps callData encoded",
		"callDataLen", len(callData),
		"callDataHex", hexutil.Encode(callData))

	// Estimate gas for the call, add 15% safety margin
	from := api.b.sequencerAddress
	args := ethapi.TransactionArgs{
		From: &from,
		To:   &ep,
		Data: (*hexutil.Bytes)(&callData),
		// Fees are irrelevant for estimation
		Value: (*hexutil.Big)(big.NewInt(0)),
	}
	log.Info("[BuildSignedUserOpsTempDebug] Starting gas estimation",
		"from", from.Hex(),
		"to", ep.Hex(),
		"callDataLen", len(callData))

	estGas, err := ethapi.DoEstimateGas(
		ctx,
		api.b,
		args,
		rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber),
		nil,
		nil,
		api.b.RPCGasCap(),
	)
	if err != nil {
		errData := map[string]any{"reason": err.Error()}

		var codeCarrier interface {
			ErrorCode() int
		}
		if errors.As(err, &codeCarrier) {
			errData["errorCode"] = codeCarrier.ErrorCode()
		}

		var dataCarrier interface {
			ErrorData() interface{}
		}
		if errors.As(err, &dataCarrier) {
			switch payload := dataCarrier.ErrorData().(type) {
			case string:
				lower := strings.ToLower(payload)
				if strings.HasPrefix(lower, "0x") {
					enrichRevertData(errData, payload)
				} else if payload != "" {
					errData["detail"] = payload
				}
			case []byte:
				if len(payload) > 0 {
					enrichRevertData(errData, hexutil.Encode(payload))
				}
			case map[string]any:
				for k, v := range payload {
					errData[k] = v
				}
			case nil:
				// Nothing to add.
			default:
				errData["errorData"] = payload
			}
		}

		callDataHex := hexutil.Encode(callData)
		errData["callData"] = callDataHex
		if len(callData) >= 4 {
			selector := callData[:4]
			selectorHex := hexutil.Encode(selector)
			errData["entryPointSelector"] = selectorHex
			if method, methodErr := parsedABI.MethodById(selector); methodErr == nil {
				errData["entryPointFunction"] = method.RawName
			}
		}

		var (
			revertData any
			decoded    any
		)
		if v, ok := errData["revertData"]; ok {
			revertData = v
		}
		if v, ok := errData["decoded"]; ok {
			decoded = v
		}
		if revertData != nil {
			errData["revertData"] = revertData
		}
		if decoded != nil {
			errData["decoded"] = decoded
		} else if reasonText := extractRevertText(err); reasonText != "" {
			errData["revertReason"] = reasonText
		}

		log.Warn("[SSV] handleOps simulation failed",
			"err", err,
			"callData", callDataHex,
			"revertData", revertData,
			"decoded", decoded,
			"details", errData,
		)
		return nil, &rpc.JsonError{
			Code:    -32006,
			Message: "simulateValidationFailed",
			Data:    errData,
		}
	}
	gas := uint64(estGas)
	gas = gas + gas/6 // + ~16.6% safety

	log.Info("[BuildSignedUserOpsTempDebug] Gas estimation completed",
		"estimatedGas", estGas,
		"gasWithSafety", gas,
		"safetyMargin", "16.6%")

	// Decide outer tx fee caps so we don't overpay beyond reimbursement limits.
	if minUserTip == nil || minUserFeeCap == nil {
		return nil, &rpc.JsonError{
			Code:    -32602,
			Message: "invalidUserOperation",
			Data:    map[string]any{"reason": "no userOps"},
		}
	}
	// Quick sanity: ensure current inclusion is feasible
	// Require minUserFeeCap >= baseFee + minUserTip
	effNow := new(big.Int).Add(baseFee, minUserTip)
	log.Info("[BuildSignedUserOpsTempDebug] Validating fee caps",
		"baseFee", baseFee.String(),
		"minUserTip", minUserTip.String(),
		"minUserFeeCap", minUserFeeCap.String(),
		"effectiveRequired", effNow.String(),
		"sufficient", minUserFeeCap.Cmp(effNow) >= 0)

	if minUserFeeCap.Cmp(effNow) < 0 {
		log.Warn("[BuildSignedUserOpsTempDebug] User fee caps below current baseFee",
			"baseFee", baseFee.String(),
			"minUserTip", minUserTip.String(),
			"minUserFeeCap", minUserFeeCap.String(),
			"effectiveRequired", effNow.String(),
			"tipSuggestion", tipSuggestion.String())
		return nil, &rpc.JsonError{
			Code:    -32003,
			Message: "invalidUserOperation",
			Data: map[string]any{
				"reason":        "user fee caps below current baseFee",
				"baseFee":       baseFee.String(),
				"minUserTip":    minUserTip.String(),
				"minUserFeeCap": minUserFeeCap.String(),
				"tipSuggestion": tipSuggestion.String(),
			},
		}
	}

	// Compose and sign a type-2 tx from the sequencer EOA
	nonce, err := api.b.GetPoolNonce(ctx, from)
	if err != nil {
		log.Error("[BuildSignedUserOpsTempDebug] Failed to get nonce",
			"from", from.Hex(),
			"err", err)
		return nil, fmt.Errorf("get nonce: %w", err)
	}

	log.Info("[BuildSignedUserOpsTempDebug] Building transaction",
		"from", from.Hex(),
		"nonce", nonce,
		"gasTipCap", minUserTip.String(),
		"gasFeeCap", minUserFeeCap.String(),
		"gas", gas,
		"to", ep.Hex(),
		"chainID", api.b.ChainConfig().ChainID.String())

	txData := &types.DynamicFeeTx{
		ChainID:   api.b.ChainConfig().ChainID,
		Nonce:     nonce,
		GasTipCap: new(big.Int).Set(minUserTip),
		GasFeeCap: new(big.Int).Set(minUserFeeCap),
		Gas:       gas,
		To:        &ep,
		Value:     big.NewInt(0),
		Data:      callData,
	}
	tx := types.NewTx(txData)
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(api.b.ChainConfig().ChainID), api.b.sequencerKey)
	if err != nil {
		log.Error("[BuildSignedUserOpsTempDebug] Failed to sign transaction",
			"err", err)
		return nil, fmt.Errorf("sign tx: %w", err)
	}

	log.Info("[SSV] Signed user transaction with sequencer key",
		"txHash", signedTx.Hash().Hex(),
		"chainID", api.b.ChainConfig().ChainID,
		"sequencerAddr", api.b.sequencerAddress.Hex(),
		"nonce", nonce,
		"userOpsCount", len(userOps),
		"gas", gas,
		"to", ep.Hex(),
		"callDataLen", len(callData),
		"callData", hexutil.Encode(callData))

	raw, err := signedTx.MarshalBinary()
	if err != nil {
		log.Error("[BuildSignedUserOpsTempDebug] Failed to marshal transaction",
			"err", err)
		return nil, fmt.Errorf("marshal tx: %w", err)
	}

	// Build response
	resp := &SignedTxResp{
		Raw:                  "0x" + hex.EncodeToString(raw),
		Hash:                 signedTx.Hash().Hex(),
		To:                   ep.Hex(),
		ChainID:              chainID,
		Gas:                  hexutil.Uint64(gas).String(),
		MaxFeePerGas:         (*hexutil.Big)(minUserFeeCap).String(),
		MaxPriorityFeePerGas: (*hexutil.Big)(minUserTip).String(),
		UserOpHashes:         userOpHashes,
	}

	log.Info("[BuildSignedUserOpsTempDebug] BuildSignedUserOpsTx completed successfully",
		"txHash", resp.Hash,
		"rawLen", len(raw),
		"userOpHashesCount", len(userOpHashes),
		"gas", resp.Gas,
		"maxFeePerGas", resp.MaxFeePerGas,
		"maxPriorityFeePerGas", resp.MaxPriorityFeePerGas)

	return resp, nil
}

// Backward-compatible aliases: keep older names mapping to the main method.
func (api *composeUserOpsAPI) ComposeBuildSignedUserOpsTx(
	ctx context.Context,
	userOps []userOperationV07,
	opts composeOpts,
) (*SignedTxResp, error) {
	return api.BuildSignedUserOpsTx(ctx, userOps, opts)
}

func (api *composeUserOpsAPI) Compose_buildSignedUserOpsTx(
	ctx context.Context,
	userOps []userOperationV07,
	opts composeOpts,
) (*SignedTxResp, error) {
	return api.BuildSignedUserOpsTx(ctx, userOps, opts)
}

// Helper: pack two uint128 values into a bytes32: (hi, lo)
func packPairToBytes32(hi, lo *big.Int) ([32]byte, bool) {
	var out [32]byte
	// Ensure both fit in 128 bits
	max128 := new(big.Int).Lsh(big.NewInt(1), 128)
	if hi.Sign() < 0 || lo.Sign() < 0 || hi.Cmp(max128) >= 0 || lo.Cmp(max128) >= 0 {
		return out, false
	}
	val := new(big.Int).Lsh(hi, 128)
	val.Add(val, lo)
	b := val.FillBytes(make([]byte, 32))
	copy(out[:], b)
	return out, true
}

func toBig(x *hexutil.Big) *big.Int {
	if x == nil {
		return new(big.Int)
	}
	return (*big.Int)(x)
}

// callUint256 calls a view method and returns the single uint256 output.
func (api *composeUserOpsAPI) callUint256(
	ctx context.Context,
	to common.Address,
	data []byte,
	at rpc.BlockNumberOrHash,
) (*big.Int, error) {
	from := api.b.sequencerAddress
	args := ethapi.TransactionArgs{From: &from, To: &to, Data: (*hexutil.Bytes)(&data)}
	res, err := ethapi.DoCall(ctx, api.b, args, at, nil, nil, api.b.RPCEVMTimeout(), api.b.RPCGasCap())
	if err != nil {
		return nil, err
	}
	// Return data should be 32 bytes
	if len(res.ReturnData) != 32 {
		return nil, errors.New("unexpected return length")
	}
	return new(big.Int).SetBytes(res.ReturnData), nil
}

// callBytes32 calls a view method and returns the single bytes32 output.
func (api *composeUserOpsAPI) callBytes32(
	ctx context.Context,
	to common.Address,
	data []byte,
	at rpc.BlockNumberOrHash,
) ([32]byte, error) {
	from := api.b.sequencerAddress
	args := ethapi.TransactionArgs{From: &from, To: &to, Data: (*hexutil.Bytes)(&data)}
	res, err := ethapi.DoCall(ctx, api.b, args, at, nil, nil, api.b.RPCEVMTimeout(), api.b.RPCGasCap())
	if err != nil {
		return [32]byte{}, err
	}
	if len(res.ReturnData) != 32 {
		return [32]byte{}, errors.New("unexpected return length")
	}
	var out [32]byte
	copy(out[:], res.ReturnData)
	return out, nil
}

// GetComposeUserOpsAPI returns the RPC API descriptor for registration.
func GetComposeUserOpsAPI(b *EthAPIBackend) rpc.API {
	return rpc.API{
		Namespace: "compose",
		Service:   &composeUserOpsAPI{b: b},
	}
}

// enrichRevertData records the raw revert payload and best-effort decoded information.
func enrichRevertData(errData map[string]any, revertDataHex string) {
	if revertDataHex == "" {
		return
	}
	errData["revertData"] = revertDataHex
	if revertBytes, decodeErr := hexutil.Decode(revertDataHex); decodeErr == nil && len(revertBytes) > 0 {
		if decoded := decodeRevertReason(revertBytes); decoded != nil {
			errData["decoded"] = decoded
		}
	}
}

func normalizeABIValue(value interface{}) any {
	switch v := value.(type) {
	case nil:
		return nil
	case *big.Int:
		if v == nil {
			return nil
		}
		return v.String()
	case big.Int:
		return v.String()
	case common.Address:
		return v.Hex()
	case *common.Address:
		if v == nil {
			return nil
		}
		return v.Hex()
	case common.Hash:
		return v.Hex()
	case *common.Hash:
		if v == nil {
			return nil
		}
		return v.Hex()
	case []byte:
		if len(v) == 0 {
			return "0x"
		}
		return "0x" + hex.EncodeToString(v)
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil
	}

	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nil
		}
		return normalizeABIValue(rv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			buf := make([]byte, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				buf[i] = byte(rv.Index(i).Uint())
			}
			if len(buf) == 0 {
				return "0x"
			}
			return "0x" + hex.EncodeToString(buf)
		}
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, normalizeABIValue(rv.Index(i).Interface()))
		}
		return out
	case reflect.Map:
		out := map[string]any{}
		iter := rv.MapRange()
		for iter.Next() {
			out[fmt.Sprint(iter.Key().Interface())] = normalizeABIValue(iter.Value().Interface())
		}
		return out
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return fmt.Sprintf("%d", rv.Uint())
	case reflect.Bool:
		return rv.Bool()
	case reflect.String:
		return rv.String()
	default:
		return fmt.Sprintf("%v", value)
	}
}

func extractRevertText(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if msg == "" {
		return ""
	}
	const prefix = "execution reverted"
	idx := strings.Index(msg, prefix)
	if idx == -1 {
		return ""
	}
	after := strings.TrimSpace(msg[idx+len(prefix):])
	after = strings.TrimPrefix(after, ":")
	after = strings.TrimSpace(after)
	return after
}

// decodeEntryPointError attempts to decode EntryPoint custom errors using the ABI.
// Returns a map with decoded fields if successful, nil otherwise.
// Recursively decodes nested revert data (e.g., inner reverts in FailedOpWithRevert).
func decodeEntryPointError(revertDataHex string) map[string]any {
	revertData, err := hexutil.Decode(revertDataHex)
	if err != nil || len(revertData) < 4 {
		return nil
	}

	parsedABI, err := abi.JSON(strings.NewReader(entryPointV07ABI))
	if err != nil {
		return nil
	}

	selector := revertData[:4]
	data := revertData[4:]

	for name, customErr := range parsedABI.Errors {
		if !bytes.Equal(customErr.ID[:4], selector) {
			continue
		}

		values, err := customErr.Inputs.Unpack(data)
		if err != nil {
			return nil
		}

		result := map[string]any{"error": name}
		for i, arg := range customErr.Inputs {
			if i >= len(values) {
				break
			}
			fieldName := arg.Name
			if fieldName == "" {
				fieldName = fmt.Sprintf("arg%d", i)
			}
			result[fieldName] = normalizeABIValue(values[i])
		}

		switch name {
		case "FailedOpWithRevert":
			if len(values) >= 3 {
				if innerBytes, ok := values[2].([]byte); ok && len(innerBytes) > 0 {
					innerHex := "0x" + hex.EncodeToString(innerBytes)
					result["inner"] = innerHex
					if decoded := decodeRevertReason(innerBytes); decoded != nil {
						result["innerDecoded"] = decoded
					}
				}
			}
		case "PostOpReverted":
			if len(values) >= 1 {
				if returnData, ok := values[0].([]byte); ok && len(returnData) > 0 {
					result["returnData"] = "0x" + hex.EncodeToString(returnData)
					if decoded := decodeRevertReason(returnData); decoded != nil {
						result["decoded"] = decoded
					}
				}
			}
		case "ExecutionResult":
			if len(values) >= 6 {
				if targetResult, ok := values[5].([]byte); ok && len(targetResult) > 0 {
					result["targetResult"] = "0x" + hex.EncodeToString(targetResult)
					if decoded := decodeRevertReason(targetResult); decoded != nil {
						result["targetDecoded"] = decoded
					}
				}
			}
		case "DelegateAndRevert":
			if len(values) >= 2 {
				if ret, ok := values[1].([]byte); ok && len(ret) > 0 {
					result["ret"] = "0x" + hex.EncodeToString(ret)
					if decoded := decodeRevertReason(ret); decoded != nil {
						result["retDecoded"] = decoded
					}
				}
			}
		}

		return result
	}

	return nil
}

// decodeRevertReason attempts to decode standard Solidity revert reasons.
// Handles Error(string) and Panic(uint256), and recursively tries EntryPoint errors.
func decodeRevertReason(revertData []byte) map[string]any {
	if len(revertData) < 4 {
		return nil
	}

	selector := revertData[:4]
	data := revertData[4:]

	// Standard Error(string) selector: 0x08c379a0
	errorSelector := []byte{0x08, 0xc3, 0x79, 0xa0}
	// Standard Panic(uint256) selector: 0x4e487b71
	panicSelector := []byte{0x4e, 0x48, 0x7b, 0x71}

	if len(selector) == 4 && selector[0] == errorSelector[0] && selector[1] == errorSelector[1] &&
		selector[2] == errorSelector[2] && selector[3] == errorSelector[3] {
		// Decode Error(string)
		stringType, _ := abi.NewType("string", "", nil)
		args := abi.Arguments{{Type: stringType}}
		decoded, err := args.Unpack(data)
		if err == nil && len(decoded) > 0 {
			return map[string]any{
				"type":    "Error",
				"message": fmt.Sprintf("%v", decoded[0]),
			}
		}
	}

	if len(selector) == 4 && selector[0] == panicSelector[0] && selector[1] == panicSelector[1] &&
		selector[2] == panicSelector[2] && selector[3] == panicSelector[3] {
		// Decode Panic(uint256)
		uint256Type, _ := abi.NewType("uint256", "", nil)
		args := abi.Arguments{{Type: uint256Type}}
		decoded, err := args.Unpack(data)
		if err == nil && len(decoded) > 0 {
			if code, ok := decoded[0].(*big.Int); ok {
				return map[string]any{
					"type": "Panic",
					"code": code.String(),
				}
			}
		}
	}

	// Try to decode as EntryPoint error recursively
	revertHex := "0x" + hex.EncodeToString(revertData)
	if entryPointErr := decodeEntryPointError(revertHex); entryPointErr != nil {
		return entryPointErr
	}

	return nil
}
