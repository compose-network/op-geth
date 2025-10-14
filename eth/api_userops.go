package eth

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
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
  {"type":"error","name":"PostOpReverted","inputs":[{"internalType":"bytes","name":"returnData","type":"bytes"}]}
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
	// Canonicalize & quick policy checks
	chainID := api.b.ChainConfig().ChainID.Uint64()
	if opts.ChainID == 0 || opts.ChainID != chainID {
		return nil, &rpc.JsonError{Code: -32001, Message: "wrongChainId", Data: map[string]any{"expected": chainID}}
	}

	// Always use the canonical v0.7 EntryPoint address.
	ep := common.HexToAddress("0x0000000071727de22e5e9d8baf0edac6f37da032")

	if len(userOps) == 0 {
		return nil, &rpc.JsonError{
			Code:    -32602,
			Message: "invalidUserOperation",
			Data:    map[string]any{"reason": "empty userOps"},
		}
	}
	if len(userOps) > 10 {
		return nil, &rpc.JsonError{
			Code:    -32007,
			Message: "rateLimited",
			Data:    map[string]any{"reason": "batch too large"},
		}
	}

	// Pull network fee context (for checks only; we no longer mutate user fee fields)
	tipSuggestion, err := api.b.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, err
	}
	head := api.b.CurrentHeader()
	baseFee := new(big.Int)
	if head != nil && head.BaseFee != nil {
		baseFee = new(big.Int).Set(head.BaseFee)
	}

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
			return nil, &rpc.JsonError{
				Code:    -32005,
				Message: "gasCapExceeded",
				Data:    map[string]any{"opIndex": i, "reason": "gas exceeds uint128 bounds"},
			}
		}

		// Use user-provided fees; do not mutate to preserve signature validity
		uTip := toBig(op.MaxPriorityFeePerGas)
		uFeeCap := toBig(op.MaxFeePerGas)
		gfees, ok := packPairToBytes32(uTip, uFeeCap)
		if !ok {
			return nil, &rpc.JsonError{
				Code:    -32005,
				Message: "gasCapExceeded",
				Data:    map[string]any{"opIndex": i, "reason": "fee exceeds uint128 bounds"},
			}
		}

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

		// balanceOf(sender) or paymaster if sponsored
		balanceTarget := op.Sender
		if paymasterAddr != (common.Address{}) {
			balanceTarget = paymasterAddr
		}
		data, err := parsedABI.Pack("balanceOf", balanceTarget)
		if err != nil {
			return nil, fmt.Errorf("abi pack balanceOf: %w", err)
		}
		bal, err := api.callUint256(ctx, ep, data, callAt)
		if err != nil {
			return nil, fmt.Errorf("balanceOf call failed: %w", err)
		}
		if bal.Cmp(prefundBound) < 0 {
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
			return nil, fmt.Errorf("abi pack getUserOpHash: %w", err)
		}
		hashBytes, err := api.callBytes32(ctx, ep, data, callAt)
		if err != nil {
			return nil, fmt.Errorf("getUserOpHash call failed: %w", err)
		}
		userOpHashes = append(userOpHashes, "0x"+hex.EncodeToString(hashBytes[:]))
		packedOps = append(packedOps, p)

		// Maintain minimum user fee caps across batch
		if minUserTip == nil || uTip.Cmp(minUserTip) < 0 {
			minUserTip = new(big.Int).Set(uTip)
		}
		if minUserFeeCap == nil || uFeeCap.Cmp(minUserFeeCap) < 0 {
			minUserFeeCap = new(big.Int).Set(uFeeCap)
		}
	}

	// Encode handleOps(ops, beneficiary)
	beneficiary := api.b.sequencerAddress // enforce reimbursement to sequencer
	callData, err := parsedABI.Pack("handleOps", packedOps, beneficiary)
	if err != nil {
		return nil, fmt.Errorf("abi pack handleOps: %w", err)
	}

	// Estimate gas for the call, add 15% safety margin
	from := api.b.sequencerAddress
	args := ethapi.TransactionArgs{
		From: &from,
		To:   &ep,
		Data: (*hexutil.Bytes)(&callData),
		// Fees are irrelevant for estimation
		Value: (*hexutil.Big)(big.NewInt(0)),
	}
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
		// If estimation reverted, check if we have a revertError with detailed data
		type errorCoder interface {
			ErrorCode() int
			ErrorData() interface{}
		}
		errData := map[string]any{"reason": err.Error()}

		if ec, ok := err.(errorCoder); ok {
			revertDataHex, ok := ec.ErrorData().(string)
			if ok && len(revertDataHex) > 2 {
				errData["revertData"] = revertDataHex
				if decoded := decodeEntryPointError(revertDataHex); decoded != nil {
					errData["decoded"] = decoded
				}
			}
		}
		log.Warn("[SSV] handleOps simulation failed", "err", err, "callData", hexutil.Encode(callData))
		return nil, &rpc.JsonError{
			Code:    -32006,
			Message: "simulateValidationFailed",
			Data:    errData,
		}
	}
	gas := uint64(estGas)
	gas = gas + gas/6 // + ~16.6% safety

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
	if minUserFeeCap.Cmp(effNow) < 0 {
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
		return nil, fmt.Errorf("get nonce: %w", err)
	}

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
		if hex.EncodeToString(customErr.ID[:4]) == hex.EncodeToString(selector) {
			values, err := customErr.Inputs.Unpack(data)
			if err != nil {
				return nil
			}

			result := map[string]any{"error": name}

			switch name {
			case "FailedOp":
				if len(values) >= 2 {
					result["opIndex"] = fmt.Sprintf("%v", values[0])
					result["reason"] = fmt.Sprintf("%v", values[1])
				}

			case "FailedOpWithRevert":
				if len(values) >= 3 {
					result["opIndex"] = fmt.Sprintf("%v", values[0])
					result["reason"] = fmt.Sprintf("%v", values[1])
					// Recursively decode inner revert data
					if innerBytes, ok := values[2].([]byte); ok && len(innerBytes) > 0 {
						innerHex := "0x" + hex.EncodeToString(innerBytes)
						result["innerRevert"] = innerHex
						// Try to decode as standard Error(string) or nested EntryPoint error
						if decoded := decodeRevertReason(innerBytes); decoded != nil {
							result["innerDecoded"] = decoded
						}
					}
				}

			case "SignatureValidationFailed":
				if len(values) >= 1 {
					if addr, ok := values[0].(common.Address); ok {
						result["aggregator"] = addr.Hex()
					}
				}

			case "PostOpReverted":
				if len(values) >= 1 {
					if returnData, ok := values[0].([]byte); ok {
						result["returnData"] = "0x" + hex.EncodeToString(returnData)
						if decoded := decodeRevertReason(returnData); decoded != nil {
							result["decoded"] = decoded
						}
					}
				}
			}

			return result
		}
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
