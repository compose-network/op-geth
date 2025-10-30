package native

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/core/ssv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

const RollupAChainID = 77777
const RollupBChainID = 88888

var (
	chainIDToMailboxMu sync.RWMutex
	ChainIDToMailbox   = map[uint64]string{}
)

// ReplaceChainIDToMailbox swaps the exported ChainIDToMailbox map with the
// provided mapping, skipping any zero-value addresses so callers don't read
// placeholders.
func ReplaceChainIDToMailbox(mapping map[uint64]common.Address) {
	chainIDToMailboxMu.Lock()
	defer chainIDToMailboxMu.Unlock()

	updated := make(map[uint64]string, len(mapping))
	for chainID, addr := range mapping {
		if (addr == common.Address{}) {
			continue
		}
		updated[chainID] = addr.Hex()
	}

	ChainIDToMailbox = updated
}

// MailboxAddressForChain returns the configured mailbox address for the
// supplied chain ID, if present.
func MailboxAddressForChain(chainID uint64) (common.Address, bool) {
	chainIDToMailboxMu.RLock()
	defer chainIDToMailboxMu.RUnlock()

	addrHex, ok := ChainIDToMailbox[chainID]
	if !ok || !common.IsHexAddress(addrHex) {
		return common.Address{}, false
	}

	return common.HexToAddress(addrHex), true
}

func init() {
	tracers.DefaultDirectory.Register("ssvTracer", newSSVTracer, false)
}

// SSVTracer is a minimal tracer for SSV sequencer needs
type SSVTracer struct {
	env              *tracing.VMContext // The VM environment
	operations       []ssv.SSVOperation // Flat list of operations
	interrupt        atomic.Bool        // Atomic flag to signal execution interruption
	reason           error              // Textual reason for the interruption
	watchedAddresses map[common.Address]bool
	currentDepth     int
	currentFrom      common.Address
	currentTo        common.Address
}

// newSSVTracer is the registered constructor.
func newSSVTracer(ctx *tracers.Context, cfg json.RawMessage, chainConfig *params.ChainConfig) (*tracers.Tracer, error) {

	t := &SSVTracer{
		operations:       make([]ssv.SSVOperation, 0),
		watchedAddresses: make(map[common.Address]bool),
	}

	return &tracers.Tracer{
			Hooks: &tracing.Hooks{
				OnEnter:   t.OnEnter,
				OnExit:    t.OnExit,
				OnTxStart: t.OnTxStart,
				OnTxEnd:   t.OnTxEnd,
			},
			GetResult: t.GetResult,
			Stop:      t.Stop,
		},
		nil
}

func (t *SSVTracer) OnTxStart(env *tracing.VMContext, tx *types.Transaction, from common.Address) {
	log.Info("[SSV] OnTxStart called", "txHash", tx.Hash().Hex(), "from", from.Hex())

	t.env = env
}

func (t *SSVTracer) OnTxEnd(_ *types.Receipt, err error) {
	log.Info("[SSV] OnTxEnd called")

	if err != nil {
		return
	}
}

func decodeTransactionInput(contractABIJSON string, input []byte) {
	if len(input) == 0 {
		fmt.Println("Input is empty. Likely a simple Ether transfer.")
		return
	}

	parsedABI, err := abi.JSON(strings.NewReader(contractABIJSON))
	if err != nil {
		fmt.Println("Err", err)
	}

	if len(input) < 4 {
		fmt.Println("Input is too short to contain a function selector.")
		return
	}
	methodID := input[:4]
	packedArgs := input[4:]

	method, err := parsedABI.MethodById(methodID)
	if err != nil {
		fmt.Println("Err", err)
		return
	}

	fmt.Printf("Method Name: %s\n", method.Name)

	args, err := method.Inputs.Unpack(packedArgs)
	if err != nil {
		fmt.Println("Err", err)
		return
	}

	fmt.Println("Decoded Arguments:")
	for i, arg := range args {
		fmt.Printf("  - Arg %d (%s): %v\n", i, method.Inputs[i].Name, arg)

		switch v := arg.(type) {
		case *big.Int:
			fmt.Printf("    (Value as big.Int: %s)\n", v.String())
		case common.Address:
			fmt.Printf("    (Value as Address: %s)\n", v.Hex())
		}
	}
}

func (t *SSVTracer) OnEnter(depth int, typ byte, from common.Address, to common.Address, input []byte, gas uint64, value *big.Int) {
	log.Info("[SSV] OnEnter called", "depth", depth, "type", vm.OpCode(typ).String(), "from", from.Hex(), "to", to.Hex(), "gas", gas, "value", value)

	if t.interrupt.Load() {
		return
	}

	t.currentDepth = depth
	t.currentFrom = from
	t.currentTo = to

	// Track calls to watched addresses, including internal calls
	if t.watchedAddresses[to] {
		//decodeTransactionInput(mailboxABI, input)

		op := ssv.SSVOperation{
			Type:     vm.OpCode(typ),
			Address:  to,
			From:     from,
			CallData: common.CopyBytes(input),
			Gas:      gas,
		}

		log.Info("[SSV] Operation recorded")
		t.operations = append(t.operations, op)
	}
}

func (t *SSVTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if t.interrupt.Load() {
		return
	}

	fields := []interface{}{
		"depth", depth,
		"gasUsed", gasUsed,
		"reverted", reverted,
	}

	if len(output) > 0 {
		fields = append(fields, "outputLen", len(output))
		if len(output) <= 64 {
			fields = append(fields, "output", common.Bytes2Hex(output))
		} else {
			fields = append(fields, "outputPreview", common.Bytes2Hex(output[:32])+"...")
		}
	}

	if err != nil {
		fields = append(fields, "err", err.Error())
	}

	log.Info("[SSV] OnExit", fields...)

	if depth == 0 {
		log.Info("[SSV] Root call completed",
			"from", t.currentFrom.Hex(),
			"to", t.currentTo.Hex(),
			"gasUsed", gasUsed,
			"success", err == nil && !reverted,
			"operationCount", len(t.operations),
		)
	}

	if depth > 0 {
		t.currentDepth = depth - 1
	}
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *SSVTracer) Stop(err error) {
	t.reason = err
	t.interrupt.Store(true)
}

// GetResult returns the json-encoded flat list of operations
func (t *SSVTracer) GetResult() (json.RawMessage, error) {
	result := ssv.SSVTraceResult{
		Operations: t.operations,
	}

	res, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return res, t.reason
}

/////////// Public API for creating a new SSVTracer instance ///////////

func NewSSVTracer(mailboxAddresses []common.Address) *SSVTracer {
	watchedAddresses := make(map[common.Address]bool)
	for _, addr := range mailboxAddresses {
		watchedAddresses[addr] = true
	}

	log.Info("[SSV] Creating new SSVTracer", "mailboxes", watchedAddresses)

	return &SSVTracer{
		operations:       make([]ssv.SSVOperation, 0),
		watchedAddresses: watchedAddresses,
	}
}

func (t *SSVTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter:   t.OnEnter,
		OnTxStart: t.OnTxStart,
		OnTxEnd:   t.OnTxEnd,
		OnExit:    t.OnExit,
	}
}

// GetTraceResult returns the SSVTraceResult containing the operations
func (t *SSVTracer) GetTraceResult() *ssv.SSVTraceResult {
	return &ssv.SSVTraceResult{
		Operations:      t.operations,
		ExecutionResult: nil, // Execution result is not used in this tracer
	}
}
