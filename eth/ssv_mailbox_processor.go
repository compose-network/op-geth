package eth

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/ssv"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/eth/tracers/native"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	rollupv1 "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	spconsensus "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/consensus"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/superblock/sequencer"
	"github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/x/transport"
	"github.com/ethereum/go-ethereum/log"

	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

const mailboxABI = `[{"type":"constructor","inputs":[{"name":"_coordinator","type":"address","internalType":"address"}],"stateMutability":"nonpayable"},{"type":"function","name":"COORDINATOR","inputs":[],"outputs":[{"name":"","type":"address","internalType":"address"}],"stateMutability":"view"},{"type":"function","name":"chainIDsInbox","inputs":[{"name":"","type":"uint256","internalType":"uint256"}],"outputs":[{"name":"","type":"uint256","internalType":"uint256"}],"stateMutability":"view"},{"type":"function","name":"chainIDsOutbox","inputs":[{"name":"","type":"uint256","internalType":"uint256"}],"outputs":[{"name":"","type":"uint256","internalType":"uint256"}],"stateMutability":"view"},{"type":"function","name":"computeKey","inputs":[{"name":"id","type":"uint256","internalType":"uint256"}],"outputs":[{"name":"","type":"bytes32","internalType":"bytes32"}],"stateMutability":"view"},{"type":"function","name":"createdKeys","inputs":[{"name":"key","type":"bytes32","internalType":"bytes32"}],"outputs":[{"name":"used","type":"bool","internalType":"bool"}],"stateMutability":"view"},{"type":"function","name":"getKey","inputs":[{"name":"chainMessageSender","type":"uint256","internalType":"uint256"},{"name":"chainMessageRecipient","type":"uint256","internalType":"uint256"},{"name":"sender","type":"address","internalType":"address"},{"name":"receiver","type":"address","internalType":"address"},{"name":"sessionId","type":"uint256","internalType":"uint256"},{"name":"label","type":"bytes","internalType":"bytes"}],"outputs":[{"name":"key","type":"bytes32","internalType":"bytes32"}],"stateMutability":"pure"},{"type":"function","name":"inbox","inputs":[{"name":"key","type":"bytes32","internalType":"bytes32"}],"outputs":[{"name":"message","type":"bytes","internalType":"bytes"}],"stateMutability":"view"},{"type":"function","name":"inboxRootPerChain","inputs":[{"name":"chainId","type":"uint256","internalType":"uint256"}],"outputs":[{"name":"inboxRoot","type":"bytes32","internalType":"bytes32"}],"stateMutability":"view"},{"type":"function","name":"messageHeaderListInbox","inputs":[{"name":"","type":"uint256","internalType":"uint256"}],"outputs":[{"name":"chainSrc","type":"uint256","internalType":"uint256"},{"name":"chainDest","type":"uint256","internalType":"uint256"},{"name":"sender","type":"address","internalType":"address"},{"name":"receiver","type":"address","internalType":"address"},{"name":"sessionId","type":"uint256","internalType":"uint256"},{"name":"label","type":"bytes","internalType":"bytes"}],"stateMutability":"view"},{"type":"function","name":"messageHeaderListOutbox","inputs":[{"name":"","type":"uint256","internalType":"uint256"}],"outputs":[{"name":"chainSrc","type":"uint256","internalType":"uint256"},{"name":"chainDest","type":"uint256","internalType":"uint256"},{"name":"sender","type":"address","internalType":"address"},{"name":"receiver","type":"address","internalType":"address"},{"name":"sessionId","type":"uint256","internalType":"uint256"},{"name":"label","type":"bytes","internalType":"bytes"}],"stateMutability":"view"},{"type":"function","name":"outbox","inputs":[{"name":"key","type":"bytes32","internalType":"bytes32"}],"outputs":[{"name":"message","type":"bytes","internalType":"bytes"}],"stateMutability":"view"},{"type":"function","name":"outboxRootPerChain","inputs":[{"name":"chainId","type":"uint256","internalType":"uint256"}],"outputs":[{"name":"outboxRoot","type":"bytes32","internalType":"bytes32"}],"stateMutability":"view"},{"type":"function","name":"putInbox","inputs":[{"name":"chainMessageSender","type":"uint256","internalType":"uint256"},{"name":"sender","type":"address","internalType":"address"},{"name":"receiver","type":"address","internalType":"address"},{"name":"sessionId","type":"uint256","internalType":"uint256"},{"name":"label","type":"bytes","internalType":"bytes"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"read","inputs":[{"name":"chainMessageSender","type":"uint256","internalType":"uint256"},{"name":"sender","type":"address","internalType":"address"},{"name":"sessionId","type":"uint256","internalType":"uint256"},{"name":"label","type":"bytes","internalType":"bytes"}],"outputs":[{"name":"message","type":"bytes","internalType":"bytes"}],"stateMutability":"view"},{"type":"function","name":"write","inputs":[{"name":"chainMessageRecipient","type":"uint256","internalType":"uint256"},{"name":"receiver","type":"address","internalType":"address"},{"name":"sessionId","type":"uint256","internalType":"uint256"},{"name":"label","type":"bytes","internalType":"bytes"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"event","name":"NewInboxKey","inputs":[{"name":"index","type":"uint256","indexed":true,"internalType":"uint256"},{"name":"key","type":"bytes32","indexed":false,"internalType":"bytes32"}],"anonymous":false},{"type":"event","name":"NewOutboxKey","inputs":[{"name":"index","type":"uint256","indexed":true,"internalType":"uint256"},{"name":"key","type":"bytes32","indexed":false,"internalType":"bytes32"}],"anonymous":false},{"type":"error","name":"InvalidCoordinator","inputs":[]},{"type":"error","name":"MessageNotFound","inputs":[]}]`

var messageNotFoundSelector = []byte{0x28, 0x91, 0x5a, 0xc7}

type MailboxCall struct {
	// For read() calls
	ChainMessageSender *big.Int       // chainMessageSender parameter
	Sender             common.Address // sender parameter (on source chain)

	// For write() calls
	ChainMessageRecipient *big.Int       // chainMessageRecipient parameter
	Receiver              common.Address // receiver parameter (on dest chain)

	// Common fields
	SessionId *big.Int
	Label     []byte
	Data      []byte

	// Call type flags
	IsRead  bool
	IsWrite bool

	// Derived fields for processing
	ChainSrc  *big.Int // Computed: block.chainid for write, chainMessageSender for read
	ChainDest *big.Int // Computed: chainMessageRecipient for write, block.chainid for read
}

type CrossRollupDependency struct {
	SourceChainID uint64
	DestChainID   uint64
	Sender        common.Address
	Receiver      common.Address
	SessionID     *big.Int
	Label         []byte
	RequiredData  bool
	IsInboxRead   bool
	Data          []byte // Fulfilled by CIRCMessage
}

type CrossRollupMessage struct {
	SourceChainID uint64
	DestChainID   uint64
	Sender        common.Address
	Receiver      common.Address
	SessionID     *big.Int
	Data          []byte
	Label         []byte
	MessageType   string
	IsOutboxWrite bool
}

type SimulationState struct {
	// TODO: consider naming Revert and invert bool checks
	Success          bool // success when not revert()'ed
	Dependencies     []CrossRollupDependency
	OutboundMessages []CrossRollupMessage
	Tx               *types.Transaction
}

func (s SimulationState) RequiresCoordination() bool {
	return len(s.Dependencies) > 0 || len(s.OutboundMessages) > 0
}

type MailboxProcessor struct {
	chainID              uint64
	mailboxAddresses     []common.Address
	sequencerClients     map[string]transport.Client
	sequencerCoordinator sequencer.Coordinator
	backend              interface{}
	coordinatorKey       *ecdsa.PrivateKey
	coordinatorAddr      common.Address
}

func NewMailboxProcessor(
	chainID uint64,
	mailboxAddrs []common.Address,
	sequencerClients map[string]transport.Client,
	coordinator sequencer.Coordinator,
	coordinatorKey *ecdsa.PrivateKey,
	coordinatorAddr common.Address,
	backend *EthAPIBackend,
) *MailboxProcessor {
	return &MailboxProcessor{
		chainID:              chainID,
		mailboxAddresses:     mailboxAddrs,
		sequencerClients:     sequencerClients,
		sequencerCoordinator: coordinator,
		backend:              backend,
		coordinatorKey:       coordinatorKey,
		coordinatorAddr:      coordinatorAddr,
	}
}

func (mp *MailboxProcessor) AnalyzeTransaction(
	traceResult *ssv.SSVTraceResult,
	sentOutboundMsgs []CrossRollupMessage,
	fullFilledDeps []CrossRollupDependency,
	tx *types.Transaction,
) (*SimulationState, error) {
	txHashHex := tx.Hash().Hex()
	simState, err := mp.analyzeTransaction(traceResult, sentOutboundMsgs, fullFilledDeps, txHashHex)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze transaction: %w", err)
	}

	simState.Tx = tx

	if !simState.RequiresCoordination() {
		log.Info("[SSV] Transaction requires no cross-rollup coordination", "txHash", txHashHex)
		return simState, nil
	}

	log.Info("[SSV] Transaction requires cross-rollup coordination",
		"txHash", txHashHex,
		"dependencies", len(simState.Dependencies),
		"outbound", len(simState.OutboundMessages))

	return simState, nil
}

func parseCallType(call *MailboxCall) string {
	if call.IsRead {
		return "read"
	}
	if call.IsWrite {
		return "write"
	}

	return "unknown"
}

func (mp *MailboxProcessor) analyzeTransaction(
	traceResult *ssv.SSVTraceResult,
	sentOutboundMsgs []CrossRollupMessage,
	fullfilledDeps []CrossRollupDependency,
	txHashHex string,
) (*SimulationState, error) {
	simState := &SimulationState{
		Success:          traceResult.ExecutionResult.Err == nil,
		Dependencies:     make([]CrossRollupDependency, 0),
		OutboundMessages: make([]CrossRollupMessage, 0),
	}

	log.Info("[SSV] Analyzing transaction trace",
		"txHash", txHashHex,
		"success", simState.Success,
		"operations", len(traceResult.Operations))

	if traceResult.ExecutionResult.Err != nil {
		log.Warn("[SSV] Cross-chain transaction reverted during simulation",
			"txHash", txHashHex,
			"error", traceResult.ExecutionResult.Err,
			"revert", traceResult.ExecutionResult.Revert(),
			"continuing_analysis", true)
	}

	for i, op := range traceResult.Operations {
		if !mp.isMailboxAddress(op.Address) {
			continue
		}

		log.Info("[SSV] Found mailbox operation",
			"index", i,
			"type", op.Type.String(),
			"address", op.Address.Hex(),
			"from", op.From.Hex(),
			"callDataLen", len(op.CallData))

		// Handle both CALL (write) and STATICCALL (read) operations
		if (op.Type == vm.CALL || op.Type == vm.STATICCALL) && len(op.CallData) >= 4 {
			call, err := mp.parseMailboxCall(op.CallData)
			if err != nil {
				log.Info("[SSV] Failed to parse mailbox call", "error", err)
				continue
			}

			if call.IsRead {
				log.Info("[SSV] Parsed mailbox read call",
					"chainMessageSender", call.ChainMessageSender,
					"sender", call.Sender.Hex(),
					"sessionId", call.SessionId,
					"label", string(call.Label))
			} else if call.IsWrite {
				log.Info("[SSV] Parsed mailbox write call",
					"chainMessageRecipient", call.ChainMessageRecipient,
					"receiver", call.Receiver.Hex(),
					"sessionId", call.SessionId,
					"label", string(call.Label),
					"dataLen", len(call.Data))
			}

			// mailbox.read(...)
			if call.IsRead {
				// If we're reading (chainDest  == chainID) this is a dependency
				if awaitRead(call, mp.chainID) {
					dep := CrossRollupDependency{
						SourceChainID: call.ChainSrc.Uint64(),
						DestChainID:   call.ChainDest.Uint64(),
						Sender:        call.Sender,
						Receiver:      op.From, // The contract calling read() is the receiver
						SessionID:     call.SessionId,
						Label:         call.Label,
						RequiredData:  true,
						IsInboxRead:   true,
					}

					if !containsDependency(fullfilledDeps, dep) {
						simState.Dependencies = append(simState.Dependencies, dep)

						log.Info("[SSV] Detected new mailbox read call",
							"chainSrc", dep.SourceChainID,
							"chainDest", dep.DestChainID,
							"sender", dep.Sender.Hex(),
							"receiver", dep.Receiver.Hex(),
							"sessionId", dep.SessionID)
					} else {
						log.Info("[SSV] Ignore mailbox read call: already fulfilled",
							"chainSrc", call.ChainSrc.Uint64(),
							"chainDest", call.ChainDest.Uint64(),
							"localChain", mp.chainID)
					}
				} else {
					log.Info("[SSV] Ignore mailbox read call: chainDest is another chain",
						"chainSrc", call.ChainSrc.Uint64(),
						"chainDest", call.ChainDest.Uint64(),
						"localChain", mp.chainID)
				}
			}

			// mailbox.write(...)
			if call.IsWrite {
				// If we're writing (chainSrc == chainID) this is an outbound message
				if mustWrite(call, mp.chainID) {
					msg := CrossRollupMessage{
						SourceChainID: call.ChainSrc.Uint64(),
						DestChainID:   call.ChainDest.Uint64(),
						Sender:        op.From,
						Receiver:      call.Receiver,
						SessionID:     call.SessionId,
						Data:          call.Data,
						Label:         call.Label,
						MessageType:   "mailbox_write",
						IsOutboxWrite: true,
					}

					if !alreadySent(sentOutboundMsgs, msg) {
						simState.OutboundMessages = append(simState.OutboundMessages, msg)

						log.Info("[SSV] Detected new mailbox write call",
							"chainSrc", msg.SourceChainID,
							"chainDest", msg.DestChainID,
							"sender", msg.Sender.Hex(),
							"receiver", msg.Receiver.Hex(),
							"sessionId", msg.SessionID,
							"dataLen", len(msg.Data))
					} else {
						log.Info("[SSV] Ignore mailbox write call: already sent",
							"chainSrc", call.ChainSrc.Uint64(),
							"chainDest", call.ChainDest.Uint64(),
							"localChain", mp.chainID)
					}
				} else {
					log.Info("[SSV] Ignore mailbox write call: chainSrc is another chain",
						"chainSrc", call.ChainSrc.Uint64(),
						"chainDest", call.ChainDest.Uint64(),
						"localChain", mp.chainID)
				}
			}
		} else if op.Type != vm.CALL && op.Type != vm.STATICCALL {
			log.Info("[SSV] Ignoring non-CALL/STATICCALL operation to mailbox", "type", op.Type.String(), "address", op.Address.Hex())
		}
	}

	log.Info("[SSV] Transaction analysis complete",
		"txHash", txHashHex,
		"requiresCoordination", simState.RequiresCoordination(),
		"dependencies", len(simState.Dependencies),
		"outboundMessages", len(simState.OutboundMessages))

	if simState.RequiresCoordination() {
		depCount := len(simState.Dependencies)
		outCount := len(simState.OutboundMessages)
		depPreview := make([]string, 0, 2)
		for i := 0; i < depCount && i < 2; i++ {
			d := simState.Dependencies[i]
			depPreview = append(depPreview, fmt.Sprintf("%d:%s->%s", d.SourceChainID, d.Sender.Hex(), d.Receiver.Hex()))
		}
		outPreview := make([]string, 0, 2)
		for i := 0; i < outCount && i < 2; i++ {
			o := simState.OutboundMessages[i]
			outPreview = append(outPreview, fmt.Sprintf("%d:%s->%s:%s", o.DestChainID, o.Sender.Hex(), o.Receiver.Hex(), string(o.Label)))
		}
		log.Info("[SSV] Coordination classification",
			"txHash", txHashHex,
			"deps", depCount,
			"deps_preview", depPreview,
			"outbound", outCount,
			"out_preview", outPreview,
		)
	}

	return simState, nil
}

func alreadySent(msgs []CrossRollupMessage, msg CrossRollupMessage) bool {
	for _, m := range msgs {
		if m.SourceChainID == msg.SourceChainID &&
			m.DestChainID == msg.DestChainID &&
			m.Sender == msg.Sender &&
			m.Receiver == msg.Receiver &&
			m.SessionID.Cmp(msg.SessionID) == 0 &&
			bytes.Equal(m.Data, msg.Data) &&
			bytes.Equal(m.Label, msg.Label) &&
			m.MessageType == msg.MessageType &&
			m.IsOutboxWrite == msg.IsOutboxWrite {
			return true
		}
	}

	return false
}

func containsDependency(deps []CrossRollupDependency, dep CrossRollupDependency) bool {
	for _, d := range deps {
		if d.SourceChainID == dep.SourceChainID &&
			d.DestChainID == dep.DestChainID &&
			d.Sender == dep.Sender &&
			d.Receiver == dep.Receiver &&
			d.SessionID.Cmp(dep.SessionID) == 0 &&
			bytes.Equal(d.Label, dep.Label) &&
			d.RequiredData == dep.RequiredData &&
			d.IsInboxRead == dep.IsInboxRead {
			return true
		}
	}

	return false
}

// Check whether our sequencer must write
func mustWrite(call *MailboxCall, chainID uint64) bool {
	return call.ChainSrc.Uint64() == chainID && call.ChainDest.Uint64() != chainID
}

// Checker whether our sequencer should wait for other sequencer "mustWrite"
func awaitRead(call *MailboxCall, chainID uint64) bool {
	return call.ChainSrc.Uint64() != chainID && call.ChainDest.Uint64() == chainID
}

func (mp *MailboxProcessor) parseMailboxCall(callData []byte) (*MailboxCall, error) {
	if len(callData) < 4 {
		return nil, fmt.Errorf("invalid call data length")
	}

	methodSig := callData[:4]
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		return nil, err
	}

	// Check for read() method - 4 parameters
	if bytes.Equal(methodSig, parsedABI.Methods["read"].ID) {
		call, err := mp.parseReadCall(callData[4:])
		if err != nil {
			return nil, err
		}
		call.IsRead = true
		// For read: chainSrc = chainMessageSender, chainDest = this chain
		call.ChainSrc = call.ChainMessageSender
		call.ChainDest = new(big.Int).SetUint64(mp.chainID)
		return call, nil
	}

	// Check for write() method - 5 parameters
	if bytes.Equal(methodSig, parsedABI.Methods["write"].ID) {
		call, err := mp.parseWriteCall(callData[4:])
		if err != nil {
			return nil, err
		}
		call.IsWrite = true
		// For write: chainSrc = this chain, chainDest = chainMessageRecipient
		call.ChainSrc = new(big.Int).SetUint64(mp.chainID)
		call.ChainDest = call.ChainMessageRecipient
		return call, nil
	}

	return nil, fmt.Errorf("unknown mailbox method")
}

func (mp *MailboxProcessor) parseReadCall(data []byte) (*MailboxCall, error) {
	parsedABI, _ := abi.JSON(strings.NewReader(mailboxABI))
	values, err := parsedABI.Methods["read"].Inputs.Unpack(data)
	if err != nil {
		return nil, err
	}

	// read(chainMessageSender, sender, sessionId, label)
	call := &MailboxCall{
		ChainMessageSender: values[0].(*big.Int),
		Sender:             values[1].(common.Address),
		SessionId:          values[2].(*big.Int),
		Label:              values[3].([]byte),
	}
	// For read: chainSrc = chainMessageSender, chainDest = this chain
	call.ChainSrc = call.ChainMessageSender
	call.ChainDest = new(big.Int).SetUint64(mp.chainID)
	return call, nil
}

func (mp *MailboxProcessor) parseWriteCall(data []byte) (*MailboxCall, error) {
	parsedABI, _ := abi.JSON(strings.NewReader(mailboxABI))
	values, err := parsedABI.Methods["write"].Inputs.Unpack(data)
	if err != nil {
		return nil, err
	}

	// write(chainMessageRecipient, receiver, sessionId, label, data)
	call := &MailboxCall{
		ChainMessageRecipient: values[0].(*big.Int),
		Receiver:              values[1].(common.Address),
		SessionId:             values[2].(*big.Int),
		Label:                 values[3].([]byte),
		Data:                  values[4].([]byte),
	}
	// For write: chainSrc = this chain, chainDest = chainMessageRecipient
	call.ChainSrc = new(big.Int).SetUint64(mp.chainID)
	call.ChainDest = call.ChainMessageRecipient
	return call, nil
}

func (mp *MailboxProcessor) handleCrossRollupCoordination(
	ctx context.Context,
	simState *SimulationState,
	xtID *rollupv1.XtID,
) ([]CrossRollupMessage, []CrossRollupDependency, error) {
	sentMsgs := make([]CrossRollupMessage, 0)
	// Send outbound CIRC messages
	for _, outMsg := range simState.OutboundMessages {
		if err := mp.sendCIRCMessage(ctx, &outMsg, xtID); err != nil {
			return nil, nil, fmt.Errorf("failed to send CIRC message: %w", err)
		}

		sentMsgs = append(sentMsgs, outMsg)
	}

	circDeps := make([]CrossRollupDependency, 0)

	for _, dep := range simState.Dependencies {
		sourceBytes := new(big.Int).SetUint64(dep.SourceChainID).Bytes()
		sourceKey := spconsensus.ChainKeyBytes(sourceBytes)
		circMsg, err := mp.waitForCIRCMessage(ctx, xtID, sourceKey)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to wait for CIRC message: %w", err)
		}

		// Populate dependency with authoritative fields from the CIRC message.
		// The Mailbox key uses (chainSrc, chainId, sender, receiver, sessionId, label),
		// where 'sender' in the outbox is msg.sender of the contract that performed write(...).
		// Ensure we mirror exactly what the source chain wrote so putInbox matches read(...).
		if len(circMsg.Source) > 0 {
			dep.Sender = common.BytesToAddress(circMsg.Source[0])
		}
		if len(circMsg.Receiver) > 0 {
			dep.Receiver = common.BytesToAddress(circMsg.Receiver[0])
		}
		dep.Data = circMsg.Data[0]
		dep.SessionID = new(big.Int).SetUint64(circMsg.SessionId)
		circDeps = append(circDeps, dep)
	}

	log.Info(
		"[SSV] Cross-rollup coordination completed",
		"xtID",
		xtID.Hex(),
		"sent",
		len(sentMsgs),
		"received",
		len(circDeps),
	)
	return sentMsgs, circDeps, nil
}

func (mp *MailboxProcessor) sendCIRCMessage(ctx context.Context, msg *CrossRollupMessage, xtID *rollupv1.XtID) error {
	var sessionID uint64
	if msg.SessionID != nil {
		sessionID = msg.SessionID.Uint64()
	}

	// Build CIRC payload
	circMsg := &rollupv1.CIRCMessage{
		SourceChain:      new(big.Int).SetUint64(msg.SourceChainID).Bytes(),
		DestinationChain: new(big.Int).SetUint64(msg.DestChainID).Bytes(),
		Source:           [][]byte{msg.Sender.Bytes()},
		Receiver:         [][]byte{msg.Receiver.Bytes()},
		XtId:             xtID,
		Label:            string(msg.Label),
		Data:             [][]byte{msg.Data},
		SessionId:        sessionID,
	}

	spMsg := &rollupv1.Message{
		SenderId: strconv.FormatUint(mp.chainID, 10),
		Payload: &rollupv1.Message_CircMessage{
			CircMessage: circMsg,
		},
	}

	backend, ok := mp.backend.(*EthAPIBackend)
	if !ok {
		return fmt.Errorf("backend not available")
	}

	destChainID := spconsensus.ChainKeyUint64(msg.DestChainID)
	sequencerClient := backend.sequencerClients[destChainID]
	if sequencerClient == nil {
		// Collect available peer keys for diagnostics
		keys := make([]string, 0, len(backend.sequencerClients))
		for k := range backend.sequencerClients {
			keys = append(keys, k)
		}
		log.Error("[SSV] Missing sequencer client for destination chain",
			"want", destChainID,
			"available", keys,
		)
		return fmt.Errorf("no client for destination chain %s", destChainID)
	}
	if err := sequencerClient.Send(ctx, spMsg); err != nil {
		log.Error("[SSV] Failed to send CIRC message",
			"xtID", xtID.Hex(),
			"destChain", spconsensus.ChainKeyUint64(msg.DestChainID),
			"err", err,
		)
		return err
	}
	return nil
}

func (mp *MailboxProcessor) waitForCIRCMessage(
	ctx context.Context,
	xtID *rollupv1.XtID,
	sourceChainID string,
) (*rollupv1.CIRCMessage, error) {
	// Wait for CIRC message with a bounded timeout to respect SBCP slot cutover.
	// Hardcoded for 20s slot with 0.90 seal cutover: use ~12s window.
	timeoutMs := 12000
	timeout := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timeout.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	ticks := 0

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			// Diagnostics: list known CIRC queues for this xtID
			backend := mp.backend.(*EthAPIBackend)
			if backend != nil && backend.coordinator != nil && backend.coordinator.Consensus() != nil {
				if st, ok := backend.coordinator.Consensus().GetState(xtID); ok && st != nil {
					// Best-effort read (no locking API available here)
					counts := make(map[string]int)
					for k, v := range st.CIRCMessages {
						counts[k] = len(v)
					}
					log.Warn("[SSV] Timeout waiting for CIRC message",
						"xtID", xtID.Hex(),
						"from", sourceChainID,
						"queues", counts,
					)
				}
			}
			return nil, fmt.Errorf("timeout waiting for CIRC message from chain %s", sourceChainID)
		case <-ticker.C:
			backend := mp.backend.(*EthAPIBackend)
			circMsg, err := backend.coordinator.Consensus().ConsumeCIRCMessage(xtID, sourceChainID)
			if err != nil {
				// Periodic info to confirm we're still waiting
				ticks++
				if ticks%10 == 0 { // ~1s interval
					log.Info("[SSV] Still waiting for CIRC message",
						"xtID", xtID.Hex(),
						"from", sourceChainID,
						"wait_ms", timeoutMs-(ticks*100),
						"err", err.Error(),
					)
				}
				continue // Keep waiting
			}

			log.Info("[SSV] Consumed CIRC message",
				"from", sourceChainID,
				"dataLen", len(circMsg.Data[0]),
			)

			return circMsg, nil
		}
	}
}

func (mp *MailboxProcessor) getCoordinatorAddress(ctx context.Context, addr common.Address) (common.Address, error) {
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		return common.Address{}, err
	}

	// The Mailbox ABI exposes the coordinator under the "COORDINATOR" view
	callData, err := parsedABI.Pack("COORDINATOR")
	if err != nil {
		return common.Address{}, err
	}

	// Build a call transaction
	data := hexutil.Bytes(callData)
	args := ethapi.TransactionArgs{
		To:   &addr,
		Data: &data,
	}
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	res, err := ethapi.DoCall(
		ctx,
		mp.backend.(*EthAPIBackend),
		args,
		latest,
		nil,
		nil,
		mp.backend.(*EthAPIBackend).RPCEVMTimeout(),
		mp.backend.(*EthAPIBackend).RPCGasCap(),
	)
	if err != nil {
		return common.Address{}, err
	}
	if len(res.Return()) != 32 && len(res.Return()) != 20 {
		return common.Address{}, fmt.Errorf("unexpected return size: %d", len(res.Return()))
	}
	// The ABI-encoded return for address is 32 bytes right-padded, extract last 20
	out := res.Return()
	if len(out) == 32 {
		out = out[12:]
	}
	return common.BytesToAddress(out), nil
}

func (mp *MailboxProcessor) createPutInboxTx(dep CrossRollupDependency, nonce uint64) (*types.Transaction, error) {
	parsedABI, err := abi.JSON(strings.NewReader(mailboxABI))
	if err != nil {
		return nil, err
	}

	// putInbox(chainMessageSender, sender, receiver, sessionId, label, data)
	callData, err := parsedABI.Pack("putInbox",
		new(big.Int).SetUint64(dep.SourceChainID),
		dep.Sender,
		dep.Receiver,
		dep.SessionID,
		dep.Label,
		dep.Data,
	)
	if err != nil {
		return nil, err
	}

	var mailboxAddr common.Address
	switch mp.chainID {
	case native.RollupAChainID:
		mailboxAddr = mp.mailboxAddresses[0]
	case native.RollupBChainID:
		mailboxAddr = mp.mailboxAddresses[1]
	default:
		return nil, fmt.Errorf("unable to select mailbox addr. Unsupported \"%d\" chain id", mp.chainID)
	}

	txData := &types.DynamicFeeTx{
		ChainID:    new(big.Int).SetUint64(mp.chainID),
		Nonce:      nonce,
		GasTipCap:  big.NewInt(1000000000),
		GasFeeCap:  big.NewInt(20000000000),
		Gas:        500000,
		To:         &mailboxAddr,
		Value:      big.NewInt(0),
		Data:       callData,
		AccessList: nil,
	}

	mp.traceTransaction(callData, mailboxAddr)

	tx := types.NewTx(txData)
	signedTx, err := types.SignTx(tx, types.NewLondonSigner(new(big.Int).SetUint64(mp.chainID)), mp.coordinatorKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign tx %v", err)
	}

	log.Info("[SSV] Created putInbox transaction",
		"txHash", signedTx.Hash().Hex(),
		"nonce", nonce,
		"sessionId", dep.SessionID,
		"mailbox", mailboxAddr.Hex(),
		"sourceChain", dep.SourceChainID,
		"sender", dep.Sender.Hex(),
		"receiver", dep.Receiver.Hex(),
		"label_len", len(dep.Label),
		"data_len", len(dep.Data),
		"gasTipCap", txData.GasTipCap,
		"gasFeeCap", txData.GasFeeCap,
	)

	return signedTx, nil
}

func (mp *MailboxProcessor) traceTransaction(callData []byte, mailboxAddr common.Address) {
	api := tracers.NewAPI(mp.backend.(tracers.Backend))
	res, err := api.TraceCall(context.Background(), ethapi.TransactionArgs{
		Data:  (*hexutil.Bytes)(&callData),
		From:  &mp.coordinatorAddr,
		To:    &mailboxAddr,
		Value: (*hexutil.Big)(big.NewInt(0))},
		rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber),
		nil,
	)
	if err != nil {
		log.Warn("putInbox tx tracing failed", "calldata", string(callData), "err", err)
		return
	}

	var traceResult struct {
		Failed      bool   `json:"failed"`
		ReturnValue string `json:"returnValue"`
	}

	if err := json.Unmarshal(res.(json.RawMessage), &traceResult); err != nil {
		log.Warn("unable to parse putInbox tx trace result", "error", err)
	} else {
		if traceResult.Failed {
			log.Warn("putInbox tx fails during trace", "calldata", hexutil.Encode(callData), "returnValue", traceResult.ReturnValue, "res", string(res.(json.RawMessage)))
		}
	}
}

func (mp *MailboxProcessor) isMailboxAddress(addr common.Address) bool {
	for _, mailboxAddr := range mp.mailboxAddresses {
		if addr == mailboxAddr {
			return true
		}
	}
	return false
}

// reSimulateForACKMessages re-simulates the transaction after putInbox creation
func (mp *MailboxProcessor) reSimulateForACKMessages(
	ctx context.Context,
	tx *types.Transaction,
	xtID *rollupv1.XtID,
	alreadySentMsgs []CrossRollupMessage,
) ([]CrossRollupMessage, error) {
	backend, ok := mp.backend.(*EthAPIBackend)
	if !ok {
		return nil, fmt.Errorf("backend not available for re-simulation")
	}

	log.Info("[SSV] Re-simulating transaction for ACK detection", "txHash", tx.Hash().Hex(), "xtID", xtID.Hex())

	// Re-simulate the transaction against pending state (which should include putInbox transactions)
	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	traceResult, err := backend.SimulateTransaction(ctx, tx, blockNrOrHash)
	if err != nil {
		return nil, fmt.Errorf("failed to re-simulate transaction: %w", err)
	}

	// Analyze the re-simulation result for new outbound messages
	emptyDeps := make([]CrossRollupDependency, 0) // No dependencies needed for re-simulation
	simState, err := mp.analyzeTransaction(traceResult, alreadySentMsgs, emptyDeps, tx.Hash().Hex())
	if err != nil {
		return nil, fmt.Errorf("failed to analyze re-simulation: %w", err)
	}

	newOutboundMsgs := make([]CrossRollupMessage, 0)

	// Send any new outbound messages detected in re-simulation
	for _, outMsg := range simState.OutboundMessages {
		log.Info("[SSV] Detected new ACK message in re-simulation",
			"xtID", xtID.Hex(),
			"srcChain", outMsg.SourceChainID,
			"destChain", outMsg.DestChainID,
			"sessionId", outMsg.SessionID,
			"label", string(outMsg.Label))

		if err := mp.sendCIRCMessage(ctx, &outMsg, xtID); err != nil {
			log.Error("[SSV] Failed to send ACK CIRC message", "error", err, "xtID", xtID.Hex())
			return nil, fmt.Errorf("failed to send ACK CIRC message: %w", err)
		}

		newOutboundMsgs = append(newOutboundMsgs, outMsg)
	}

	if len(newOutboundMsgs) > 0 {
		log.Info("[SSV] Successfully sent ACK CIRC messages", "xtID", xtID.Hex(), "count", len(newOutboundMsgs))
	}
	return newOutboundMsgs, nil
}
