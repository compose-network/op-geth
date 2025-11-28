package xsimulation

import (
	"errors"
	"sync"

	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
)

// SimulatorFunc is the callback signature used by the execution engine to
// delegate mailbox-aware transaction simulation to the host environment.
type SimulatorFunc func(request instanceproto.SimulationRequest) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage, error)

type simulationEngine struct {
	chainID          compose.ChainID
	simulate         SimulatorFunc
	blockStateLocker sync.Mutex
}

// NewSimulationEngine constructs a sequencer execution engine scoped to the provided chain.
func NewSimulationEngine(chainID compose.ChainID) *simulationEngine {
	return &simulationEngine{
		chainID: chainID,
	}
}

// AcquireState locks the block state for simulation.
func (se *simulationEngine) AcquireState() (release func()) {
	se.blockStateLocker.Lock()
	return func() {
		se.blockStateLocker.Unlock()
	}
}

// SetSimulator installs the concrete simulation callback supplied by the host execution environment.
func (se *simulationEngine) SetSimulator(fn SimulatorFunc) {
	se.simulate = fn
}

// ChainID reports the chain identifier this engine instance was created for.
func (se *simulationEngine) ChainID() compose.ChainID {
	return se.chainID
}

// Simulate delegates execution to the configured simulator. The callback is expected to perform
// mailbox-aware simulation and return the first missing mailbox read (if any), the set of messages
// written during execution, or an error when the bundle cannot be executed.
func (se *simulationEngine) Simulate(request instanceproto.SimulationRequest) (*instanceproto.MailboxMessageHeader, []instanceproto.MailboxMessage, error) {
	if se.simulate == nil {
		return nil, nil, errors.New("simulation runner not configured")
	}
	// lock block state during simulation to prevent concurrent modifications
	se.blockStateLocker.Lock()
	defer se.blockStateLocker.Unlock()

	return se.simulate(request)
}
