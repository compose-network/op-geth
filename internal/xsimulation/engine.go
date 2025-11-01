package xsimulation

import (
	"github.com/compose-network/specs/compose"
	instanceproto "github.com/compose-network/specs/compose/scp"
)

type simulationEngine struct {
}

func NewSimulationEngine() instanceproto.ExecutionEngine {
	return &simulationEngine{}
}

func (se *simulationEngine) ChainID() compose.ChainID {
	return 0
}

func (se *simulationEngine) Simulate(request instanceproto.SimulationRequest) (readRequest *instanceproto.MailboxMessageHeader, writeMessages []instanceproto.MailboxMessage, err error) {
	return nil, nil, nil
}
