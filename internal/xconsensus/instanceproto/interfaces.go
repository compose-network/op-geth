package instanceproto

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/compose-network/specs/compose"
	composeproto "github.com/compose-network/specs/compose/proto"
	instanceproto "github.com/compose-network/specs/compose/scp"
	"github.com/rs/zerolog"
)

type Sequencer interface {
	StartInstance(instance *composeproto.StartInstance) error
	ProcessMailboxMessage(instanceID compose.InstanceID, mailboxMessage *instanceproto.MailboxMessage) error
	Decide(instance compose.InstanceID, decision bool) error
}

type sequencerNetworkFactory interface {
	ForInstance(instance compose.Instance) instanceproto.SequencerNetwork
}

type InstanceExecutionEngine interface {
	instanceproto.ExecutionEngine
}

type InstanceSequencer struct {
	instanceMap map[compose.InstanceID]instanceproto.SequencerInstance
	lock        sync.RWMutex
	execEngine  InstanceExecutionEngine
	seqNetwork  instanceproto.SequencerNetwork
	log         zerolog.Logger
}

func NewSequencer(executionEngine InstanceExecutionEngine, seqNetwork instanceproto.SequencerNetwork, log zerolog.Logger) Sequencer {
	is := InstanceSequencer{
		instanceMap: make(map[compose.InstanceID]instanceproto.SequencerInstance),
		execEngine:  executionEngine,
		seqNetwork:  seqNetwork,
		log:         log,
	}

	return &is
}

func (s *InstanceSequencer) StartInstance(startInstance *composeproto.StartInstance) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	instance := convertToSpecInstance(startInstance)
	network := s.seqNetwork

	if factory, ok := s.seqNetwork.(sequencerNetworkFactory); ok {
		network = factory.ForInstance(instance)
	}

	seqInstance, err := instanceproto.NewSequencerInstance(
		instance,
		s.execEngine,
		network,
		compose.StateRoot{},
		s.log,
	)
	if err != nil {
		return fmt.Errorf("could not create instance from start instance: %w", err)
	}

	s.instanceMap[instance.ID] = seqInstance

	return seqInstance.Run()
}

func (s *InstanceSequencer) ProcessMailboxMessage(instanceID compose.InstanceID, message *instanceproto.MailboxMessage) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	seqInstance, ok := s.instanceMap[instanceID]
	if !ok {
		return fmt.Errorf("could not find sequencer instance by %s", hex.EncodeToString(instanceID[:]))
	}

	return seqInstance.ProcessMailboxMessage(*message)
}

func (s *InstanceSequencer) Decide(instance compose.InstanceID, decision bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	seqInstance, ok := s.instanceMap[instance]
	/*
		Instance could have been decided "false" when we voted - false so the instance was deleted already.
		In this case decision "false" can arrive twice, once from current sequencer and once from SP.
		If decision is true and we can't find the instance, it's an error.
	*/
	if !ok && decision {
		return fmt.Errorf("could not find sequencer instance by %s", hex.EncodeToString(instance[:]))
	}

	if err := seqInstance.ProcessDecidedMessage(decision); err != nil {
		return fmt.Errorf("failed to process decided message: %w", err)
	}

	delete(s.instanceMap, instance)

	return nil
}

func convertToSpecInstance(instance *composeproto.StartInstance) compose.Instance {
	xtRequest := instance.XtRequest
	trs := make([]compose.TransactionRequest, 0)

	for _, xtr := range xtRequest.TransactionRequests {
		trs = append(trs, compose.TransactionRequest{
			ChainID:      compose.ChainID(xtr.ChainId),
			Transactions: xtr.Transaction,
		})
	}

	return compose.Instance{
		ID:             convertInstanceIDToArray(instance.InstanceId),
		SequenceNumber: compose.SequenceNumber(instance.SequenceNumber),
		PeriodID:       compose.PeriodID(instance.PeriodId),
		XTRequest: compose.XTRequest{
			trs,
		},
	}
}

func convertInstanceIDToArray(instanceIDAsSlice []byte) [32]byte {
	id := [32]byte{}
	copy(id[:], instanceIDAsSlice)
	return id
}
