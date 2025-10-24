package consensus

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type contextKey string

func TestProtocolHandlerHandleXTRequest(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))
		xtReq := buildXTRequest([]byte{0x01})
		ctx := context.WithValue(context.Background(), contextKey("test"), "value")
		msg := &pb.Message{
			Payload: &pb.Message_XtRequest{XtRequest: xtReq},
		}

		err := handler.Handle(ctx, "sp", msg)
		require.NoError(t, err)

		require.Len(t, stub.startCalls, 1)
		call := stub.startCalls[0]
		assert.Same(t, ctx, call.ctx)
		assert.Equal(t, "sp", call.from)
		assert.Equal(t, xtReq, call.req)
	})

	t.Run("propagates error", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		stub.startErr = errors.New("start failed")
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))
		xtReq := buildXTRequest([]byte{0x02})
		msg := &pb.Message{
			Payload: &pb.Message_XtRequest{XtRequest: xtReq},
		}

		err := handler.Handle(context.Background(), "sp", msg)
		require.ErrorIs(t, err, stub.startErr)
		require.Len(t, stub.startCalls, 1)
	})
}

func TestProtocolHandlerHandleVote(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		stub.voteDecision = StateCommit
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))

		xtID := &pb.XtID{Hash: []byte{0xaa}}
		vote := &pb.Vote{
			SenderChainId: []byte{0x0a},
			XtId:          xtID,
			Vote:          true,
		}
		msg := &pb.Message{
			Payload: &pb.Message_Vote{Vote: vote},
		}

		err := handler.Handle(context.Background(), "peer", msg)
		require.NoError(t, err)

		require.Len(t, stub.voteCalls, 1)
		call := stub.voteCalls[0]
		assert.Equal(t, xtID, call.xtID)
		assert.Equal(t, ChainKeyBytes(vote.SenderChainId), call.chainID)
		assert.True(t, call.vote)
	})

	t.Run("propagates error", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		stub.voteErr = errors.New("vote failed")
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))

		msg := &pb.Message{
			Payload: &pb.Message_Vote{
				Vote: &pb.Vote{
					SenderChainId: []byte{0x01},
					XtId:          &pb.XtID{Hash: []byte{0x01}},
					Vote:          false,
				},
			},
		}

		err := handler.Handle(context.Background(), "peer", msg)
		require.ErrorIs(t, err, stub.voteErr)
		require.Len(t, stub.voteCalls, 1)
	})
}

func TestProtocolHandlerHandleDecided(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))

		decided := &pb.Decided{
			XtId:     &pb.XtID{Hash: []byte{0x0b}},
			Decision: true,
		}
		msg := &pb.Message{
			Payload: &pb.Message_Decided{Decided: decided},
		}

		err := handler.Handle(context.Background(), "sp", msg)
		require.NoError(t, err)

		require.Len(t, stub.decisionCalls, 1)
		call := stub.decisionCalls[0]
		assert.Equal(t, decided.XtId, call.xtID)
		assert.Equal(t, decided.Decision, call.decision)
	})

	t.Run("propagates error", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		stub.decisionErr = errors.New("decision failed")
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))

		msg := &pb.Message{
			Payload: &pb.Message_Decided{
				Decided: &pb.Decided{
					XtId:     &pb.XtID{Hash: []byte{0x0c}},
					Decision: false,
				},
			},
		}

		err := handler.Handle(context.Background(), "sp", msg)
		require.ErrorIs(t, err, stub.decisionErr)
		require.Len(t, stub.decisionCalls, 1)
	})
}

func TestProtocolHandlerHandleCIRCMessage(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))

		circ := &pb.CIRCMessage{
			SourceChain:      []byte{0x01},
			DestinationChain: []byte{0x02},
			XtId:             &pb.XtID{Hash: []byte{0x0d}},
			Data:             [][]byte{[]byte("payload")},
		}
		msg := &pb.Message{
			Payload: &pb.Message_CircMessage{CircMessage: circ},
		}

		err := handler.Handle(context.Background(), "peer", msg)
		require.NoError(t, err)

		require.Len(t, stub.circCalls, 1)
		assert.Equal(t, circ, stub.circCalls[0].message)
	})

	t.Run("propagates error", func(t *testing.T) {
		t.Parallel()

		stub := newStubCoordinator(t)
		stub.circErr = errors.New("circ failed")
		handler := NewProtocolHandler(stub, zerolog.New(io.Discard))

		msg := &pb.Message{
			Payload: &pb.Message_CircMessage{
				CircMessage: &pb.CIRCMessage{
					SourceChain: []byte{0x01},
				},
			},
		}

		err := handler.Handle(context.Background(), "peer", msg)
		require.ErrorIs(t, err, stub.circErr)
		require.Len(t, stub.circCalls, 1)
	})
}

func TestProtocolHandlerHandleUnknownMessage(t *testing.T) {
	t.Parallel()

	stub := newStubCoordinator(t)
	handler := NewProtocolHandler(stub, zerolog.New(io.Discard))
	err := handler.Handle(context.Background(), "peer", &pb.Message{})
	require.Error(t, err)
}

func TestProtocolHandlerCanHandleAndName(t *testing.T) {
	t.Parallel()

	stub := newStubCoordinator(t)
	handler := NewProtocolHandler(stub, zerolog.New(io.Discard))

	assert.Equal(t, "SCP", handler.GetProtocolName())

	assert.False(t, handler.CanHandle(nil))
	assert.False(t, handler.CanHandle(&pb.Message{
		Payload: &pb.Message_HandshakeRequest{HandshakeRequest: &pb.HandshakeRequest{}},
	}))
	assert.True(t, handler.CanHandle(&pb.Message{
		Payload: &pb.Message_XtRequest{XtRequest: buildXTRequest([]byte{0x01})},
	}))
}

type (
	startCall struct {
		ctx  context.Context
		from string
		req  *pb.XTRequest
	}
	voteCall struct {
		xtID    *pb.XtID
		chainID string
		vote    bool
	}
	decisionCall struct {
		xtID     *pb.XtID
		decision bool
	}
	circCall struct {
		message *pb.CIRCMessage
	}
)

type stubCoordinator struct {
	t *testing.T

	startCalls []startCall
	startErr   error

	voteCalls    []voteCall
	voteDecision DecisionState
	voteErr      error

	decisionCalls []decisionCall
	decisionErr   error

	circCalls []circCall
	circErr   error
}

func newStubCoordinator(t *testing.T) *stubCoordinator {
	t.Helper()
	return &stubCoordinator{
		t:            t,
		voteDecision: StateUndecided,
	}
}

func (s *stubCoordinator) StartTransaction(ctx context.Context, from string, xtReq *pb.XTRequest) error {
	s.startCalls = append(s.startCalls, startCall{ctx: ctx, from: from, req: xtReq})
	return s.startErr
}

func (s *stubCoordinator) RecordVote(xtID *pb.XtID, chainID string, vote bool) (DecisionState, error) {
	s.voteCalls = append(s.voteCalls, voteCall{xtID: xtID, chainID: chainID, vote: vote})
	return s.voteDecision, s.voteErr
}

func (s *stubCoordinator) RecordDecision(xtID *pb.XtID, decision bool) error {
	s.decisionCalls = append(s.decisionCalls, decisionCall{xtID: xtID, decision: decision})
	return s.decisionErr
}

func (s *stubCoordinator) GetTransactionState(*pb.XtID) (DecisionState, error) {
	s.unexpected("GetTransactionState")
	return StateUndecided, nil
}

func (s *stubCoordinator) GetActiveTransactions() []*pb.XtID {
	s.unexpected("GetActiveTransactions")
	return nil
}

func (s *stubCoordinator) GetState(*pb.XtID) (*TwoPCState, bool) {
	s.unexpected("GetState")
	return nil, false
}

func (s *stubCoordinator) RecordCIRCMessage(circMessage *pb.CIRCMessage) error {
	s.circCalls = append(s.circCalls, circCall{message: circMessage})
	return s.circErr
}

func (s *stubCoordinator) ConsumeCIRCMessage(*pb.XtID, string) (*pb.CIRCMessage, error) {
	s.unexpected("ConsumeCIRCMessage")
	return nil, nil
}

func (s *stubCoordinator) SetStartCallback(StartFn) {
	s.unexpected("SetStartCallback")
}

func (s *stubCoordinator) SetVoteCallback(VoteFn) {
	s.unexpected("SetVoteCallback")
}

func (s *stubCoordinator) SetDecisionCallback(DecisionFn) {
	s.unexpected("SetDecisionCallback")
}

func (s *stubCoordinator) SetBlockCallback(BlockFn) {
	s.unexpected("SetBlockCallback")
}

func (s *stubCoordinator) OnBlockCommitted(context.Context, *types.Block) error {
	s.unexpected("OnBlockCommitted")
	return nil
}

func (s *stubCoordinator) OnL2BlockCommitted(context.Context, *pb.L2Block) error {
	s.unexpected("OnL2BlockCommitted")
	return nil
}

func (s *stubCoordinator) Start(context.Context) error {
	s.unexpected("Start")
	return nil
}

func (s *stubCoordinator) Stop(context.Context) error {
	s.unexpected("Stop")
	return nil
}

func (s *stubCoordinator) unexpected(method string) {
	if s.t != nil {
		s.t.Helper()
		s.t.Fatalf("unexpected call to %s", method)
	}
}
