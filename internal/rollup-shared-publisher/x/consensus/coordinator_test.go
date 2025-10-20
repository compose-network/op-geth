package consensus

import (
	"context"
	"testing"
	"time"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorStartTransactionRegistersStateAndCallback(t *testing.T) {
	coord := newTestCoordinator(t, Leader)

	chainA := []byte{0x01}
	chainB := []byte{0x02}
	xtReq := buildXTRequest(chainA, chainB)

	startCalled := make(chan struct{}, 1)
	coord.SetStartCallback(func(ctx context.Context, from string, req *pb.XTRequest) error {
		startCalled <- struct{}{}
		return nil
	})

	require.NoError(t, coord.StartTransaction(context.Background(), "sp", xtReq))

	select {
	case <-startCalled:
	case <-time.After(time.Second):
		t.Fatal("expected start callback invocation")
	}

	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	state, ok := coord.GetState(xtID)
	require.True(t, ok)

	assert.Equal(t, StateUndecided, state.GetDecision())
	assert.Len(t, state.ParticipatingChains, 2)
	assert.NotNil(t, state.Timer, "leader should arm timeout timer")
}

func TestCoordinatorRecordVoteCommitTriggersDecision(t *testing.T) {
	coord := newTestCoordinator(t, Leader)

	chainA := []byte{0x01}
	chainB := []byte{0x02}
	chainAKey := ChainKeyBytes(chainA)
	chainBKey := ChainKeyBytes(chainB)

	xtReq := buildXTRequest(chainA, chainB)
	require.NoError(t, coord.StartTransaction(context.Background(), "sp", xtReq))

	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	decisionCh := make(chan bool, 1)
	coord.SetDecisionCallback(func(ctx context.Context, id *pb.XtID, decision bool) error {
		if id.Hex() == xtID.Hex() {
			decisionCh <- decision
		}
		return nil
	})

	state, ok := coord.GetState(xtID)
	require.True(t, ok)

	result, err := coord.RecordVote(xtID, chainAKey, true)
	require.NoError(t, err)
	assert.Equal(t, StateUndecided, result)

	result, err = coord.RecordVote(xtID, chainBKey, true)
	require.NoError(t, err)
	assert.Equal(t, StateCommit, result)

	select {
	case decision := <-decisionCh:
		assert.True(t, decision)
	case <-time.After(time.Second):
		t.Fatal("expected decision callback for commit")
	}

	assert.Equal(t, StateCommit, state.GetDecision())
	assert.Len(t, state.Votes, 2)
}

func TestCoordinatorRecordVoteAbort(t *testing.T) {
	coord := newTestCoordinator(t, Leader)

	chainA := []byte{0x01}
	chainB := []byte{0x02}
	chainAKey := ChainKeyBytes(chainA)

	xtReq := buildXTRequest(chainA, chainB)
	require.NoError(t, coord.StartTransaction(context.Background(), "sp", xtReq))

	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	decisionCh := make(chan bool, 1)
	coord.SetDecisionCallback(func(ctx context.Context, id *pb.XtID, decision bool) error {
		if id.Hex() == xtID.Hex() {
			decisionCh <- decision
		}
		return nil
	})

	state, ok := coord.GetState(xtID)
	require.True(t, ok)

	result, err := coord.RecordVote(xtID, chainAKey, false)
	require.NoError(t, err)
	assert.Equal(t, StateAbort, result)

	select {
	case decision := <-decisionCh:
		assert.False(t, decision)
	case <-time.After(time.Second):
		t.Fatal("expected decision callback for abort")
	}

	assert.Equal(t, StateAbort, state.GetDecision())
}

func TestCoordinatorRecordVoteRejectsUnknownParticipant(t *testing.T) {
	coord := newTestCoordinator(t, Leader)

	chainA := []byte{0x01}
	chainB := []byte{0x02}
	xtReq := buildXTRequest(chainA, chainB)
	require.NoError(t, coord.StartTransaction(context.Background(), "sp", xtReq))

	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	_, err = coord.RecordVote(xtID, "ff", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not participating")
}

func TestCoordinatorRecordCIRCMessageAndConsume(t *testing.T) {
	coord := newTestCoordinator(t, Leader)

	chainA := []byte{0x01}
	chainB := []byte{0x02}
	xtReq := buildXTRequest(chainA, chainB)
	require.NoError(t, coord.StartTransaction(context.Background(), "sp", xtReq))

	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	circ := &pb.CIRCMessage{
		SourceChain:      append([]byte(nil), chainB...),
		DestinationChain: append([]byte(nil), chainA...),
		XtId:             xtID,
		Data:             [][]byte{[]byte("payload")},
	}

	require.NoError(t, coord.RecordCIRCMessage(circ))

	msg, err := coord.ConsumeCIRCMessage(xtID, ChainKeyBytes(chainB))
	require.NoError(t, err)
	assert.Equal(t, circ, msg)

	_, err = coord.ConsumeCIRCMessage(xtID, ChainKeyBytes(chainB))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no messages available")

	err = coord.RecordCIRCMessage(&pb.CIRCMessage{
		SourceChain: []byte{0xff},
		XtId:        xtID,
		Data:        [][]byte{[]byte("payload")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not participating")
}

func TestCoordinatorRecordDecisionFollower(t *testing.T) {
	coord := newTestCoordinator(t, Follower)

	chainA := []byte{0x01}
	chainB := []byte{0x02}
	xtReq := buildXTRequest(chainA, chainB)
	require.NoError(t, coord.StartTransaction(context.Background(), "sp", xtReq))

	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	decisionCh := make(chan bool, 1)
	coord.SetDecisionCallback(func(ctx context.Context, id *pb.XtID, decision bool) error {
		if id.Hex() == xtID.Hex() {
			decisionCh <- decision
		}
		return nil
	})

	require.NoError(t, coord.RecordDecision(xtID, true))

	select {
	case decision := <-decisionCh:
		assert.True(t, decision)
	case <-time.After(time.Second):
		t.Fatal("expected decision callback for follower")
	}

	state, ok := coord.GetState(xtID)
	require.True(t, ok)
	assert.Equal(t, StateCommit, state.GetDecision())
}

func TestCoordinatorRecordDecisionRejectedForLeader(t *testing.T) {
	coord := newTestCoordinator(t, Leader)

	chainA := []byte{0x01}
	chainB := []byte{0x02}
	xtReq := buildXTRequest(chainA, chainB)
	require.NoError(t, coord.StartTransaction(context.Background(), "sp", xtReq))

	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	err = coord.RecordDecision(xtID, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only followers can record decisions")

	state, ok := coord.GetState(xtID)
	require.True(t, ok)
	assert.Equal(t, StateUndecided, state.GetDecision())
}
