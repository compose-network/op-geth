package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTwoPCState(t *testing.T) (*TwoPCState, map[string]struct{}) {
	t.Helper()

	chainA := []byte{0x01}
	chainB := []byte{0x02}

	xtReq := buildXTRequest(chainA, chainB)
	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	chains := map[string]struct{}{
		ChainKeyBytes(chainA): {},
		ChainKeyBytes(chainB): {},
	}

	state := NewTwoPCState(xtID, xtReq, chains)
	return state, chains
}

func TestNewTwoPCStateInitializesFields(t *testing.T) {
	t.Parallel()

	state, chains := newTestTwoPCState(t)

	assert.NotNil(t, state.XTID)
	assert.Equal(t, StateUndecided, state.GetDecision())
	assert.Equal(t, len(chains), state.GetParticipantCount())
	assert.Equal(t, 0, state.GetVoteCount())
	assert.NotNil(t, state.XTRequest)
	assert.NotNil(t, state.CIRCMessages)
	assert.Nil(t, state.Timer)
	assert.False(t, state.StartTime.IsZero())
}

func TestTwoPCStateAddVote(t *testing.T) {
	t.Parallel()

	state, chains := newTestTwoPCState(t)
	// Get frist chain key
	var chain string
	for k := range chains {
		chain = k
		break
	}

	added := state.AddVote(chain, true)
	assert.True(t, added)
	assert.Equal(t, 1, state.GetVoteCount())

	readded := state.AddVote(chain, false)
	assert.False(t, readded)

	votes := state.GetVotes()
	assert.Equal(t, true, votes[chain])
}

func TestTwoPCStateDecisionFlow(t *testing.T) {
	t.Parallel()

	state, _ := newTestTwoPCState(t)

	assert.False(t, state.IsComplete())
	assert.Equal(t, StateUndecided, state.GetDecision())

	state.SetDecision(StateCommit)

	assert.True(t, state.IsComplete())
	assert.Equal(t, StateCommit, state.GetDecision())
}

func TestTwoPCStateGetVotesReturnsCopy(t *testing.T) {
	t.Parallel()

	state, chains := newTestTwoPCState(t)

	for chain := range chains {
		require.True(t, state.AddVote(chain, true))
	}

	clone := state.GetVotes()
	for k := range clone {
		clone[k] = false
	}

	original := state.GetVotes()
	for chain := range chains {
		assert.True(t, original[chain])
	}
}

func TestTwoPCStateGetDurationIncreases(t *testing.T) {
	t.Parallel()

	state, _ := newTestTwoPCState(t)
	initial := state.GetDuration()
	assert.GreaterOrEqual(t, int64(initial), int64(0))

	time.Sleep(5 * time.Millisecond)

	later := state.GetDuration()
	assert.Greater(t, later, initial)
}

func TestTwoPCState2VotesDoesNotMeanComplete(t *testing.T) {
	t.Parallel()

	state, chains := newTestTwoPCState(t)

	count := 0
	for chain := range chains {
		require.True(t, state.AddVote(chain, true))
		count++
		if count < state.GetParticipantCount() {
			assert.False(t, state.IsComplete())
		}
	}

	assert.Equal(t, state.GetParticipantCount(), state.GetVoteCount())
	assert.False(t, state.IsComplete(), "adding votes should not set completion")
	assert.Equal(t, StateUndecided, state.GetDecision()) // decision should remain undecided
}
