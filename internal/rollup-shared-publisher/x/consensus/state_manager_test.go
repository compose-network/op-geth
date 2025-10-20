package consensus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateManagerAddGetRemove(t *testing.T) {
	sm := NewStateManager()
	defer sm.Shutdown()

	xtReq := buildXTRequest([]byte{0x01})
	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	state, err := sm.AddState(xtID, xtReq, xtReq.ChainIDs())
	require.NoError(t, err)
	require.NotNil(t, state)

	retrieved, ok := sm.GetState(xtID)
	require.True(t, ok)
	require.Equal(t, state, retrieved)

	_, err = sm.AddState(xtID, xtReq, xtReq.ChainIDs())
	require.Error(t, err)

	active := sm.GetAllActiveIDs()
	assert.Len(t, active, 1)

	sm.RemoveState(xtID)
	_, ok = sm.GetState(xtID)
	assert.False(t, ok)
}

func TestStateManagerCleanupRemovesCompletedStates(t *testing.T) {
	sm := NewStateManager()
	defer sm.Shutdown()

	xtReq := buildXTRequest([]byte{0x01})
	xtID, err := xtReq.XtID()
	require.NoError(t, err)

	state, err := sm.AddState(xtID, xtReq, xtReq.ChainIDs())
	require.NoError(t, err)

	state.SetDecision(StateCommit)
	state.StartTime = time.Now().Add(-11 * time.Minute)

	sm.cleanup()

	_, ok := sm.GetState(xtID)
	assert.False(t, ok, "cleanup should remove old committed state")
}
