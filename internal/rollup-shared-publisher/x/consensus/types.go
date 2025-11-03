package consensus

import (
	"sync"
	"time"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
)

// Role represents the coordinator role
type Role int

const (
	RoleUnknown  = "unknown"
	RoleFollower = "follower"
	RoleLeader   = "leader"

	StateUnknownStr = "unknown"
	StateCommitStr  = "commit"
	StateAbortStr   = "abort"
)

const (
	Follower Role = iota
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return RoleFollower
	case Leader:
		return RoleLeader
	default:
		return RoleUnknown
	}
}

// DecisionState represents 2PC transaction states
type DecisionState int

const (
	StateUndecided DecisionState = iota
	StateCommit
	StateAbort
)

func (s DecisionState) String() string {
	switch s {
	case StateUndecided:
		return "undecided"
	case StateCommit:
		return StateCommitStr
	case StateAbort:
		return StateAbortStr
	default:
		return StateUnknownStr
	}
}

// TwoPCState holds state for a single cross-chain transaction
// Important: it doesn't automatically set the decision state based on votes;
// that logic must be handled externally to allow for custom decision policies.
// TODO: we should add the decision logic here as well, making it a complete 2PC state manager.
type TwoPCState struct {
	mu                  sync.RWMutex
	XTID                *pb.XtID
	Decision            DecisionState
	ParticipatingChains map[string]struct{}
	Votes               map[string]bool
	Timer               *time.Timer
	StartTime           time.Time
	XTRequest           *pb.XTRequest
	CIRCMessages        map[string][]*pb.CIRCMessage
}

// NewTwoPCState creates a new 2PC state
func NewTwoPCState(xtID *pb.XtID, req *pb.XTRequest, chains map[string]struct{}) *TwoPCState {
	return &TwoPCState{
		XTID:                xtID,
		Decision:            StateUndecided,
		ParticipatingChains: chains,
		Votes:               make(map[string]bool),
		StartTime:           time.Now(),
		XTRequest:           req,
		CIRCMessages:        make(map[string][]*pb.CIRCMessage),
	}
}

// GetVoteCount returns the number of votes received (thread-safe)
func (t *TwoPCState) GetVoteCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Votes)
}

// GetParticipantCount returns the number of participating chains (thread-safe)
func (t *TwoPCState) GetParticipantCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.ParticipatingChains)
}

// IsComplete returns true if transaction has reached final decision (thread-safe)
func (t *TwoPCState) IsComplete() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Decision != StateUndecided
}

// GetDuration returns duration since transaction started (thread-safe)
func (t *TwoPCState) GetDuration() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return time.Since(t.StartTime)
}

// SetDecision atomically sets the decision state
func (t *TwoPCState) SetDecision(decision DecisionState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Decision = decision
}

// GetDecision atomically gets the decision state
func (t *TwoPCState) GetDecision() DecisionState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Decision
}

// AddVote atomically adds a vote if not already present
func (t *TwoPCState) AddVote(chainID string, vote bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.Votes[chainID]; exists {
		return false // Already voted
	}

	t.Votes[chainID] = vote
	return true
}

// GetVotes returns a copy of current votes (thread-safe)
func (t *TwoPCState) GetVotes() map[string]bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	votes := make(map[string]bool, len(t.Votes))
	for k, v := range t.Votes {
		votes[k] = v
	}
	return votes
}
