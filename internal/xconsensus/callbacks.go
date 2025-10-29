package xconsensus

import (
	"context"
	pb "github.com/ethereum/go-ethereum/internal/xproto/rollup/v1"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
)

// CallbackManager manages coordinator callbacks with error handling and timeouts
type CallbackManager struct {
	startFn    StartFn
	voteFn     VoteFn
	decisionFn DecisionFn
	blockFn    BlockFn

	timeout time.Duration
	log     zerolog.Logger
}

// NewCallbackManager creates a new callback manager
func NewCallbackManager(timeout time.Duration, log zerolog.Logger) *CallbackManager {
	return &CallbackManager{
		timeout: timeout,
		log:     log.With().Str("component", "callback-manager").Logger(),
	}
}

// SetStartCallback sets the start callback
func (cm *CallbackManager) SetStartCallback(fn StartFn) {
	cm.startFn = fn
}

// SetVoteCallback sets the vote callback
func (cm *CallbackManager) SetVoteCallback(fn VoteFn) {
	cm.voteFn = fn
}

// SetDecisionCallback sets the decision callback
func (cm *CallbackManager) SetDecisionCallback(fn DecisionFn) {
	cm.decisionFn = fn
}

// SetBlockCallback sets the block callback
func (cm *CallbackManager) SetBlockCallback(fn BlockFn) {
	cm.blockFn = fn
}

// InvokeStart calls the start callback with timeout and error handling
func (cm *CallbackManager) InvokeStart(ctx context.Context, from string, xtReq *pb.XTRequest) {
	if cm.startFn == nil {
		return
	}

	xtID, _ := xtReq.XtID()

	go func() {
		ctx, cancel := context.WithTimeout(ctx, cm.timeout)
		defer cancel()

		if err := cm.startFn(ctx, from, xtReq); err != nil {
			cm.log.Error().
				Err(err).
				Str("xt_id", xtID.Hex()).
				Str("from", from).
				Msg("Start callback failed")
		}
	}()
}

// InvokeVote calls the vote callback with timeout and error handling
func (cm *CallbackManager) InvokeVote(xtID *pb.XtID, vote bool, duration time.Duration) {
	if cm.voteFn == nil {
		return
	}

	cm.invokeCallback("vote", xtID, func(ctx context.Context) error {
		return cm.voteFn(ctx, xtID, vote)
	})
}

// InvokeDecision calls the decision callback with timeout and error handling
func (cm *CallbackManager) InvokeDecision(xtID *pb.XtID, decision bool, duration time.Duration) {
	if cm.decisionFn == nil {
		return
	}

	cm.invokeCallback("decision", xtID, func(ctx context.Context) error {
		return cm.decisionFn(ctx, xtID, decision)
	})
}

// InvokeBlock calls the block callback with timeout and error handling
func (cm *CallbackManager) InvokeBlock(ctx context.Context, block *types.Block, xtIDs []*pb.XtID) {
	if cm.blockFn == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(ctx, cm.timeout)
		defer cancel()
		if err := cm.blockFn(ctx, block, xtIDs); err != nil {
			cm.log.Error().
				Err(err).
				Int("xt_count", len(xtIDs)).
				Msg("Block callback failed")
		}
	}()
}

// invokeCallback is a helper to invoke callbacks with error handling and timeout
func (cm *CallbackManager) invokeCallback(callbackType string, xtID *pb.XtID, fn func(context.Context) error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cm.timeout)
		defer cancel()

		if err := fn(ctx); err != nil {
			cm.log.Error().
				Err(err).
				Str("xt_id", xtID.Hex()).
				Str("type", callbackType).
				Msg("Callback failed")
		}
	}()
}
