package xconsensus

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/compose-network/specs/compose"
	composeproto "github.com/compose-network/specs/compose/proto"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
)

// CallbackManager manages coordinator callbacks with error handling and timeouts
type CallbackManager struct {
	startFn      StartFn
	voteFn       VoteFn
	mailboxMsgFn MailboxMsgFn
	decisionFn   DecisionFn
	blockFn      BlockFn

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

// Set sets the vote callback
func (cm *CallbackManager) SetMailboxMsgCallback(fn MailboxMsgFn) {
	cm.mailboxMsgFn = fn
}

// SetBlockCallback sets the block callback
func (cm *CallbackManager) SetBlockCallback(fn BlockFn) {
	cm.blockFn = fn
}

func (cm *CallbackManager) SetDecisionCallback(fn DecisionFn) {
	cm.decisionFn = fn
}

// InvokeStart calls the start callback with timeout and error handling
func (cm *CallbackManager) InvokeStart(ctx context.Context, from string, instance *composeproto.StartInstance) {
	if cm.startFn == nil {
		return
	}

	instanceID := instance.GetInstanceId()

	go func() {
		ctx, cancel := context.WithTimeout(ctx, cm.timeout)
		defer cancel()

		if err := cm.startFn(ctx, from, instance); err != nil {
			// if we cannot start, we vote NO by default
			cm.InvokeVote((*compose.InstanceID)(instanceID), false, 0)
			cm.log.Error().
				Err(err).
				Str("xt_id", hex.EncodeToString(instanceID)).
				Str("from", from).
				Msg("Start callback failed")
		}
	}()
}

// InvokeVote calls the vote callback with timeout and error handling
func (cm *CallbackManager) InvokeVote(instanceID *compose.InstanceID, vote bool, duration time.Duration) {
	if cm.voteFn == nil {
		return
	}

	cm.invokeCallback("vote", instanceID, func(ctx context.Context) error {
		return cm.voteFn(ctx, instanceID, vote)
	})
}

// InvokeMailboxMessage routes mailbox messages to the registered callback
func (cm *CallbackManager) InvokeMailboxMessage(instanceID *compose.InstanceID, mailboxMsg *composeproto.MailboxMessage) {
	if cm.mailboxMsgFn == nil || instanceID == nil || mailboxMsg == nil {
		return
	}

	msgCopy := *mailboxMsg
	cm.invokeCallback("mailbox", instanceID, func(ctx context.Context) error {
		return cm.mailboxMsgFn(ctx, instanceID, msgCopy)
	})
}

// InvokeDecision routes decided events to the registered callback
func (cm *CallbackManager) InvokeDecision(instanceID *compose.InstanceID, decision bool) {
	if cm.decisionFn == nil || instanceID == nil {
		return
	}

	cm.invokeCallback("decision", instanceID, func(ctx context.Context) error {
		return cm.decisionFn(ctx, instanceID, decision)
	})
}

// InvokeBlock calls the block callback with timeout and error handling
func (cm *CallbackManager) InvokeBlock(ctx context.Context, block *types.Block, xtIDs []*compose.InstanceID) {
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
func (cm *CallbackManager) invokeCallback(callbackType string, instanceID *compose.InstanceID, fn func(context.Context) error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cm.timeout)
		defer cancel()

		if err := fn(ctx); err != nil {
			cm.log.Error().
				Err(err).
				Str("xt_id", instanceID.String()).
				Str("type", callbackType).
				Msg("Callback failed")
		}
	}()
}
