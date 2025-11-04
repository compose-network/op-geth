package consensus

import (
	"math/big"
	"sync"
	"testing"

	pb "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircMessageBuffer_AddAndFlush(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	xtID := &pb.XtID{Hash: []byte("test-xt-1")}
	sourceChain := big.NewInt(77777).Bytes()

	msg1 := &pb.CIRCMessage{
		XtId:        xtID,
		SourceChain: sourceChain,
	}
	msg2 := &pb.CIRCMessage{
		XtId:        xtID,
		SourceChain: sourceChain,
	}

	buffer.Add(msg1)
	buffer.Add(msg2)

	flushed := buffer.Flush(xtID)
	require.Len(t, flushed, 2)
	assert.Equal(t, msg1, flushed[0])
	assert.Equal(t, msg2, flushed[1])

	flushedAgain := buffer.Flush(xtID)
	assert.Empty(t, flushedAgain)
}

func TestCircMessageBuffer_FlushNonExistent(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	xtID := &pb.XtID{Hash: []byte("non-existent")}

	flushed := buffer.Flush(xtID)
	assert.Empty(t, flushed)
}

func TestCircMessageBuffer_MultipleTransactions(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	xtID1 := &pb.XtID{Hash: []byte("test-xt-1")}
	xtID2 := &pb.XtID{Hash: []byte("test-xt-2")}

	msg1 := &pb.CIRCMessage{
		XtId:        xtID1,
		SourceChain: big.NewInt(77777).Bytes(),
	}
	msg2 := &pb.CIRCMessage{
		XtId:        xtID2,
		SourceChain: big.NewInt(88888).Bytes(),
	}

	buffer.Add(msg1)
	buffer.Add(msg2)

	flushed1 := buffer.Flush(xtID1)
	require.Len(t, flushed1, 1)
	assert.Equal(t, msg1, flushed1[0])

	flushed2 := buffer.Flush(xtID2)
	require.Len(t, flushed2, 1)
	assert.Equal(t, msg2, flushed2[0])
}

func TestCircMessageBuffer_ProcessBuffered(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	chainID1 := uint64(77777)
	chainID2 := uint64(88888)
	sourceChain1 := big.NewInt(int64(chainID1)).Bytes()
	sourceChain2 := big.NewInt(int64(chainID2)).Bytes()

	xtID := &pb.XtID{Hash: []byte("test-xt-1")}

	msg1 := &pb.CIRCMessage{
		XtId:        xtID,
		SourceChain: sourceChain1,
	}
	msg2 := &pb.CIRCMessage{
		XtId:        xtID,
		SourceChain: sourceChain2,
	}

	buffer.Add(msg1)
	buffer.Add(msg2)

	chains := []uint64{chainID1, chainID2}
	state := &TwoPCState{
		ParticipatingChains: make(map[string]struct{}),
		CIRCMessages:        make(map[string][]*pb.CIRCMessage),
	}

	for _, chainID := range chains {
		state.ParticipatingChains[ChainKeyUint64(chainID)] = struct{}{}
	}

	buffer.ProcessBuffered(xtID, state, log)

	assert.Len(t, state.CIRCMessages, 2)
	assert.Len(t, state.CIRCMessages[ChainKeyBytes(sourceChain1)], 1)
	assert.Len(t, state.CIRCMessages[ChainKeyBytes(sourceChain2)], 1)

	flushedAgain := buffer.Flush(xtID)
	assert.Empty(t, flushedAgain)
}

func TestCircMessageBuffer_ProcessBufferedNonParticipant(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	chainID1 := uint64(77777)
	chainID2 := uint64(88888)
	sourceChain1 := big.NewInt(int64(chainID1)).Bytes()
	sourceChain2 := big.NewInt(int64(chainID2)).Bytes()

	xtID := &pb.XtID{Hash: []byte("test-xt-1")}

	msg1 := &pb.CIRCMessage{
		XtId:        xtID,
		SourceChain: sourceChain1,
	}
	msg2 := &pb.CIRCMessage{
		XtId:        xtID,
		SourceChain: sourceChain2,
	}

	buffer.Add(msg1)
	buffer.Add(msg2)

	state := &TwoPCState{
		ParticipatingChains: make(map[string]struct{}),
		CIRCMessages:        make(map[string][]*pb.CIRCMessage),
	}

	state.ParticipatingChains[ChainKeyUint64(chainID1)] = struct{}{}

	buffer.ProcessBuffered(xtID, state, log)

	assert.Len(t, state.CIRCMessages, 1)
	assert.Len(t, state.CIRCMessages[ChainKeyBytes(sourceChain1)], 1)
	assert.NotContains(t, state.CIRCMessages, ChainKeyBytes(sourceChain2))
}

func TestCircMessageBuffer_ProcessBufferedEmpty(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	xtID := &pb.XtID{Hash: []byte("test-xt-1")}

	state := &TwoPCState{
		ParticipatingChains: make(map[string]struct{}),
		CIRCMessages:        make(map[string][]*pb.CIRCMessage),
	}

	buffer.ProcessBuffered(xtID, state, log)

	assert.Empty(t, state.CIRCMessages)
}

func TestCircMessageBuffer_ConcurrentAddAndFlush(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	const numTransactions = 100
	const messagesPerTx = 10

	var wg sync.WaitGroup

	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func(txNum int) {
			defer wg.Done()

			xtID := &pb.XtID{Hash: []byte{byte(txNum)}}

			for j := 0; j < messagesPerTx; j++ {
				msg := &pb.CIRCMessage{
					XtId:        xtID,
					SourceChain: big.NewInt(int64(77777 + j)).Bytes(),
				}
				buffer.Add(msg)
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < numTransactions; i++ {
		xtID := &pb.XtID{Hash: []byte{byte(i)}}
		flushed := buffer.Flush(xtID)
		assert.Len(t, flushed, messagesPerTx)
	}
}

func TestCircMessageBuffer_ConcurrentProcessBuffered(t *testing.T) {
	log := zerolog.Nop()
	buffer := newCircMessageBuffer(log)

	const numTransactions = 50
	chainID := uint64(77777)
	sourceChain := big.NewInt(int64(chainID)).Bytes()

	for i := 0; i < numTransactions; i++ {
		xtID := &pb.XtID{Hash: []byte{byte(i)}}
		msg := &pb.CIRCMessage{
			XtId:        xtID,
			SourceChain: sourceChain,
		}
		buffer.Add(msg)
	}

	var wg sync.WaitGroup

	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func(txNum int) {
			defer wg.Done()

			xtID := &pb.XtID{Hash: []byte{byte(txNum)}}
			state := &TwoPCState{
				ParticipatingChains: make(map[string]struct{}),
				CIRCMessages:        make(map[string][]*pb.CIRCMessage),
			}
			state.ParticipatingChains[ChainKeyUint64(chainID)] = struct{}{}

			buffer.ProcessBuffered(xtID, state, log)

			assert.Len(t, state.CIRCMessages, 1)
			assert.Len(t, state.CIRCMessages[ChainKeyBytes(sourceChain)], 1)
		}(i)
	}

	wg.Wait()
}
