package xconsensus

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChainKeyBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "empty byte slice",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "nil byte slice",
			input:    nil,
			expected: "",
		},
		{
			name:     "single byte zero",
			input:    []byte{0},
			expected: "00",
		},
		{
			name:     "single byte non-zero",
			input:    []byte{255},
			expected: "ff",
		},
		{
			name:     "multiple bytes",
			input:    []byte{1, 2, 3, 4},
			expected: "01020304",
		},
		{
			name:     "ethereum mainnet chain id bytes",
			input:    []byte{1},
			expected: "01",
		},
		{
			name:     "polygon chain id bytes",
			input:    []byte{0x89},
			expected: "89",
		},
		{
			name:     "arbitrum chain id bytes",
			input:    []byte{0xa4, 0xb1},
			expected: "a4b1",
		},
		{
			name:     "large chain id bytes",
			input:    []byte{0x01, 0x00, 0x00, 0x00},
			expected: "01000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ChainKeyBytes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestChainKeyUint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    uint64
		expected string
	}{
		{
			name:     "zero",
			input:    0,
			expected: "00",
		},
		{
			name:     "ethereum mainnet",
			input:    1,
			expected: "01",
		},
		{
			name:     "polygon",
			input:    137,
			expected: "89",
		},
		{
			name:     "arbitrum one",
			input:    42161,
			expected: "a4b1",
		},
		{
			name:     "optimism",
			input:    10,
			expected: "0a",
		},
		{
			name:     "binance smart chain",
			input:    56,
			expected: "38",
		},
		{
			name:     "avalanche",
			input:    43114,
			expected: "a86a",
		},
		{
			name:     "fantom",
			input:    250,
			expected: "fa",
		},
		{
			name:     "large chain id",
			input:    16777216, // 2^24
			expected: "01000000",
		},
		{
			name:     "max uint64",
			input:    18446744073709551615,
			expected: "ffffffffffffffff",
		},
		{
			name:     "power of 2",
			input:    256,
			expected: "0100",
		},
		{
			name:     "single byte max",
			input:    255,
			expected: "ff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ChainKeyUint64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestChainKeyConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		chainID  uint64
		expected []byte
	}{
		{
			name:     "ethereum mainnet consistency",
			chainID:  1,
			expected: []byte{1},
		},
		{
			name:     "polygon consistency",
			chainID:  137,
			expected: []byte{0x89},
		},
		{
			name:     "arbitrum consistency",
			chainID:  42161,
			expected: []byte{0xa4, 0xb1},
		},
		{
			name:     "large number consistency",
			chainID:  16777216,
			expected: []byte{0x01, 0x00, 0x00, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			keyFromUint64 := ChainKeyUint64(tt.chainID)
			keyFromBytes := ChainKeyBytes(tt.expected)

			assert.Equal(t, keyFromUint64, keyFromBytes,
				"ChainKeyUint64(%d) should equal ChainKeyBytes(%v)", tt.chainID, tt.expected)
		})
	}
}

func TestChainKeyZeroSpecialCase(t *testing.T) {
	t.Parallel()

	t.Run("zero uint64 returns 00", func(t *testing.T) {
		t.Parallel()
		result := ChainKeyUint64(0)
		assert.Equal(t, "00", result)
	})

	t.Run("zero byte slice returns empty string", func(t *testing.T) {
		t.Parallel()
		result := ChainKeyBytes([]byte{0})
		assert.Equal(t, "00", result)
	})

	t.Run("empty byte slice returns empty string", func(t *testing.T) {
		t.Parallel()
		result := ChainKeyBytes([]byte{})
		assert.Equal(t, "", result)
	})
}

func BenchmarkChainKeyBytes(b *testing.B) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{"small", []byte{1}},
		{"medium", []byte{1, 2, 3, 4}},
		{"large", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ChainKeyBytes(tc.input)
			}
		})
	}
}

func BenchmarkChainKeyUint64(b *testing.B) {
	testCases := []struct {
		name  string
		input uint64
	}{
		{"small", 1},
		{"medium", 42161},
		{"large", 18446744073709551615},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ChainKeyUint64(tc.input)
			}
		})
	}
}

func TestChainKeyRoundTrip(t *testing.T) {
	t.Parallel()

	chainIDs := []uint64{1, 10, 56, 137, 250, 42161, 43114}

	for _, chainID := range chainIDs {
		t.Run(fmt.Sprintf("chain_%d", chainID), func(t *testing.T) {
			t.Parallel()

			directKey := ChainKeyUint64(chainID)

			bigIntBytes := new(big.Int).SetUint64(chainID).Bytes()
			if chainID == 0 {
				bigIntBytes = []byte{0}
			}
			bytesKey := ChainKeyBytes(bigIntBytes)

			if chainID == 0 {
				assert.Equal(t, "00", directKey)
				assert.Equal(t, "00", bytesKey)
			} else {
				assert.Equal(t, directKey, bytesKey,
					"Round trip failed for chain ID %d", chainID)
			}
		})
	}
}
