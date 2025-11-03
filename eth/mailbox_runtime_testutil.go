package eth

import (
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	runtime2 "github.com/ethereum/go-ethereum/core/vm/runtime"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// LoadMailboxRuntimeForCoordinator executes the compiled Mailbox.sol bytecode with the
// provided coordinator address and returns the deployed runtime, initial storage, and ABI.
// It relies on the Forge artifact located at ./Mailbox.json.
// The helper is meant for tests and experimentation; callers can install the returned
// runtime and storage in a StateDB and interact with it using the ABI.
func LoadMailboxRuntimeForCoordinator(t *testing.T, coordinator common.Address) ([]byte, map[common.Hash]common.Hash, abi.ABI) {
	t.Helper()

	abiInstance, creationBytecode := mailboxArtifact(t)

	args, err := abiInstance.Constructor.Inputs.Pack(coordinator)
	if err != nil {
		t.Fatalf("encode mailbox constructor args: %v", err)
	}

	input := append(append([]byte{}, creationBytecode...), args...)

	cfg := &runtime2.Config{
		GasLimit:    20_000_000,
		ChainConfig: runtimeChainConfig(),
	}

	code, addr, _, err := runtime2.Create(input, cfg)
	if err != nil {
		t.Fatalf("execute mailbox constructor: %v", err)
	}
	if len(code) == 0 {
		t.Fatalf("mailbox runtime empty after constructor execution")
	}

	if cfg.State == nil {
		t.Fatalf("runtime state missing after mailbox constructor execution")
	}
	if _, err := cfg.State.Commit(0, true, false); err != nil {
		t.Fatalf("commit mailbox state: %v", err)
	}
	initStorage := dumpContractStorage(t, cfg.State, addr)

	return code, initStorage, abiInstance
}

func dumpContractStorage(t *testing.T, stateDB *state.StateDB, addr common.Address) map[common.Hash]common.Hash {
	storageTrie, err := stateDB.OpenStorageTrie(addr)
	if err != nil {
		t.Fatalf("open mailbox storage trie: %v", err)
	}
	iter, err := storageTrie.NodeIterator(nil)
	if err != nil {
		t.Fatalf("iterate mailbox storage trie: %v", err)
	}
	storage := make(map[common.Hash]common.Hash)
	for it := trie.NewIterator(iter); it.Next(); {
		_, content, _, err := rlp.Split(it.Value)
		if err != nil {
			t.Fatalf("decode mailbox storage slot: %v", err)
		}
		key := storageTrie.GetKey(it.Key)
		if key == nil {
			continue
		}
		storage[common.BytesToHash(key)] = common.BytesToHash(content)
	}
	return storage
}

func mailboxArtifact(t *testing.T) (abi.ABI, []byte) {
	t.Helper()

	// Read Mailbox.json (result from Forge compilation of the mailbox contract)
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("unable to resolve mailbox artifact path")
	}
	path := filepath.Join(filepath.Dir(filename), "Mailbox.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read mailbox artifact: %v", err)
	}

	// Decode ABI and Bytecode
	var artifact struct {
		ABI      json.RawMessage `json:"abi"`
		Bytecode struct {
			Object string `json:"object"`
		} `json:"bytecode"`
	}
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("decode mailbox artifact: %v", err)
	}

	creation := common.FromHex(artifact.Bytecode.Object)
	if len(creation) == 0 {
		t.Fatalf("mailbox creation bytecode empty")
	}

	abiInstance, err := abi.JSON(bytes.NewReader(artifact.ABI))
	if err != nil {
		t.Fatalf("parse mailbox ABI: %v", err)
	}

	return abiInstance, creation
}

func runtimeChainConfig() *params.ChainConfig {
	cfg := *params.AllDevChainProtocolChanges
	cfg.ChainID = big.NewInt(99)
	return &cfg
}
