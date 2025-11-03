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
	runtime2 "github.com/ethereum/go-ethereum/core/vm/runtime"
	"github.com/ethereum/go-ethereum/params"
)

// LoadMailboxRuntimeForCoordinator executes the compiled Mailbox.sol bytecode with the
// provided coordinator address and returns the deployed runtime along with the ABI.
// It relies on the Forge artifact located at ./Mailbox.json.
// The helper is meant for tests and experimentation; callers can install the returned
// runtime in a StateDB and interact with it using the ABI.
func LoadMailboxRuntimeForCoordinator(t *testing.T, coordinator common.Address) ([]byte, abi.ABI) {
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

	code, _, _, err := runtime2.Create(input, cfg)
	if err != nil {
		t.Fatalf("execute mailbox constructor: %v", err)
	}
	if len(code) == 0 {
		t.Fatalf("mailbox runtime empty after constructor execution")
	}

	return code, abiInstance
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
	shanghai := new(uint64)
	cancun := new(uint64)
	*shanghai = 0
	*cancun = 0
	return &params.ChainConfig{
		ChainID:      big.NewInt(99),
		ShanghaiTime: shanghai,
		CancunTime:   cancun,
	}
}
