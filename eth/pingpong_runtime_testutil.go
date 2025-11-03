package eth

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	runtime2 "github.com/ethereum/go-ethereum/core/vm/runtime"
)

// LoadPingPongRuntime executes the compiled PingPong.sol bytecode with the provided
// mailbox address and returns the deployed runtime bytecode along with its ABI.
func LoadPingPongRuntime(t *testing.T, mailbox common.Address) ([]byte, abi.ABI) {
	t.Helper()

	abiInstance, creationBytecode := pingPongArtifact(t)
	args, err := abiInstance.Constructor.Inputs.Pack(mailbox)
	if err != nil {
		t.Fatalf("encode pingpong constructor args: %v", err)
	}

	input := append(append([]byte{}, creationBytecode...), args...)
	cfg := &runtime2.Config{
		GasLimit:    20_000_000,
		ChainConfig: runtimeChainConfig(),
	}

	code, _, _, err := runtime2.Create(input, cfg)
	if err != nil {
		t.Fatalf("execute pingpong constructor: %v", err)
	}
	if len(code) == 0 {
		t.Fatalf("pingpong runtime empty after constructor execution")
	}

	return code, abiInstance
}

func pingPongArtifact(t *testing.T) (abi.ABI, []byte) {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("unable to resolve pingpong artifact path")
	}
	path := filepath.Join(filepath.Dir(filename), "PingPong.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pingpong artifact: %v", err)
	}

	var artifact struct {
		ABI      json.RawMessage `json:"abi"`
		Bytecode struct {
			Object string `json:"object"`
		} `json:"bytecode"`
	}
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("decode pingpong artifact: %v", err)
	}

	creation := common.FromHex(artifact.Bytecode.Object)
	if len(creation) == 0 {
		t.Fatalf("pingpong creation bytecode empty")
	}

	abiInstance, err := abi.JSON(bytes.NewReader(artifact.ABI))
	if err != nil {
		t.Fatalf("parse pingpong ABI: %v", err)
	}

	return abiInstance, creation
}
