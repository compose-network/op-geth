package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"

	rollupv1 "github.com/compose-network/publisher/proto/rollup/v1"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"

	"log"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	sendTxRPCMethod = "eth_sendXTransaction"
	configFile      = "config.yml"
)

type Rollup struct {
	RPC        string `yaml:"rpc"`
	ChainID    int64  `yaml:"chain_id"`
	PrivateKey string `yaml:"private_key"`
}

func (r *Rollup) GetChainID() *big.Int {
	return big.NewInt(r.ChainID)
}

type Config struct {
	Rollups map[string]Rollup `yaml:"rollups"`
}

func main() {
	config := loadConfigFromYAML(configFile)

	rollupA, exists := config.Rollups["A"]
	if !exists {
		log.Fatal("Rollup 'A' not found in configuration")
	}

	rollupB, exists := config.Rollups["B"]
	if !exists {
		log.Fatal("Rollup 'B' not found in configuration")
	}

	chainAId := rollupA.GetChainID()
	chainBId := rollupB.GetChainID()

	privateKeyA := parsePrivateKey(rollupA.PrivateKey)
	privateKeyB := parsePrivateKey(rollupB.PrivateKey)

	publicKey := privateKeyA.Public()
	publicKeyECDSA, _ := publicKey.(*ecdsa.PublicKey)
	addressA := crypto.PubkeyToAddress(*publicKeyECDSA)

	publicKey = privateKeyB.Public()
	publicKeyECDSA, _ = publicKey.(*ecdsa.PublicKey)
	addressB := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Create ping-pong parameters
	sessionId := generateRandomSessionID()
	pingData := []byte("hello from rollup A")
	pongData := []byte("hello from rollup B")

	pingPongContractAddr := common.HexToAddress(pingPongAddr)

	// Create a ping transaction on Chain A
	// The `ping` function sends a PING message and expects a PONG message back.
	// The sender/receiver of these cross-chain messages are the PingPong contracts themselves.
	pingParams := PingPongParams{
		TxChainID: chainAId,             // Transaction runs on chain A
		ChainSrc:  chainAId,             // Source chain (for reference)
		ChainDest: chainBId,             // otherChain parameter = chain B
		Sender:    pingPongContractAddr, // pongSender: the PingPong contract on chain B
		Receiver:  pingPongContractAddr, // pingReceiver: the PingPong contract on chain B
		SessionId: sessionId,
		Data:      pingData,
	}

	nonceA, err := getNonceFor(rollupA.RPC, addressA)
	if err != nil {
		log.Fatal(err)
	}

	signedTx1, err := createPingTransaction(pingParams, nonceA, privateKeyA)
	if err != nil {
		log.Fatal("Failed to create ping transaction:", err)
	}

	rlpSignedTx1, err := signedTx1.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}

	// Create a pong transaction on Chain B
	// The `pong` function sends a PONG message and expects a PING message back.
	// The sender/receiver of these cross-chain messages are the PingPong contracts themselves.
	pongParams := PingPongParams{
		TxChainID: chainBId,             // Transaction runs on chain B
		ChainSrc:  chainAId,             // otherChain parameter = chain A (where PING comes from)
		ChainDest: chainBId,             // Destination chain (for reference)
		Sender:    pingPongContractAddr, // pingSender: the PingPong contract on chain A
		Receiver:  pingPongContractAddr, // pongReceiver: the PingPong contract on chain A
		SessionId: sessionId,
		Data:      pongData,
	}

	nonceB, err := getNonceFor(rollupB.RPC, addressB)
	if err != nil {
		log.Fatal(err)
	}

	signedTx2, err := createPongTransaction(pongParams, nonceB, privateKeyB)
	if err != nil {
		log.Fatal("Failed to create pong transaction:", err)
	}

	rlpSignedTx2, err := signedTx2.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}

	xtRequest := &rollupv1.XTRequest{
		Transactions: []*rollupv1.TransactionRequest{
			{
				ChainId: chainAId.Bytes(),
				Transaction: [][]byte{
					rlpSignedTx1,
				},
			},
			{
				ChainId: chainBId.Bytes(),
				Transaction: [][]byte{
					rlpSignedTx2,
				},
			},
		},
	}

	spMsg := &rollupv1.Message{
		SenderId: "client",
		Payload: &rollupv1.Message_XtRequest{
			XtRequest: xtRequest,
		},
	}

	encodedPayload, err := proto.Marshal(spMsg)
	if err != nil {
		log.Fatalf("Failed to marshal XTRequest: %v", err)
	}

	fmt.Printf("Successfully encoded ping-pong payload. Size: %d bytes\n", len(encodedPayload))
	fmt.Printf("Session ID: %d\n", sessionId.Int64())
	fmt.Printf("Chain A ID: %d\n", chainAId.Int64())
	fmt.Printf("Chain B ID: %d\n", chainBId.Int64())
	fmt.Printf("Ping data: %s\n", string(pingData))
	fmt.Printf("Pong data: %s\n", string(pongData))
	fmt.Printf("PingPong contract A: %s\n", pingPongAddr)
	fmt.Printf("PingPong contract B: %s\n", pingPongAddr)

	l1Client, err := rpc.Dial(rollupA.RPC)
	if err != nil {
		log.Fatalf("could not connect to custom rpc: %v", err)
	}
	defer l1Client.Close()

	var resultHashes []common.Hash
	err = l1Client.CallContext(context.Background(), &resultHashes, sendTxRPCMethod, hexutil.Encode(encodedPayload))
	if err != nil {
		log.Fatalf("RPC call failed: %v", err)
	}

	fmt.Printf("Transaction submitted successfully\n")
	if len(resultHashes) > 0 {
		fmt.Printf("Result hashes:\n")
		for i, hash := range resultHashes {
			fmt.Printf("  [%d]: %s\n", i, hash.Hex())
		}
	}
}

func loadConfigFromYAML(filename string) Config {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read config file %s: %v", filename, err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Failed to parse YAML config: %v", err)
	}

	return config
}

func parsePrivateKey(privKeyHex string) *ecdsa.PrivateKey {
	if privKeyHex == "" {
		log.Fatal("Private key cannot be empty")
	}

	if len(privKeyHex) >= 2 && privKeyHex[:2] == "0x" {
		privKeyHex = privKeyHex[2:]
	}

	privateKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		log.Fatalf("Failed to parse private key: %v", err)
	}

	return privateKey
}

func getNonceFor(networkRPCAddr string, address common.Address) (uint64, error) {
	client, err := ethclient.Dial(networkRPCAddr)
	if err != nil {
		return 0, err
	}

	nonce, err := client.PendingNonceAt(context.Background(), address)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve pending nonce: %w", err)
	}

	return nonce, nil
}

// generateRandomSessionID returns a random big.Int in the range [0, 2^63-1]
// TODO: use [0, 2^256)
func generateRandomSessionID() *big.Int {
	max := new(big.Int).Lsh(big.NewInt(1), 63) // 2^63
	// 	max := new(big.Int).Lsh(big.NewInt(1), 256)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		log.Fatalf("failed to generate random session ID: %v", err)
	}
	return n
}
