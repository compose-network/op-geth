package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	rollupv1 "github.com/ethereum/go-ethereum/internal/rollup-shared-publisher/proto/rollup/v1"

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
	Bridge     string `yaml:"bridge"`
}

func (r *Rollup) GetChainID() *big.Int {
	return big.NewInt(r.ChainID)
}

type Config struct {
	Token   string            `yaml:"token"`
	Rollups map[string]Rollup `yaml:"rollups"`
}

func main() {
	batchMode := flag.Bool("batch", false, "Enable batch mode to send multiple transactions")
	numTxs := flag.Int("num", 1, "Number of transactions to send in batch mode")
	delayMs := flag.Int("delay", 500, "Delay in milliseconds between transactions in batch mode")
	amountValue := flag.Int64("amount", 100000, "Token amount to send")
	flag.Parse()

	if *batchMode && *numTxs < 1 {
		log.Fatal("Number of transactions must be at least 1 in batch mode")
	}

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

	tokenA := common.HexToAddress(config.Token)
	bridgeA := common.HexToAddress(rollupA.Bridge)
	bridgeB := common.HexToAddress(rollupB.Bridge)

	// Get starting nonces
	startingNonceA, err := getNonceFor(rollupA.RPC, addressA)
	if err != nil {
		log.Fatal("Failed to get nonce for address A:", err)
	}

	startingNonceB, err := getNonceFor(rollupB.RPC, addressB)
	if err != nil {
		log.Fatal("Failed to get nonce for address B:", err)
	}

	if *batchMode {
		log.Printf("Running in BATCH mode: %d transactions with %dms delay", *numTxs, *delayMs)
		log.Printf("Address A: %s (starting nonce: %d)", addressA.Hex(), startingNonceA)
		log.Printf("Address B: %s (starting nonce: %d)", addressB.Hex(), startingNonceB)
		runBatchMode(
			rollupA, rollupB,
			chainAId, chainBId,
			privateKeyA, privateKeyB,
			addressA, addressB,
			tokenA, bridgeA, bridgeB,
			startingNonceA, startingNonceB,
			*numTxs, time.Duration(*delayMs)*time.Millisecond,
			big.NewInt(*amountValue),
		)
	} else {
		log.Printf("Running in SINGLE mode")
		log.Printf("Address A: %s (nonce: %d)", addressA.Hex(), startingNonceA)
		log.Printf("Address B: %s (nonce: %d)", addressB.Hex(), startingNonceB)
		runSingleMode(
			rollupA, rollupB,
			chainAId, chainBId,
			privateKeyA, privateKeyB,
			addressA, addressB,
			tokenA, bridgeA, bridgeB,
			startingNonceA, startingNonceB,
			big.NewInt(*amountValue),
		)
	}
}

func runSingleMode(
	rollupA, rollupB Rollup,
	chainAId, chainBId *big.Int,
	privateKeyA, privateKeyB *ecdsa.PrivateKey,
	addressA, addressB common.Address,
	tokenA, bridgeA, bridgeB common.Address,
	nonceA, nonceB uint64,
	amount *big.Int,
) {
	ctx := context.Background()

	// Create bridge parameters
	sessionId := generateRandomSessionID()

	log.Printf("Creating single bridge transaction with session ID: %s", sessionId.String())

	// Create transaction pair
	xtRequest, err := createBridgeTransactionPair(
		chainAId, chainBId,
		privateKeyA, privateKeyB,
		addressA, addressB,
		tokenA, bridgeA, bridgeB,
		nonceA, nonceB,
		amount, sessionId,
	)
	if err != nil {
		log.Fatalf("Failed to create transaction pair: %v", err)
	}

	// Send the transaction
	err = sendXTRequest(ctx, rollupA.RPC, xtRequest)
	if err != nil {
		log.Fatalf("Failed to send transaction: %v", err)
	}

	fmt.Printf("✓ Successfully submitted cross-chain transaction\n")
}

func runBatchMode(
	rollupA, rollupB Rollup,
	chainAId, chainBId *big.Int,
	privateKeyA, privateKeyB *ecdsa.PrivateKey,
	addressA, addressB common.Address,
	tokenA, bridgeA, bridgeB common.Address,
	startingNonceA, startingNonceB uint64,
	numTxs int,
	delay time.Duration,
	amount *big.Int,
) {
	ctx := context.Background()

	log.Printf("Starting batch execution...")

	successCount := 0
	failCount := 0

	for i := 0; i < numTxs; i++ {
		currentNonceA := startingNonceA + uint64(i)
		currentNonceB := startingNonceB + uint64(i)

		// Generate unique session ID for each transaction pair
		sessionId := generateRandomSessionID()

		log.Printf("[%d/%d] Creating transaction pair with nonces A=%d, B=%d, session=%s",
			i+1, numTxs, currentNonceA, currentNonceB, sessionId.String())

		// Create transaction pair
		xtRequest, err := createBridgeTransactionPair(
			chainAId, chainBId,
			privateKeyA, privateKeyB,
			addressA, addressB,
			tokenA, bridgeA, bridgeB,
			currentNonceA, currentNonceB,
			amount, sessionId,
		)
		if err != nil {
			log.Printf("✗ [%d/%d] Failed to create transaction pair: %v", i+1, numTxs, err)
			failCount++
			continue
		}

		// Send the transaction
		err = sendXTRequest(ctx, rollupA.RPC, xtRequest)
		if err != nil {
			log.Printf("✗ [%d/%d] Failed to send: %v", i+1, numTxs, err)
			failCount++
			continue
		}

		log.Printf("✓ [%d/%d] Successfully submitted", i+1, numTxs)
		successCount++

		// Sleep before next transaction (except for the last one)
		if i < numTxs-1 {
			time.Sleep(delay)
		}
	}

	fmt.Printf("\n=== Batch Execution Summary ===\n")
	fmt.Printf("Total transactions: %d\n", numTxs)
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failCount)
	fmt.Printf("Success rate: %.1f%%\n", float64(successCount)/float64(numTxs)*100)
}

func createBridgeTransactionPair(
	chainAId, chainBId *big.Int,
	privateKeyA, privateKeyB *ecdsa.PrivateKey,
	addressA, addressB common.Address,
	tokenA, bridgeA, bridgeB common.Address,
	nonceA, nonceB uint64,
	amount, sessionId *big.Int,
) (*rollupv1.XTRequest, error) {
	// Create send transaction (A -> B)
	sendParams := BridgeParams{
		ChainSrc:   chainAId,
		ChainDest:  chainBId,
		Token:      tokenA,
		Sender:     addressA,
		Receiver:   addressB,
		Amount:     amount,
		SessionId:  sessionId,
		DestBridge: bridgeB,
		SrcBridge:  bridgeA,
	}

	signedTx1, err := createSendTransaction(sendParams, nonceA, privateKeyA, bridgeA)
	if err != nil {
		return nil, fmt.Errorf("failed to create send transaction: %w", err)
	}

	rlpSignedTx1, err := signedTx1.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal send transaction: %w", err)
	}

	// Create receive transaction (B receives from A)
	receiveParams := BridgeParams{
		ChainSrc:   chainAId,
		ChainDest:  chainBId,
		Token:      tokenA,
		Sender:     addressA,
		Receiver:   addressB,
		Amount:     amount,
		SessionId:  sessionId,
		DestBridge: bridgeB,
		SrcBridge:  bridgeA,
	}

	signedTx2, err := createReceiveTransaction(receiveParams, nonceB, privateKeyB, bridgeB)
	if err != nil {
		return nil, fmt.Errorf("failed to create receive transaction: %w", err)
	}

	rlpSignedTx2, err := signedTx2.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal receive transaction: %w", err)
	}

	// Create XTRequest
	return &rollupv1.XTRequest{
		Transactions: []*rollupv1.TransactionRequest{
			{
				ChainId:     chainAId.Bytes(),
				Transaction: [][]byte{rlpSignedTx1},
			},
			{
				ChainId:     chainBId.Bytes(),
				Transaction: [][]byte{rlpSignedTx2},
			},
		},
	}, nil
}

func sendXTRequest(ctx context.Context, rpcURL string, xtRequest *rollupv1.XTRequest) error {
	spMsg := &rollupv1.Message{
		SenderId: "xbridge-client",
		Payload: &rollupv1.Message_XtRequest{
			XtRequest: xtRequest,
		},
	}

	encodedPayload, err := proto.Marshal(spMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal XTRequest: %w", err)
	}

	client, err := rpc.DialContext(ctx, rpcURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RPC: %w", err)
	}
	defer client.Close()

	// Call eth_sendXTransaction - response is ignored, we just check if RPC succeeded
	var result interface{}
	err = client.CallContext(ctx, &result, sendTxRPCMethod, hexutil.Encode(encodedPayload))
	if err != nil {
		return fmt.Errorf("RPC call failed: %w", err)
	}

	return nil
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
	max := new(big.Int).Lsh(big.NewInt(1), 63)
	// 	max := new(big.Int).Lsh(big.NewInt(1), 256)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		log.Fatalf("failed to generate random session ID: %v", err)
	}
	return n
}
