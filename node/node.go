// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package node

import (
	"context"
	"crypto/ecdsa"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	ssvlog "github.com/compose-network/publisher/log"
	"github.com/compose-network/publisher/x/auth"
	"github.com/compose-network/publisher/x/consensus"
	"github.com/compose-network/publisher/x/superblock/sequencer"
	"github.com/compose-network/publisher/x/superblock/sequencer/bootstrap"
	"github.com/compose-network/publisher/x/superblock/slot"
	"github.com/compose-network/publisher/x/transport"
	"github.com/compose-network/publisher/x/transport/tcp"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/rs/zerolog"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gofrs/flock"
)

// Node is a container on which services can be registered.
type Node struct {
	eventmux      *event.TypeMux
	config        *Config
	accman        *accounts.Manager
	log           log.Logger
	keyDir        string        // key store directory
	keyDirTemp    bool          // If true, key directory will be removed by Stop
	dirLock       *flock.Flock  // prevents concurrent use of instance directory
	stop          chan struct{} // Channel to wait for termination notifications
	server        *p2p.Server   // Currently running P2P networking layer
	startStopLock sync.Mutex    // Start/Stop are protected by an additional lock
	state         int           // Tracks state of node lifecycle

	lock          sync.Mutex
	lifecycles    []Lifecycle // All registered backends, services, and auxiliary services that have a lifecycle
	rpcAPIs       []rpc.API   // List of APIs currently provided by the node
	http          *httpServer //
	ws            *httpServer //
	httpAuth      *httpServer //
	wsAuth        *httpServer //
	ipc           *ipcServer  // Stores information about the ipc http server
	inprocHandler *rpc.Server // In-process RPC request handler to process the API requests

	coordinator          consensus.Coordinator
	spClient             transport.Client
	sequencerAddrs       map[string]string // Map of sequencer chain IDs to their addresses
	sequencerKey         *ecdsa.PrivateKey
	coordinatorKey       *ecdsa.PrivateKey
	sequencerCoordinator *sequencer.SequencerCoordinator
	ssvLogger            zerolog.Logger
	sequencerRuntime     *bootstrap.Runtime

	databases map[*closeTrackingDB]struct{} // All open databases
}

const (
	initializingState = iota
	runningState
	closedState
)

// New creates a new P2P node, ready for protocol registration.
func New(conf *Config) (*Node, error) {
	// Copy config and resolve the datadir so future changes to the current
	// working directory don't affect the node.
	confCopy := *conf
	conf = &confCopy
	if conf.DataDir != "" {
		absdatadir, err := filepath.Abs(conf.DataDir)
		if err != nil {
			return nil, err
		}
		conf.DataDir = absdatadir
	}
	if conf.Logger == nil {
		conf.Logger = log.New()
	}

	// Ensure that the instance name doesn't cause weird conflicts with
	// other files in the data directory.
	if strings.ContainsAny(conf.Name, `/\`) {
		return nil, errors.New(`Config.Name must not contain '/' or '\'`)
	}
	if conf.Name == datadirDefaultKeyStore {
		return nil, errors.New(`Config.Name cannot be "` + datadirDefaultKeyStore + `"`)
	}
	if strings.HasSuffix(conf.Name, ".ipc") {
		return nil, errors.New(`Config.Name cannot end in ".ipc"`)
	}
	server := rpc.NewServer()
	server.SetBatchLimits(conf.BatchRequestLimit, conf.BatchResponseMaxSize)

	// SSV logger
	ssvLogger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("component", "ssv").
		Logger()

	node := &Node{
		config:        conf,
		inprocHandler: server,
		eventmux:      new(event.TypeMux),
		log:           conf.Logger,
		stop:          make(chan struct{}),
		server:        &p2p.Server{Config: conf.P2P},
		databases:     make(map[*closeTrackingDB]struct{}),
		ssvLogger:     ssvLogger,
	}

	// Register built-in APIs.
	node.rpcAPIs = append(node.rpcAPIs, node.apis()...)

	// Acquire the instance directory lock.
	if err := node.openDataDir(); err != nil {
		return nil, err
	}
	keyDir, isEphem, err := conf.GetKeyStoreDir()
	if err != nil {
		return nil, err
	}
	node.keyDir = keyDir
	node.keyDirTemp = isEphem
	// Creates an empty AccountManager with no backends. Callers (e.g. cmd/geth)
	// are required to add the backends later on.
	node.accman = accounts.NewManager(nil)

	// Initialize the p2p server. This creates the node key and discovery databases.
	node.server.Config.PrivateKey = node.config.NodeKey()
	node.server.Config.Name = node.config.NodeName()
	node.server.Config.Logger = node.log
	node.config.checkLegacyFiles()
	if node.server.Config.NodeDatabase == "" {
		node.server.Config.NodeDatabase = node.config.NodeDB()
	}

	// Check HTTP/WS prefixes are valid.
	if err := validatePrefix("HTTP", conf.HTTPPathPrefix); err != nil {
		return nil, err
	}
	if err := validatePrefix("WebSocket", conf.WSPathPrefix); err != nil {
		return nil, err
	}

	// Configure RPC servers.
	node.http = newHTTPServer(node.log, conf.HTTPTimeouts)
	node.httpAuth = newHTTPServer(node.log, conf.HTTPTimeouts)
	node.ws = newHTTPServer(node.log, rpc.DefaultHTTPTimeouts)
	node.wsAuth = newHTTPServer(node.log, rpc.DefaultHTTPTimeouts)
	node.ipc = newIPCServer(node.log, conf.IPCEndpoint())
	serverCfg := tcp.DefaultServerConfig()
	serverCfg.ListenAddr = conf.SPListenAddr

	var authManager auth.Manager
	if conf.SequencerKey != "" {
		privKey := parsePrivateKey(conf.SequencerKey)
		//authManager = auth.NewManager(privKey)
		node.sequencerKey = privKey

		ssvLogger.Info().
			//Str("public_key", fmt.Sprintf("%x", authManager.PublicKeyBytes())).
			//Str("address", authManager.Address()).
			Msg("Sequencer initialized with key")
	}

	if conf.CoordinatorKey == "" {
		return nil, errors.New("coordinator key is required (use --coordinator.key flag)")
	}
	node.coordinatorKey = parsePrivateKey(conf.CoordinatorKey)
	if node.coordinatorKey == nil {
		return nil, errors.New("failed to parse coordinator key")
	}
	coordinatorAddr := crypto.PubkeyToAddress(node.coordinatorKey.PublicKey)
	ssvLogger.Info().
		Str("address", coordinatorAddr.Hex()).
		Msg("Coordinator initialized with key")

	//if authManager != nil {
	//	myChainID := fmt.Sprintf("%d", chainID)
	//	setupSequencerAuth(myChainID, authManager)
	//}

	// Determine our ChainID
	if conf.NetworkId == 0 {
		return nil, errors.New("networkid is required for rollup/SP wiring; set --networkid to your L2 chain id")
	}
	chainIDInt64 := int64(conf.NetworkId)

	// Parse SequencerAddrs
	addrByID := map[uint64]string{}
	if strings.TrimSpace(conf.SequencerAddrs) == "" {
		node.log.Warn("sequencer.addrs is empty; proceeding without P2P peers (SP/CIRC limited)")
	}

	entries := strings.Split(conf.SequencerAddrs, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			node.log.Error("Invalid sequencer address format, expected id:host:port", "entry", entry)
			continue
		}
		idStr := strings.TrimSpace(parts[0])
		host := strings.TrimSpace(parts[1])
		port := strings.TrimSpace(parts[2])

		idBig, ok := new(big.Int).SetString(idStr, 10)
		if !ok || idBig.Sign() <= 0 {
			return nil, fmt.Errorf("invalid chain id %q in sequencer.addrs entry %q (decimal expected)", idStr, entry)
		}
		id := idBig.Uint64()
		if _, exists := addrByID[id]; exists {
			return nil, fmt.Errorf("duplicate chain id %d in sequencer.addrs", id)
		}
		addrByID[id] = net.JoinHostPort(host, port)
	}

	// If our chain id exists in the list and listen port differs: warn about it.
	if selected, ok := addrByID[conf.NetworkId]; ok {
		if _, listenPort, err := net.SplitHostPort(conf.SPListenAddr); err == nil {
			if _, selPort, err2 := net.SplitHostPort(selected); err2 == nil && listenPort != "" && selPort != "" && selPort != listenPort {
				node.log.Warn("rollup listen port differs from sequencer.addrs entry",
					"chainID", conf.NetworkId, "listen", conf.SPListenAddr, "selected", selected)
			}
		}
	} else {
		node.log.Info("no self entry in sequencer.addrs; proceeding", "chainID", conf.NetworkId)
	}

	_, addrs := generateClients(conf.SequencerAddrs, authManager)
	// Exclude self from peer list to avoid dialing ourselves
	selfKey := consensus.ChainKeyUint64(uint64(chainIDInt64))
	delete(addrs, selfKey)
	node.sequencerAddrs = addrs
	node.sequencerKey = parsePrivateKey(conf.SequencerKey)

	// Bootstrap SBCP runtime (coordinator, SP client, P2P) - handles all connections
	chainIDBytes := big.NewInt(chainIDInt64).Bytes()
	rt, err := bootstrap.Setup(context.Background(), bootstrap.Config{
		ChainID:         chainIDBytes,
		SPAddr:          conf.SPAddr,
		PeerAddrs:       addrs,
		P2PListenAddr:   conf.SPListenAddr,
		Log:             ssvLogger,
		SlotDuration:    6 * time.Second,
		SlotSealCutover: 2.0 / 3.0,
	})
	if err != nil {
		return nil, fmt.Errorf("bootstrap setup failed: %w", err)
	}
	node.sequencerRuntime = rt
	node.spClient = rt.SPClient
	if sc, ok := rt.Coordinator.(*sequencer.SequencerCoordinator); ok {
		node.sequencerCoordinator = sc
	} else {
		node.sequencerCoordinator = sequencer.NewSequencerCoordinator(rt.Coordinator.Consensus(), sequencer.Config{ChainID: chainIDBytes, Slot: slot.Config{Duration: 12 * time.Second, SealCutover: 2.0 / 3.0, GenesisTime: time.Now()}}, rt.SPClient, ssvLogger)
	}

	ssvLogger.Info().
		Int64("chain_id", chainIDInt64).
		Str("sp_addr", conf.SPAddr).
		Msg("Node initialized with SBCP runtime")

	return node, nil
}

func setupSequencerAuth(myChainID string, authManager auth.Manager) {
	spPublicKey := "03c720e214dccd730db38d10daca5f86d9e95f068ca5eb3930b7d0126e7a37c4a1"

	spPubKeyBytes, err := hex.DecodeString(spPublicKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode SP public key: %v", err))
	}

	if err := authManager.AddTrustedKey("shared-publisher", spPubKeyBytes); err != nil {
		panic(fmt.Sprintf("Failed to add SP trusted key: %v", err))
	}

	fmt.Printf("Added SP public key: %x\n", spPubKeyBytes)

	otherSequencers := map[string]string{
		"11111": "0210b140eb38ee476dc182ea1e3847055d4e168e665bed78f4a5e6eee9ede64994",
		"22222": "03eb9bbd096168554c6b9fcd82dcd663e73b8b3b7ad1c247fd3e392d7194315323",
	}

	for id, pubKeyHex := range otherSequencers {
		if id != myChainID {
			pubKeyBytes, err := hex.DecodeString(pubKeyHex)
			if err != nil {
				panic(fmt.Sprintf("Failed to decode public key for %s: %v", id, err))
			}
			if err := authManager.AddTrustedKey(id, pubKeyBytes); err != nil {
				panic(fmt.Sprintf("Failed to add trusted key for %s: %v", id, err))
			}
			fmt.Printf("Added trusted sequencer: %s with key %x\n", id, pubKeyBytes)
		}
	}
}

func parsePrivateKey(privKeyHex string) *ecdsa.PrivateKey {
	if privKeyHex == "" {
		log.Error("Private key cannot be empty")
	}

	if len(privKeyHex) >= 2 && privKeyHex[:2] == "0x" {
		privKeyHex = privKeyHex[2:]
	}

	privateKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		log.Error("Failed to parse private key: %v", err)
	}

	return privateKey
}

func generateClients(addrs string, authManager auth.Manager) (map[string]transport.Client, map[string]string) {
	clients := make(map[string]transport.Client)
	addresses := make(map[string]string)

	if addrs == "" {
		return clients, addresses
	}

	entries := strings.Split(addrs, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		// Parse id:host:port format
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			log.Error("Invalid sequencer address format, expected id:host:port", "entry", entry)
			continue
		}

		rawID := strings.TrimSpace(parts[0])
		host := strings.TrimSpace(parts[1])
		port := strings.TrimSpace(parts[2])

		if rawID == "" || host == "" || port == "" {
			log.Error("Empty component in sequencer address", "entry", entry)
			continue
		}

		serverAddr := net.JoinHostPort(host, port)
		// Normalize chain ID key: accept decimal or hex and store canonical hex key
		var key string
		if bi, ok := new(big.Int).SetString(rawID, 10); ok {
			key = consensus.ChainKeyUint64(bi.Uint64())
		} else {
			// Assume hex; strip optional 0x and lowercase
			rid := strings.ToLower(strings.TrimPrefix(rawID, "0x"))
			key = rid
		}

		addresses[key] = serverAddr

		clientConfig := tcp.ClientConfig{
			ServerAddr:      serverAddr,
			ConnectTimeout:  10 * time.Second,
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    30 * time.Second,
			ReconnectDelay:  5 * time.Second,
			MaxMessageSize:  10 * 1024 * 1024,
			KeepAlive:       true,
			KeepAlivePeriod: 30 * time.Second,
			ClientID:        key,
		}

		slog := ssvlog.New("info", true)
		tcpClient := tcp.NewClient(clientConfig, slog.Logger)
		clients[key] = tcpClient
	}
	return clients, addresses
}

// Start starts all registered lifecycles, RPC services and p2p networking.
// Node can only be started once.
func (n *Node) Start() error {
	n.startStopLock.Lock()
	defer n.startStopLock.Unlock()

	n.lock.Lock()
	switch n.state {
	case runningState:
		n.lock.Unlock()
		return ErrNodeRunning
	case closedState:
		n.lock.Unlock()
		return ErrNodeStopped
	}
	n.state = runningState
	// open networking and RPC endpoints
	err := n.openEndpoints()
	lifecycles := make([]Lifecycle, len(n.lifecycles))
	copy(lifecycles, n.lifecycles)
	n.lock.Unlock()

	// Check if endpoint startup failed.
	if err != nil {
		n.doClose(nil)
		return err
	}
	// Start all registered lifecycles.
	var started []Lifecycle
	for _, lifecycle := range lifecycles {
		if err = lifecycle.Start(); err != nil {
			break
		}
		started = append(started, lifecycle)
	}
	// Check if any lifecycle failed to start.
	if err != nil {
		n.stopServices(started)
		n.doClose(nil)
	}
	return err
}

// Close stops the Node and releases resources acquired in
// Node constructor New.
func (n *Node) Close() error {
	n.startStopLock.Lock()
	defer n.startStopLock.Unlock()

	n.lock.Lock()
	state := n.state
	n.lock.Unlock()
	switch state {
	case initializingState:
		// The node was never started.
		return n.doClose(nil)
	case runningState:
		// The node was started, release resources acquired by Start().
		var errs []error
		if err := n.stopServices(n.lifecycles); err != nil {
			errs = append(errs, err)
		}
		return n.doClose(errs)
	case closedState:
		return ErrNodeStopped
	default:
		panic(fmt.Sprintf("node is in unknown state %d", state))
	}
}

// doClose releases resources acquired by New(), collecting errors.
func (n *Node) doClose(errs []error) error {
	// Close databases. This needs the lock because it needs to
	// synchronize with OpenDatabase*.
	n.lock.Lock()
	n.state = closedState
	errs = append(errs, n.closeDatabases()...)
	n.lock.Unlock()

	if err := n.accman.Close(); err != nil {
		errs = append(errs, err)
	}
	if n.keyDirTemp {
		if err := os.RemoveAll(n.keyDir); err != nil {
			errs = append(errs, err)
		}
	}

	// Release instance directory lock.
	n.closeDataDir()

	// Unblock n.Wait.
	close(n.stop)

	// Report any errors that might have occurred.
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return fmt.Errorf("%v", errs)
	}
}

// openEndpoints starts all network and RPC endpoints.
func (n *Node) openEndpoints() error {
	// start networking endpoints
	n.log.Info("Starting peer-to-peer node", "instance", n.server.Name)
	if err := n.server.Start(); err != nil {
		return convertFileLockError(err)
	}

	// P2P server is managed by SBCP runtime; do not start a separate server here

	//// Connect to shared publisher - pass the address
	//err := n.spClient.Connect(context.Background(), n.config.SPAddr)
	//if err != nil {
	//	return err
	//}

	// Start SBCP runtime (SP client, P2P server, and peers)
	if n.sequencerRuntime != nil {
		if err := n.sequencerRuntime.Start(context.Background()); err != nil {
			n.log.Error("Failed to start SBCP runtime", "err", err)
		}
	}

	// SBCP runtime connects peers; no manual dialing needed here
	// start RPC endpoints
	err := n.startRPC()
	if err != nil {
		n.stopRPC()
		n.server.Stop()
	}
	return err
}

// stopServices terminates running services, RPC and p2p networking.
// It is the inverse of Start.
func (n *Node) stopServices(running []Lifecycle) error {
	ctx := context.Background()
	n.stopRPC()

	// Stop running lifecycles in reverse order.
	failure := &StopError{Services: make(map[reflect.Type]error)}
	for i := len(running) - 1; i >= 0; i-- {
		if err := running[i].Stop(); err != nil {
			failure.Services[reflect.TypeOf(running[i])] = err
		}
	}

	if n.sequencerRuntime != nil {
		_ = n.sequencerRuntime.Stop(ctx)
	}

	// Stop p2p networking.
	n.server.Stop()

	if n.spClient != nil {
		_ = n.spClient.Disconnect(ctx)
	}
	if n.coordinator != nil {
		n.coordinator.Stop(ctx)
	}

	if len(failure.Services) > 0 {
		return failure
	}
	return nil
}

func (n *Node) openDataDir() error {
	if n.config.DataDir == "" {
		return nil // ephemeral
	}

	instdir := filepath.Join(n.config.DataDir, n.config.name())
	if err := os.MkdirAll(instdir, 0700); err != nil {
		return err
	}
	// Lock the instance directory to prevent concurrent use by another instance as well as
	// accidental use of the instance directory as a database.
	n.dirLock = flock.New(filepath.Join(instdir, "LOCK"))

	if locked, err := n.dirLock.TryLock(); err != nil {
		return err
	} else if !locked {
		return ErrDatadirUsed
	}
	return nil
}

func (n *Node) closeDataDir() {
	// Release instance directory lock.
	if n.dirLock != nil && n.dirLock.Locked() {
		n.dirLock.Unlock()
		n.dirLock = nil
	}
}

// ObtainJWTSecret loads the jwt-secret from the provided config. If the file is not
// present, it generates a new secret and stores to the given location.
func ObtainJWTSecret(fileName string) ([]byte, error) {
	// try reading from file
	if data, err := os.ReadFile(fileName); err == nil {
		jwtSecret := common.FromHex(strings.TrimSpace(string(data)))
		if len(jwtSecret) == 32 {
			log.Info("Loaded JWT secret file", "path", fileName, "crc32", fmt.Sprintf("%#x", crc32.ChecksumIEEE(jwtSecret)))
			return jwtSecret, nil
		}
		log.Error("Invalid JWT secret", "path", fileName, "length", len(jwtSecret))
		return nil, errors.New("invalid JWT secret")
	}
	// Need to generate one
	jwtSecret := make([]byte, 32)
	crand.Read(jwtSecret)
	// if we're in --dev mode, don't bother saving, just show it
	if fileName == "" {
		log.Info("Generated ephemeral JWT secret", "secret", hexutil.Encode(jwtSecret))
		return jwtSecret, nil
	}
	if err := os.WriteFile(fileName, []byte(hexutil.Encode(jwtSecret)), 0600); err != nil {
		return nil, err
	}
	log.Info("Generated JWT secret", "path", fileName)
	return jwtSecret, nil
}

// obtainJWTSecret loads the jwt-secret, either from the provided config,
// or from the default location. If neither of those are present, it generates
// a new secret and stores to the default location.
func (n *Node) obtainJWTSecret(cliParam string) ([]byte, error) {
	fileName := cliParam
	if len(fileName) == 0 {
		// no path provided, use default
		fileName = n.ResolvePath(datadirJWTKey)
	}
	return ObtainJWTSecret(fileName)
}

// startRPC is a helper method to configure all the various RPC endpoints during node
// startup. It's not meant to be called at any time afterwards as it makes certain
// assumptions about the state of the node.
func (n *Node) startRPC() error {
	if err := n.startInProc(n.rpcAPIs); err != nil {
		return err
	}

	// Configure IPC.
	if n.ipc.endpoint != "" {
		if err := n.ipc.start(n.rpcAPIs); err != nil {
			return err
		}
	}
	var (
		servers           []*httpServer
		openAPIs, allAPIs = n.getAPIs()
	)

	rpcConfig := rpcEndpointConfig{
		batchItemLimit:         n.config.BatchRequestLimit,
		batchResponseSizeLimit: n.config.BatchResponseMaxSize,
	}

	initHttp := func(server *httpServer, port int) error {
		if err := server.setListenAddr(n.config.HTTPHost, port); err != nil {
			return err
		}
		if err := server.enableRPC(openAPIs, httpConfig{
			CorsAllowedOrigins: n.config.HTTPCors,
			Vhosts:             n.config.HTTPVirtualHosts,
			Modules:            n.config.HTTPModules,
			prefix:             n.config.HTTPPathPrefix,
			rpcEndpointConfig:  rpcConfig,
		}); err != nil {
			return err
		}
		servers = append(servers, server)
		return nil
	}

	initWS := func(port int) error {
		server := n.wsServerForPort(port, false)
		if err := server.setListenAddr(n.config.WSHost, port); err != nil {
			return err
		}
		if err := server.enableWS(openAPIs, wsConfig{
			Modules:           n.config.WSModules,
			Origins:           n.config.WSOrigins,
			prefix:            n.config.WSPathPrefix,
			rpcEndpointConfig: rpcConfig,
		}); err != nil {
			return err
		}
		servers = append(servers, server)
		return nil
	}

	initAuth := func(port int, secret []byte) error {
		authModules := DefaultAuthModules
		if slices.Contains(n.config.HTTPModules, "miner") {
			authModules = append(authModules, "miner")
		}
		if slices.Contains(n.config.HTTPModules, "debug") {
			authModules = append(authModules, "debug")
		}

		// Enable auth via HTTP
		server := n.httpAuth
		if err := server.setListenAddr(n.config.AuthAddr, port); err != nil {
			return err
		}
		sharedConfig := rpcEndpointConfig{
			jwtSecret:              secret,
			batchItemLimit:         engineAPIBatchItemLimit,
			batchResponseSizeLimit: engineAPIBatchResponseSizeLimit,
			httpBodyLimit:          engineAPIBodyLimit,
		}
		err := server.enableRPC(allAPIs, httpConfig{
			CorsAllowedOrigins: DefaultAuthCors,
			Vhosts:             n.config.AuthVirtualHosts,
			Modules:            authModules,
			prefix:             DefaultAuthPrefix,
			rpcEndpointConfig:  sharedConfig,
		})
		if err != nil {
			return err
		}
		servers = append(servers, server)

		// Enable auth via WS
		server = n.wsServerForPort(port, true)
		if err := server.setListenAddr(n.config.AuthAddr, port); err != nil {
			return err
		}
		if err := server.enableWS(allAPIs, wsConfig{
			Modules:           DefaultAuthModules,
			Origins:           DefaultAuthOrigins,
			prefix:            DefaultAuthPrefix,
			rpcEndpointConfig: sharedConfig,
		}); err != nil {
			return err
		}
		servers = append(servers, server)
		return nil
	}

	// Set up HTTP.
	if n.config.HTTPHost != "" {
		// Configure legacy unauthenticated HTTP.
		if err := initHttp(n.http, n.config.HTTPPort); err != nil {
			return err
		}
	}
	// Configure WebSocket.
	if n.config.WSHost != "" {
		// legacy unauthenticated
		if err := initWS(n.config.WSPort); err != nil {
			return err
		}
	}
	// Configure authenticated API
	if len(openAPIs) != len(allAPIs) {
		jwtSecret, err := n.obtainJWTSecret(n.config.JWTSecret)
		if err != nil {
			return err
		}
		if err := initAuth(n.config.AuthPort, jwtSecret); err != nil {
			return err
		}
	}
	// Start the servers
	for _, server := range servers {
		if err := server.start(); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) wsServerForPort(port int, authenticated bool) *httpServer {
	httpServer, wsServer := n.http, n.ws
	if authenticated {
		httpServer, wsServer = n.httpAuth, n.wsAuth
	}
	if n.config.HTTPHost == "" || httpServer.port == port {
		return httpServer
	}
	return wsServer
}

func (n *Node) stopRPC() {
	n.http.stop()
	n.ws.stop()
	n.httpAuth.stop()
	n.wsAuth.stop()
	n.ipc.stop()
	n.stopInProc()
}

// startInProc registers all RPC APIs on the inproc server.
func (n *Node) startInProc(apis []rpc.API) error {
	for _, api := range apis {
		if err := n.inprocHandler.RegisterName(api.Namespace, api.Service); err != nil {
			return err
		}
	}
	return nil
}

// stopInProc terminates the in-process RPC endpoint.
func (n *Node) stopInProc() {
	n.inprocHandler.Stop()
}

// Wait blocks until the node is closed.
func (n *Node) Wait() {
	<-n.stop
}

// RegisterLifecycle registers the given Lifecycle on the node.
func (n *Node) RegisterLifecycle(lifecycle Lifecycle) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.state != initializingState {
		panic("can't register lifecycle on running/stopped node")
	}
	if slices.Contains(n.lifecycles, lifecycle) {
		panic(fmt.Sprintf("attempt to register lifecycle %T more than once", lifecycle))
	}
	n.lifecycles = append(n.lifecycles, lifecycle)
}

// RegisterProtocols adds backend's protocols to the node's p2p server.
func (n *Node) RegisterProtocols(protocols []p2p.Protocol) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.state != initializingState {
		panic("can't register protocols on running/stopped node")
	}
	n.server.Protocols = append(n.server.Protocols, protocols...)
}

// RegisterAPIs registers the APIs a service provides on the node.
func (n *Node) RegisterAPIs(apis []rpc.API) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.state != initializingState {
		panic("can't register APIs on running/stopped node")
	}
	n.rpcAPIs = append(n.rpcAPIs, apis...)
}

// getAPIs return two sets of APIs, both the ones that do not require
// authentication, and the complete set
func (n *Node) getAPIs() (unauthenticated, all []rpc.API) {
	for _, api := range n.rpcAPIs {
		if !api.Authenticated {
			unauthenticated = append(unauthenticated, api)
		}
	}
	return unauthenticated, n.rpcAPIs
}

// RegisterHandler mounts a handler on the given path on the canonical HTTP server.
//
// The name of the handler is shown in a log message when the HTTP server starts
// and should be a descriptive term for the service provided by the handler.
func (n *Node) RegisterHandler(name, path string, handler http.Handler) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.state != initializingState {
		panic("can't register HTTP handler on running/stopped node")
	}

	n.http.mux.Handle(path, handler)
	n.http.handlerNames[path] = name
}

// Attach creates an RPC client attached to an in-process API handler.
func (n *Node) Attach() *rpc.Client {
	return rpc.DialInProc(n.inprocHandler)
}

// RPCHandler returns the in-process RPC request handler.
func (n *Node) RPCHandler() (*rpc.Server, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.state == closedState {
		return nil, ErrNodeStopped
	}
	return n.inprocHandler, nil
}

// Config returns the configuration of node.
func (n *Node) Config() *Config {
	return n.config
}

// Server retrieves the currently running P2P network layer. This method is meant
// only to inspect fields of the currently running server. Callers should not
// start or stop the returned server.
func (n *Node) Server() *p2p.Server {
	n.lock.Lock()
	defer n.lock.Unlock()

	return n.server
}

func (n *Node) SPClient() transport.Client {
	n.lock.Lock()
	defer n.lock.Unlock()

	return n.spClient
}

func (n *Node) Coordinator() sequencer.Coordinator {
	n.lock.Lock()
	defer n.lock.Unlock()

	return n.sequencerCoordinator
}

func (n *Node) SequencerClients() map[string]transport.Client {
	n.lock.Lock()
	defer n.lock.Unlock()

	if n.sequencerRuntime != nil {
		return n.sequencerRuntime.Peers
	}
	return make(map[string]transport.Client)
}

func (n *Node) SequencerKey() *ecdsa.PrivateKey {
	n.lock.Lock()
	defer n.lock.Unlock()

	return n.sequencerKey
}

func (n *Node) CoordinatorKey() *ecdsa.PrivateKey {
	n.lock.Lock()
	defer n.lock.Unlock()

	return n.coordinatorKey
}

// DataDir retrieves the current datadir used by the protocol stack.
// Deprecated: No files should be stored in this directory, use InstanceDir instead.
func (n *Node) DataDir() string {
	return n.config.DataDir
}

// InstanceDir retrieves the instance directory used by the protocol stack.
func (n *Node) InstanceDir() string {
	return n.config.instanceDir()
}

// KeyStoreDir retrieves the key directory
func (n *Node) KeyStoreDir() string {
	return n.keyDir
}

// AccountManager retrieves the account manager used by the protocol stack.
func (n *Node) AccountManager() *accounts.Manager {
	return n.accman
}

// IPCEndpoint retrieves the current IPC endpoint used by the protocol stack.
func (n *Node) IPCEndpoint() string {
	return n.ipc.endpoint
}

// HTTPEndpoint returns the URL of the HTTP server. Note that this URL does not
// contain the JSON-RPC path prefix set by HTTPPathPrefix.
func (n *Node) HTTPEndpoint() string {
	return "http://" + n.http.listenAddr() //nolint:all
}

// WSEndpoint returns the current JSON-RPC over WebSocket endpoint.
func (n *Node) WSEndpoint() string {
	if n.http.wsAllowed() {
		return "ws://" + n.http.listenAddr() + n.http.wsConfig.prefix
	}
	return "ws://" + n.ws.listenAddr() + n.ws.wsConfig.prefix
}

// HTTPAuthEndpoint returns the URL of the authenticated HTTP server.
func (n *Node) HTTPAuthEndpoint() string {
	return "http://" + n.httpAuth.listenAddr()
}

// WSAuthEndpoint returns the current authenticated JSON-RPC over WebSocket endpoint.
func (n *Node) WSAuthEndpoint() string {
	if n.httpAuth.wsAllowed() {
		return "ws://" + n.httpAuth.listenAddr() + n.httpAuth.wsConfig.prefix
	}
	return "ws://" + n.wsAuth.listenAddr() + n.wsAuth.wsConfig.prefix
}

// EventMux retrieves the event multiplexer used by all the network services in
// the current protocol stack.
func (n *Node) EventMux() *event.TypeMux {
	return n.eventmux
}

// OpenDatabaseWithOptions opens an existing database with the given name (or creates one if no
// previous can be found) from within the node's instance directory. If the node has no
// data directory, an in-memory database is returned.
func (n *Node) OpenDatabaseWithOptions(name string, opt DatabaseOptions) (ethdb.Database, error) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if n.state == closedState {
		return nil, ErrNodeStopped
	}
	var db ethdb.Database
	var err error
	if n.config.DataDir == "" {
		db, _ = rawdb.Open(memorydb.New(), rawdb.OpenOptions{
			MetricsNamespace: opt.MetricsNamespace,
			ReadOnly:         opt.ReadOnly,
		})
	} else {
		opt.AncientsDirectory = n.ResolveAncient(name, opt.AncientsDirectory)
		db, err = openDatabase(internalOpenOptions{
			directory:       n.ResolvePath(name),
			dbEngine:        n.config.DBEngine,
			DatabaseOptions: opt,
		})
	}
	if err == nil {
		db = n.wrapDatabase(db)
	}
	return db, err
}

// OpenDatabase opens an existing database with the given name (or creates one if no
// previous can be found) from within the node's instance directory.
// If the node has no data directory, an in-memory database is returned.
// Deprecated: use OpenDatabaseWithOptions instead.
func (n *Node) OpenDatabase(name string, cache, handles int, namespace string, readonly bool) (ethdb.Database, error) {
	return n.OpenDatabaseWithOptions(name, DatabaseOptions{
		MetricsNamespace: namespace,
		Cache:            cache,
		Handles:          handles,
		ReadOnly:         readonly,
	})
}

// OpenDatabaseWithFreezer opens an existing database with the given name (or
// creates one if no previous can be found) from within the node's data directory.
// If the node has no data directory, an in-memory database is returned.
// Deprecated: use OpenDatabaseWithOptions instead.
func (n *Node) OpenDatabaseWithFreezer(name string, cache, handles int, ancient string, namespace string, readonly bool) (ethdb.Database, error) {
	return n.OpenDatabaseWithOptions(name, DatabaseOptions{
		AncientsDirectory: n.ResolveAncient(name, ancient),
		MetricsNamespace:  namespace,
		Cache:             cache,
		Handles:           handles,
		ReadOnly:          readonly,
	})
}

// ResolvePath returns the absolute path of a resource in the instance directory.
func (n *Node) ResolvePath(x string) string {
	return n.config.ResolvePath(x)
}

// ResolveAncient returns the absolute path of the root ancient directory.
func (n *Node) ResolveAncient(name string, ancient string) string {
	switch {
	case ancient == "":
		ancient = filepath.Join(n.ResolvePath(name), "ancient")
	case !filepath.IsAbs(ancient):
		ancient = n.ResolvePath(ancient)
	}
	return ancient
}

// closeTrackingDB wraps the Close method of a database. When the database is closed by the
// service, the wrapper removes it from the node's database map. This ensures that Node
// won't auto-close the database if it is closed by the service that opened it.
type closeTrackingDB struct {
	ethdb.Database
	n *Node
}

func (db *closeTrackingDB) Close() error {
	db.n.lock.Lock()
	delete(db.n.databases, db)
	db.n.lock.Unlock()
	return db.Database.Close()
}

// wrapDatabase ensures the database will be auto-closed when Node is closed.
func (n *Node) wrapDatabase(db ethdb.Database) ethdb.Database {
	wrapper := &closeTrackingDB{db, n}
	n.databases[wrapper] = struct{}{}
	return wrapper
}

// closeDatabases closes all open databases.
func (n *Node) closeDatabases() (errors []error) {
	for db := range n.databases {
		delete(n.databases, db)
		if err := db.Database.Close(); err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}
