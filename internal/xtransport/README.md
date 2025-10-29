# Transport Package

High-performance, authenticated TCP transport layer for the Rollup Shared Publisher SDK.

## Overview

The transport package provides a robust, production-ready networking layer with:
- **TCP-based communication** with connection pooling and health monitoring
- **Optional ECDSA authentication** using Ethereum's secp256k1 curve
- **Protobuf message serialization** with length-prefixed framing
- **Zero-copy optimizations** using buffer pools
- **Automatic reconnection** with exponential backoff
- **Metrics and monitoring** via Prometheus

## Architecture

```
transport/
├── interfaces.go      # Core transport interfaces
├── manager.go         # Connection lifecycle management
├── metrics.go         # Prometheus metrics
└── tcp/
├── server.go      # TCP server implementation
├── client.go      # TCP client implementation
├── codec.go       # Message encoding/decoding with auth
├── connection.go  # Connection wrapper with metrics
└── worker.go      # Worker pool for connection handling
```

## Best Practices

### 1. Server Implementation (Shared Publisher)

```go
import (
    "github.com/ssvlabs/rollup-shared-publisher/x/auth"
    "github.com/ssvlabs/rollup-shared-publisher/x/transport"
    "github.com/ssvlabs/rollup-shared-publisher/x/transport/tcp"
)

// Create server with authentication
func createServer(log zerolog.Logger) transport.Server {
    // Generate or load auth manager
    authManager, err := auth.GenerateManager()
    if err != nil {
        log.Fatal().Err(err).Msg("Failed to create auth")
    }

    // Configure transport
    config := transport.Config{
        ListenAddr:      ":8080",
        MaxConnections:  1000,
        ReadTimeout:     30 * time.Second,
        WriteTimeout:    30 * time.Second,
        MaxMessageSize:  10 * 1024 * 1024, // 10MB
        KeepAlive:       true,
        KeepAlivePeriod: 30 * time.Second,
    }

    // Create server with auth
    server := tcp.NewServer(config, log)
    if authManager != nil {
        server = server.(*tcp.Server).WithAuth(authManager)
    }

    // Set message handler
    server.SetHandler(handleMessage)

    return server
}

// Message handler with authentication awareness
func handleMessage(ctx context.Context, from string, msg *pb.Message) error {
    // 'from' is the verified identity if auth is enabled
    // Format: "sequencer-id" for known keys
    //         "unknown:xxxx" for valid but unknown keys
    //         "unauthenticated" for non-signed messages

    log.Info().
        Str("from", from).
        Str("type", fmt.Sprintf("%T", msg.Payload)).
        Msg("Received message")

    switch payload := msg.Payload.(type) {
    case *pb.Message_XtRequest:
        return handleXTRequest(ctx, from, payload.XtRequest)
    case *pb.Message_Vote:
        return handleVote(ctx, from, payload.Vote)
    default:
        return fmt.Errorf("unknown message type")
    }
}
```

### 2. Client Implementation (Sequencer)

```go
// Create client for sequencer
func createSequencerClient(sequencerKey *ecdsa.PrivateKey, log zerolog.Logger) transport.Client {
    // Create auth manager from sequencer's key
    var authManager auth.Manager
    if sequencerKey != nil {
        authManager = auth.NewManager(sequencerKey)
    }

    config := tcp.ClientConfig{
        ServerAddr:      "publisher.example.com:8080",
        ConnectTimeout:  10 * time.Second,
        ReadTimeout:     30 * time.Second,
        WriteTimeout:    10 * time.Second,
        ReconnectDelay:  5 * time.Second,
        MaxMessageSize:  10 * 1024 * 1024,
        KeepAlive:       true,
        KeepAlivePeriod: 30 * time.Second,
    }

    client := tcp.NewClient(config, log)
    if authManager != nil {
        client = client.WithAuth(authManager)
    }

    // Set handler for incoming messages
    client.SetHandler(handlePublisherMessage)

    return client
}

// Connect with retry
func connectToPublisher(client transport.Client) error {
    ctx := context.Background()

    // Try to connect with exponential backoff
    err := client.ConnectWithRetry(ctx, "", 5) // 5 retries
    if err != nil {
        return fmt.Errorf("failed to connect after retries: %w", err)
    }

    // Monitor connection in background
    go monitorConnection(client)

    return nil
}

// Monitor and auto-reconnect
func monitorConnection(client transport.Client) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        if !client.IsConnected() {
            log.Warn().Msg("Connection lost, attempting reconnect")

            ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
            if err := client.Reconnect(ctx); err != nil {
                log.Error().Err(err).Msg("Reconnection failed")
            } else {
                log.Info().Msg("Reconnected successfully")
            }
            cancel()
        }
    }
}
```

### 3. Authentication Setup

```go
// Generate new key pair
func generateKeyPair() (*ecdsa.PrivateKey, error) {
    return crypto.GenerateKey()
}

// Save key to file
func saveKey(key *ecdsa.PrivateKey, filename string) error {
    keyHex := hex.EncodeToString(crypto.FromECDSA(key))
    return os.WriteFile(filename, []byte(keyHex), 0600)
}

// Load key from file
func loadKey(filename string) (*ecdsa.PrivateKey, error) {
    data, err := os.ReadFile(filename)
    if err != nil {
        return nil, err
    }
    return crypto.HexToECDSA(string(data))
}

// Setup trusted keys on publisher
func setupTrustedSequencers(authManager auth.Manager) error {
    // Add known sequencer public keys
    sequencers := map[string]string{
        "optimism": "02abc...", // compressed public key hex
        "arbitrum": "03def...",
        "base":     "02789...",
    }

    for id, pubKeyHex := range sequencers {
        pubKey, err := hex.DecodeString(pubKeyHex)
        if err != nil {
            return fmt.Errorf("invalid key for %s: %w", id, err)
        }

        if err := authManager.AddTrustedKey(id, pubKey); err != nil {
            return fmt.Errorf("failed to add %s: %w", id, err)
        }
    }

    return nil
}
```

### 4. Message Sending Patterns

```go
// Send authenticated message
func sendVote(client transport.Client, xtID *pb.XtID, vote bool) error {
    msg := &pb.Message{
        SenderId: client.GetID(), // Will be overridden by verified identity
        Payload: &pb.Message_Vote{
            Vote: &pb.Vote{
                SenderChainId: chainIDBytes,
                XtId:          xtID,
                Vote:          vote,
            },
        },
    }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    return client.Send(ctx, msg)
}

// Broadcast to all except sender (server-side)
func broadcastDecision(server transport.Server, decision *pb.Decided, excludeID string) error {
    msg := &pb.Message{
        SenderId: "shared-publisher",
        Payload: &pb.Message_Decided{
            Decided: decision,
        },
    }

    ctx := context.Background()
    return server.Broadcast(ctx, msg, excludeID)
}
```

### 5. Error Handling

```go
// Robust error handling
func handleConnectionErrors(err error) {
    switch {
    case errors.Is(err, io.EOF):
        log.Info().Msg("Peer disconnected gracefully")

    case errors.Is(err, context.DeadlineExceeded):
        log.Warn().Msg("Operation timed out")

    case errors.Is(err, net.ErrClosed):
        log.Warn().Msg("Connection closed")

    default:
        var netErr net.Error
        if errors.As(err, &netErr) {
            if netErr.Timeout() {
                log.Warn().Msg("Network timeout")
            } else if netErr.Temporary() {
                log.Warn().Msg("Temporary network error")
            } else {
                log.Error().Err(err).Msg("Network error")
            }
        } else {
            log.Error().Err(err).Msg("Unexpected error")
        }
    }
}
```

### 6. Performance Optimization

```go
// Configure for high throughput
config := transport.Config{
    ListenAddr:      ":8080",
    MaxConnections:  10000,           // Support many sequencers
    ReadTimeout:     60 * time.Second, // Longer for slow networks
    WriteTimeout:    30 * time.Second,
    MaxMessageSize:  50 * 1024 * 1024, // 50MB for large blocks
    BufferSize:      65536,             // 64KB buffers
    KeepAlive:       true,
    KeepAlivePeriod: 15 * time.Second, // Detect disconnects faster
}

// Use worker pools for CPU-intensive operations
server := tcp.NewServer(config, log)
server.(*tcp.Server).SetWorkerCount(runtime.NumCPU() * 2)
```

### 7. Monitoring and Metrics

```go
// Enable Prometheus metrics
import "github.com/prometheus/client_golang/prometheus/promhttp"

func setupMetrics() {
    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe(":9090", nil)
}

// Track custom metrics
func trackMessageLatency(start time.Time, msgType string) {
    duration := time.Since(start)
    messageLatency.WithLabelValues(msgType).Observe(duration.Seconds())
}

// Monitor connection health
func getConnectionStats(server transport.Server) {
    conns := server.GetConnections()

    for _, conn := range conns {
        log.Info().
            Str("id", conn.ID).
            Str("remote", conn.RemoteAddr).
            Str("chain", conn.ChainID).
            Uint64("bytes_in", conn.BytesRead).
            Uint64("bytes_out", conn.BytesWritten).
            Msg("Connection stats")
    }
}
```

### 8. Testing

```go
// Unit test with mock auth
func TestAuthenticatedMessaging(t *testing.T) {
    // Create test keys
    serverKey, _ := crypto.GenerateKey()
    clientKey, _ := crypto.GenerateKey()

    serverAuth := auth.NewManager(serverKey)
    clientAuth := auth.NewManager(clientKey)

    // Add mutual trust
    serverAuth.AddTrustedKey("test-client",
        crypto.CompressPubkey(&clientKey.PublicKey))
    clientAuth.AddTrustedKey("test-server",
        crypto.CompressPubkey(&serverKey.PublicKey))

    // Create server
    server := tcp.NewServer(testConfig, testLog).
        (*tcp.Server).WithAuth(serverAuth)

    // Create client
    client := tcp.NewClient(clientConfig, testLog).
        WithAuth(clientAuth)

    // Test messaging...
}

// Integration test
func TestEndToEnd(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Start server
    server := createTestServer(t)
    require.NoError(t, server.Start(ctx))
    defer server.Stop(ctx)

    // Connect multiple clients
    var clients []transport.Client
    for i := 0; i < 10; i++ {
        client := createTestClient(t, i)
        require.NoError(t, client.Connect(ctx, "localhost:8080"))
        defer client.Disconnect(ctx)
        clients = append(clients, client)
    }

    // Test broadcast
    testMsg := createTestMessage()
    require.NoError(t, server.Broadcast(ctx, testMsg, ""))

    // Verify all clients received
    // ...
}
```

## Security Considerations

1. **Key Management**
    - Store private keys securely (use hardware security modules in production)
    - Rotate keys periodically
    - Never log or transmit private keys

2. **Network Security**
    - Use TLS in addition to message signing for defense in depth
    - Implement rate limiting to prevent DoS
    - Validate message sizes before allocation

3. **Authentication**
    - Always verify message signatures before processing
    - Maintain allowlist of trusted public keys
    - Log authentication failures for monitoring

4. **Error Handling**
    - Don't leak sensitive information in error messages
    - Implement circuit breakers for failing connections
    - Use timeouts for all network operations

## Performance Tips

1. **Connection Pooling**: Reuse connections instead of creating new ones
2. **Buffer Tuning**: Adjust buffer sizes based on message patterns
3. **Batch Operations**: Send multiple messages in a single write when possible
4. **Compression**: Consider compression for large messages (not implemented yet)
5. **Zero-Copy**: Use the built-in buffer pools to minimize allocations

## Troubleshooting

| Issue | Possible Cause | Solution |
|-------|---------------|----------|
| "Connection refused" | Server not running | Check server logs and network connectivity |
| "Signature verification failed" | Key mismatch | Verify public keys are correctly configured |
| "Message size exceeds max" | Large payload | Increase MaxMessageSize or split message |
| "Too many connections" | Limit reached | Increase MaxConnections or implement connection pooling |
| "Identity mismatch" | Multiple keys used | Ensure consistent key usage per connection |

## Example Configuration

```yaml
# Publisher config
transport:
  listen_addr: ":8080"
  max_connections: 5000
  read_timeout: 30s
  write_timeout: 30s
  max_message_size: 52428800  # 50MB

auth:
  enabled: true
  private_key: ""  # Will generate if empty
  trusted_sequencers:
    optimism: "02abc..."
    arbitrum: "03def..."
    base: "02789..."

# Sequencer config
transport:
  server_addr: "publisher.example.com:8080"
  connect_timeout: 10s
  read_timeout: 30s
  write_timeout: 10s
  reconnect_delay: 5s

auth:
  enabled: true
  private_key: "abc123..."  # Your sequencer's private key
```
