# xbridge - Cross-Chain Bridge Transaction Tool

A CLI tool for sending cross-chain bridge transactions using the Compose protocol. Supports both single transaction mode
and batch mode for stress testing.

- **Single Mode**: Send one bridge transaction pair (send + receive)
- **Batch Mode**: Send multiple transaction pairs with incremental nonces

## Configuration

Create a `config.yml` file in the same directory:

```yaml
token: "0x6d19CB7639DeB366c334BD69f030A38e226BA6d2"

rollups:
  A:
    rpc: "http://localhost:8545"
    chain_id: 77777
    private_key: "0xYOUR_PRIVATE_KEY_A"
    bridge: "0xBRIDGE_CONTRACT_ADDRESS_A"

  B:
    rpc: "http://localhost:8546"
    chain_id: 88888
    private_key: "0xYOUR_PRIVATE_KEY_B"
    bridge: "0xBRIDGE_CONTRACT_ADDRESS_B"
```

## Usage

### Single Transaction Mode (Default)

Send one bridge transaction:

```bash
./xbridge
```

With custom amount:

```bash
./xbridge -amount 50000
```

### Batch Mode

Send multiple transactions with incremental nonces:

```bash
# Send 3 transactions with 500ms delay between them
./xbridge -batch -num 3 -delay 500

# Send 10 transactions with 1 second delay
./xbridge -batch -num 10 -delay 1000

# Send 5 transactions with custom amount
./xbridge -batch -num 5 -amount 200000
```

## Command-Line Flags

| Flag          | Type   | Default | Description                                           |
|---------------|--------|---------|-------------------------------------------------------|
| `-batch`      | bool   | false   | Enable batch mode for multiple transactions           |
| `-num`        | int    | 1       | Number of transactions to send in batch mode          |
| `-delay`      | int    | 500     | Delay in milliseconds between transactions            |
| `-amount`     | int64  | 100000  | Token amount to send per transaction                  |
| `-fail-rollup`| string | ""      | Simulate rollup failure (A, B, or empty for normal)   |

## Examples

### Example 1: Single Transaction

```bash
$ ./xbridge
2024/10/16 12:00:00 Running in SINGLE mode
2024/10/16 12:00:00 Address A: 0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C (nonce: 46)
2024/10/16 12:00:00 Address B: 0x803682E13c47dA42ffb04037588f24b0bc0950F2 (nonce: 22)
2024/10/16 12:00:00 Creating single bridge transaction with session ID: 4915138964436146229
✓ Successfully submitted cross-chain transaction
```

### Example 2: Batch Stress Test

```bash
$ ./xbridge -batch -num 3 -delay 500
2024/10/16 12:00:00 Running in BATCH mode: 3 transactions with 500ms delay
2024/10/16 12:00:00 Address A: 0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C (starting nonce: 46)
2024/10/16 12:00:00 Address B: 0x803682E13c47dA42ffb04037588f24b0bc0950F2 (starting nonce: 22)
2024/10/16 12:00:00 Starting batch execution...
2024/10/16 12:00:00 [1/3] Creating transaction pair with nonces A=46, B=22, session=4915138964436146229
2024/10/16 12:00:01 ✓ [1/3] Successfully submitted
2024/10/16 12:00:01 [2/3] Creating transaction pair with nonces A=47, B=23, session=5386905574332575365
2024/10/16 12:00:02 ✓ [2/3] Successfully submitted
2024/10/16 12:00:02 [3/3] Creating transaction pair with nonces A=48, B=24, session=6123456789012345678
2024/10/16 12:00:03 ✓ [3/3] Successfully submitted

=== Batch Execution Summary ===
Total transactions: 3
Successful: 3
Failed: 0
Success rate: 100.0%
```

### Example 3: Testing Execution Failures

Test cross-chain transaction behavior when one rollup fails during execution:

```bash
# Simulate rollup A failing during execution
$ ./xbridge -fail-rollup A
2024/10/16 12:00:00 ⚠️  FAILURE MODE: Rollup A transactions will be sent but designed to fail during execution
2024/10/16 12:00:00 Running in SINGLE mode
2024/10/16 12:00:00 Address A: 0x31c57E2910496e46Bb883EDeb1eB2bee8E3Ee82C (nonce: 46)
2024/10/16 12:00:00 Address B: 0x803682E13c47dA42ffb04037588f24b0bc0950F2 (nonce: 22)
2024/10/16 12:00:00 Creating single bridge transaction with session ID: 4915138964436146229
2024/10/16 12:00:00   Corrupting rollup A transaction with mismatched session ID (will fail during execution)
⚠️  Cross-chain transaction submitted with Rollup A designed to fail during execution

# Simulate rollup B failing in batch mode
$ ./xbridge -batch -num 2 -fail-rollup B
2024/10/16 12:00:00 ⚠️  FAILURE MODE: Rollup B transactions will be sent but designed to fail during execution
2024/10/16 12:00:00 Running in BATCH mode: 2 transactions with 500ms delay
2024/10/16 12:00:00 Starting batch execution...
2024/10/16 12:00:00 [1/2] Creating transaction pair with nonces A=46, B=22, session=4915138964436146229
2024/10/16 12:00:00   Corrupting rollup B transaction with mismatched session ID (will fail during execution)
2024/10/16 12:00:01 ✓ [1/2] Successfully submitted
2024/10/16 12:00:01 [2/2] Creating transaction pair with nonces A=47, B=23, session=5386905574332575365
2024/10/16 12:00:01   Corrupting rollup B transaction with mismatched session ID (will fail during execution)
2024/10/16 12:00:02 ✓ [2/2] Successfully submitted

=== Batch Execution Summary ===
Total transactions: 2
Successful: 2
Failed: 0
Success rate: 100.0%
```

## Building

```bash
cd op-geth/cmd/xbridge
go build -o xbridge
```

## How It Works

The tool operates in **fire-and-forget** mode, matching the behavior of integration tests:

1. **Creates transaction pairs** with incremental nonces (A sends, B receives)
2. **Bundles into XTRequest** proto message
3. **Submits via RPC** to `eth_sendXTransaction` endpoint
4. **Does NOT wait** for transaction hashes or confirmations
5. **Success = RPC accepted** the request (not that transactions executed)

Each transaction pair gets a unique random session ID for mailbox coordination.

### Failure Simulation

The `-fail-rollup` flag allows testing execution failure scenarios where one rollup's transaction fails:

- When set to `A`: Both transactions are sent, but rollup A's transaction uses a corrupted session ID
- When set to `B`: Both transactions are sent, but rollup B's transaction uses a corrupted session ID
- The mismatched session ID causes the failing transaction to revert during execution
- This simulates execution failures, state conflicts, or validation errors

### Note on "Successfully Submitted"

✓ "Successfully submitted" means the RPC endpoint accepted the request, NOT that:
- Transactions were executed
- Transactions were included in blocks  
- Cross-chain coordination succeeded

To verify actual execution, check the rollup logs or query block explorers.


