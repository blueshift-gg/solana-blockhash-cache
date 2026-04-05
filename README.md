# solana-blockhash-cache

A Rust library for maintaining a real-time cache of Solana blockhashes, enabling fine-grained control over transaction expiry and blockhash validity checks.

## Why

Solana blockhashes expire after ~150 slots (~60 seconds). Most applications just fetch the latest blockhash and hope the transaction lands in time. This library lets you:

- **Choose a blockhash with a specific expiry** — e.g. give a transaction exactly 30 seconds to land, rather than up to 60
- **Check blockhash validity** locally without an RPC call
- **Fall back to RPC** gracefully when the cache is not yet warm

This is particularly useful for MEV bots, trading bots, and latency-sensitive applications where you want precise control over transaction timeout strategy.

## How it works

The cache subscribes to Solana block metadata via Yellowstone gRPC and maintains a sequence of `BlockhashLink` entries — each linking a slot's blockhash to its parent. On startup, it backfills the last 151 slots from RPC so the cache is immediately usable.

The cache stores up to 300 entries (2x the blockhash TTL window) as a `VecDeque`, ordered oldest to newest.

## Usage

```toml
[dependencies]
solana-blockhash-cache = "0.1"
```

```rust
use solana_blockhash_cache::{BlockhashCacheService, BlockhashSource, ServiceConfig};
use solana_commitment_config::CommitmentConfig;
use tokio_util::sync::CancellationToken;
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ServiceConfig::try_new(
        std::env::var("SOLANA_RPC_URL")?,
        CommitmentConfig::processed(),
        BlockhashSource::Grpc {
            url: Url::parse(&std::env::var("GRPC_URL")?)?,
            x_token: std::env::var("GRPC_X_TOKEN").ok(),
        },
    )?;

    let shutdown = CancellationToken::new();
    let cache = BlockhashCacheService::start(config, shutdown.clone()).await;

    // Get the latest blockhash
    let blockhash = cache.get_latest_blockhash().await?;
    println!("latest blockhash: {}", blockhash);

    // Get a blockhash that expires in ~10 seconds (25 slots)
    let blockhash = cache.get_blockhash_with_expiry(10_000).await?;
    println!("blockhash with 10s expiry: {}", blockhash);

    // Check if a blockhash is still valid
    let is_valid = cache.is_blockhash_valid(&blockhash).await?;
    println!("is valid: {}", is_valid);

    Ok(())
}
```

## API

### `BlockhashCacheService::start`

Starts the cache service. Blocks until the initial backfill is complete.

### `get_latest_blockhash() -> anyhow::Result<Hash>`

Returns the most recent blockhash. Falls back to RPC if the cache is unhealthy.

### `get_blockhash_with_expiry(expiry_in_ms: u64) -> anyhow::Result<Hash>`

Returns a blockhash that will remain valid for at least `expiry_in_ms` milliseconds. Useful for setting precise transaction timeouts.

- Minimum: one slot duration (~400ms)
- Maximum: `BLOCKHASH_MAX_AGE_SLOTS * SLOT_DURATION_MS` (~60 seconds)
- Returns an error if the cache is unhealthy or the expiry is out of range

### `is_blockhash_valid(blockhash: &Hash) -> anyhow::Result<bool>`

Checks whether a blockhash is still within the valid window. Falls back to RPC if the cache is unhealthy.

### `is_healthy() -> BlockhashCacheState`

Returns `Healthy` if the cache has at least 151 entries, or `Unhealthy { slots_missing }` otherwise.

## Configuration

| Field            | Description                                                 |
| ---------------- | ----------------------------------------------------------- |
| `solana_rpc_url` | HTTP RPC URL used for backfill and fallback                 |
| `commitment`     | Commitment level (recommend `processed` for lowest latency) |
| `grpc_url`       | GRPC url to receive block meta updates                      |
| `grpc_x_token`   | GRPC X_TOKEN                                                |

## Environment variables (for the example)

```
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=...
GRPC_URL=https://...
GRPC_X_TOKEN=...
```

## Requirements

A gRPC endpoint that supports Yellowstone `blocks_meta` subscription (e.g. Helius, Triton One).
