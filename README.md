# solana-blockhash-cache

A Rust library for maintaining a real-time cache of Solana blockhashes, enabling fine-grained control over transaction expiry and local blockhash validity checks.

## Why

A Solana blockhash is valid for `BLOCKHASH_MAX_AGE_SLOTS = 151` slots (~60s at 400ms/slot). A validator processing your transaction at slot `S` accepts it only if the embedded blockhash was minted at a slot `>= S - 150`. The "freshness" of the blockhash you sign with directly determines how long your transaction has to land before validators reject it as expired.

Most clients call `getLatestBlockhash` and accept the full ~60s window. This library lets you instead:

- **Pick a blockhash with a chosen expiry window** — e.g. give a transaction exactly 10s of validity rather than up to 60s, so it auto-expires on stale market data instead of landing late.
- **Check blockhash validity locally** without an RPC call.
- **Fall back to RPC** automatically when the cache is not yet warm.

Useful for trading bots, MEV, and any latency-sensitive client that wants explicit control over transaction timeout.

## How it works

The cache maintains an oldest-to-newest `VecDeque<BlockhashLink>` of recent blocks. Each `BlockhashLink` records `(parent_slot, parent_hash, current_slot, current_hash)`, so adjacent entries form a verifiable chain.

Three components keep it populated:

1. **gRPC subscriber** (`grpc_subscribe.rs`) — subscribes to Yellowstone `blocks_meta` and appends each new block to the back of the deque. Trims to `BLOCKHASH_MAX_AGE_SLOTS` once it grows past the threshold. Reconnects with exponential backoff (up to 5 attempts) on failure.
2. **Backfiller** (`backfiller.rs`) — on startup, the gRPC subscriber signals the first observed `parent_slot`. The backfiller walks backwards from that slot via `getBlock` RPC, pushing each ancestor onto the front of the deque until 151 entries are present. `BlockhashCacheService::start` blocks on this, so the cache is immediately usable when `start` returns.
3. **Service API** (`blockhash_cache_service.rs`) — read-only accessors over the cache, with RPC fallback when the cache is `Unhealthy` (fewer than 151 entries).

### The expiry mechanic

`get_blockhash_with_expiry(expiry_in_ms)` does not produce a new blockhash. It computes how many slots of remaining validity the caller wants, then returns an already-aged blockhash from earlier in the deque:

```
computed_slots = ceil(expiry_in_ms / 400)     // desired slots of runway
age            = 151 - computed_slots         // slots of life already consumed
index          = cache_len - age - 1          // position in the deque (0 = oldest)
```

A 10s expiry asks for 25 slots of runway, so the library returns a blockhash already 126 slots old. Validators will accept it for ~25 more slots, then reject it as expired.

### Chain integrity

`verify_blockhash_chain()` walks the deque and confirms every adjacent pair satisfies `prev.current_hash == next.parent_hash` and `prev.current_slot == next.parent_slot`. A break indicates a reorg has poisoned the cache.

## Usage

```toml
[dependencies]
solana-blockhash-cache = { git = "git@github.com:blueshift-gg/solana-blockhash-cache.git", branch = "master" }
```

```rust
use solana_blockhash_cache::blockhash_cache_service::BlockhashCacheService;
use solana_blockhash_cache::config::ServiceConfig;
use std::str::FromStr;
use tokio_util::sync::CancellationToken;
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ServiceConfig::try_new(
        std::env::var("SOLANA_RPC_URL")?,
        Url::from_str(&std::env::var("GRPC_URL")?)?,
        std::env::var("GRPC_X_TOKEN")?,
    )?;

    let shutdown = CancellationToken::new();
    let cache = BlockhashCacheService::start(config, shutdown.clone()).await;

    let latest = cache.get_latest_blockhash().await?;
    println!("latest blockhash: {}", latest);

    // Blockhash with ~10s of remaining validity.
    let short_lived = cache.get_blockhash_with_expiry(10_000).await?;
    println!("10s-expiry blockhash: {}", short_lived);

    let valid = cache.is_blockhash_valid(&short_lived).await?;
    println!("is valid: {}", valid);

    shutdown.cancel();
    Ok(())
}
```

## API

| Method | Description |
| --- | --- |
| `BlockhashCacheService::start(config, shutdown)` | Spawns the gRPC subscriber and runs backfill. Blocks until 151 entries are cached or backfill times out (100s). |
| `get_latest_blockhash() -> Hash` | Returns the most recent cached blockhash. Falls back to RPC if unhealthy. |
| `get_blockhash_with_expiry(expiry_in_ms) -> Hash` | Returns a blockhash with at least `expiry_in_ms` of remaining validity. Valid range: `[400, 60_400]` ms (1 slot to 151 slots). Errors if the cache is unhealthy. |
| `is_blockhash_valid(&hash) -> bool` | Scans the cache's 151-slot window for the hash. Falls back to RPC if unhealthy. |
| `is_healthy() -> BlockhashCacheState` | `Healthy` if the cache holds ≥151 entries, else `Unhealthy { slots_missing }`. |
| `verify_blockhash_chain() -> bool` | Returns `true` if every adjacent pair in the deque is linked by matching parent slot+hash. Use to detect reorg-induced corruption. |

## Configuration

| Field | Description |
| --- | --- |
| `solana_rpc_url` | HTTP RPC URL used for backfill and fallback. |
| `grpc_url` | Yellowstone gRPC endpoint serving `blocks_meta`. |
| `grpc_x_token` | Auth token for the gRPC endpoint. |

### Commitment

The gRPC subscription and the RPC fallback paths (`is_blockhash_valid`, `get_latest_blockhash`) operate at `processed`. This matches the level at which the bank registers blockhashes ([agave `bank.rs::register_recent_blockhash`](https://github.com/anza-xyz/agave/blob/0e24419adb73c446ebe4026e378f5af337770072/runtime/src/bank.rs#L3199)) — blockhash queue membership is independent of confirmation, so there is no meaningful `confirmed`/`finalized` alternative for this cache. The backfiller uses `confirmed` only because `getBlock` requires it.

Since `processed` accepts the leader's most recent block, a brief reorg can leave the cache holding a hash from a dropped slot. Transactions referencing such a hash will silently fail to land; `verify_blockhash_chain()` detects this after the fact.

## Environment variables (for the example)

```
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=...
GRPC_URL=https://...
GRPC_X_TOKEN=...
```

## Requirements

A gRPC endpoint that supports Yellowstone `blocks_meta` subscription (e.g. Helius, Triton One).
