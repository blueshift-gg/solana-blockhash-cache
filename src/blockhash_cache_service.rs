use crate::backfiller;
use crate::config::ServiceConfig;
use crate::grpc_subscribe::GrpcBlockhashSubscribe;
use anyhow::bail;
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub const BLOCKHASH_CACHE_SIZE: usize = 300;
pub const BLOCKHASH_MAX_AGE_SLOTS: usize = 151;
const SLOT_DURATION_MS: u64 = 400;
const BLOCKHASH_TTL_MS: u64 = SLOT_DURATION_MS * (BLOCKHASH_MAX_AGE_SLOTS as u64);

pub type Slot = u64;

#[derive(Debug)]
pub struct BlockhashLink {
    pub parent_hash: Hash,
    pub parent_slot: Slot,
    pub current_hash: Hash,
    pub current_slot: Slot,
}

impl BlockhashLink {
    pub fn new(
        parent_hash: Hash,
        parent_slot: Slot,
        current_hash: Hash,
        current_slot: Slot,
    ) -> Self {
        Self {
            parent_hash,
            parent_slot,
            current_hash,
            current_slot,
        }
    }
}

#[derive(Debug)]
pub struct BlockhashSequence(RwLock<VecDeque<BlockhashLink>>);

impl BlockhashSequence {
    pub fn new() -> Self {
        Self(RwLock::new(VecDeque::with_capacity(BLOCKHASH_CACHE_SIZE)))
    }

    pub async fn acquire_read(&self) -> RwLockReadGuard<'_, VecDeque<BlockhashLink>> {
        self.0.read().await
    }

    pub async fn acquire_write(&self) -> RwLockWriteGuard<'_, VecDeque<BlockhashLink>> {
        self.0.write().await
    }
}

pub enum BlockhashCacheState {
    Healthy,
    Unhealthy { slots_missing: u64 },
}

pub struct BlockhashCacheService {
    cache: Arc<BlockhashSequence>,
    rpc_client: Arc<RpcClient>,
    commitment: CommitmentConfig,
}

impl BlockhashCacheService {
    pub async fn start(config: ServiceConfig, shutdown_token: CancellationToken) -> Self {
        let ServiceConfig {
            solana_rpc_url,
            commitment,
            grpc_url,
            grpc_x_token,
        } = config;

        info!(
            commitment = ?commitment,
            "starting blockhash cache service"
        );

        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            solana_rpc_url.to_string(),
            commitment,
        ));

        let backfill_completed = Arc::new(AtomicBool::new(false));
        let cache = Arc::new(BlockhashSequence::new());

        let (signal_blockhash_backfill_tx, signal_blockhash_backfill_rx) =
            oneshot::channel::<Slot>();

        let grpc_sub = GrpcBlockhashSubscribe::new(
            grpc_url,
            Some(grpc_x_token),
            commitment,
            Arc::clone(&cache),
            signal_blockhash_backfill_tx,
            shutdown_token.clone(),
        );
        grpc_sub.start();

        // blocking call until 151 blockhashes are backfilled
        backfiller::run_blockhash_backfill_on_startup(
            Arc::clone(&cache),
            Arc::clone(&rpc_client),
            signal_blockhash_backfill_rx,
            Arc::clone(&backfill_completed),
        )
        .await;

        Self {
            cache,
            rpc_client,
            commitment,
        }
    }

    pub async fn is_healthy(&self) -> BlockhashCacheState {
        let current_seq_len = { self.cache.acquire_read().await.len() };

        if current_seq_len >= BLOCKHASH_MAX_AGE_SLOTS {
            BlockhashCacheState::Healthy
        } else {
            let slots_missing = (BLOCKHASH_MAX_AGE_SLOTS - current_seq_len) as u64;
            warn!(slots_missing, "blockhash cache is unhealthy");
            BlockhashCacheState::Unhealthy { slots_missing }
        }
    }

    pub async fn is_blockhash_valid(&self, blockhash: &Hash) -> anyhow::Result<bool> {
        match self.is_healthy().await {
            BlockhashCacheState::Healthy => {
                let valid = {
                    let cache = self.cache.acquire_read().await;
                    let stale_blockhashes_to_skip = cache.len() - BLOCKHASH_MAX_AGE_SLOTS;
                    cache
                        .iter()
                        .skip(stale_blockhashes_to_skip)
                        .any(|h| &h.current_hash == blockhash)
                };
                debug!(blockhash = %blockhash, valid, "checked blockhash validity from cache");
                Ok(valid)
            }
            BlockhashCacheState::Unhealthy { slots_missing: _ } => {
                warn!(blockhash = %blockhash, "cache unhealthy, falling back to rpc for blockhash validity");
                self.rpc_client
                    .is_blockhash_valid(blockhash, self.commitment)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))
            }
        }
    }

    pub async fn get_latest_blockhash(&self) -> anyhow::Result<Hash> {
        match self.is_healthy().await {
            BlockhashCacheState::Healthy => {
                let latest_blockhash = {
                    let cache = self.cache.acquire_read().await;
                    let Some(b) = cache.back() else {
                        bail!("failed to get latest blockhash from cache")
                    };
                    b.current_hash.to_owned()
                };
                debug!(blockhash = %latest_blockhash, "got latest blockhash from cache");
                Ok(latest_blockhash)
            }
            BlockhashCacheState::Unhealthy { slots_missing: _ } => {
                warn!("cache unhealthy, falling back to rpc for latest blockhash");
                let latest_blockhash = self
                    .rpc_client
                    .get_latest_blockhash_with_commitment(self.rpc_client.commitment())
                    .await
                    .map_err(|e| anyhow::anyhow!(e));
                latest_blockhash.map(|b| b.0)
            }
        }
    }

    pub async fn get_blockhash_with_expiry(&self, expiry_in_ms: u64) -> anyhow::Result<Hash> {
        if expiry_in_ms > BLOCKHASH_TTL_MS {
            bail!("expiry exceeds max blockhash TTL");
        }
        if expiry_in_ms < SLOT_DURATION_MS {
            bail!("expiry should be greater than slot duration");
        }

        let computed_slots = expiry_in_ms.div_ceil(SLOT_DURATION_MS) as usize;

        if computed_slots > BLOCKHASH_MAX_AGE_SLOTS {
            bail!("computed_slots exceeds max blockhash age");
        }

        match self.is_healthy().await {
            BlockhashCacheState::Healthy => {
                let (maybe_blockhash, index) = {
                    let cache = self.cache.acquire_read().await;
                    let cache_len = cache.len();
                    let index = cache_len
                        .saturating_sub(BLOCKHASH_MAX_AGE_SLOTS.saturating_sub(computed_slots))
                        .saturating_sub(1);

                    if index >= cache_len {
                        bail!("cache does not have enough entries to satisfy the requested expiry");
                    }

                    (cache.get(index).map(|b| b.current_hash.to_owned()), index)
                };

                debug!(
                    expiry_in_ms,
                    computed_slots,
                    cache_index = index,
                    "getting blockhash with expiry",
                );

                let Some(blockhash) = maybe_blockhash else {
                    bail!("failed to get blockhash at the given expiry")
                };

                Ok(blockhash)
            }
            BlockhashCacheState::Unhealthy { slots_missing } => bail!(
                "blockhash cache is unhealthy with {} missing slots",
                slots_missing
            ),
        }
    }

    pub async fn verify_blockhash_chain(&self) -> bool {
        let links = self.cache.acquire_read().await;
        links.iter().zip(links.iter().skip(1)).all(|(prev, next)| {
            prev.current_hash == next.parent_hash && prev.current_slot == next.parent_slot
        })
    }
}
