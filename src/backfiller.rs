use crate::blockhash_cache_service::{
    BLOCKHASH_MAX_AGE_SLOTS, BlockhashLink, BlockhashSequence, Slot,
};
use anyhow::bail;
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_transaction_status_client_types::{TransactionDetails, UiConfirmedBlock};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{info, trace, warn};

const BLOCKHASH_CACHE_BACKFILL_TIMEOUT_DURATION: Duration = Duration::from_secs(100);
const RPC_FETCH_TIMEOUT_DURATION: Duration = Duration::from_millis(5_000);
const RPC_FETCH_RETRY_DELAY: Duration = Duration::from_millis(50);

pub async fn run_blockhash_backfill_on_startup(
    cache: Arc<BlockhashSequence>,
    rpc_client: Arc<RpcClient>,
    signal_blockhash_backfill_rx: oneshot::Receiver<Slot>,
    backfill_completed: Arc<AtomicBool>,
) {
    info!("waiting for backfill signal from blockhash source");

    let parent_slot = match signal_blockhash_backfill_rx.await {
        Ok(slot) => {
            info!(slot, "received backfill signal, starting backfill");
            slot
        }
        Err(_e) => {
            warn!("backfill signal sender dropped, skipping backfill");
            backfill_completed.store(true, Ordering::Release);
            return;
        }
    };

    let started = tokio::time::Instant::now();
    let mut current_slot = parent_slot;
    let mut blockhashes_backfilled = 0;

    while blockhashes_backfilled < BLOCKHASH_MAX_AGE_SLOTS {
        if started.elapsed() > BLOCKHASH_CACHE_BACKFILL_TIMEOUT_DURATION {
            warn!(
                blockhashes_backfilled,
                elapsed_ms = started.elapsed().as_millis(),
                "backfill timed out"
            );
            break;
        }

        let maybe_block = fetch_block_with_timeout(&rpc_client, current_slot).await;

        match maybe_block {
            Ok(block) => {
                let parent = Hash::from_str(&block.previous_blockhash);
                let current = Hash::from_str(&block.blockhash);

                match (parent, current) {
                    (Ok(parent), Ok(current)) => {
                        let cache_len = {
                            let mut cache = cache.acquire_write().await;
                            cache.push_front(BlockhashLink::new(
                                parent,
                                block.parent_slot,
                                current,
                                current_slot,
                            ));
                            cache.len()
                        };

                        blockhashes_backfilled += 1;
                        trace!(
                            slot = current_slot,
                            blockhashes_backfilled, cache_len, "backfilled blockhash"
                        );

                        if cache_len >= BLOCKHASH_MAX_AGE_SLOTS {
                            info!(blockhashes_backfilled, "backfill complete");
                            break;
                        }

                        current_slot = block.parent_slot;
                    }
                    _ => {
                        warn!(
                            slot = current_slot,
                            "failed to parse blockhash from block, stopping backfill"
                        );
                        break;
                    }
                }
            }
            Err(e) => {
                warn!(slot = current_slot, error = %e, "failed to fetch block during backfill");
            }
        }
    }

    backfill_completed.store(true, Ordering::Release);
    info!(
        blockhashes_backfilled,
        elapsed_ms = started.elapsed().as_millis(),
        "blockhash backfill finished"
    );
}

pub async fn fetch_block_with_timeout(
    rpc_client: &RpcClient,
    slot: Slot,
) -> anyhow::Result<UiConfirmedBlock> {
    let mut retries: u64 = 0;
    let mut last_err = None;

    let result = tokio::time::timeout(RPC_FETCH_TIMEOUT_DURATION, async {
        loop {
            match rpc_client
                .get_block_with_config(
                    slot,
                    RpcBlockConfig {
                        encoding: None,
                        transaction_details: Some(TransactionDetails::None),
                        rewards: Some(false),
                        commitment: Some(CommitmentConfig::confirmed()),
                        max_supported_transaction_version: Some(0),
                    },
                )
                .await
            {
                Ok(block) => return Ok(block),
                Err(e) => {
                    retries += 1;
                    last_err = Some(e);
                    tokio::time::sleep(RPC_FETCH_RETRY_DELAY).await;
                }
            }
        }
    })
    .await;

    match result {
        Ok(Ok(block)) => Ok(block),
        Ok(Err(e)) => Err(e),
        Err(_) => bail!(
            "get_block_with_config timed out after {} retries within {:?}: {:?}",
            retries,
            RPC_FETCH_TIMEOUT_DURATION,
            last_err
        ),
    }
}
