use crate::blockhash_cache_service::{
    BLOCKHASH_MAX_AGE_SLOTS, BlockhashLink, BlockhashSequence, Slot,
};
use futures_util::StreamExt;
use solana_hash::Hash;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};
use url::Url;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocksMeta,
};

const MAX_RETRY_ATTEMPTS: u32 = 5;

pub struct GrpcBlockhashSubscribe {
    grpc_url: Url,
    grpc_x_token: Option<String>,
    blockhash_cache: Arc<BlockhashSequence>,
    signal_blockhash_backfill_tx: oneshot::Sender<Slot>,
    shutdown_token: CancellationToken,
}

impl GrpcBlockhashSubscribe {
    pub fn new(
        grpc_url: Url,
        grpc_x_token: Option<String>,
        blockhash_cache: Arc<BlockhashSequence>,
        signal_blockhash_backfill_tx: oneshot::Sender<Slot>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            grpc_url,
            grpc_x_token,
            blockhash_cache,
            signal_blockhash_backfill_tx,
            shutdown_token,
        }
    }

    pub fn start(self) {
        tokio::spawn(async move {
            let GrpcBlockhashSubscribe {
                grpc_url,
                grpc_x_token,
                blockhash_cache,
                signal_blockhash_backfill_tx,
                shutdown_token,
            } = self;

            let mut signal_blockhash_backfill_tx = Some(signal_blockhash_backfill_tx);

            info!("starting grpc blockhash subscriber");

            let mut attempt: u32 = 1;

            'outer: loop {
                if shutdown_token.is_cancelled() {
                    break;
                }

                let backoff = Duration::from_secs_f32(1.5_f32.powi(attempt as i32).min(15.0));

                let mut client = match GeyserGrpcClient::build_from_shared(grpc_url.to_string())
                    .and_then(|b| b.x_token(grpc_x_token.clone()))
                    .and_then(|b| b.tls_config(ClientTlsConfig::new().with_native_roots()))
                    .map_err(|e| anyhow::anyhow!(e))
                {
                    Ok(b) => match b.connect().await {
                        Ok(c) => {
                            info!("grpc client connected");
                            c
                        }
                        Err(e) => {
                            error!(error = %e, attempt, backoff_secs = backoff.as_secs_f32(), "failed to connect grpc client");
                            tokio::select! {
                                _ = tokio::time::sleep(backoff) => {
                                        if attempt > MAX_RETRY_ATTEMPTS {
                                            error!("max attempts reached while retrying, ending...");
                                            break;
                                        }
                                        attempt += 1;
                                        continue
                                    },
                                _ = shutdown_token.cancelled() => break,
                            }
                        }
                    },
                    Err(e) => {
                        error!(error = %e, "failed to build grpc client, aborting");
                        break;
                    }
                };

                attempt = 1;

                'subscribe: loop {
                    if shutdown_token.is_cancelled() {
                        break 'outer;
                    }

                    let (mut _subscribe_tx, mut stream) = match client
                        .subscribe_with_request(Some(blockhash_subscription()))
                        .await
                    {
                        Ok(s) => {
                            info!("subscribed to grpc block stream");
                            s
                        }
                        Err(e) => {
                            warn!(error = %e, attempt, "failed to subscribe, retrying in 1s");
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                                        if attempt > MAX_RETRY_ATTEMPTS {
                                            error!("max attempts reached while retrying, ending...");
                                            break 'outer;
                                        }
                                        attempt += 1;
                                        continue 'subscribe
                                    },
                                _ = shutdown_token.cancelled() => break 'outer,
                            }
                        }
                    };

                    let mut blockhashes_recorded_grpc = 0;

                    loop {
                        tokio::select! {
                            _ = shutdown_token.cancelled() => {
                                info!("shutdown signal received, stopping grpc subscriber");
                                break 'outer;
                            }
                            maybe_msg = stream.next() => {
                                match maybe_msg {
                                    Some(Ok(msg)) => {
                                        if let Some(UpdateOneof::BlockMeta(block)) = msg.update_oneof {
                                            if let Some(tx) = signal_blockhash_backfill_tx.take() {
                                                info!(slot = block.slot, "signaling blockhash backfill");
                                                let _ = tx.send(block.parent_slot);
                                            }

                                            let parent = Hash::from_str(&block.parent_blockhash);
                                            let current = Hash::from_str(&block.blockhash);

                                            match (parent, current) {
                                                (Ok(parent), Ok(current)) => {
                                                    let mut cache = blockhash_cache.acquire_write().await;
                                                    // dedup
                                                    if cache.back().map(|b| b.current_slot) != Some(block.slot) {
                                                        cache.push_back(BlockhashLink::new(parent, block.parent_slot, current, block.slot));
                                                    }
                                                }
                                                _ => {
                                                    warn!("failed to parse blockhash from grpc block meta update");
                                                }
                                            }
                                        }
                                    }
                                    Some(Err(e)) => {
                                        warn!(error = %e, "grpc stream error, resubscribing");
                                        continue 'subscribe;
                                    }
                                    None => {
                                        warn!("grpc stream ended, resubscribing");
                                        continue 'subscribe;
                                    }
                                }
                            }
                            _ = async {}, if blockhashes_recorded_grpc > BLOCKHASH_MAX_AGE_SLOTS => {
                                trace!(
                                    count = blockhashes_recorded_grpc,
                                    "Cleaning up BlockhashCache"
                                );

                                blockhashes_recorded_grpc = 0;

                                let mut cache = blockhash_cache.acquire_write().await;

                                if cache.len() > BLOCKHASH_MAX_AGE_SLOTS {
                                    let excess = cache.len() - BLOCKHASH_MAX_AGE_SLOTS;
                                    cache.drain(..excess);
                                }

                            }
                        }
                    }
                }
            }

            info!("grpc blockhash subscriber stopped");
        });
    }
}

fn blockhash_subscription() -> SubscribeRequest {
    let mut blockhash_subs = HashMap::new();
    blockhash_subs.insert(
        "blockhash_sub".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );
    SubscribeRequest {
        blocks_meta: blockhash_subs,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    }
}
