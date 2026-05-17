use anyhow::bail;
use solana_blockhash_cache::blockhash_cache_service::BlockhashCacheService;
use solana_blockhash_cache::config::ServiceConfig;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use url::Url;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let solana_rpc_url = env::var("SOLANA_RPC_URL")?.to_string();
    let solana_rpc_client = RpcClient::new(solana_rpc_url.clone());

    let config = ServiceConfig::try_new(
        solana_rpc_url,
        CommitmentConfig::processed(),
        Url::from_str(&env::var("GRPC_URL")?)?,
        env::var("GRPC_X_TOKEN")?,
    )?;

    let shutdown_token = CancellationToken::new();

    let blockhash_cache_service =
        BlockhashCacheService::start(config, shutdown_token.clone()).await;

    let latest_blockhash = blockhash_cache_service.get_latest_blockhash().await?;

    let is_blockhash_valid = solana_rpc_client
        .is_blockhash_valid(&latest_blockhash, CommitmentConfig::confirmed())
        .await?;

    match is_blockhash_valid {
        true => info!("blockhash is valid : checked from solana rpc client"),
        false => {
            bail!("blockhash is expired , this should not happen")
        }
    }

    match blockhash_cache_service.verify_blockhash_chain().await {
        true => info!("blockhash chain verified"),
        false => error!("failed to verify blockhash chain"),
    }

    let blockhash_with_expiry = blockhash_cache_service
        .get_blockhash_with_expiry(10_000)
        .await?;

    // sleep for 10 seconds
    tokio::time::sleep(Duration::from_secs(10)).await;

    let is_blockhash_valid = solana_rpc_client
        .is_blockhash_valid(&blockhash_with_expiry, CommitmentConfig::confirmed())
        .await?;

    match is_blockhash_valid {
        true => bail!("checked blockhash is valid from solana rpc - this should not happen"),
        false => info!("blockhash is expired as expected"),
    }

    shutdown_token.cancel();

    Ok(())
}
