use solana_commitment_config::CommitmentConfig;
use std::str::FromStr;
use url::Url;

#[derive(Clone)]
pub struct ServiceConfig {
    pub solana_rpc_url: Url,
    pub commitment: CommitmentConfig,
    pub grpc_url: Url,
    pub grpc_x_token: String,
}

impl ServiceConfig {
    pub fn try_new(
        solana_rpc_url: String,
        commitment: CommitmentConfig,
        grpc_url: Url,
        grpc_x_token: String,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            solana_rpc_url: Url::from_str(&solana_rpc_url)?,
            commitment,
            grpc_url,
            grpc_x_token,
        })
    }
}
