use std::str::FromStr;
use url::Url;

#[derive(Clone)]
pub struct ServiceConfig {
    pub solana_rpc_url: Url,
    pub grpc_url: Url,
    pub grpc_x_token: String,
}

impl ServiceConfig {
    pub fn try_new(
        solana_rpc_url: String,
        grpc_url: Url,
        grpc_x_token: String,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            solana_rpc_url: Url::from_str(&solana_rpc_url)?,
            grpc_url,
            grpc_x_token,
        })
    }
}
