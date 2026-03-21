use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        AccountBalanceSnapshot, AssetTransferStatus, OrderFill, OrderFillReconciliation,
        OrderRequest, PerpLiquiditySnapshot, PositionSnapshot, Venue, VenueMarketSnapshot,
    },
    venue::VenueAdapter,
};

pub struct GateLiveAdapter;

impl GateLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        _runtime: &RuntimeConfig,
        _symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Gate {
            return Err(anyhow!("gate live adapter requires gate config"));
        }
        Ok(Self)
    }
}

#[async_trait]
impl VenueAdapter for GateLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Gate
    }

    async fn fetch_market_snapshot(&self, _symbols: &[String]) -> Result<VenueMarketSnapshot> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }

    async fn place_order(&self, _request: OrderRequest) -> Result<OrderFill> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }

    async fn fetch_position(&self, _symbol: &str) -> Result<PositionSnapshot> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }

    async fn fetch_all_positions(&self) -> Result<Option<Vec<PositionSnapshot>>> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }

    fn enforces_entry_balance_gate(&self) -> bool {
        true
    }

    async fn fetch_order_fill_reconciliation(
        &self,
        _symbol: &str,
        _order_id: &str,
        _client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }

    async fn normalize_quantity(&self, _symbol: &str, quantity: f64) -> Result<f64> {
        Ok(quantity)
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        _symbol: &str,
    ) -> Result<Option<PerpLiquiditySnapshot>> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }

    async fn fetch_transfer_statuses(
        &self,
        _assets: &[String],
    ) -> Result<Vec<AssetTransferStatus>> {
        Err(anyhow!("gate live adapter not yet implemented"))
    }
}
