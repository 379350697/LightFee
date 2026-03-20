use anyhow::Result;
use async_trait::async_trait;

use crate::models::{
    AssetTransferStatus, OrderFill, OrderRequest, PositionSnapshot, Venue, VenueMarketSnapshot,
};

#[async_trait]
pub trait VenueAdapter: Send + Sync {
    fn venue(&self) -> Venue;

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot>;

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill>;

    fn cached_position(&self, _symbol: &str) -> Option<PositionSnapshot> {
        None
    }

    async fn fetch_position(&self, symbol: &str) -> Result<PositionSnapshot>;

    async fn normalize_quantity(&self, _symbol: &str, quantity: f64) -> Result<f64> {
        Ok(quantity)
    }

    async fn fetch_transfer_statuses(
        &self,
        _assets: &[String],
    ) -> Result<Vec<AssetTransferStatus>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
