use anyhow::Result;
use async_trait::async_trait;

use crate::models::{
    AccountBalanceSnapshot, AssetTransferStatus, OrderFill, OrderFillReconciliation, OrderRequest,
    PositionSnapshot, Venue, VenueMarketSnapshot,
};

#[async_trait]
pub trait VenueAdapter: Send + Sync {
    fn venue(&self) -> Venue;

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot>;

    async fn refresh_market_snapshot(&self, symbol: &str) -> Result<VenueMarketSnapshot> {
        self.fetch_market_snapshot(&[symbol.to_string()]).await
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill>;

    fn cached_position(&self, _symbol: &str) -> Option<PositionSnapshot> {
        None
    }

    async fn fetch_position(&self, symbol: &str) -> Result<PositionSnapshot>;

    async fn fetch_all_positions(&self) -> Result<Option<Vec<PositionSnapshot>>> {
        Ok(None)
    }

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        Ok(None)
    }

    async fn fetch_order_fill_reconciliation(
        &self,
        _symbol: &str,
        _order_id: &str,
        _client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        Ok(None)
    }

    async fn normalize_quantity(&self, _symbol: &str, quantity: f64) -> Result<f64> {
        Ok(quantity)
    }

    fn min_entry_notional_quote_hint(
        &self,
        _symbol: &str,
        _price_hint: Option<f64>,
    ) -> Option<f64> {
        None
    }

    async fn fetch_transfer_statuses(
        &self,
        _assets: &[String],
    ) -> Result<Vec<AssetTransferStatus>> {
        Ok(Vec::new())
    }

    fn supported_symbols(&self, _requested_symbols: &[String]) -> Option<Vec<String>> {
        None
    }

    async fn live_startup_prewarm(&self) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}
