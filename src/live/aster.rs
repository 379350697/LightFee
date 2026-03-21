use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::{
    config::{RuntimeConfig, VenueConfig},
    live::BinanceLiveAdapter,
    models::{
        AccountBalanceSnapshot, AssetTransferStatus, OrderFill, OrderFillReconciliation,
        OrderRequest, PerpLiquiditySnapshot, PositionSnapshot, Venue, VenueMarketSnapshot,
    },
    venue::VenueAdapter,
};

pub struct AsterLiveAdapter {
    inner: BinanceLiveAdapter,
}

impl AsterLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        runtime: &RuntimeConfig,
        symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Aster {
            return Err(anyhow!("aster live adapter requires aster config"));
        }

        let mut binance_like = config.clone();
        binance_like.venue = Venue::Binance;
        if binance_like.live.base_url.is_none() {
            binance_like.live.base_url = Some("https://fapi.asterdex.com".to_string());
        }

        Ok(Self {
            inner: BinanceLiveAdapter::new(&binance_like, runtime, symbols).await?,
        })
    }

    fn rewrite_market_snapshot(mut snapshot: VenueMarketSnapshot) -> VenueMarketSnapshot {
        snapshot.venue = Venue::Aster;
        snapshot
    }

    fn rewrite_position(mut snapshot: PositionSnapshot) -> PositionSnapshot {
        snapshot.venue = Venue::Aster;
        snapshot
    }

    fn rewrite_balance(mut snapshot: AccountBalanceSnapshot) -> AccountBalanceSnapshot {
        snapshot.venue = Venue::Aster;
        snapshot
    }

    fn rewrite_fill(mut fill: OrderFill) -> OrderFill {
        fill.venue = Venue::Aster;
        fill
    }

    fn rewrite_reconciliation(
        reconciliation: Option<OrderFillReconciliation>,
    ) -> Option<OrderFillReconciliation> {
        reconciliation
    }

    fn rewrite_transfer_statuses(statuses: Vec<AssetTransferStatus>) -> Vec<AssetTransferStatus> {
        statuses
            .into_iter()
            .map(|mut status| {
                status.venue = Venue::Aster;
                status
            })
            .collect()
    }

    fn rewrite_liquidity(
        mut snapshot: Option<PerpLiquiditySnapshot>,
    ) -> Option<PerpLiquiditySnapshot> {
        if let Some(snapshot_ref) = snapshot.as_mut() {
            snapshot_ref.venue = Venue::Aster;
        }
        snapshot
    }
}

#[async_trait]
impl VenueAdapter for AsterLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Aster
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        Ok(Self::rewrite_market_snapshot(
            self.inner.fetch_market_snapshot(symbols).await?,
        ))
    }

    async fn refresh_market_snapshot(&self, symbol: &str) -> Result<VenueMarketSnapshot> {
        Ok(Self::rewrite_market_snapshot(
            self.inner.refresh_market_snapshot(symbol).await?,
        ))
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        Ok(Self::rewrite_fill(self.inner.place_order(request).await?))
    }

    fn cached_position(&self, symbol: &str) -> Option<PositionSnapshot> {
        self.inner
            .cached_position(symbol)
            .map(Self::rewrite_position)
    }

    async fn fetch_position(&self, symbol: &str) -> Result<PositionSnapshot> {
        Ok(Self::rewrite_position(
            self.inner.fetch_position(symbol).await?,
        ))
    }

    async fn fetch_all_positions(&self) -> Result<Option<Vec<PositionSnapshot>>> {
        Ok(self
            .inner
            .fetch_all_positions()
            .await?
            .map(|positions| positions.into_iter().map(Self::rewrite_position).collect()))
    }

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        Ok(self
            .inner
            .fetch_account_balance_snapshot()
            .await?
            .map(Self::rewrite_balance))
    }

    fn enforces_entry_balance_gate(&self) -> bool {
        true
    }

    async fn fetch_order_fill_reconciliation(
        &self,
        symbol: &str,
        order_id: &str,
        client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        Ok(Self::rewrite_reconciliation(
            self.inner
                .fetch_order_fill_reconciliation(symbol, order_id, client_order_id)
                .await?,
        ))
    }

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        self.inner.normalize_quantity(symbol, quantity).await
    }

    async fn ensure_entry_leverage(&self, symbol: &str, leverage: u32) -> Result<()> {
        self.inner.ensure_entry_leverage(symbol, leverage).await
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<PerpLiquiditySnapshot>> {
        Ok(Self::rewrite_liquidity(
            self.inner.fetch_perp_liquidity_snapshot(symbol).await?,
        ))
    }

    fn min_entry_notional_quote_hint(&self, symbol: &str, price_hint: Option<f64>) -> Option<f64> {
        self.inner.min_entry_notional_quote_hint(symbol, price_hint)
    }

    async fn fetch_transfer_statuses(&self, assets: &[String]) -> Result<Vec<AssetTransferStatus>> {
        Ok(Self::rewrite_transfer_statuses(
            self.inner.fetch_transfer_statuses(assets).await?,
        ))
    }

    fn supported_symbols(&self, requested_symbols: &[String]) -> Option<Vec<String>> {
        self.inner.supported_symbols(requested_symbols)
    }

    fn supports_market_data_activity_control(&self) -> bool {
        self.inner.supports_market_data_activity_control()
    }

    async fn set_market_data_active(&self, active: bool, symbols: &[String]) -> Result<()> {
        self.inner.set_market_data_active(active, symbols).await
    }

    async fn live_startup_prewarm(&self) -> Result<()> {
        self.inner.live_startup_prewarm().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.inner.shutdown().await
    }
}
