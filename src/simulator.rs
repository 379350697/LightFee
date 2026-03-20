use std::{collections::BTreeMap, fs, path::Path, sync::Mutex};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;

use crate::{
    models::{OrderFill, OrderRequest, PositionSnapshot, Side, Venue, VenueMarketSnapshot},
    venue::VenueAdapter,
};

#[derive(Debug)]
struct Inner {
    snapshots: Vec<VenueMarketSnapshot>,
    cursor: usize,
    last_index: usize,
    positions: BTreeMap<String, f64>,
    fail_next_orders: usize,
    next_order_id: u64,
}

#[derive(Debug)]
pub struct ScriptedVenueAdapter {
    venue: Venue,
    taker_fee_bps: f64,
    inner: Mutex<Inner>,
}

impl ScriptedVenueAdapter {
    pub fn new(venue: Venue, taker_fee_bps: f64, snapshots: Vec<VenueMarketSnapshot>) -> Self {
        Self {
            venue,
            taker_fee_bps,
            inner: Mutex::new(Inner {
                snapshots,
                cursor: 0,
                last_index: 0,
                positions: BTreeMap::new(),
                fail_next_orders: 0,
                next_order_id: 1,
            }),
        }
    }

    pub async fn from_file(venue: Venue, taker_fee_bps: f64, path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read scenario {}", path.display()))?;
        let snapshots = serde_json::from_str::<Vec<VenueMarketSnapshot>>(&raw)
            .with_context(|| format!("failed to parse scenario {}", path.display()))?;
        Ok(Self::new(venue, taker_fee_bps, snapshots))
    }

    pub fn fail_next_orders(&self, count: usize) {
        let mut inner = self.inner.lock().expect("lock");
        inner.fail_next_orders = count;
    }

    pub fn set_position_size(&self, symbol: &str, size: f64) {
        let mut inner = self.inner.lock().expect("lock");
        inner.positions.insert(symbol.to_string(), size);
    }

    pub fn position_size(&self, symbol: &str) -> f64 {
        let inner = self.inner.lock().expect("lock");
        inner.positions.get(symbol).copied().unwrap_or_default()
    }

    fn current_snapshot(inner: &Inner) -> Result<&VenueMarketSnapshot> {
        inner
            .snapshots
            .get(inner.last_index)
            .or_else(|| inner.snapshots.first())
            .ok_or_else(|| anyhow!("scripted venue {} has no snapshots", inner.last_index))
    }
}

#[async_trait]
impl VenueAdapter for ScriptedVenueAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut inner = self.inner.lock().expect("lock");
        if inner.snapshots.is_empty() {
            return Err(anyhow!("venue {} has no scripted snapshots", self.venue));
        }

        let index = inner.cursor.min(inner.snapshots.len() - 1);
        inner.last_index = index;
        if inner.cursor + 1 < inner.snapshots.len() {
            inner.cursor += 1;
        }

        let mut snapshot = inner.snapshots[index].clone();
        snapshot.venue = self.venue;
        if !symbols.is_empty() {
            snapshot
                .symbols
                .retain(|item| symbols.iter().any(|symbol| symbol == &item.symbol));
        }
        Ok(snapshot)
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        let mut inner = self.inner.lock().expect("lock");
        if inner.fail_next_orders > 0 {
            inner.fail_next_orders -= 1;
            return Err(anyhow!(
                "{} scripted order failure for {}",
                self.venue,
                request.client_order_id
            ));
        }

        let snapshot = Self::current_snapshot(&inner)?.clone();
        let symbol = snapshot
            .symbols
            .into_iter()
            .find(|item| item.symbol == request.symbol)
            .with_context(|| {
                format!("{} missing market data for {}", self.venue, request.symbol)
            })?;
        let mut quantity = request.quantity;
        let position = inner.positions.entry(request.symbol.clone()).or_insert(0.0);
        if request.reduce_only {
            let reducible = match request.side {
                Side::Buy if *position < 0.0 => (-*position).min(quantity),
                Side::Sell if *position > 0.0 => position.abs().min(quantity),
                _ => 0.0,
            };
            if reducible <= 0.0 {
                return Err(anyhow!(
                    "{} reduce_only order cannot reduce {}",
                    self.venue,
                    request.symbol
                ));
            }
            quantity = reducible;
        }

        let average_price = match request.side {
            Side::Buy => symbol.best_ask,
            Side::Sell => symbol.best_bid,
        };
        *position += request.side.signed_qty(quantity);
        let order_id = format!("{}-{}", self.venue, inner.next_order_id);
        inner.next_order_id += 1;

        Ok(OrderFill {
            venue: self.venue,
            symbol: request.symbol,
            side: request.side,
            quantity,
            average_price,
            fee_quote: average_price * quantity * self.taker_fee_bps / 10_000.0,
            order_id,
            filled_at_ms: snapshot.observed_at_ms,
            timing: None,
        })
    }

    fn cached_position(&self, symbol: &str) -> Option<PositionSnapshot> {
        let inner = self.inner.lock().expect("lock");
        Some(PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: inner.positions.get(symbol).copied().unwrap_or_default(),
            updated_at_ms: Self::current_snapshot(&inner)
                .map(|snapshot| snapshot.observed_at_ms)
                .unwrap_or_default(),
        })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<PositionSnapshot> {
        let inner = self.inner.lock().expect("lock");
        let observed_at_ms = Self::current_snapshot(&inner)
            .map(|snapshot| snapshot.observed_at_ms)
            .unwrap_or_default();
        Ok(PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: inner.positions.get(symbol).copied().unwrap_or_default(),
            updated_at_ms: observed_at_ms,
        })
    }

    async fn normalize_quantity(&self, _symbol: &str, quantity: f64) -> Result<f64> {
        Ok(quantity)
    }
}
