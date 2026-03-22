use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};

use crate::{models::OrderFill, resilience::ConnectionHealth};

const DEFAULT_MAX_ORDER_ENTRIES: usize = 512;
const PRIVATE_ORDER_POLL_INTERVAL_MS: u64 = 5;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PrivateOrderUpdate {
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub filled_quantity: Option<f64>,
    pub average_price: Option<f64>,
    pub fee_quote: Option<f64>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PrivatePositionUpdate {
    pub symbol: String,
    pub size: f64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Default)]
struct PrivateOrderCache {
    entries: HashMap<String, PrivateOrderUpdate>,
    client_index: HashMap<String, String>,
    order_index: HashMap<String, String>,
}

#[derive(Debug)]
pub(crate) struct WsPrivateState {
    orders: RwLock<PrivateOrderCache>,
    positions: RwLock<HashMap<String, PrivatePositionUpdate>>,
    health: RwLock<ConnectionHealth>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    max_order_entries: usize,
}

impl Default for WsPrivateState {
    fn default() -> Self {
        Self {
            orders: RwLock::new(PrivateOrderCache::default()),
            positions: RwLock::new(HashMap::new()),
            health: RwLock::new(ConnectionHealth::default()),
            workers: Mutex::new(Vec::new()),
            max_order_entries: DEFAULT_MAX_ORDER_ENTRIES,
        }
    }
}

impl WsPrivateState {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_order_capacity(DEFAULT_MAX_ORDER_ENTRIES)
    }

    pub(crate) fn with_order_capacity(max_order_entries: usize) -> Arc<Self> {
        Arc::new(Self {
            max_order_entries: max_order_entries.max(1),
            ..Self::default()
        })
    }

    pub(crate) fn record_order(&self, update: PrivateOrderUpdate) {
        let Some(default_key) = canonical_order_key(&update) else {
            return;
        };

        let mut orders = self.orders.write().expect("lock");
        let cache_key = orders
            .order_index
            .get(&update.order_id)
            .cloned()
            .or_else(|| {
                update
                    .client_order_id
                    .as_ref()
                    .and_then(|client_id| orders.client_index.get(client_id).cloned())
            })
            .unwrap_or(default_key);
        if let Some(existing) = orders.entries.get(&cache_key) {
            if existing.updated_at_ms > update.updated_at_ms {
                return;
            }
        }

        orders.entries.insert(cache_key.clone(), update.clone());
        if let Some(client_id) = update.client_order_id.as_ref() {
            orders
                .client_index
                .insert(client_id.clone(), cache_key.clone());
        }
        if !update.order_id.is_empty() {
            orders
                .order_index
                .insert(update.order_id.clone(), cache_key);
        }

        while orders.entries.len() > self.max_order_entries {
            let Some((oldest_key, oldest_entry)) = orders
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.updated_at_ms)
                .map(|(key, entry)| (key.clone(), entry.clone()))
            else {
                break;
            };
            orders.entries.remove(&oldest_key);
            if let Some(client_id) = oldest_entry.client_order_id.as_ref() {
                if orders
                    .client_index
                    .get(client_id)
                    .is_some_and(|current| current == &oldest_key)
                {
                    orders.client_index.remove(client_id);
                }
            }
            if !oldest_entry.order_id.is_empty()
                && orders
                    .order_index
                    .get(&oldest_entry.order_id)
                    .is_some_and(|current| current == &oldest_key)
            {
                orders.order_index.remove(&oldest_entry.order_id);
            }
        }
    }

    pub(crate) fn order_by_client_id(&self, client_order_id: &str) -> Option<PrivateOrderUpdate> {
        let orders = self.orders.read().expect("lock");
        let key = orders.client_index.get(client_order_id)?;
        orders.entries.get(key).cloned()
    }

    pub(crate) fn order_by_order_id(&self, order_id: &str) -> Option<PrivateOrderUpdate> {
        let orders = self.orders.read().expect("lock");
        let key = orders.order_index.get(order_id)?;
        orders.entries.get(key).cloned()
    }

    pub(crate) fn update_position(&self, symbol: &str, size: f64, updated_at_ms: i64) {
        let mut positions = self.positions.write().expect("lock");
        if positions
            .get(symbol)
            .is_some_and(|existing| existing.updated_at_ms > updated_at_ms)
        {
            return;
        }
        positions.insert(
            symbol.to_string(),
            PrivatePositionUpdate {
                symbol: symbol.to_string(),
                size,
                updated_at_ms,
            },
        );
    }

    pub(crate) fn position(&self, symbol: &str) -> Option<PrivatePositionUpdate> {
        self.positions.read().expect("lock").get(symbol).cloned()
    }

    pub(crate) fn position_if_fresh(
        &self,
        symbol: &str,
        max_age_ms: i64,
        wall_clock_now_ms: i64,
    ) -> Option<PrivatePositionUpdate> {
        let position = self.position(symbol)?;
        if max_age_ms <= 0 {
            return Some(position);
        }
        let age_ms = wall_clock_now_ms.saturating_sub(position.updated_at_ms);
        if age_ms > max_age_ms {
            None
        } else {
            Some(position)
        }
    }

    pub(crate) fn record_connection_success(&self, now_ms: i64) {
        self.health.write().expect("lock").record_success(now_ms);
    }

    pub(crate) fn record_connection_failure(
        &self,
        now_ms: i64,
        unhealthy_after_failures: usize,
        error: String,
    ) {
        self.health
            .write()
            .expect("lock")
            .record_failure(now_ms, unhealthy_after_failures, error);
    }

    pub(crate) fn push_worker(&self, worker: JoinHandle<()>) {
        let mut workers = self.workers.lock().expect("lock");
        prune_finished_workers(&mut workers);
        workers.push(worker);
    }

    pub(crate) fn abort_workers(&self) {
        let mut workers = self.workers.lock().expect("lock");
        prune_finished_workers(&mut workers);
        for worker in workers.drain(..) {
            worker.abort();
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        let mut workers = self.workers.lock().expect("lock");
        prune_finished_workers(&mut workers);
        workers.len()
    }
}

pub(crate) fn enrich_fill_from_private(
    mut fill: OrderFill,
    update: &PrivateOrderUpdate,
) -> OrderFill {
    if let Some(quantity) = update
        .filled_quantity
        .filter(|quantity| quantity.is_finite() && *quantity > 0.0)
    {
        fill.quantity = quantity;
    }
    if let Some(price) = update
        .average_price
        .filter(|price| price.is_finite() && *price > 0.0)
    {
        fill.average_price = price;
    }
    if let Some(fee_quote) = update
        .fee_quote
        .filter(|fee_quote| fee_quote.is_finite() && *fee_quote >= 0.0)
    {
        fill.fee_quote = fee_quote;
    }
    if !update.order_id.is_empty() {
        fill.order_id = update.order_id.clone();
    }
    if update.updated_at_ms > 0 {
        fill.filled_at_ms = update.updated_at_ms;
    }
    fill
}

pub(crate) async fn lookup_or_wait_private_order(
    private_state: &Arc<WsPrivateState>,
    client_order_id: Option<&str>,
    order_id: Option<&str>,
    wait_ms: u64,
) -> Option<PrivateOrderUpdate> {
    let lookup = || {
        client_order_id
            .and_then(|client_id| private_state.order_by_client_id(client_id))
            .or_else(|| order_id.and_then(|order_id| private_state.order_by_order_id(order_id)))
    };
    if let Some(update) = lookup() {
        return Some(update);
    }
    if wait_ms == 0 {
        return None;
    }

    let deadline = Instant::now() + Duration::from_millis(wait_ms);
    loop {
        let now = Instant::now();
        if now >= deadline {
            return lookup();
        }
        sleep(Duration::from_millis(
            PRIVATE_ORDER_POLL_INTERVAL_MS.min((deadline - now).as_millis() as u64),
        ))
        .await;
        if let Some(update) = lookup() {
            return Some(update);
        }
    }
}

fn canonical_order_key(update: &PrivateOrderUpdate) -> Option<String> {
    if !update.order_id.is_empty() {
        Some(format!("order:{}", update.order_id))
    } else {
        update
            .client_order_id
            .as_ref()
            .map(|client_id| format!("client:{client_id}"))
    }
}

fn prune_finished_workers(workers: &mut Vec<JoinHandle<()>>) {
    workers.retain(|worker| !worker.is_finished());
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::models::{OrderFill, Side, Venue};
    use tokio::time::{sleep, Instant};

    use super::{
        enrich_fill_from_private, lookup_or_wait_private_order, PrivateOrderUpdate,
        PrivatePositionUpdate, WsPrivateState,
    };

    #[test]
    fn private_state_keeps_newest_order_update_and_enriches_fill() {
        let state = WsPrivateState::with_order_capacity(4);
        state.record_order(PrivateOrderUpdate {
            symbol: "ETHUSDT".to_string(),
            order_id: "old-order".to_string(),
            client_order_id: Some("entry-1".to_string()),
            filled_quantity: Some(0.01),
            average_price: Some(2140.0),
            fee_quote: Some(0.001),
            updated_at_ms: 10,
        });
        state.record_order(PrivateOrderUpdate {
            symbol: "ETHUSDT".to_string(),
            order_id: "new-order".to_string(),
            client_order_id: Some("entry-1".to_string()),
            filled_quantity: Some(0.011),
            average_price: Some(2141.5),
            fee_quote: Some(0.002),
            updated_at_ms: 20,
        });

        let update = state
            .order_by_client_id("entry-1")
            .expect("cached order update");
        assert_eq!(update.order_id, "new-order");
        assert_eq!(update.filled_quantity, Some(0.011));

        let fill = enrich_fill_from_private(
            OrderFill {
                venue: Venue::Bybit,
                symbol: "ETHUSDT".to_string(),
                side: Side::Buy,
                quantity: 0.01,
                average_price: 2140.0,
                fee_quote: 0.0,
                order_id: "rest-order".to_string(),
                filled_at_ms: 5,
                timing: None,
            },
            &update,
        );
        assert_eq!(fill.order_id, "new-order");
        assert_eq!(fill.quantity, 0.011);
        assert_eq!(fill.average_price, 2141.5);
        assert_eq!(fill.fee_quote, 0.002);
        assert_eq!(fill.filled_at_ms, 20);
    }

    #[tokio::test]
    async fn worker_count_prunes_finished_handles() {
        let state = WsPrivateState::new();
        state.push_worker(tokio::spawn(async {}));
        sleep(Duration::from_millis(10)).await;

        assert_eq!(state.worker_count(), 0);
    }

    #[test]
    fn private_state_tracks_latest_position_only() {
        let state = WsPrivateState::new();
        state.update_position("ETHUSDT", 0.02, 20);
        state.update_position("ETHUSDT", 0.01, 10);

        let position = state.position("ETHUSDT").expect("cached position");
        assert_eq!(position.symbol, "ETHUSDT");
        assert_eq!(position.size, 0.02);
        assert_eq!(position.updated_at_ms, 20);
    }

    #[test]
    fn private_state_rejects_stale_positions_when_freshness_ttl_expires() {
        let state = WsPrivateState::new();
        state.update_position("ETHUSDT", 0.75, 1_000);

        let fresh = state.position_if_fresh("ETHUSDT", 5_000, 5_999);
        let stale = state.position_if_fresh("ETHUSDT", 5_000, 6_001);

        assert_eq!(
            fresh,
            Some(PrivatePositionUpdate {
                symbol: "ETHUSDT".to_string(),
                size: 0.75,
                updated_at_ms: 1_000,
            })
        );
        assert!(stale.is_none());
    }

    #[test]
    fn private_state_caps_order_cache() {
        let state = WsPrivateState::with_order_capacity(2);
        state.record_order(PrivateOrderUpdate {
            symbol: "ETHUSDT".to_string(),
            order_id: "order-1".to_string(),
            client_order_id: Some("entry-1".to_string()),
            filled_quantity: Some(0.01),
            average_price: Some(2140.0),
            fee_quote: None,
            updated_at_ms: 10,
        });
        state.record_order(PrivateOrderUpdate {
            symbol: "ETHUSDT".to_string(),
            order_id: "order-2".to_string(),
            client_order_id: Some("entry-2".to_string()),
            filled_quantity: Some(0.01),
            average_price: Some(2141.0),
            fee_quote: None,
            updated_at_ms: 20,
        });
        state.record_order(PrivateOrderUpdate {
            symbol: "ETHUSDT".to_string(),
            order_id: "order-3".to_string(),
            client_order_id: Some("entry-3".to_string()),
            filled_quantity: Some(0.01),
            average_price: Some(2142.0),
            fee_quote: None,
            updated_at_ms: 30,
        });

        assert!(state.order_by_client_id("entry-1").is_none());
        assert!(state.order_by_order_id("order-1").is_none());
        assert!(state.order_by_client_id("entry-2").is_some());
        assert!(state.order_by_client_id("entry-3").is_some());
    }

    #[tokio::test]
    async fn lookup_or_wait_private_order_returns_none_immediately_when_disabled() {
        let state = WsPrivateState::new();
        let state_for_task = state.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(15)).await;
            state_for_task.record_order(PrivateOrderUpdate {
                symbol: "ETHUSDT".to_string(),
                order_id: "order-1".to_string(),
                client_order_id: Some("entry-1".to_string()),
                filled_quantity: Some(0.01),
                average_price: Some(2140.0),
                fee_quote: None,
                updated_at_ms: 15,
            });
        });

        let started_at = Instant::now();
        let update = lookup_or_wait_private_order(&state, Some("entry-1"), None, 0).await;
        assert!(update.is_none());
        assert!(started_at.elapsed() < Duration::from_millis(10));
    }

    #[tokio::test]
    async fn lookup_or_wait_private_order_captures_late_private_fill_with_short_wait() {
        let state = WsPrivateState::new();
        let state_for_task = state.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(15)).await;
            state_for_task.record_order(PrivateOrderUpdate {
                symbol: "ETHUSDT".to_string(),
                order_id: "order-2".to_string(),
                client_order_id: Some("entry-2".to_string()),
                filled_quantity: Some(0.011),
                average_price: Some(2141.0),
                fee_quote: Some(0.002),
                updated_at_ms: 15,
            });
        });

        let update = lookup_or_wait_private_order(&state, Some("entry-2"), None, 60).await;
        assert_eq!(
            update.expect("late private update").order_id,
            "order-2".to_string()
        );
    }
}
