use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Mutex,
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client, Method, StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha512};
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        AccountBalanceSnapshot, AccountFeeSnapshot, AssetTransferStatus, OrderExecutionTiming,
        OrderFill, OrderFillReconciliation, OrderRequest, PerpLiquiditySnapshot, PositionSnapshot,
        Side, SymbolMarketSnapshot, Venue, VenueMarketSnapshot,
    },
    venue::VenueAdapter,
};

use super::{
    base_asset, build_http_client, build_query, cache_is_fresh, enrich_fill_from_private,
    estimate_fee_quote, filter_transfer_statuses, floor_to_step, format_decimal,
    is_benign_ws_disconnect_error, load_account_fee_snapshot_cache, load_json_cache,
    lookup_or_wait_private_order, now_ms, parse_f64, parse_i64, parse_text_message, spawn_ws_loop,
    store_account_fee_snapshot_cache, store_json_cache, transfer_cache_ttl_ms, venue_symbol,
    PrivateOrderUpdate, VenueTransferStatusCache, WsMarketState, WsPrivateState,
    SYMBOL_CACHE_TTL_MS,
};
use crate::resilience::FailureBackoff;

const GATE_SETTLE: &str = "usdt";
const GATE_PERP_LIQUIDITY_CACHE_TTL_MS: i64 = 60 * 1_000;
const GATE_CLIENT_TEXT_PREFIX: &str = "t-";
const GATE_PRIVATE_FILL_WAIT_MS: u64 = 120;

type HmacSha512 = Hmac<Sha512>;

pub struct GateLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    client: Client,
    base_url: String,
    wallet_base_url: String,
    metadata: Mutex<HashMap<String, GateContractMeta>>,
    supported_symbols: Mutex<HashSet<String>>,
    market_ws: std::sync::Arc<WsMarketState>,
    market_subscription_symbols: Mutex<Vec<String>>,
    private_ws: std::sync::Arc<WsPrivateState>,
    account_fee_snapshot: Mutex<Option<AccountFeeSnapshot>>,
    transfer_status_cache: Mutex<Option<VenueTransferStatusCache>>,
    perp_liquidity_cache: Mutex<HashMap<String, PerpLiquiditySnapshot>>,
    configured_leverage: Mutex<HashMap<String, u32>>,
}

impl GateLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        runtime: &RuntimeConfig,
        _symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Gate {
            return Err(anyhow!("gate live adapter requires gate config"));
        }

        let persisted_catalog = load_json_cache::<GateSymbolCatalogCache>("gate-symbols.json");
        let persisted_transfer_cache =
            load_json_cache::<VenueTransferStatusCache>("gate-transfer-status.json");
        let account_fee_snapshot = load_account_fee_snapshot_cache(Venue::Gate, "gate-fees.json");
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        if let Some(cache) = persisted_catalog {
            if !cache_is_fresh(cache.updated_at_ms, now_ms(), SYMBOL_CACHE_TTL_MS) {
                debug!("gate symbol catalog cache is stale; using as fallback seed");
            }
            metadata.extend(cache.metadata);
            supported_symbols.extend(cache.supported_symbols);
        }
        let transfer_status_cache = persisted_transfer_cache.filter(|cache| {
            cache_is_fresh(
                cache.observed_at_ms,
                now_ms(),
                transfer_cache_ttl_ms(runtime.transfer_status_cache_ms),
            )
        });

        let adapter = Self {
            config: config.clone(),
            runtime: runtime.clone(),
            client: build_http_client(runtime.exchange_http_timeout_ms)?,
            base_url: config
                .live
                .base_url
                .clone()
                .unwrap_or_else(|| "https://api.gateio.ws".to_string()),
            wallet_base_url: config
                .live
                .wallet_base_url
                .clone()
                .unwrap_or_else(|| "https://api.gateio.ws".to_string()),
            metadata: Mutex::new(metadata),
            supported_symbols: Mutex::new(supported_symbols),
            market_ws: WsMarketState::new(),
            market_subscription_symbols: Mutex::new(Vec::new()),
            private_ws: WsPrivateState::new(),
            account_fee_snapshot: Mutex::new(account_fee_snapshot),
            transfer_status_cache: Mutex::new(transfer_status_cache),
            perp_liquidity_cache: Mutex::new(HashMap::new()),
            configured_leverage: Mutex::new(HashMap::new()),
        };
        if let Err(error) = adapter.refresh_symbol_catalog().await {
            if adapter.supported_symbols.lock().expect("lock").is_empty() {
                return Err(error);
            }
            warn!(
                ?error,
                "gate symbol catalog refresh failed; using persisted cache"
            );
        }
        let tracked_symbols = adapter.tracked_symbols(_symbols);
        *adapter.market_subscription_symbols.lock().expect("lock") = tracked_symbols.clone();
        adapter.start_market_ws(&tracked_symbols);
        adapter.start_private_ws(&tracked_symbols);
        Ok(adapter)
    }

    fn tracked_symbols(&self, requested_symbols: &[String]) -> Vec<String> {
        requested_symbols
            .iter()
            .filter(|symbol| {
                self.supported_symbols
                    .lock()
                    .expect("lock")
                    .contains(symbol.as_str())
            })
            .cloned()
            .collect()
    }

    fn supports_symbol(&self, symbol: &str) -> bool {
        self.supported_symbols
            .lock()
            .expect("lock")
            .contains(symbol)
    }

    fn fee_reference_symbol(&self) -> Option<String> {
        self.market_subscription_symbols
            .lock()
            .expect("lock")
            .first()
            .cloned()
            .or_else(|| {
                self.supported_symbols
                    .lock()
                    .expect("lock")
                    .iter()
                    .next()
                    .cloned()
            })
    }

    fn store_account_fee_snapshot(&self, snapshot: &AccountFeeSnapshot) {
        self.account_fee_snapshot
            .lock()
            .expect("lock")
            .replace(snapshot.clone());
        store_account_fee_snapshot_cache("gate-fees.json", snapshot);
    }

    async fn refresh_account_fee_snapshot(&self) -> Result<Option<AccountFeeSnapshot>> {
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                "/api/v4/wallet/fee",
                Some(build_query(&[("settle", GATE_SETTLE.to_string())])),
                None,
                "failed to request gate wallet fee",
            )
            .await?;
        let snapshot = match parse_gate_fee_snapshot(&payload) {
            Ok(snapshot) => snapshot,
            Err(wallet_error) => {
                debug!(
                    ?wallet_error,
                    "gate wallet fee missing futures fee fields; trying contract catalog rate"
                );
                let Some(symbol) = self.fee_reference_symbol() else {
                    return Ok(self.cached_account_fee_snapshot());
                };
                let Some(meta) = self.metadata.lock().expect("lock").get(&symbol).cloned() else {
                    return Ok(self.cached_account_fee_snapshot());
                };
                let (Some(taker_fee_bps), Some(maker_fee_bps)) =
                    (meta.taker_fee_rate_bps, meta.maker_fee_rate_bps)
                else {
                    return Ok(self.cached_account_fee_snapshot());
                };
                AccountFeeSnapshot {
                    venue: Venue::Gate,
                    taker_fee_bps,
                    maker_fee_bps,
                    observed_at_ms: now_ms(),
                    source: format!("gate_contract_fee:{}", venue_symbol(&self.config, &symbol)),
                }
            }
        };
        self.store_account_fee_snapshot(&snapshot);
        Ok(Some(snapshot))
    }

    fn start_market_ws(&self, symbols: &[String]) {
        if symbols.is_empty() {
            return;
        }
        let contracts = symbols
            .iter()
            .map(|symbol| venue_symbol(&self.config, symbol))
            .collect::<Vec<_>>();
        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let now_s = (now_ms() / 1_000) as u64;
        let subscribe_messages = vec![
            serde_json::json!({
                "time": now_s,
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": contracts,
            })
            .to_string(),
            serde_json::json!({
                "time": now_s,
                "channel": "futures.tickers",
                "event": "subscribe",
                "payload": symbol_map.keys().cloned().collect::<Vec<_>>(),
            })
            .to_string(),
        ];
        let state = self.market_ws.clone();
        spawn_ws_loop(
            "gate",
            gate_ws_url(&self.base_url).to_string(),
            subscribe_messages,
            state,
            self.runtime.ws_reconnect_initial_ms,
            self.runtime.ws_reconnect_max_ms,
            self.runtime.ws_unhealthy_after_failures,
            move |cache, raw| {
                let payload = parse_text_message(raw)?;
                let channel = payload
                    .get("channel")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let rows = if let Some(rows) = payload.get("result").and_then(Value::as_array) {
                    rows.clone()
                } else if let Some(rows) = payload.get("data").and_then(Value::as_array) {
                    rows.clone()
                } else if let Some(row) = payload.get("result") {
                    vec![row.clone()]
                } else {
                    Vec::new()
                };
                for row in rows {
                    let contract = json_string(&row, &["contract", "name"]).unwrap_or_default();
                    let Some(symbol) = symbol_map.get(&contract) else {
                        continue;
                    };
                    match channel {
                        "futures.book_ticker" => {
                            cache.update_quote(
                                symbol,
                                json_required_f64(
                                    &row,
                                    &["b", "highest_bid", "highestBid"],
                                    "gate ws best bid",
                                )?,
                                json_required_f64(
                                    &row,
                                    &["a", "lowest_ask", "lowestAsk"],
                                    "gate ws best ask",
                                )?,
                                json_f64(&row, &["B", "highest_size"]).unwrap_or_default(),
                                json_f64(&row, &["A", "lowest_size"]).unwrap_or_default(),
                                json_i64(&row, &["t", "time_ms", "time"]).unwrap_or_else(now_ms),
                            );
                        }
                        "futures.tickers" => {
                            if let Some(mark_price) =
                                json_f64(&row, &["mark_price", "markPrice", "last"])
                            {
                                cache.update_mark_price(symbol, mark_price);
                            }
                            if let (Some(funding_rate), Some(funding_ts)) = (
                                json_f64(&row, &["funding_rate", "fundingRate"]),
                                json_i64(&row, &["funding_next_apply", "funding_next_time"]),
                            ) {
                                let funding_ts = if funding_ts < 10_000_000_000 {
                                    funding_ts * 1_000
                                } else {
                                    funding_ts
                                };
                                cache.update_funding(symbol, funding_rate, funding_ts);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            },
        );
    }

    fn cached_snapshot(&self, symbol: &str) -> Option<SymbolMarketSnapshot> {
        self.market_ws.snapshot(symbol)
    }

    fn start_private_ws(&self, symbols: &[String]) {
        let (Some(api_key), Some(api_secret)) = (
            self.config.live.resolved_api_key(),
            self.config.live.resolved_api_secret(),
        ) else {
            return;
        };
        if symbols.is_empty() {
            return;
        }
        let private_state = self.private_ws.clone();
        let reconnect_initial_ms = self.runtime.ws_reconnect_initial_ms;
        let reconnect_max_ms = self.runtime.ws_reconnect_max_ms;
        let unhealthy_after_failures = self.runtime.ws_unhealthy_after_failures;
        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let url = gate_ws_url(&self.base_url).to_string();
        let task = tokio::spawn(async move {
            let mut reconnect_backoff =
                FailureBackoff::new(reconnect_initial_ms, reconnect_max_ms, Venue::Gate as u64);
            loop {
                match connect_async(url.as_str()).await {
                    Ok((mut socket, _)) => {
                        reconnect_backoff.on_success();
                        private_state.record_connection_success(now_ms());
                        let mut ping_interval = interval(Duration::from_secs(20));
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        let now_s = (now_ms() / 1_000) as u64;
                        let auth = match gate_ws_auth(
                            &api_key,
                            &api_secret,
                            "futures.orders",
                            "subscribe",
                            now_s,
                        ) {
                            Ok(auth) => auth,
                            Err(error) => {
                                private_state.record_connection_failure(
                                    now_ms(),
                                    unhealthy_after_failures,
                                    error.to_string(),
                                );
                                warn!(?error, "gate private websocket auth sign failed");
                                sleep(Duration::from_millis(
                                    reconnect_backoff.on_failure_with_jitter(),
                                ))
                                .await;
                                continue;
                            }
                        };
                        let positions_auth = match gate_ws_auth(
                            &api_key,
                            &api_secret,
                            "futures.positions",
                            "subscribe",
                            now_s,
                        ) {
                            Ok(auth) => auth,
                            Err(error) => {
                                private_state.record_connection_failure(
                                    now_ms(),
                                    unhealthy_after_failures,
                                    error.to_string(),
                                );
                                warn!(?error, "gate private websocket auth sign failed");
                                sleep(Duration::from_millis(
                                    reconnect_backoff.on_failure_with_jitter(),
                                ))
                                .await;
                                continue;
                            }
                        };
                        let orders_sub = serde_json::json!({
                            "time": now_s,
                            "channel": "futures.orders",
                            "event": "subscribe",
                            "payload": symbol_map.keys().cloned().collect::<Vec<_>>(),
                            "auth": auth,
                        })
                        .to_string();
                        let positions_sub = serde_json::json!({
                            "time": now_s,
                            "channel": "futures.positions",
                            "event": "subscribe",
                            "payload": symbol_map.keys().cloned().collect::<Vec<_>>(),
                            "auth": positions_auth,
                        })
                        .to_string();
                        if let Err(error) = socket.send(Message::Text(orders_sub.into())).await {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                error.to_string(),
                            );
                            sleep(Duration::from_millis(
                                reconnect_backoff.on_failure_with_jitter(),
                            ))
                            .await;
                            continue;
                        }
                        if let Err(error) = socket.send(Message::Text(positions_sub.into())).await {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                error.to_string(),
                            );
                            sleep(Duration::from_millis(
                                reconnect_backoff.on_failure_with_jitter(),
                            ))
                            .await;
                            continue;
                        }
                        loop {
                            tokio::select! {
                                _ = ping_interval.tick() => {
                                    if let Err(error) = socket.send(Message::Text("ping".to_string().into())).await {
                                        private_state.record_connection_failure(now_ms(), unhealthy_after_failures, error.to_string());
                                        break;
                                    }
                                }
                                message = socket.next() => {
                                    match message {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Err(error) = handle_gate_private_message(&private_state, &symbol_map, text.as_ref()) {
                                                debug!(?error, "gate private websocket message ignored");
                                            }
                                        }
                                        Some(Ok(Message::Ping(payload))) => {
                                            if let Err(error) = socket.send(Message::Pong(payload)).await {
                                                private_state.record_connection_failure(now_ms(), unhealthy_after_failures, error.to_string());
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Close(frame))) => {
                                            private_state.record_connection_failure(now_ms(), unhealthy_after_failures, format!("closed:{frame:?}"));
                                            break;
                                        }
                                        Some(Ok(_)) => {}
                                        Some(Err(error)) => {
                                            private_state.record_connection_failure(now_ms(), unhealthy_after_failures, error.to_string());
                                            if is_benign_ws_disconnect_error(&error) {
                                                debug!(?error, "gate private websocket receive disconnected");
                                            }
                                            break;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        private_state.record_connection_failure(
                            now_ms(),
                            unhealthy_after_failures,
                            error.to_string(),
                        );
                        warn!(?error, "gate private websocket connect failed");
                    }
                }
                sleep(Duration::from_millis(
                    reconnect_backoff.on_failure_with_jitter(),
                ))
                .await;
            }
        });
        self.private_ws.push_worker(task);
    }

    fn cached_transfer_statuses(
        &self,
        wanted: &BTreeSet<String>,
        now_ms: i64,
    ) -> Option<Vec<AssetTransferStatus>> {
        let cache = self.transfer_status_cache.lock().expect("lock");
        let cache = cache.as_ref()?;
        if !cache_is_fresh(
            cache.observed_at_ms,
            now_ms,
            transfer_cache_ttl_ms(self.runtime.transfer_status_cache_ms),
        ) {
            return None;
        }
        Some(filter_transfer_statuses(cache, wanted))
    }

    fn cached_perp_liquidity_snapshot(
        &self,
        symbol: &str,
        observed_at_ms: i64,
    ) -> Option<PerpLiquiditySnapshot> {
        let snapshot = self
            .perp_liquidity_cache
            .lock()
            .expect("lock")
            .get(symbol)
            .cloned()?;
        if !cache_is_fresh(
            snapshot.observed_at_ms,
            observed_at_ms,
            GATE_PERP_LIQUIDITY_CACHE_TTL_MS,
        ) {
            return None;
        }
        Some(snapshot)
    }

    fn persist_symbol_catalog(&self) {
        let metadata = self.metadata.lock().expect("lock").clone();
        let supported_symbols = self
            .supported_symbols
            .lock()
            .expect("lock")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        store_json_cache(
            "gate-symbols.json",
            &GateSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols,
                metadata,
            },
        );
    }

    async fn refresh_symbol_catalog(&self) -> Result<()> {
        let payload = self
            .public_request_json(
                &self.base_url,
                Method::GET,
                &format!("/api/v4/futures/{GATE_SETTLE}/contracts"),
                None,
                "failed to request gate futures contracts",
            )
            .await?;
        let rows = payload
            .as_array()
            .ok_or_else(|| anyhow!("gate futures contracts response is not an array"))?;
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        for row in rows {
            if !gate_contract_is_tradeable(row) {
                continue;
            }
            let (symbol, meta) = parse_gate_contract_meta(row)?;
            metadata.insert(symbol.clone(), meta);
            supported_symbols.insert(symbol);
        }
        *self.metadata.lock().expect("lock") = metadata;
        *self.supported_symbols.lock().expect("lock") = supported_symbols;
        self.persist_symbol_catalog();
        Ok(())
    }

    async fn symbol_meta(&self, symbol: &str) -> Result<GateContractMeta> {
        if let Some(meta) = self.metadata.lock().expect("lock").get(symbol).cloned() {
            return Ok(meta);
        }
        self.refresh_symbol_catalog().await?;
        self.metadata
            .lock()
            .expect("lock")
            .get(symbol)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "gate contract metadata missing for {}",
                    venue_symbol(&self.config, symbol)
                )
            })
    }

    async fn public_request_json(
        &self,
        base_url: &str,
        method: Method,
        path: &str,
        query: Option<String>,
        context: &str,
    ) -> Result<Value> {
        let url = if let Some(query) = query.as_ref() {
            format!("{base_url}{path}?{query}")
        } else {
            format!("{base_url}{path}")
        };
        let response = self
            .client
            .request(method, url)
            .send()
            .await
            .with_context(|| context.to_string())?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format_gate_http_error(status, &body));
        }
        response
            .json::<Value>()
            .await
            .with_context(|| context.to_string())
    }

    async fn signed_request_json(
        &self,
        base_url: &str,
        method: Method,
        path: &str,
        query: Option<String>,
        body: Option<String>,
        context: &str,
    ) -> Result<Value> {
        let api_key = self
            .config
            .live
            .resolved_api_key()
            .ok_or_else(|| anyhow!("gate api key is not configured"))?;
        let api_secret = self
            .config
            .live
            .resolved_api_secret()
            .ok_or_else(|| anyhow!("gate api secret is not configured"))?;
        let timestamp = (now_ms() / 1_000).to_string();
        let query_payload = query.clone().unwrap_or_default();
        let body_payload = body.clone().unwrap_or_default();
        let body_hash = hex::encode(Sha512::digest(body_payload.as_bytes()));
        let sign_payload = format!(
            "{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            path,
            query_payload,
            body_hash,
            timestamp
        );
        let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes())
            .context("failed to init gate hmac")?;
        mac.update(sign_payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let mut headers = HeaderMap::new();
        headers.insert("KEY", HeaderValue::from_str(&api_key)?);
        headers.insert("Timestamp", HeaderValue::from_str(&timestamp)?);
        headers.insert("SIGN", HeaderValue::from_str(&signature)?);
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        let url = if query_payload.is_empty() {
            format!("{base_url}{path}")
        } else {
            format!("{base_url}{path}?{query_payload}")
        };
        let request = self.client.request(method, url).headers(headers);
        let request = if let Some(body) = body {
            request.body(body)
        } else {
            request
        };
        let response = request.send().await.with_context(|| context.to_string())?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format_gate_http_error(status, &body));
        }
        response
            .json::<Value>()
            .await
            .with_context(|| context.to_string())
    }

    async fn fetch_tickers_map(&self) -> Result<HashMap<String, Value>> {
        let payload = self
            .public_request_json(
                &self.base_url,
                Method::GET,
                &format!("/api/v4/futures/{GATE_SETTLE}/tickers"),
                None,
                "failed to request gate futures tickers",
            )
            .await?;
        let rows = payload
            .as_array()
            .ok_or_else(|| anyhow!("gate futures tickers response is not an array"))?;
        let mut by_symbol = HashMap::new();
        for row in rows {
            let Some(contract) = json_string(row, &["contract", "name"]) else {
                continue;
            };
            by_symbol.insert(normalize_gate_contract(&contract), row.clone());
        }
        Ok(by_symbol)
    }

    async fn submit_order_once(
        &self,
        request: &OrderRequest,
        contracts: f64,
        meta: &GateContractMeta,
    ) -> Result<GateOrderResponseWithTiming> {
        let sign_started_at = Instant::now();
        let size = match request.side {
            Side::Buy => contracts,
            Side::Sell => -contracts,
        };
        let body = serde_json::json!({
            "contract": venue_symbol(&self.config, &request.symbol),
            "size": format_decimal(size, meta.contract_step),
            "price": "0",
            "tif": "ioc",
            "reduce_only": request.reduce_only,
            "text": gate_client_text(&request.client_order_id),
        })
        .to_string();
        let request_sign_ms = elapsed_ms(sign_started_at);
        let submit_started_at = Instant::now();
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::POST,
                &format!("/api/v4/futures/{GATE_SETTLE}/orders"),
                None,
                Some(body),
                "failed to submit gate futures order",
            )
            .await?;
        let submit_http_ms = elapsed_ms(submit_started_at);
        let response_decode_ms = 0;
        Ok(GateOrderResponseWithTiming {
            response: payload,
            request_sign_ms,
            submit_http_ms,
            response_decode_ms,
        })
    }

    async fn fetch_order_reconciliation_with_retry(
        &self,
        symbol: &str,
        order_id: &str,
        client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        for attempt in 0..3 {
            if let Some(reconciliation) = self
                .fetch_order_fill_reconciliation(symbol, order_id, client_order_id)
                .await?
            {
                return Ok(Some(reconciliation));
            }
            sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
        }
        Ok(None)
    }
}

#[async_trait]
impl VenueAdapter for GateLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Gate
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let tracked = self.tracked_symbols(symbols);
        if tracked.is_empty() {
            return Err(anyhow!(
                "gate market snapshot unavailable for requested symbols"
            ));
        }
        let mut snapshots = Vec::new();
        let mut missing = Vec::new();
        for symbol in tracked {
            if let Some(snapshot) = self.cached_snapshot(&symbol) {
                snapshots.push(snapshot);
            } else {
                missing.push(symbol);
            }
        }
        if missing.is_empty() && !snapshots.is_empty() {
            return Ok(VenueMarketSnapshot {
                venue: Venue::Gate,
                observed_at_ms: now_ms(),
                symbols: snapshots,
            });
        }
        let rows = self.fetch_tickers_map().await?;
        let observed_at_ms = now_ms();
        for symbol in missing {
            let Some(row) = rows.get(&symbol) else {
                continue;
            };
            let meta = self.symbol_meta(&symbol).await?;
            snapshots.push(parse_gate_market_snapshot(&symbol, row, &meta)?);
        }
        if snapshots.is_empty() {
            return Err(anyhow!(
                "gate market snapshot unavailable for requested symbols"
            ));
        }
        Ok(VenueMarketSnapshot {
            venue: Venue::Gate,
            observed_at_ms,
            symbols: snapshots,
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        ensure_gate_client_order_id(&request.client_order_id)?;
        let order_prepare_started_at = Instant::now();
        let meta = self.symbol_meta(&request.symbol).await?;
        let contracts = floor_to_step(
            request.quantity / meta.contract_multiplier,
            meta.contract_step,
        );
        if contracts < meta.min_contracts {
            return Err(anyhow!(
                "gate quantity rounded below minimum contract size for {}",
                request.symbol
            ));
        }
        if let Some(max_contracts) = meta.max_contracts {
            if contracts > max_contracts + 1e-9 {
                return Err(anyhow!(
                    "gate quantity exceeds maximum contract size for {}",
                    request.symbol
                ));
            }
        }
        let quantity = contracts * meta.contract_multiplier;
        if let Some(price_hint) = request
            .price_hint
            .filter(|price| price.is_finite() && *price > 0.0)
        {
            let min_notional =
                meta.min_contracts.max(meta.contract_step) * meta.contract_multiplier * price_hint;
            if quantity * price_hint + 1e-9 < min_notional {
                return Err(anyhow!(
                    "gate notional below minimum for {}",
                    request.symbol
                ));
            }
        }
        let order_prepare_ms = elapsed_ms(order_prepare_started_at);
        let submit_started_at = Instant::now();
        let response = self.submit_order_once(&request, contracts, &meta).await?;
        let submit_ack_ms = elapsed_ms(submit_started_at);
        let order_id =
            json_string(&response.response, &["id"]).unwrap_or_else(|| "gate-unknown".to_string());
        let mut fill = OrderFill {
            venue: Venue::Gate,
            symbol: request.symbol.clone(),
            side: request.side,
            quantity,
            average_price: request.price_hint.unwrap_or_default(),
            fee_quote: estimate_fee_quote(
                request.price_hint.unwrap_or_default(),
                quantity,
                self.config.taker_fee_bps,
            ),
            order_id: order_id.clone(),
            filled_at_ms: now_ms(),
            timing: Some(OrderExecutionTiming {
                quote_resolve_ms: None,
                order_prepare_ms: Some(order_prepare_ms),
                request_sign_ms: Some(response.request_sign_ms),
                submit_http_ms: Some(response.submit_http_ms),
                response_decode_ms: Some(response.response_decode_ms),
                private_fill_wait_ms: None,
                submit_ack_ms: Some(submit_ack_ms),
            }),
        };
        let private_fill_wait_started_at = Instant::now();
        if let Some(private_fill) = lookup_or_wait_private_order(
            &self.private_ws,
            Some(&request.client_order_id),
            Some(&order_id),
            GATE_PRIVATE_FILL_WAIT_MS,
        )
        .await
        {
            fill = enrich_fill_from_private(fill, &private_fill);
        }
        let reconcile_started_at = Instant::now();
        if let Some(reconciliation) = self
            .fetch_order_reconciliation_with_retry(
                &request.symbol,
                &order_id,
                Some(&request.client_order_id),
            )
            .await?
        {
            fill.quantity = reconciliation.quantity;
            fill.average_price = reconciliation.average_price;
            fill.fee_quote = reconciliation.fee_quote.unwrap_or_else(|| {
                estimate_fee_quote(fill.average_price, fill.quantity, self.config.taker_fee_bps)
            });
            fill.filled_at_ms = reconciliation.filled_at_ms;
        } else if let Some(price_hint) = request.price_hint {
            fill.average_price = price_hint;
            fill.filled_at_ms = request.observed_at_ms.unwrap_or_else(now_ms);
        } else {
            let snapshot = self
                .fetch_market_snapshot(&[request.symbol.clone()])
                .await?;
            let (price, ts_ms) = match request.side {
                Side::Buy => {
                    let quote = snapshot
                        .symbols
                        .first()
                        .ok_or_else(|| anyhow!("gate fallback quote missing"))?;
                    (
                        quote.best_ask,
                        snapshot
                            .observed_at_ms
                            .max(quote.funding_timestamp_ms.min(now_ms())),
                    )
                }
                Side::Sell => {
                    let quote = snapshot
                        .symbols
                        .first()
                        .ok_or_else(|| anyhow!("gate fallback quote missing"))?;
                    (
                        quote.best_bid,
                        snapshot
                            .observed_at_ms
                            .max(quote.funding_timestamp_ms.min(now_ms())),
                    )
                }
            };
            fill.average_price = price;
            fill.filled_at_ms = ts_ms;
        }
        if let Some(timing) = fill.timing.as_mut() {
            timing.private_fill_wait_ms = Some(elapsed_ms(private_fill_wait_started_at));
            if timing.private_fill_wait_ms == Some(0) {
                timing.private_fill_wait_ms = Some(elapsed_ms(reconcile_started_at));
            }
        }
        Ok(fill)
    }

    fn cached_position(&self, symbol: &str) -> Option<PositionSnapshot> {
        self.private_ws
            .position_if_fresh(symbol, self.runtime.private_position_max_age_ms, now_ms())
            .map(|position| PositionSnapshot {
                venue: Venue::Gate,
                symbol: symbol.to_string(),
                size: position.size,
                updated_at_ms: position.updated_at_ms,
            })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<PositionSnapshot> {
        if let Some(position) = self.private_ws.position_if_fresh(
            symbol,
            self.runtime.private_position_max_age_ms,
            now_ms(),
        ) {
            return Ok(PositionSnapshot {
                venue: Venue::Gate,
                symbol: symbol.to_string(),
                size: position.size,
                updated_at_ms: position.updated_at_ms,
            });
        }
        let positions = self.fetch_all_positions().await?.unwrap_or_default();
        Ok(positions
            .into_iter()
            .find(|position| position.symbol == symbol)
            .unwrap_or(PositionSnapshot {
                venue: Venue::Gate,
                symbol: symbol.to_string(),
                size: 0.0,
                updated_at_ms: now_ms(),
            }))
    }

    async fn fetch_all_positions(&self) -> Result<Option<Vec<PositionSnapshot>>> {
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                &format!("/api/v4/futures/{GATE_SETTLE}/positions"),
                None,
                None,
                "failed to request gate futures positions",
            )
            .await?;
        let rows = payload
            .as_array()
            .ok_or_else(|| anyhow!("gate positions response is not an array"))?;
        let mut positions = Vec::new();
        for row in rows {
            let Some(contract) = json_string(row, &["contract"]) else {
                continue;
            };
            let symbol = normalize_gate_contract(&contract);
            let Some(meta) = self.metadata.lock().expect("lock").get(&symbol).cloned() else {
                continue;
            };
            let contracts = json_f64(row, &["size"]).unwrap_or_default();
            if contracts.abs() <= 0.0 {
                continue;
            }
            positions.push(PositionSnapshot {
                venue: Venue::Gate,
                symbol,
                size: contracts * meta.contract_multiplier,
                updated_at_ms: json_i64(row, &["update_time_ms", "update_time"])
                    .map(|value| {
                        if value < 10_000_000_000 {
                            value * 1_000
                        } else {
                            value
                        }
                    })
                    .unwrap_or_else(now_ms),
            });
        }
        Ok(Some(positions))
    }

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                &format!("/api/v4/futures/{GATE_SETTLE}/accounts"),
                None,
                None,
                "failed to request gate futures account",
            )
            .await?;
        Ok(Some(AccountBalanceSnapshot {
            venue: Venue::Gate,
            equity_quote: json_required_f64(
                &payload,
                &["total", "total_usdt", "equity"],
                "gate total equity",
            )?,
            wallet_balance_quote: json_f64(&payload, &["total", "total_usdt", "equity"]),
            available_balance_quote: json_f64(&payload, &["available", "available_balance"]),
            observed_at_ms: now_ms(),
        }))
    }

    fn cached_account_fee_snapshot(&self) -> Option<AccountFeeSnapshot> {
        self.account_fee_snapshot.lock().expect("lock").clone()
    }

    fn enforces_entry_balance_gate(&self) -> bool {
        true
    }

    async fn fetch_order_fill_reconciliation(
        &self,
        symbol: &str,
        order_id: &str,
        _client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        let contract = venue_symbol(&self.config, symbol);
        let query = build_query(&[
            ("order", order_id.to_string()),
            ("contract", contract.clone()),
        ]);
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                &format!("/api/v4/futures/{GATE_SETTLE}/my_trades"),
                Some(query),
                None,
                "failed to request gate futures trades",
            )
            .await?;
        let rows = payload
            .as_array()
            .ok_or_else(|| anyhow!("gate my_trades response is not an array"))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let meta = self.symbol_meta(symbol).await?;
        let mut total_quantity = 0.0;
        let mut weighted_notional = 0.0;
        let mut total_fee_quote = 0.0;
        let mut latest_fill_ms = 0_i64;
        for row in rows {
            let contracts = json_f64(row, &["size", "fill_size"])
                .unwrap_or_default()
                .abs();
            if contracts <= 0.0 {
                continue;
            }
            let quantity = contracts * meta.contract_multiplier;
            let price = json_required_f64(row, &["price", "fill_price"], "gate trade price")?;
            total_quantity += quantity;
            weighted_notional += quantity * price;
            total_fee_quote += json_f64(row, &["fee"]).unwrap_or_default().abs();
            latest_fill_ms = latest_fill_ms.max(
                json_i64(row, &["create_time_ms", "create_time"])
                    .map(|value| {
                        if value < 10_000_000_000 {
                            value * 1_000
                        } else {
                            value
                        }
                    })
                    .unwrap_or_else(now_ms),
            );
        }
        if total_quantity <= 0.0 {
            return Ok(None);
        }
        Ok(Some(OrderFillReconciliation {
            order_id: order_id.to_string(),
            client_order_id: None,
            quantity: total_quantity,
            average_price: weighted_notional / total_quantity,
            fee_quote: Some(total_fee_quote).filter(|value| *value > 0.0),
            filled_at_ms: latest_fill_ms.max(now_ms()),
        }))
    }

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        let meta = self.symbol_meta(symbol).await?;
        let contracts = floor_to_step(quantity / meta.contract_multiplier, meta.contract_step);
        if contracts < meta.min_contracts {
            return Ok(0.0);
        }
        Ok(contracts * meta.contract_multiplier)
    }

    async fn ensure_entry_leverage(&self, symbol: &str, leverage: u32) -> Result<()> {
        if self
            .configured_leverage
            .lock()
            .expect("lock")
            .get(symbol)
            .copied()
            == Some(leverage)
        {
            return Ok(());
        }
        let body = serde_json::json!({
            "leverage": leverage.to_string(),
        })
        .to_string();
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::POST,
                &format!(
                    "/api/v4/futures/{GATE_SETTLE}/positions/{}/leverage",
                    venue_symbol(&self.config, symbol)
                ),
                None,
                Some(body),
                "failed to set gate leverage",
            )
            .await?;
        let _ = payload;
        self.configured_leverage
            .lock()
            .expect("lock")
            .insert(symbol.to_string(), leverage);
        Ok(())
    }

    fn min_entry_notional_quote_hint(&self, symbol: &str, price_hint: Option<f64>) -> Option<f64> {
        let price_hint = price_hint.filter(|price| price.is_finite() && *price > 0.0)?;
        let meta = self.metadata.lock().expect("lock").get(symbol)?.clone();
        Some(meta.min_contracts.max(meta.contract_step) * meta.contract_multiplier * price_hint)
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<PerpLiquiditySnapshot>> {
        if !self.supports_symbol(symbol) {
            return Ok(None);
        }
        let observed_at_ms = now_ms();
        if let Some(snapshot) = self.cached_perp_liquidity_snapshot(symbol, observed_at_ms) {
            return Ok(Some(snapshot));
        }
        let rows = self.fetch_tickers_map().await?;
        let Some(row) = rows.get(symbol) else {
            return Ok(None);
        };
        let snapshot = parse_gate_liquidity_snapshot(symbol, row)?;
        self.perp_liquidity_cache
            .lock()
            .expect("lock")
            .insert(symbol.to_string(), snapshot.clone());
        Ok(Some(snapshot))
    }

    async fn fetch_transfer_statuses(&self, assets: &[String]) -> Result<Vec<AssetTransferStatus>> {
        let wanted = assets
            .iter()
            .map(|asset| base_asset(asset))
            .collect::<BTreeSet<_>>();
        if wanted.is_empty() {
            return Ok(Vec::new());
        }
        let observed_at_ms = now_ms();
        if let Some(statuses) = self.cached_transfer_statuses(&wanted, observed_at_ms) {
            return Ok(statuses);
        }
        let payload = self
            .public_request_json(
                &self.wallet_base_url,
                Method::GET,
                "/api/v4/wallet/currency_chains",
                None,
                "failed to request gate currency chains",
            )
            .await?;
        let rows = payload
            .as_array()
            .ok_or_else(|| anyhow!("gate currency chains response is not an array"))?;
        let statuses = rows
            .iter()
            .fold(
                HashMap::<String, AssetTransferStatus>::new(),
                |mut acc, row| {
                    let Some(asset) = json_string(row, &["currency"]) else {
                        return acc;
                    };
                    let asset = base_asset(&asset);
                    if !wanted.contains(asset.as_str()) {
                        return acc;
                    }
                    let entry = acc.entry(asset.clone()).or_insert(AssetTransferStatus {
                        venue: Venue::Gate,
                        asset,
                        deposit_enabled: false,
                        withdraw_enabled: false,
                        observed_at_ms,
                        source: "gate".to_string(),
                    });
                    entry.deposit_enabled |=
                        !json_bool(row, &["is_deposit_disabled", "deposit_disabled"])
                            .unwrap_or(false);
                    entry.withdraw_enabled |=
                        !json_bool(row, &["is_withdraw_disabled", "withdraw_disabled"])
                            .unwrap_or(false);
                    acc
                },
            )
            .into_values()
            .collect::<Vec<_>>();
        let cache = VenueTransferStatusCache {
            observed_at_ms,
            statuses: statuses.clone(),
        };
        store_json_cache("gate-transfer-status.json", &cache);
        self.transfer_status_cache
            .lock()
            .expect("lock")
            .replace(cache);
        Ok(statuses)
    }

    fn supported_symbols(&self, requested_symbols: &[String]) -> Option<Vec<String>> {
        Some(self.tracked_symbols(requested_symbols))
    }

    fn supports_market_data_activity_control(&self) -> bool {
        true
    }

    async fn set_market_data_active(&self, active: bool, symbols: &[String]) -> Result<()> {
        let tracked_symbols = self.tracked_symbols(symbols);
        let mut current_symbols = self.market_subscription_symbols.lock().expect("lock");
        if !active || tracked_symbols.is_empty() {
            if self.market_ws.has_worker() || !current_symbols.is_empty() {
                self.market_ws.abort_worker();
                self.market_ws.clear();
                current_symbols.clear();
            }
            return Ok(());
        }
        if self.market_ws.has_worker() && *current_symbols == tracked_symbols {
            return Ok(());
        }
        self.market_ws.abort_worker();
        self.market_ws.clear();
        self.start_market_ws(&tracked_symbols);
        *current_symbols = tracked_symbols;
        Ok(())
    }

    fn market_worker_count(&self) -> usize {
        self.market_ws.worker_count()
    }

    fn private_worker_count(&self) -> usize {
        self.private_ws.worker_count()
    }

    async fn live_startup_prewarm(&self) -> Result<()> {
        let _ = self.fetch_account_balance_snapshot().await?;
        if let Err(error) = self.refresh_account_fee_snapshot().await {
            warn!(?error, "gate account fee snapshot prewarm failed");
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.market_ws.abort_worker();
        self.private_ws.abort_workers();
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GateContractMeta {
    contract_multiplier: f64,
    min_contracts: f64,
    contract_step: f64,
    max_contracts: Option<f64>,
    maker_fee_rate_bps: Option<f64>,
    taker_fee_rate_bps: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GateSymbolCatalogCache {
    updated_at_ms: i64,
    supported_symbols: Vec<String>,
    metadata: HashMap<String, GateContractMeta>,
}

struct GateOrderResponseWithTiming {
    response: Value,
    request_sign_ms: u64,
    submit_http_ms: u64,
    response_decode_ms: u64,
}

fn parse_gate_contract_meta(row: &Value) -> Result<(String, GateContractMeta)> {
    let symbol = normalize_gate_contract(&json_required_string(
        row,
        &["name", "contract"],
        "gate contract name",
    )?);
    Ok((
        symbol,
        GateContractMeta {
            contract_multiplier: json_required_f64(
                row,
                &["quanto_multiplier", "quantoMultiplier"],
                "gate quanto multiplier",
            )?,
            min_contracts: json_required_f64(
                row,
                &["order_size_min", "orderSizeMin"],
                "gate minimum order size",
            )?,
            contract_step: json_f64(row, &["order_size_round", "orderSizeRound"]).unwrap_or(1.0),
            max_contracts: json_f64(
                row,
                &[
                    "market_order_size_max",
                    "order_size_max",
                    "orderSizeMax",
                    "max_order_size",
                ],
            ),
            maker_fee_rate_bps: json_f64(row, &["maker_fee_rate", "makerFeeRate"])
                .map(|value| value * 10_000.0),
            taker_fee_rate_bps: json_f64(row, &["taker_fee_rate", "takerFeeRate"])
                .map(|value| value * 10_000.0),
        },
    ))
}

fn parse_gate_fee_snapshot(payload: &Value) -> Result<AccountFeeSnapshot> {
    let taker_fee_bps = json_f64(
        payload,
        &[
            "futures_taker_fee",
            "futuresTakerFee",
            "taker_fee_rate",
            "taker_fee",
            "takerFeeRate",
            "takerFee",
        ],
    )
    .ok_or_else(|| anyhow!("gate taker fee missing"))?
        * 10_000.0;
    let maker_fee_bps = json_f64(
        payload,
        &[
            "futures_maker_fee",
            "futuresMakerFee",
            "maker_fee_rate",
            "maker_fee",
            "makerFeeRate",
            "makerFee",
        ],
    )
    .ok_or_else(|| anyhow!("gate maker fee missing"))?
        * 10_000.0;
    Ok(AccountFeeSnapshot {
        venue: Venue::Gate,
        taker_fee_bps,
        maker_fee_bps,
        observed_at_ms: now_ms(),
        source: "gate_wallet_fee".to_string(),
    })
}

fn parse_gate_liquidity_snapshot(symbol: &str, row: &Value) -> Result<PerpLiquiditySnapshot> {
    let volume_24h_quote = json_required_f64(
        row,
        &["volume_24h_quote", "volume_24h_usd", "turnover_24h"],
        "gate 24h quote volume",
    )?;
    let mark_price =
        json_required_f64(row, &["mark_price", "markPrice", "last"], "gate mark price")?;
    let open_interest_contracts = json_required_f64(
        row,
        &["open_interest", "openInterest"],
        "gate open interest",
    )?;
    let contract_multiplier = json_f64(row, &["quanto_multiplier"]).unwrap_or(1.0);
    let observed_at_ms = json_i64(row, &["funding_next_apply", "funding_next_time"])
        .map(|value| {
            if value < 10_000_000_000 {
                value * 1_000
            } else {
                value
            }
        })
        .unwrap_or_else(now_ms);
    Ok(PerpLiquiditySnapshot {
        venue: Venue::Gate,
        symbol: symbol.to_string(),
        volume_24h_quote,
        open_interest_quote: open_interest_contracts * contract_multiplier * mark_price,
        observed_at_ms,
    })
}

fn parse_gate_market_snapshot(
    symbol: &str,
    row: &Value,
    meta: &GateContractMeta,
) -> Result<SymbolMarketSnapshot> {
    let highest_size = json_f64(row, &["highest_size", "highestSize"]).unwrap_or_default();
    let lowest_size = json_f64(row, &["lowest_size", "lowestSize"]).unwrap_or_default();
    Ok(SymbolMarketSnapshot {
        symbol: symbol.to_string(),
        best_bid: json_required_f64(row, &["highest_bid", "highestBid"], "gate best bid")?,
        best_ask: json_required_f64(row, &["lowest_ask", "lowestAsk"], "gate best ask")?,
        bid_size: highest_size * meta.contract_multiplier,
        ask_size: lowest_size * meta.contract_multiplier,
        mark_price: json_f64(row, &["mark_price", "markPrice", "last"]),
        funding_rate: json_f64(row, &["funding_rate", "fundingRate"]).unwrap_or_default(),
        funding_timestamp_ms: json_i64(row, &["funding_next_apply", "funding_next_time"])
            .map(|value| {
                if value < 10_000_000_000 {
                    value * 1_000
                } else {
                    value
                }
            })
            .unwrap_or_else(|| now_ms() + 8 * 60 * 60 * 1_000),
    })
}

fn gate_contract_is_tradeable(row: &Value) -> bool {
    if json_bool(row, &["in_delisting", "delisted"]).unwrap_or(false) {
        return false;
    }
    match json_string(row, &["status", "trade_status"]) {
        Some(status) => {
            let status = status.to_ascii_lowercase();
            matches!(
                status.as_str(),
                "trading" | "tradable" | "open" | "online" | "normal"
            )
        }
        None => true,
    }
}

fn ensure_gate_client_order_id(client_order_id: &str) -> Result<()> {
    let len = client_order_id.chars().count();
    if len == 0 || len > 28 {
        return Err(anyhow!(
            "gate client order id must be between 1 and 28 characters before t- prefix"
        ));
    }
    if client_order_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        Ok(())
    } else {
        Err(anyhow!(
            "gate client order id contains unsupported characters"
        ))
    }
}

fn gate_client_text(client_order_id: &str) -> String {
    format!("{GATE_CLIENT_TEXT_PREFIX}{client_order_id}")
}

fn gate_ws_url(base_url: &str) -> &'static str {
    if base_url.contains("testnet") {
        "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt"
    } else {
        "wss://fx-ws.gateio.ws/v4/ws/usdt"
    }
}

fn gate_ws_auth(
    api_key: &str,
    api_secret: &str,
    channel: &str,
    event: &str,
    time: u64,
) -> Result<Value> {
    let payload = format!("channel={channel}&event={event}&time={time}");
    let mut mac =
        HmacSha512::new_from_slice(api_secret.as_bytes()).context("failed to init gate ws hmac")?;
    mac.update(payload.as_bytes());
    Ok(serde_json::json!({
        "method": "api_key",
        "KEY": api_key,
        "SIGN": hex::encode(mac.finalize().into_bytes()),
    }))
}

fn handle_gate_private_message(
    private_state: &std::sync::Arc<WsPrivateState>,
    symbol_map: &HashMap<String, String>,
    raw: &str,
) -> Result<()> {
    let payload = parse_text_message(raw)?;
    let channel = payload
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let rows = payload
        .get("result")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| payload.get("data").and_then(Value::as_array).cloned())
        .or_else(|| payload.get("result").cloned().map(|row| vec![row]))
        .unwrap_or_default();
    match channel {
        "futures.orders" | "futures.usertrades" => {
            for row in rows {
                let contract = json_string(&row, &["contract"]).unwrap_or_default();
                let symbol = symbol_map
                    .get(&contract)
                    .cloned()
                    .unwrap_or_else(|| normalize_gate_contract(&contract));
                let contracts = json_f64(&row, &["size", "fill_size"])
                    .unwrap_or_default()
                    .abs();
                private_state.record_order(PrivateOrderUpdate {
                    symbol,
                    order_id: json_string(&row, &["id", "order_id"]).unwrap_or_default(),
                    client_order_id: None,
                    filled_quantity: Some(contracts),
                    average_price: json_f64(&row, &["price", "fill_price"]),
                    fee_quote: json_f64(&row, &["fee"]).map(f64::abs),
                    updated_at_ms: json_i64(
                        &row,
                        &["finish_time_ms", "create_time_ms", "time_ms", "time"],
                    )
                    .map(|value| {
                        if value < 10_000_000_000 {
                            value * 1_000
                        } else {
                            value
                        }
                    })
                    .unwrap_or_else(now_ms),
                });
            }
        }
        "futures.positions" => {
            for row in rows {
                let contract = json_string(&row, &["contract"]).unwrap_or_default();
                let symbol = symbol_map
                    .get(&contract)
                    .cloned()
                    .unwrap_or_else(|| normalize_gate_contract(&contract));
                let size = json_f64(&row, &["size"]).unwrap_or_default();
                private_state.update_position(
                    &symbol,
                    size,
                    json_i64(&row, &["update_time_ms", "update_time", "time_ms", "time"])
                        .map(|value| {
                            if value < 10_000_000_000 {
                                value * 1_000
                            } else {
                                value
                            }
                        })
                        .unwrap_or_else(now_ms),
                );
            }
        }
        _ => {}
    }
    Ok(())
}

fn normalize_gate_contract(raw: &str) -> String {
    raw.trim()
        .to_ascii_uppercase()
        .replace('_', "")
        .replace('-', "")
}

fn format_gate_http_error(status: StatusCode, body: &str) -> anyhow::Error {
    let payload = serde_json::from_str::<Value>(body).ok();
    let label = payload
        .as_ref()
        .and_then(|value| json_string(value, &["label"]))
        .unwrap_or_default();
    let message = payload
        .as_ref()
        .and_then(|value| json_string(value, &["message", "detail"]))
        .unwrap_or_else(|| body.trim().to_string());
    anyhow!(
        "gate private/public endpoint returned non-success status: status={} label={} msg={}",
        status,
        label,
        message
    )
}

fn json_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::String(raw) => Some(raw.to_string()),
            Value::Number(raw) => Some(raw.to_string()),
            Value::Bool(raw) => Some(raw.to_string()),
            _ => None,
        }
    })
}

fn json_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::String(raw) => parse_f64(raw).ok(),
            Value::Number(raw) => raw.as_f64(),
            _ => None,
        }
    })
}

fn json_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::String(raw) => parse_i64(raw).ok(),
            Value::Number(raw) => raw
                .as_i64()
                .or_else(|| raw.as_f64().map(|value| value as i64)),
            _ => None,
        }
    })
}

fn json_bool(value: &Value, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        match value {
            Value::Bool(raw) => Some(*raw),
            Value::String(raw) => Some(matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )),
            Value::Number(raw) => Some(raw.as_i64().unwrap_or_default() != 0),
            _ => None,
        }
    })
}

fn json_required_string(value: &Value, keys: &[&str], field: &str) -> Result<String> {
    json_string(value, keys).ok_or_else(|| anyhow!("{field} missing"))
}

fn json_required_f64(value: &Value, keys: &[&str], field: &str) -> Result<f64> {
    json_f64(value, keys).ok_or_else(|| anyhow!("{field} missing"))
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ensure_gate_client_order_id, gate_client_text, gate_ws_auth, gate_ws_url,
        handle_gate_private_message, parse_gate_contract_meta, parse_gate_fee_snapshot,
        parse_gate_liquidity_snapshot,
    };
    use crate::live::WsPrivateState;
    use std::collections::HashMap;

    #[test]
    fn parses_gate_contract_meta() {
        let row = json!({
            "name": "BTC_USDT",
            "order_size_min": 1,
            "order_size_round": 1,
            "quanto_multiplier": "0.001",
            "maker_fee_rate": "-0.00005",
            "taker_fee_rate": "0.0005"
        });

        let (symbol, meta) = parse_gate_contract_meta(&row).expect("parse contract");
        assert_eq!(symbol, "BTCUSDT");
        assert!((meta.min_contracts - 1.0).abs() < 1e-9);
        assert!((meta.contract_step - 1.0).abs() < 1e-9);
        assert!((meta.contract_multiplier - 0.001).abs() < 1e-9);
        assert_eq!(meta.max_contracts, None);
        assert_eq!(meta.maker_fee_rate_bps, Some(-0.5));
        assert_eq!(meta.taker_fee_rate_bps, Some(5.0));
    }

    #[test]
    fn parses_gate_wallet_fee_snapshot() {
        let row = json!({
            "futures_taker_fee": "0.0005",
            "futures_maker_fee": "-0.00005"
        });

        let snapshot = parse_gate_fee_snapshot(&row).expect("parse gate wallet fee");
        assert_eq!(snapshot.venue, crate::models::Venue::Gate);
        assert_eq!(snapshot.taker_fee_bps, 5.0);
        assert_eq!(snapshot.maker_fee_bps, -0.5);
        assert_eq!(snapshot.source, "gate_wallet_fee");
    }

    #[test]
    fn parses_gate_liquidity_snapshot() {
        let row = json!({
            "contract": "BTC_USDT",
            "volume_24h_quote": "2500000",
            "mark_price": "65000",
            "open_interest": "20",
            "quanto_multiplier": "1",
            "funding_next_apply": 1710001234
        });

        let snapshot = parse_gate_liquidity_snapshot("BTCUSDT", &row).expect("parse liquidity");
        assert_eq!(snapshot.symbol, "BTCUSDT");
        assert!((snapshot.volume_24h_quote - 2_500_000.0).abs() < 1e-6);
        assert!((snapshot.open_interest_quote - 1_300_000.0).abs() < 1e-6);
        assert_eq!(snapshot.observed_at_ms, 1_710_001_234_000);
    }

    #[test]
    fn gate_ws_helpers_use_gate_hosts_and_auth_shape() {
        assert_eq!(
            gate_ws_url("https://api.gateio.ws"),
            "wss://fx-ws.gateio.ws/v4/ws/usdt"
        );
        let auth = gate_ws_auth("key", "secret", "futures.orders", "subscribe", 1710000000)
            .expect("auth payload");
        assert_eq!(auth["method"], "api_key");
        assert_eq!(auth["KEY"], "key");
        assert!(auth["SIGN"].as_str().is_some());
    }

    #[test]
    fn gate_private_position_update_is_recorded() {
        let state = WsPrivateState::new();
        let mut symbols = HashMap::new();
        symbols.insert("BTC_USDT".to_string(), "BTCUSDT".to_string());
        let payload = json!({
            "channel": "futures.positions",
            "result": [{
                "contract": "BTC_USDT",
                "size": "-12",
                "update_time_ms": 1710000000000_i64
            }]
        })
        .to_string();
        handle_gate_private_message(&state, &symbols, &payload).expect("handle private");
        let position = state
            .position_if_fresh("BTCUSDT", 60_000, 1710000000001_i64)
            .expect("position update recorded");
        assert_eq!(position.size, -12.0);
    }

    #[test]
    fn gate_client_order_id_validation_rejects_invalid_values() {
        assert!(ensure_gate_client_order_id("abc-123_foo.bar").is_ok());
        assert!(ensure_gate_client_order_id(&"x".repeat(28)).is_ok());
        assert_eq!(gate_client_text("abc-123"), "t-abc-123");
        assert!(ensure_gate_client_order_id("").is_err());
        assert!(ensure_gate_client_order_id(&"x".repeat(29)).is_err());
        assert!(ensure_gate_client_order_id("bad id").is_err());
    }
}
