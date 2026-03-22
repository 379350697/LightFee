use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Mutex,
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client, Method, StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    estimate_fee_quote, filter_transfer_statuses, floor_to_step, format_decimal, hinted_fill,
    hmac_sha256_base64, is_benign_ws_disconnect_error, load_account_fee_snapshot_cache,
    load_json_cache, lookup_or_wait_private_order, now_ms, parse_f64, parse_i64,
    parse_text_message, quote_fill, spawn_ws_loop, store_account_fee_snapshot_cache,
    store_json_cache, transfer_cache_ttl_ms, venue_symbol, PrivateOrderUpdate,
    VenueTransferStatusCache, WsMarketState, WsPrivateState, SYMBOL_CACHE_TTL_MS,
};
use crate::resilience::FailureBackoff;

const BITGET_PRODUCT_TYPE: &str = "USDT-FUTURES";
const BITGET_MARGIN_COIN: &str = "USDT";
const BITGET_PERP_LIQUIDITY_CACHE_TTL_MS: i64 = 60 * 1_000;
const BITGET_PRIVATE_FILL_WAIT_MS: u64 = 120;

pub struct BitgetLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    client: Client,
    base_url: String,
    wallet_base_url: String,
    metadata: Mutex<HashMap<String, BitgetSymbolMeta>>,
    supported_symbols: Mutex<HashSet<String>>,
    position_mode: Mutex<Option<BitgetPositionMode>>,
    market_ws: std::sync::Arc<WsMarketState>,
    market_subscription_symbols: Mutex<Vec<String>>,
    private_ws: std::sync::Arc<WsPrivateState>,
    account_fee_snapshot: Mutex<Option<AccountFeeSnapshot>>,
    transfer_status_cache: Mutex<Option<VenueTransferStatusCache>>,
    perp_liquidity_cache: Mutex<HashMap<String, PerpLiquiditySnapshot>>,
    configured_leverage: Mutex<HashMap<String, u32>>,
}

impl BitgetLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        runtime: &RuntimeConfig,
        _symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Bitget {
            return Err(anyhow!("bitget live adapter requires bitget config"));
        }

        let persisted_catalog = load_json_cache::<BitgetSymbolCatalogCache>("bitget-symbols.json");
        let persisted_transfer_cache =
            load_json_cache::<VenueTransferStatusCache>("bitget-transfer-status.json");
        let account_fee_snapshot =
            load_account_fee_snapshot_cache(Venue::Bitget, "bitget-fees.json");
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        if let Some(cache) = persisted_catalog {
            if !cache_is_fresh(cache.updated_at_ms, now_ms(), SYMBOL_CACHE_TTL_MS) {
                debug!("bitget symbol catalog cache is stale; using as fallback seed");
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
                .unwrap_or_else(|| "https://api.bitget.com".to_string()),
            wallet_base_url: config
                .live
                .wallet_base_url
                .clone()
                .unwrap_or_else(|| "https://api.bitget.com".to_string()),
            metadata: Mutex::new(metadata),
            supported_symbols: Mutex::new(supported_symbols),
            position_mode: Mutex::new(None),
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
                "bitget symbol catalog refresh failed; using persisted cache"
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
        store_account_fee_snapshot_cache("bitget-fees.json", snapshot);
    }

    async fn refresh_account_fee_snapshot(&self) -> Result<Option<AccountFeeSnapshot>> {
        let Some(symbol) = self.fee_reference_symbol() else {
            return Ok(self.cached_account_fee_snapshot());
        };
        let venue_symbol = venue_symbol(&self.config, &symbol);
        let query = build_query(&[
            ("symbol", venue_symbol.clone()),
            ("category", BITGET_PRODUCT_TYPE.to_string()),
        ]);
        let payload = match self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                "/api/v3/account/fee-rate",
                Some(query),
                None,
                "failed to request bitget fee rate",
            )
            .await
        {
            Ok(payload) => payload,
            Err(error) if is_bitget_classic_account_fee_error(&error) => {
                debug!(?error, "bitget unified fee endpoint unsupported; trying classic trade rate");
                return self.refresh_classic_account_fee_snapshot(&symbol, &venue_symbol).await;
            }
            Err(error) => return Err(error),
        };
        let row = match bitget_data("bitget fee rate failed", &payload) {
            Ok(row) => row,
            Err(error) if is_bitget_classic_account_fee_error(&error) => {
                debug!(?error, "bitget unified fee payload reported classic account; trying classic trade rate");
                return self.refresh_classic_account_fee_snapshot(&symbol, &venue_symbol).await;
            }
            Err(error) => return Err(error),
        };
        let snapshot = AccountFeeSnapshot {
            venue: Venue::Bitget,
            taker_fee_bps: json_required_f64(row, &["takerFeeRate"], "bitget taker fee")?
                * 10_000.0,
            maker_fee_bps: json_required_f64(row, &["makerFeeRate"], "bitget maker fee")?
                * 10_000.0,
            observed_at_ms: now_ms(),
            source: format!("bitget_fee_rate:{}", symbol),
        };
        self.store_account_fee_snapshot(&snapshot);
        Ok(Some(snapshot))
    }

    async fn refresh_classic_account_fee_snapshot(
        &self,
        symbol: &str,
        venue_symbol_name: &str,
    ) -> Result<Option<AccountFeeSnapshot>> {
        let query = build_query(&[
            ("symbol", venue_symbol_name.to_string()),
            ("businessType", "mix".to_string()),
        ]);
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                "/api/v2/common/trade-rate",
                Some(query),
                None,
                "failed to request bitget classic trade rate",
            )
            .await?;
        let row = bitget_data("bitget classic trade rate failed", &payload)?;
        let snapshot = parse_bitget_trade_rate_snapshot(symbol, row)?;
        self.store_account_fee_snapshot(&snapshot);
        Ok(Some(snapshot))
    }

    fn supports_symbol(&self, symbol: &str) -> bool {
        self.supported_symbols
            .lock()
            .expect("lock")
            .contains(symbol)
    }

    fn seed_symbol(&self) -> Result<String> {
        self.supported_symbols
            .lock()
            .expect("lock")
            .iter()
            .next()
            .cloned()
            .ok_or_else(|| anyhow!("bitget supported symbol set is empty"))
    }

    fn start_market_ws(&self, symbols: &[String]) {
        if symbols.is_empty() {
            return;
        }
        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let args = symbols
            .iter()
            .map(|symbol| {
                serde_json::json!({
                    "instType": "USDT-FUTURES",
                    "channel": "ticker",
                    "instId": venue_symbol(&self.config, symbol),
                })
            })
            .collect::<Vec<_>>();
        let subscribe_message = serde_json::json!({
            "op": "subscribe",
            "args": args,
        })
        .to_string();
        let state = self.market_ws.clone();
        spawn_ws_loop(
            "bitget",
            bitget_public_ws_url(&self.base_url).to_string(),
            vec![subscribe_message],
            state,
            self.runtime.ws_reconnect_initial_ms,
            self.runtime.ws_reconnect_max_ms,
            self.runtime.ws_unhealthy_after_failures,
            move |cache, raw| {
                let payload = parse_text_message(raw)?;
                let arg = match payload.get("arg") {
                    Some(arg) => arg,
                    None => return Ok(()),
                };
                let channel = arg
                    .get("channel")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if channel != "ticker" {
                    return Ok(());
                }
                let venue_symbol = arg
                    .get("instId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("bitget ws ticker missing instId"))?;
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    return Ok(());
                };
                let rows = payload
                    .get("data")
                    .and_then(Value::as_array)
                    .ok_or_else(|| anyhow!("bitget ws ticker missing data"))?;
                for row in rows {
                    cache.update_quote(
                        symbol,
                        json_required_f64(row, &["bidPr", "bidPrice"], "bitget ws bidPr")?,
                        json_required_f64(row, &["askPr", "askPrice"], "bitget ws askPr")?,
                        json_f64(row, &["bidSz", "bidSize"]).unwrap_or_default(),
                        json_f64(row, &["askSz", "askSize"]).unwrap_or_default(),
                        json_i64(row, &["ts", "systemTime"]).unwrap_or_else(now_ms),
                    );
                    if let Some(mark_price) = json_f64(row, &["markPrice", "lastPr", "lastPrice"]) {
                        cache.update_mark_price(symbol, mark_price);
                    }
                    if let (Some(funding_rate), Some(funding_ts)) = (
                        json_f64(row, &["fundingRate"]),
                        json_i64(row, &["nextFundingTime", "fundingTime"]),
                    ) {
                        cache.update_funding(symbol, funding_rate, funding_ts);
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
        let (Some(api_key), Some(api_secret), Some(api_passphrase)) = (
            self.config.live.resolved_api_key(),
            self.config.live.resolved_api_secret(),
            self.config.live.resolved_api_passphrase(),
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
        let url = bitget_private_ws_url(&self.base_url).to_string();
        let task = tokio::spawn(async move {
            let mut reconnect_backoff =
                FailureBackoff::new(reconnect_initial_ms, reconnect_max_ms, Venue::Bitget as u64);
            loop {
                match connect_async(url.as_str()).await {
                    Ok((mut socket, _)) => {
                        reconnect_backoff.on_success();
                        private_state.record_connection_success(now_ms());
                        let timestamp = (now_ms() / 1_000).to_string();
                        let sign = match hmac_sha256_base64(api_secret.as_str(), &timestamp) {
                            Ok(signature) => signature,
                            Err(error) => {
                                private_state.record_connection_failure(
                                    now_ms(),
                                    unhealthy_after_failures,
                                    error.to_string(),
                                );
                                warn!(?error, "bitget private websocket auth sign failed");
                                sleep(Duration::from_millis(
                                    reconnect_backoff.on_failure_with_jitter(),
                                ))
                                .await;
                                continue;
                            }
                        };
                        let login = serde_json::json!({
                            "op": "login",
                            "args": [{
                                "apiKey": api_key,
                                "passphrase": api_passphrase,
                                "timestamp": timestamp,
                                "sign": sign,
                            }],
                        })
                        .to_string();
                        if let Err(error) = socket.send(Message::Text(login.into())).await {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                error.to_string(),
                            );
                            warn!(?error, "bitget private websocket login send failed");
                            sleep(Duration::from_millis(
                                reconnect_backoff.on_failure_with_jitter(),
                            ))
                            .await;
                            continue;
                        }
                        let mut subscribed = false;
                        let mut ping_interval = interval(Duration::from_secs(20));
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        loop {
                            tokio::select! {
                                _ = ping_interval.tick() => {
                                    if let Err(error) = socket.send(Message::Text("ping".to_string().into())).await {
                                        private_state.record_connection_failure(
                                            now_ms(),
                                            unhealthy_after_failures,
                                            error.to_string(),
                                        );
                                        break;
                                    }
                                }
                                message = socket.next() => {
                                    match message {
                                        Some(Ok(Message::Text(text))) => {
                                            match handle_bitget_private_message(&private_state, &symbol_map, text.as_ref(), &mut subscribed) {
                                                Ok(Some(subscribe_payload)) => {
                                                    if let Err(error) = socket.send(Message::Text(subscribe_payload.into())).await {
                                                        private_state.record_connection_failure(
                                                            now_ms(),
                                                            unhealthy_after_failures,
                                                            error.to_string(),
                                                        );
                                                        warn!(?error, "bitget private websocket subscribe send failed");
                                                        break;
                                                    }
                                                }
                                                Ok(None) => {}
                                                Err(error) => debug!(?error, "bitget private websocket message ignored"),
                                            }
                                        }
                                        Some(Ok(Message::Ping(payload))) => {
                                            if let Err(error) = socket.send(Message::Pong(payload)).await {
                                                private_state.record_connection_failure(
                                                    now_ms(),
                                                    unhealthy_after_failures,
                                                    error.to_string(),
                                                );
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Close(frame))) => {
                                            private_state.record_connection_failure(
                                                now_ms(),
                                                unhealthy_after_failures,
                                                format!("closed:{frame:?}"),
                                            );
                                            break;
                                        }
                                        Some(Ok(_)) => {}
                                        Some(Err(error)) => {
                                            private_state.record_connection_failure(
                                                now_ms(),
                                                unhealthy_after_failures,
                                                error.to_string(),
                                            );
                                            if is_benign_ws_disconnect_error(&error) {
                                                debug!(?error, "bitget private websocket receive disconnected");
                                            } else {
                                                warn!(?error, "bitget private websocket receive failed");
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
                        warn!(?error, "bitget private websocket connect failed");
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
            BITGET_PERP_LIQUIDITY_CACHE_TTL_MS,
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
            "bitget-symbols.json",
            &BitgetSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols,
                metadata,
            },
        );
    }

    async fn refresh_symbol_catalog(&self) -> Result<()> {
        let query = build_query(&[("productType", BITGET_PRODUCT_TYPE.to_string())]);
        let payload = self
            .public_request_json(
                &self.base_url,
                Method::GET,
                "/api/v2/mix/market/contracts",
                Some(query),
                "failed to request bitget contract catalog",
            )
            .await?;
        let rows = bitget_data_array("bitget contract catalog failed", &payload)?;
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        for row in rows {
            if !bitget_contract_is_tradeable(row) {
                continue;
            }
            let (symbol, meta) = parse_bitget_contract_meta(row)?;
            metadata.insert(symbol.clone(), meta);
            supported_symbols.insert(symbol);
        }
        *self.metadata.lock().expect("lock") = metadata;
        *self.supported_symbols.lock().expect("lock") = supported_symbols;
        self.persist_symbol_catalog();
        Ok(())
    }

    async fn symbol_meta(&self, symbol: &str) -> Result<BitgetSymbolMeta> {
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
                    "bitget instrument metadata missing for {}",
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
        let request_path = if let Some(query) = query.as_ref() {
            format!("{path}?{query}")
        } else {
            path.to_string()
        };
        let url = format!("{base_url}{request_path}");
        let response = self
            .client
            .request(method, url)
            .send()
            .await
            .with_context(|| context.to_string())?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format_bitget_http_error(status, &body));
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
            .ok_or_else(|| anyhow!("bitget api key is not configured"))?;
        let api_secret = self
            .config
            .live
            .resolved_api_secret()
            .ok_or_else(|| anyhow!("bitget api secret is not configured"))?;
        let passphrase = self
            .config
            .live
            .resolved_api_passphrase()
            .ok_or_else(|| anyhow!("bitget api passphrase is not configured"))?;
        let timestamp = now_ms().to_string();
        let request_path = if let Some(query) = query.as_ref() {
            format!("{path}?{query}")
        } else {
            path.to_string()
        };
        let body_payload = body.clone().unwrap_or_default();
        let payload = format!(
            "{}{}{}{}",
            timestamp,
            method.as_str(),
            request_path,
            body_payload
        );
        let signature = hmac_sha256_base64(&api_secret, &payload)?;

        let mut headers = HeaderMap::new();
        headers.insert("ACCESS-KEY", HeaderValue::from_str(&api_key)?);
        headers.insert("ACCESS-SIGN", HeaderValue::from_str(&signature)?);
        headers.insert("ACCESS-TIMESTAMP", HeaderValue::from_str(&timestamp)?);
        headers.insert("ACCESS-PASSPHRASE", HeaderValue::from_str(&passphrase)?);
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        headers.insert("locale", HeaderValue::from_static("en-US"));

        let url = format!("{base_url}{request_path}");
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
            return Err(format_bitget_http_error(status, &body));
        }
        response
            .json::<Value>()
            .await
            .with_context(|| context.to_string())
    }

    async fn fetch_tickers_map(&self) -> Result<HashMap<String, Value>> {
        let query = build_query(&[("productType", BITGET_PRODUCT_TYPE.to_string())]);
        let payload = self
            .public_request_json(
                &self.base_url,
                Method::GET,
                "/api/v2/mix/market/tickers",
                Some(query),
                "failed to request bitget tickers",
            )
            .await?;
        let rows = bitget_data_array("bitget tickers failed", &payload)?;
        let mut by_symbol = HashMap::new();
        for row in rows {
            let Some(venue_symbol) = json_string(row, &["symbol"]) else {
                continue;
            };
            by_symbol.insert(normalize_contract_symbol(&venue_symbol), row.clone());
        }
        Ok(by_symbol)
    }

    async fn submit_order_once(
        &self,
        request: &OrderRequest,
        quantity: f64,
        step_size: f64,
        position_mode: BitgetPositionMode,
    ) -> Result<BitgetOrderResponseWithTiming> {
        let sign_started_at = Instant::now();
        let mut body = serde_json::json!({
            "symbol": venue_symbol(&self.config, &request.symbol),
            "productType": BITGET_PRODUCT_TYPE,
            "marginMode": "crossed",
            "marginCoin": BITGET_MARGIN_COIN,
            "size": format_decimal(quantity, step_size),
            "side": bitget_order_side(position_mode.clone(), request.side, request.reduce_only),
            "orderType": "market",
            "force": "ioc",
            "clientOid": request.client_order_id,
        });
        if let Some(trade_side) = bitget_trade_side(position_mode, request.reduce_only) {
            body["tradeSide"] = serde_json::Value::String(trade_side.to_string());
        } else {
            body["reduceOnly"] = serde_json::Value::String(
                if request.reduce_only { "YES" } else { "NO" }.to_string(),
            );
        }
        let body = body.to_string();
        let request_sign_ms = elapsed_ms(sign_started_at);
        let submit_started_at = Instant::now();
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::POST,
                "/api/v2/mix/order/place-order",
                None,
                Some(body),
                "failed to submit bitget order",
            )
            .await?;
        let submit_http_ms = elapsed_ms(submit_started_at);
        let decode_started_at = Instant::now();
        bitget_data("bitget order failed", &payload)?;
        let response_decode_ms = elapsed_ms(decode_started_at);
        Ok(BitgetOrderResponseWithTiming {
            response: payload,
            request_sign_ms,
            submit_http_ms,
            response_decode_ms,
        })
    }

    async fn position_mode(&self) -> Result<BitgetPositionMode> {
        if let Some(mode) = self.position_mode.lock().expect("lock").clone() {
            return Ok(mode);
        }
        let seed_symbol = self.seed_symbol()?;
        let query = build_query(&[
            ("symbol", venue_symbol(&self.config, &seed_symbol)),
            ("productType", BITGET_PRODUCT_TYPE.to_string()),
            ("marginCoin", BITGET_MARGIN_COIN.to_string()),
        ]);
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                "/api/v2/mix/account/account",
                Some(query),
                None,
                "failed to request bitget account mode",
            )
            .await?;
        let data = bitget_data("bitget account mode failed", &payload)?;
        let raw_mode = json_string(data, &["posMode", "holdMode"])
            .unwrap_or_else(|| "one_way_mode".to_string());
        let mode = parse_bitget_position_mode(&raw_mode);
        self.position_mode
            .lock()
            .expect("lock")
            .replace(mode.clone());
        Ok(mode)
    }

    fn clear_position_mode(&self) {
        self.position_mode.lock().expect("lock").take();
    }

    async fn fetch_order_reconciliation_with_retry(
        &self,
        symbol: &str,
        order_id: &str,
        client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        let mut attempts = 0usize;
        let started_at = Instant::now();
        loop {
            if let Some(reconciliation) = self
                .fetch_order_fill_reconciliation(symbol, order_id, client_order_id)
                .await?
            {
                return Ok(Some(
                    reconciliation.with_private_wait_ms(elapsed_ms(started_at)),
                ));
            }
            attempts += 1;
            if attempts >= 3 {
                return Ok(None);
            }
            sleep(Duration::from_millis(50 * attempts as u64)).await;
        }
    }
}

#[async_trait]
impl VenueAdapter for BitgetLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Bitget
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let tracked = self.tracked_symbols(symbols);
        if tracked.is_empty() {
            return Err(anyhow!(
                "bitget market snapshot unavailable for requested symbols"
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
                venue: Venue::Bitget,
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
            snapshots.push(parse_bitget_market_snapshot(&symbol, row)?);
        }
        if snapshots.is_empty() {
            return Err(anyhow!(
                "bitget market snapshot unavailable for requested symbols"
            ));
        }
        Ok(VenueMarketSnapshot {
            venue: Venue::Bitget,
            observed_at_ms,
            symbols: snapshots,
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        ensure_bitget_client_oid(&request.client_order_id)?;
        let order_prepare_started_at = Instant::now();
        let meta = self.symbol_meta(&request.symbol).await?;
        let position_mode = self.position_mode().await?;
        let quantity = floor_to_step(request.quantity, meta.step_size);
        if quantity < meta.min_qty {
            return Err(anyhow!(
                "bitget quantity rounded below minimum for {}",
                request.symbol
            ));
        }
        if let Some(max_qty) = meta.max_qty {
            if quantity > max_qty + 1e-9 {
                return Err(anyhow!(
                    "bitget quantity exceeds maximum for {}",
                    request.symbol
                ));
            }
        }
        if let Some(min_notional) = meta.min_notional {
            if let Some(price_hint) = request
                .price_hint
                .filter(|price| price.is_finite() && *price > 0.0)
            {
                if quantity * price_hint + 1e-9 < min_notional {
                    return Err(anyhow!(
                        "bitget notional below minimum for {}",
                        request.symbol
                    ));
                }
            }
        }
        let order_prepare_ms = elapsed_ms(order_prepare_started_at);
        let submit_started_at = Instant::now();
        let response = match self
            .submit_order_once(&request, quantity, meta.step_size, position_mode)
            .await
        {
            Ok(response) => response,
            Err(error) if should_retry_bitget_order_error(&error) => {
                self.clear_position_mode();
                sleep(Duration::from_millis(100)).await;
                let retry_mode = self.position_mode().await?;
                self.submit_order_once(&request, quantity, meta.step_size, retry_mode)
                    .await?
            }
            Err(error) => return Err(error),
        };
        let submit_ack_ms = elapsed_ms(submit_started_at);
        let response_data = bitget_data("bitget order failed", &response.response)?;
        let order_id = json_string(response_data, &["orderId", "ordId"])
            .unwrap_or_else(|| "bitget-unknown".to_string());

        let (fallback_price, fallback_ts) = if let Some(fill) = hinted_fill(&request) {
            fill
        } else {
            let snapshot = self
                .fetch_market_snapshot(&[request.symbol.clone()])
                .await?;
            quote_fill(&snapshot, &request.symbol, request.side)?
        };
        let mut fill = OrderFill {
            venue: Venue::Bitget,
            symbol: request.symbol.clone(),
            side: request.side,
            quantity,
            average_price: fallback_price,
            fee_quote: estimate_fee_quote(fallback_price, quantity, self.config.taker_fee_bps),
            order_id: order_id.clone(),
            filled_at_ms: fallback_ts,
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
            BITGET_PRIVATE_FILL_WAIT_MS,
        )
        .await
        {
            fill = enrich_fill_from_private(fill, &private_fill);
        }
        let reconciliation_started_at = Instant::now();
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
        }
        if let Some(timing) = fill.timing.as_mut() {
            timing.private_fill_wait_ms = Some(elapsed_ms(private_fill_wait_started_at));
            if timing.private_fill_wait_ms == Some(0) {
                timing.private_fill_wait_ms = Some(elapsed_ms(reconciliation_started_at));
            }
        }
        Ok(fill)
    }

    fn cached_position(&self, symbol: &str) -> Option<PositionSnapshot> {
        self.private_ws
            .position_if_fresh(symbol, self.runtime.private_position_max_age_ms, now_ms())
            .map(|position| PositionSnapshot {
                venue: Venue::Bitget,
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
                venue: Venue::Bitget,
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
                venue: Venue::Bitget,
                symbol: symbol.to_string(),
                size: 0.0,
                updated_at_ms: now_ms(),
            }))
    }

    async fn fetch_all_positions(&self) -> Result<Option<Vec<PositionSnapshot>>> {
        let query = build_query(&[
            ("productType", BITGET_PRODUCT_TYPE.to_string()),
            ("marginCoin", BITGET_MARGIN_COIN.to_string()),
        ]);
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                "/api/v2/mix/position/all-position",
                Some(query),
                None,
                "failed to request bitget positions",
            )
            .await?;
        let rows = bitget_data_array("bitget positions failed", &payload)?;
        let mut positions = HashMap::<String, f64>::new();
        for row in rows {
            if let Some(raw_mode) = json_string(row, &["posMode", "holdMode"]) {
                self.position_mode
                    .lock()
                    .expect("lock")
                    .replace(parse_bitget_position_mode(&raw_mode));
            }
            let Some(venue_symbol) = json_string(row, &["symbol"]) else {
                continue;
            };
            let symbol = normalize_contract_symbol(&venue_symbol);
            let raw_size = json_f64(row, &["total", "available", "holdVolume", "size"])
                .unwrap_or_default()
                .abs();
            if raw_size <= 0.0 {
                continue;
            }
            let hold_side = json_string(row, &["holdSide", "posSide", "hold_mode"])
                .unwrap_or_default()
                .to_ascii_lowercase();
            let signed = match hold_side.as_str() {
                "long" | "buy" => raw_size,
                "short" | "sell" => -raw_size,
                _ => raw_size,
            };
            *positions.entry(symbol).or_insert(0.0) += signed;
        }
        Ok(Some(
            positions
                .into_iter()
                .map(|(symbol, size)| PositionSnapshot {
                    venue: Venue::Bitget,
                    symbol,
                    size,
                    updated_at_ms: now_ms(),
                })
                .collect(),
        ))
    }

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        let query = build_query(&[("productType", BITGET_PRODUCT_TYPE.to_string())]);
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                "/api/v2/mix/account/accounts",
                Some(query),
                None,
                "failed to request bitget account balances",
            )
            .await?;
        let rows = bitget_data_array("bitget account balances failed", &payload)?;
        let row = rows
            .iter()
            .find(|row| {
                json_string(row, &["marginCoin"])
                    .unwrap_or_default()
                    .eq_ignore_ascii_case(BITGET_MARGIN_COIN)
            })
            .or_else(|| rows.first())
            .ok_or_else(|| anyhow!("bitget account balance missing row"))?;

        Ok(Some(AccountBalanceSnapshot {
            venue: Venue::Bitget,
            equity_quote: json_required_f64(
                row,
                &["usdtEquity", "equity", "accountEquity"],
                "bitget account equity",
            )?,
            wallet_balance_quote: json_f64(row, &["accountEquity", "equity", "usdtEquity"]),
            available_balance_quote: json_f64(
                row,
                &["available", "availableBalance", "crossedMaxAvailable"],
            ),
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
        client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        let mut params = vec![
            ("symbol", venue_symbol(&self.config, symbol)),
            ("productType", BITGET_PRODUCT_TYPE.to_string()),
        ];
        if !order_id.is_empty() && order_id != "bitget-unknown" {
            params.push(("orderId", order_id.to_string()));
        } else if let Some(client_order_id) = client_order_id {
            params.push(("clientOid", client_order_id.to_string()));
        } else {
            return Ok(None);
        }
        let query = build_query(&params);
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::GET,
                "/api/v2/mix/order/detail",
                Some(query),
                None,
                "failed to request bitget order detail",
            )
            .await?;
        let row = bitget_data("bitget order detail failed", &payload)?;
        let quantity =
            json_f64(row, &["baseVolume", "filledQty", "fillQty", "size"]).unwrap_or_default();
        if quantity <= 0.0 {
            return Ok(None);
        }
        let average_price = json_required_f64(
            row,
            &["priceAvg", "fillPriceAvg", "averagePrice", "avgPrice"],
            "bitget average fill price",
        )?;
        let fee_quote = json_f64(row, &["fee", "totalFee", "filledFee"]).or_else(|| {
            row.get("feeDetail")
                .and_then(|value| json_f64(value, &["totalFee"]))
                .map(f64::abs)
        });
        Ok(Some(OrderFillReconciliation {
            order_id: json_string(row, &["orderId", "ordId"])
                .unwrap_or_else(|| order_id.to_string()),
            client_order_id: json_string(row, &["clientOid"]),
            quantity,
            average_price,
            fee_quote: fee_quote.filter(|value| *value > 0.0),
            filled_at_ms: json_i64(row, &["uTime", "cTime", "updateTime", "filledTime"])
                .unwrap_or_else(now_ms),
        }))
    }

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        let meta = self.symbol_meta(symbol).await?;
        let quantity = floor_to_step(quantity, meta.step_size);
        if quantity < meta.min_qty {
            return Ok(0.0);
        }
        Ok(quantity)
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
            "symbol": venue_symbol(&self.config, symbol),
            "productType": BITGET_PRODUCT_TYPE,
            "marginCoin": BITGET_MARGIN_COIN,
            "leverage": leverage.to_string(),
        })
        .to_string();
        let payload = self
            .signed_request_json(
                &self.base_url,
                Method::POST,
                "/api/v2/mix/account/set-leverage",
                None,
                Some(body),
                "failed to set bitget leverage",
            )
            .await?;
        bitget_data("bitget set leverage failed", &payload)?;
        self.configured_leverage
            .lock()
            .expect("lock")
            .insert(symbol.to_string(), leverage);
        Ok(())
    }

    fn min_entry_notional_quote_hint(&self, symbol: &str, _price_hint: Option<f64>) -> Option<f64> {
        self.metadata
            .lock()
            .expect("lock")
            .get(symbol)
            .and_then(|meta| meta.min_notional)
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
        let snapshot = parse_bitget_liquidity_snapshot(symbol, row)?;
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
                "/api/v2/spot/public/coins",
                None,
                "failed to request bitget coin info",
            )
            .await?;
        let rows = bitget_data_array("bitget coin info failed", &payload)?;
        let statuses = rows
            .iter()
            .filter_map(|row| {
                let asset = base_asset(&json_string(row, &["coin", "coinName"])?);
                if !wanted.contains(asset.as_str()) {
                    return None;
                }
                let chains = row
                    .get("chains")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let deposit_enabled = chains.iter().any(|chain| {
                    json_bool(chain, &["rechargeable", "depositEnable", "canDeposit"])
                        .unwrap_or(false)
                });
                let withdraw_enabled = chains.iter().any(|chain| {
                    json_bool(chain, &["withdrawable", "withdrawEnable", "canWithdraw"])
                        .unwrap_or(false)
                });
                Some(AssetTransferStatus {
                    venue: Venue::Bitget,
                    asset,
                    deposit_enabled,
                    withdraw_enabled,
                    observed_at_ms,
                    source: "bitget".to_string(),
                })
            })
            .collect::<Vec<_>>();
        let cache = VenueTransferStatusCache {
            observed_at_ms,
            statuses: statuses.clone(),
        };
        store_json_cache("bitget-transfer-status.json", &cache);
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

    async fn live_startup_prewarm(&self) -> Result<()> {
        let _ = self.position_mode().await?;
        if let Err(error) = self.refresh_account_fee_snapshot().await {
            warn!(?error, "bitget account fee snapshot prewarm failed");
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
struct BitgetSymbolMeta {
    min_qty: f64,
    step_size: f64,
    min_notional: Option<f64>,
    max_qty: Option<f64>,
    maker_fee_rate_bps: Option<f64>,
    taker_fee_rate_bps: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BitgetPositionMode {
    OneWay,
    Hedge,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BitgetSymbolCatalogCache {
    updated_at_ms: i64,
    supported_symbols: Vec<String>,
    metadata: HashMap<String, BitgetSymbolMeta>,
}

struct BitgetOrderResponseWithTiming {
    response: Value,
    request_sign_ms: u64,
    submit_http_ms: u64,
    response_decode_ms: u64,
}

trait WithPrivateWait {
    fn with_private_wait_ms(self, _private_fill_wait_ms: u64) -> Self;
}

impl WithPrivateWait for OrderFillReconciliation {
    fn with_private_wait_ms(self, _private_fill_wait_ms: u64) -> Self {
        self
    }
}

fn parse_bitget_contract_meta(row: &Value) -> Result<(String, BitgetSymbolMeta)> {
    let symbol =
        normalize_contract_symbol(&json_required_string(row, &["symbol"], "bitget symbol")?);
    let step_size = json_required_f64(
        row,
        &["sizeMultiplier", "sizeStep", "quantityScale"],
        "bitget size step",
    )?;
    let min_qty = json_f64(row, &["minTradeNum", "minTradeAmount"]).unwrap_or(step_size);
    let min_notional = json_f64(row, &["minTradeUSDT", "minTradeAmountUSDT"]);
    let max_qty = json_f64(row, &["maxMarketOrderQty", "maxTradeNum"]);
    Ok((
        symbol,
        BitgetSymbolMeta {
            min_qty,
            step_size,
            min_notional,
            max_qty,
            maker_fee_rate_bps: json_f64(row, &["makerFeeRate"]).map(|value| value * 10_000.0),
            taker_fee_rate_bps: json_f64(row, &["takerFeeRate"]).map(|value| value * 10_000.0),
        },
    ))
}

fn parse_bitget_trade_rate_snapshot(symbol: &str, row: &Value) -> Result<AccountFeeSnapshot> {
    Ok(AccountFeeSnapshot {
        venue: Venue::Bitget,
        taker_fee_bps: json_required_f64(row, &["takerFeeRate"], "bitget taker fee")? * 10_000.0,
        maker_fee_bps: json_required_f64(row, &["makerFeeRate"], "bitget maker fee")? * 10_000.0,
        observed_at_ms: now_ms(),
        source: format!("bitget_trade_rate_classic:{symbol}"),
    })
}

fn parse_bitget_position_mode(raw: &str) -> BitgetPositionMode {
    if raw.trim().to_ascii_lowercase().contains("hedge") {
        BitgetPositionMode::Hedge
    } else {
        BitgetPositionMode::OneWay
    }
}

fn parse_bitget_liquidity_snapshot(symbol: &str, row: &Value) -> Result<PerpLiquiditySnapshot> {
    let volume_24h_quote = json_required_f64(
        row,
        &["usdtVolume", "quoteVolume", "turnover"],
        "bitget 24h quote volume",
    )?;
    let open_interest_units = json_required_f64(
        row,
        &["holdingAmount", "openInterest", "holdVolume"],
        "bitget open interest",
    )?;
    let mark_price = json_required_f64(
        row,
        &["markPrice", "lastPr", "lastPrice"],
        "bitget mark price",
    )?;
    Ok(PerpLiquiditySnapshot {
        venue: Venue::Bitget,
        symbol: symbol.to_string(),
        volume_24h_quote,
        open_interest_quote: open_interest_units * mark_price,
        observed_at_ms: json_i64(row, &["ts", "systemTime", "requestTime"]).unwrap_or_else(now_ms),
    })
}

fn parse_bitget_market_snapshot(symbol: &str, row: &Value) -> Result<SymbolMarketSnapshot> {
    let best_bid = json_required_f64(row, &["bidPr", "bidPrice"], "bitget best bid")?;
    let best_ask = json_required_f64(row, &["askPr", "askPrice"], "bitget best ask")?;
    Ok(SymbolMarketSnapshot {
        symbol: symbol.to_string(),
        best_bid,
        best_ask,
        bid_size: json_f64(row, &["bidSz", "bidSize"]).unwrap_or_default(),
        ask_size: json_f64(row, &["askSz", "askSize"]).unwrap_or_default(),
        mark_price: json_f64(row, &["markPrice", "lastPr", "lastPrice"]),
        funding_rate: json_f64(row, &["fundingRate"]).unwrap_or_default(),
        funding_timestamp_ms: json_i64(row, &["nextFundingTime", "fundingTime"])
            .unwrap_or_else(|| now_ms() + 8 * 60 * 60 * 1_000),
    })
}

fn bitget_contract_is_tradeable(row: &Value) -> bool {
    match json_string(row, &["symbolStatus", "status"]) {
        Some(status) => {
            let status = status.to_ascii_lowercase();
            matches!(
                status.as_str(),
                "normal" | "listed" | "online" | "trading" | "live"
            )
        }
        None => true,
    }
}

fn normalize_contract_symbol(raw: &str) -> String {
    raw.trim()
        .to_ascii_uppercase()
        .replace('_', "")
        .replace('-', "")
}

fn ensure_bitget_client_oid(client_order_id: &str) -> Result<()> {
    let len = client_order_id.chars().count();
    if len == 0 || len > 32 {
        return Err(anyhow!(
            "bitget clientOid must be between 1 and 32 characters"
        ));
    }
    if client_order_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | ':'))
    {
        Ok(())
    } else {
        Err(anyhow!("bitget clientOid contains unsupported characters"))
    }
}

fn bitget_order_side(
    position_mode: BitgetPositionMode,
    side: Side,
    reduce_only: bool,
) -> &'static str {
    match (position_mode, side, reduce_only) {
        (BitgetPositionMode::OneWay, Side::Buy, _) => "buy",
        (BitgetPositionMode::OneWay, Side::Sell, _) => "sell",
        (BitgetPositionMode::Hedge, Side::Buy, false) => "buy",
        (BitgetPositionMode::Hedge, Side::Sell, false) => "sell",
        (BitgetPositionMode::Hedge, Side::Buy, true) => "sell",
        (BitgetPositionMode::Hedge, Side::Sell, true) => "buy",
    }
}

fn bitget_trade_side(position_mode: BitgetPositionMode, reduce_only: bool) -> Option<&'static str> {
    match position_mode {
        BitgetPositionMode::OneWay => None,
        BitgetPositionMode::Hedge => Some(if reduce_only { "close" } else { "open" }),
    }
}

fn should_retry_bitget_order_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("position mode")
        || message.contains("posmode")
        || message.contains("hold mode")
        || message.contains("tradeside")
}

fn bitget_public_ws_url(base_url: &str) -> &'static str {
    if base_url.contains("testnet") {
        "wss://ws.bitget.com/v2/ws/public"
    } else {
        "wss://ws.bitget.com/v2/ws/public"
    }
}

fn bitget_private_ws_url(base_url: &str) -> &'static str {
    if base_url.contains("testnet") {
        "wss://ws.bitget.com/v2/ws/private"
    } else {
        "wss://ws.bitget.com/v2/ws/private"
    }
}

fn handle_bitget_private_message(
    private_state: &std::sync::Arc<WsPrivateState>,
    symbol_map: &HashMap<String, String>,
    raw: &str,
    subscribed: &mut bool,
) -> Result<Option<String>> {
    let payload = parse_text_message(raw)?;
    if payload.get("event").and_then(Value::as_str) == Some("login")
        && payload.get("code").and_then(Value::as_str).unwrap_or("0") == "0"
        && !*subscribed
    {
        *subscribed = true;
        return Ok(Some(
            serde_json::json!({
                "op": "subscribe",
                "args": [
                    {"instType": "USDT-FUTURES", "channel": "positions"},
                    {"instType": "USDT-FUTURES", "channel": "orders"}
                ],
            })
            .to_string(),
        ));
    }
    let arg = match payload.get("arg") {
        Some(arg) => arg,
        None => return Ok(None),
    };
    let channel = arg
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let rows = match payload.get("data").and_then(Value::as_array) {
        Some(rows) => rows,
        None => return Ok(None),
    };
    match channel {
        "orders" => {
            for row in rows {
                let venue_symbol = row
                    .get("instId")
                    .or_else(|| row.get("symbol"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let symbol = symbol_map
                    .get(venue_symbol)
                    .cloned()
                    .unwrap_or_else(|| normalize_contract_symbol(venue_symbol));
                private_state.record_order(PrivateOrderUpdate {
                    symbol,
                    order_id: json_string(row, &["ordId", "orderId"]).unwrap_or_default(),
                    client_order_id: json_string(row, &["clientOid", "clOrdId"]),
                    filled_quantity: json_f64(row, &["baseVolume", "filledQty", "fillQty", "size"]),
                    average_price: json_f64(
                        row,
                        &["priceAvg", "fillPriceAvg", "averagePrice", "avgPrice"],
                    ),
                    fee_quote: json_f64(row, &["fee", "totalFee", "filledFee"]).map(f64::abs),
                    updated_at_ms: json_i64(row, &["uTime", "cTime", "updateTime"])
                        .unwrap_or_else(now_ms),
                });
            }
        }
        "positions" => {
            for row in rows {
                let venue_symbol = row
                    .get("instId")
                    .or_else(|| row.get("symbol"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let symbol = symbol_map
                    .get(venue_symbol)
                    .cloned()
                    .unwrap_or_else(|| normalize_contract_symbol(venue_symbol));
                let raw_size = json_f64(row, &["total", "available", "holdVolume", "size"])
                    .unwrap_or_default()
                    .abs();
                let hold_side = json_string(row, &["holdSide", "posSide", "hold_mode"])
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let signed = match hold_side.as_str() {
                    "long" | "buy" => raw_size,
                    "short" | "sell" => -raw_size,
                    _ => raw_size,
                };
                private_state.update_position(
                    &symbol,
                    signed,
                    json_i64(row, &["uTime", "cTime", "updateTime"]).unwrap_or_else(now_ms),
                );
            }
        }
        _ => {}
    }
    Ok(None)
}

fn bitget_data<'a>(context: &str, payload: &'a Value) -> Result<&'a Value> {
    let code = json_string(payload, &["code"]).unwrap_or_default();
    if code != "00000" && code != "0" {
        let msg =
            json_string(payload, &["msg", "message"]).unwrap_or_else(|| "unknown".to_string());
        return Err(anyhow!("{context}: code={code} msg={msg}"));
    }
    payload
        .get("data")
        .ok_or_else(|| anyhow!("{context}: response missing data"))
}

fn bitget_data_array<'a>(context: &str, payload: &'a Value) -> Result<&'a Vec<Value>> {
    bitget_data(context, payload)?
        .as_array()
        .ok_or_else(|| anyhow!("{context}: response data is not an array"))
}

fn is_bitget_classic_account_fee_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        message.contains("code=40084")
            || (message.contains("classic account mode")
                && message.contains("unified account api")
                && message.contains("not supported"))
    })
}

fn format_bitget_http_error(status: StatusCode, body: &str) -> anyhow::Error {
    let payload = serde_json::from_str::<Value>(body).ok();
    let code = payload
        .as_ref()
        .and_then(|value| json_string(value, &["code"]))
        .unwrap_or_default();
    let msg = payload
        .as_ref()
        .and_then(|value| json_string(value, &["msg", "message"]))
        .unwrap_or_else(|| body.trim().to_string());
    anyhow!(
        "bitget private/public endpoint returned non-success status: status={} code={} msg={}",
        status,
        code,
        msg
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
                "true" | "1" | "yes" | "on" | "normal"
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

    use crate::models::Side;

    use super::{
        bitget_order_side, bitget_private_ws_url, bitget_public_ws_url, bitget_trade_side,
        ensure_bitget_client_oid, handle_bitget_private_message,
        is_bitget_classic_account_fee_error, parse_bitget_contract_meta,
        parse_bitget_liquidity_snapshot, parse_bitget_trade_rate_snapshot, BitgetPositionMode,
    };
    use crate::live::WsPrivateState;
    use std::collections::HashMap;

    #[test]
    fn parses_bitget_contract_meta_from_v2_contracts_row() {
        let row = json!({
            "symbol": "BTCUSDT",
            "symbolStatus": "normal",
            "sizeMultiplier": "0.001",
            "minTradeNum": "0.001",
            "minTradeUSDT": "5",
            "maxMarketOrderQty": "1000",
            "makerFeeRate": "0.0002",
            "takerFeeRate": "0.0006"
        });

        let (symbol, meta) = parse_bitget_contract_meta(&row).expect("parse contract row");
        assert_eq!(symbol, "BTCUSDT");
        assert!((meta.step_size - 0.001).abs() < 1e-9);
        assert!((meta.min_qty - 0.001).abs() < 1e-9);
        assert_eq!(meta.min_notional, Some(5.0));
        assert_eq!(meta.max_qty, Some(1000.0));
        assert_eq!(meta.maker_fee_rate_bps, Some(2.0));
        assert!(
            meta.taker_fee_rate_bps
                .map(|value| (value - 6.0).abs() < 1e-9)
                .unwrap_or(false)
        );
    }

    #[test]
    fn parses_bitget_classic_trade_rate_snapshot() {
        let row = json!({
            "makerFeeRate": "0.0002",
            "takerFeeRate": "0.0006"
        });

        let snapshot = parse_bitget_trade_rate_snapshot("BTCUSDT", &row)
            .expect("parse classic trade rate");
        assert_eq!(snapshot.venue, crate::models::Venue::Bitget);
        assert_eq!(snapshot.maker_fee_bps, 2.0);
        assert!((snapshot.taker_fee_bps - 6.0).abs() < 1e-9);
        assert!(snapshot.source.contains("bitget_trade_rate_classic"));
    }

    #[test]
    fn classic_account_fee_errors_are_downgraded_to_fallback() {
        assert!(is_bitget_classic_account_fee_error(&anyhow::anyhow!(
            "bitget fee rate failed: code=40019 msg=Classic Account mode, Unified Account API not supported"
        )));
        assert!(is_bitget_classic_account_fee_error(&anyhow::anyhow!(
            "bitget private/public endpoint returned non-success status: status=400 Bad Request code=40084 msg=You are in Classic Account mode, and the Unified Account API is not supported at this time"
        )));
        assert!(!is_bitget_classic_account_fee_error(&anyhow::anyhow!(
            "bitget fee rate failed: code=40017 msg=parameter error"
        )));
    }

    #[test]
    fn parses_bitget_liquidity_from_ticker_row() {
        let row = json!({
            "symbol": "BTCUSDT",
            "usdtVolume": "1234567.8",
            "holdingAmount": "25.0",
            "markPrice": "64000",
            "ts": "1710000000000"
        });

        let snapshot = parse_bitget_liquidity_snapshot("BTCUSDT", &row).expect("parse liquidity");
        assert_eq!(snapshot.symbol, "BTCUSDT");
        assert!((snapshot.volume_24h_quote - 1_234_567.8).abs() < 1e-6);
        assert!((snapshot.open_interest_quote - 1_600_000.0).abs() < 1e-6);
        assert_eq!(snapshot.observed_at_ms, 1_710_000_000_000);
    }

    #[test]
    fn bitget_ws_urls_follow_bitget_hosts() {
        assert_eq!(
            bitget_public_ws_url("https://api.bitget.com"),
            "wss://ws.bitget.com/v2/ws/public"
        );
        assert_eq!(
            bitget_private_ws_url("https://api.bitget.com"),
            "wss://ws.bitget.com/v2/ws/private"
        );
    }

    #[test]
    fn bitget_private_order_update_is_recorded() {
        let state = WsPrivateState::new();
        let mut symbols = HashMap::new();
        symbols.insert("BTCUSDT".to_string(), "BTCUSDT".to_string());
        let payload = json!({
            "arg": {"channel": "orders"},
            "data": [{
                "instId": "BTCUSDT",
                "ordId": "123",
                "clientOid": "abc",
                "baseVolume": "0.5",
                "priceAvg": "65000",
                "fee": "-1.2",
                "uTime": "1710000000000"
            }]
        })
        .to_string();
        let mut subscribed = true;
        handle_bitget_private_message(&state, &symbols, &payload, &mut subscribed)
            .expect("handle private");
        let update = state
            .order_by_order_id("123")
            .expect("order update recorded");
        assert_eq!(update.client_order_id.as_deref(), Some("abc"));
        assert_eq!(update.filled_quantity, Some(0.5));
        assert_eq!(update.average_price, Some(65_000.0));
        assert_eq!(update.fee_quote, Some(1.2));
    }

    #[test]
    fn bitget_client_oid_validation_rejects_invalid_values() {
        assert!(ensure_bitget_client_oid("abc-123_foo:bar").is_ok());
        assert!(ensure_bitget_client_oid("").is_err());
        assert!(ensure_bitget_client_oid(&"x".repeat(33)).is_err());
        assert!(ensure_bitget_client_oid("bad id").is_err());
    }

    #[test]
    fn bitget_hedge_mode_maps_side_and_trade_side() {
        assert_eq!(
            bitget_order_side(BitgetPositionMode::OneWay, Side::Buy, false),
            "buy"
        );
        assert_eq!(
            bitget_order_side(BitgetPositionMode::Hedge, Side::Sell, true),
            "buy"
        );
        assert_eq!(
            bitget_order_side(BitgetPositionMode::Hedge, Side::Buy, true),
            "sell"
        );
        assert_eq!(bitget_trade_side(BitgetPositionMode::OneWay, false), None);
        assert_eq!(
            bitget_trade_side(BitgetPositionMode::Hedge, false),
            Some("open")
        );
        assert_eq!(
            bitget_trade_side(BitgetPositionMode::Hedge, true),
            Some("close")
        );
    }
}
