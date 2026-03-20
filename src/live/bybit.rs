use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client,
};
use serde::{Deserialize, Serialize};
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        AssetTransferStatus, OrderFill, OrderRequest, PositionSnapshot, Side, SymbolMarketSnapshot,
        Venue, VenueMarketSnapshot,
    },
    resilience::FailureBackoff,
    venue::VenueAdapter,
};

use super::{
    base_asset, build_http_client, build_query, cache_is_fresh, enrich_fill_from_private,
    estimate_fee_quote, floor_to_step, format_decimal, hinted_fill, hmac_sha256_hex,
    load_json_cache, lookup_or_wait_private_order, now_ms, parse_bool_flag, parse_f64,
    parse_i64, parse_text_message, quote_fill, spawn_ws_loop, store_json_cache, venue_symbol,
    PrivateOrderUpdate, WsMarketState, WsPrivateState, SYMBOL_CACHE_TTL_MS,
    TRANSFER_CACHE_TTL_MS,
};

const BYBIT_MAX_SUBSCRIBE_TOPICS_PER_MESSAGE: usize = 100;

pub struct BybitLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    client: Client,
    base_url: String,
    metadata: Mutex<HashMap<String, BybitInstrumentMeta>>,
    supported_symbols: Mutex<HashSet<String>>,
    transfer_status_cache: Mutex<Option<BybitTransferStatusCache>>,
    time_offset_ms: Mutex<Option<i64>>,
    market_ws: Arc<WsMarketState>,
    private_ws: Arc<WsPrivateState>,
}

impl BybitLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        runtime: &RuntimeConfig,
        symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Bybit {
            return Err(anyhow!("bybit live adapter requires bybit config"));
        }

        let market_ws = WsMarketState::new();
        let persisted_catalog = load_json_cache::<BybitSymbolCatalogCache>("bybit-symbols.json");
        let persisted_transfer_cache =
            load_json_cache::<BybitTransferStatusCache>("bybit-transfer-status.json");
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        if let Some(cache) = persisted_catalog {
            if !cache_is_fresh(cache.updated_at_ms, now_ms(), SYMBOL_CACHE_TTL_MS) {
                debug!("bybit symbol catalog cache is stale; using as fallback seed");
            }
            metadata.extend(cache.metadata);
            supported_symbols.extend(cache.supported_symbols);
        }
        let transfer_status_cache = persisted_transfer_cache.filter(|cache| {
            cache_is_fresh(
                cache.observed_at_ms,
                now_ms(),
                (runtime.transfer_status_cache_ms.max(TRANSFER_CACHE_TTL_MS as u64)).min(i64::MAX as u64) as i64,
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
                .unwrap_or_else(|| "https://api.bybit.com".to_string()),
            metadata: Mutex::new(metadata),
            supported_symbols: Mutex::new(supported_symbols),
            transfer_status_cache: Mutex::new(transfer_status_cache),
            time_offset_ms: Mutex::new(None),
            market_ws,
            private_ws: WsPrivateState::new(),
        };
        if let Err(error) = adapter.refresh_symbol_catalog().await {
            if adapter.supported_symbols.lock().expect("lock").is_empty() {
                return Err(error);
            }
            warn!(?error, "bybit symbol catalog refresh failed; using persisted cache");
        }
        let tracked_symbols = adapter.tracked_symbols(symbols);
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
        self.supported_symbols.lock().expect("lock").contains(symbol)
    }

    fn start_market_ws(&self, symbols: &[String]) {
        if symbols.is_empty() {
            return;
        }

        let topics = symbols
            .iter()
            .map(|symbol| format!("tickers.{}", venue_symbol(&self.config, symbol)))
            .collect::<Vec<_>>();
        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let subscribe_messages = build_bybit_subscribe_messages(&topics);
        let state = self.market_ws.clone();
        spawn_ws_loop(
            "bybit",
            bybit_public_ws_url(&self.base_url).to_string(),
            subscribe_messages,
            state,
            self.runtime.ws_reconnect_initial_ms,
            self.runtime.ws_reconnect_max_ms,
            self.runtime.ws_unhealthy_after_failures,
            move |cache, raw| {
                let payload = parse_text_message(raw)?;
                let topic = payload
                    .get("topic")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                if !topic.starts_with("tickers.") {
                    return Ok(());
                }
                let venue_symbol = topic.trim_start_matches("tickers.");
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    return Ok(());
                };
                let data = payload
                    .get("data")
                    .ok_or_else(|| anyhow!("bybit ws payload missing data"))?;
                cache.update_quote(
                    symbol,
                    parse_f64(
                        data.get("bid1Price")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| anyhow!("bybit ws missing bid1Price"))?,
                    )?,
                    parse_f64(
                        data.get("ask1Price")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| anyhow!("bybit ws missing ask1Price"))?,
                    )?,
                    parse_f64(
                        data.get("bid1Size")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| anyhow!("bybit ws missing bid1Size"))?,
                    )?,
                    parse_f64(
                        data.get("ask1Size")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| anyhow!("bybit ws missing ask1Size"))?,
                    )?,
                    payload
                        .get("ts")
                        .and_then(|value| value.as_i64())
                        .unwrap_or_else(now_ms),
                );
                cache.update_funding(
                    symbol,
                    parse_f64(
                        data.get("fundingRate")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| anyhow!("bybit ws missing fundingRate"))?,
                    )?,
                    parse_i64(
                        data.get("nextFundingTime")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| anyhow!("bybit ws missing nextFundingTime"))?,
                    )?,
                );
                Ok(())
            },
        );
    }

    fn cached_transfer_statuses(
        &self,
        wanted: &BTreeSet<String>,
        now_ms: i64,
    ) -> Option<Vec<AssetTransferStatus>> {
        let cache = self.transfer_status_cache.lock().expect("lock");
        let cache = cache.as_ref()?;
        if !bybit_transfer_status_cache_is_fresh(
            cache,
            now_ms,
            self.runtime.transfer_status_cache_ms,
        ) {
            return None;
        }
        Some(filter_bybit_transfer_statuses(cache, wanted))
    }

    fn cached_snapshot(&self, symbol: &str) -> Option<(SymbolMarketSnapshot, i64)> {
        let snapshot = self.market_ws.snapshot(symbol)?;
        let observed_at_ms = self.market_ws.quote(symbol)?.observed_at_ms;
        Some((snapshot, observed_at_ms))
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
        let url = bybit_private_ws_url(&self.base_url).to_string();
        let task = tokio::spawn(async move {
            let mut reconnect_backoff =
                FailureBackoff::new(reconnect_initial_ms, reconnect_max_ms, Venue::Bybit as u64);
            loop {
                match connect_async(url.as_str()).await {
                    Ok((mut socket, _)) => {
                        reconnect_backoff.on_success();
                        private_state.record_connection_success(now_ms());
                        let expires = now_ms() + 10_000;
                        let signature = match hmac_sha256_hex(
                            api_secret.as_str(),
                            &format!("GET/realtime{expires}"),
                        ) {
                            Ok(signature) => signature,
                            Err(error) => {
                                private_state.record_connection_failure(
                                    now_ms(),
                                    unhealthy_after_failures,
                                    error.to_string(),
                                );
                                warn!(?error, "bybit private websocket auth sign failed");
                                sleep(Duration::from_millis(
                                    reconnect_backoff.on_failure_with_jitter(),
                                ))
                                .await;
                                continue;
                            }
                        };
                        let auth = serde_json::json!({
                            "op": "auth",
                            "args": [api_key, expires, signature],
                        })
                        .to_string();
                        if let Err(error) = socket.send(Message::Text(auth.into())).await {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                error.to_string(),
                            );
                            warn!(?error, "bybit private websocket auth send failed");
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
                                    if let Err(error) = socket
                                        .send(Message::Text(serde_json::json!({ "op": "ping" }).to_string().into()))
                                        .await
                                    {
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
                                            match handle_bybit_private_message(
                                                &private_state,
                                                &symbol_map,
                                                text.as_ref(),
                                                &mut subscribed,
                                            ) {
                                                Ok(Some(subscribe_payload)) => {
                                                    if let Err(error) = socket.send(Message::Text(subscribe_payload.into())).await {
                                                        private_state.record_connection_failure(
                                                            now_ms(),
                                                            unhealthy_after_failures,
                                                            error.to_string(),
                                                        );
                                                        warn!(?error, "bybit private websocket subscribe send failed");
                                                        break;
                                                    }
                                                }
                                                Ok(None) => {}
                                                Err(error) => {
                                                    debug!(?error, "bybit private websocket message ignored");
                                                }
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
                                            debug!(?frame, "bybit private websocket closed");
                                            break;
                                        }
                                        Some(Ok(_)) => {}
                                        Some(Err(error)) => {
                                            private_state.record_connection_failure(
                                                now_ms(),
                                                unhealthy_after_failures,
                                                error.to_string(),
                                            );
                                            warn!(?error, "bybit private websocket receive failed");
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
                        warn!(?error, "bybit private websocket connect failed");
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

    async fn symbol_meta(&self, symbol: &str) -> Result<BybitInstrumentMeta> {
        if let Some(meta) = self.metadata.lock().expect("lock").get(symbol).cloned() {
            return Ok(meta);
        }
        self.refresh_symbol_catalog().await?;
        self.metadata
            .lock()
            .expect("lock")
            .get(symbol)
            .cloned()
            .with_context(|| format!("bybit instrument metadata missing for {}", venue_symbol(&self.config, symbol)))
    }

    async fn refresh_symbol_catalog(&self) -> Result<()> {
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        let mut cursor = None::<String>;

        loop {
            let mut request = self
                .client
                .get(format!("{}/v5/market/instruments-info", self.base_url))
                .query(&[("category", "linear"), ("limit", "1000")]);
            if let Some(cursor_value) = cursor.as_deref() {
                request = request.query(&[("cursor", cursor_value)]);
            }
            let response = request
                .send()
                .await
                .context("failed to request bybit instruments info")?
                .error_for_status()
                .context("bybit instruments info returned non-success status")?
                .json::<BybitApiResponse<BybitInstrumentList>>()
                .await
                .context("failed to decode bybit instruments info")?;
            if response.ret_code != 0 {
                return Err(anyhow!(
                    "bybit instruments info failed: {}",
                    response.ret_msg
                ));
            }
            let result = response.result.unwrap_or_default();
            for instrument in result.list {
                if !bybit_instrument_is_supported(&instrument) {
                    continue;
                }
                supported_symbols.insert(instrument.symbol.clone());
                metadata.insert(
                    instrument.symbol,
                    BybitInstrumentMeta {
                        qty_step: parse_f64(&instrument.lot_size_filter.qty_step)?,
                    },
                );
            }
            cursor = result.next_page_cursor.filter(|value| !value.is_empty());
            if cursor.is_none() {
                break;
            }
        }

        store_json_cache(
            "bybit-symbols.json",
            &BybitSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols: supported_symbols.iter().cloned().collect(),
                metadata: metadata.clone(),
            },
        );
        *self.metadata.lock().expect("lock") = metadata;
        *self.supported_symbols.lock().expect("lock") = supported_symbols;
        Ok(())
    }

    async fn fetch_symbol_snapshot(&self, symbol: &str) -> Result<SymbolMarketSnapshot> {
        if !self.supports_symbol(symbol) {
            return Err(anyhow!("bybit symbol not supported for {}", symbol));
        }
        let venue_symbol = venue_symbol(&self.config, symbol);
        let response = self
            .client
            .get(format!("{}/v5/market/tickers", self.base_url))
            .query(&[("category", "linear"), ("symbol", venue_symbol.as_str())])
            .send()
            .await
            .context("failed to request bybit tickers")?
            .error_for_status()
            .context("bybit tickers returned non-success status")?
            .json::<BybitApiResponse<BybitTickerList>>()
            .await
            .context("failed to decode bybit tickers")?;
        if response.ret_code != 0 {
            return Err(anyhow!("bybit tickers failed: {}", response.ret_msg));
        }
        let ticker = response
            .result
            .and_then(|result| result.list.into_iter().next())
            .with_context(|| format!("bybit ticker missing for {venue_symbol}"))?;

        Ok(SymbolMarketSnapshot {
            symbol: symbol.to_string(),
            best_bid: parse_f64(&ticker.bid1_price)?,
            best_ask: parse_f64(&ticker.ask1_price)?,
            bid_size: parse_f64(&ticker.bid1_size)?,
            ask_size: parse_f64(&ticker.ask1_size)?,
            funding_rate: parse_f64(&ticker.funding_rate)?,
            funding_timestamp_ms: parse_i64(&ticker.next_funding_time)?,
        })
    }

    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        query: Option<String>,
        body: Option<String>,
    ) -> Result<reqwest::Response> {
        let api_key = self
            .config
            .live
            .resolved_api_key()
            .ok_or_else(|| anyhow!("bybit api key is not configured"))?;
        let api_secret = self
            .config
            .live
            .resolved_api_secret()
            .ok_or_else(|| anyhow!("bybit api secret is not configured"))?;
        let timestamp = self.server_timestamp_ms().await?.to_string();
        let recv_window = "5000".to_string();
        let payload = query.clone().or_else(|| body.clone()).unwrap_or_default();
        let signature = hmac_sha256_hex(
            &api_secret,
            &format!("{timestamp}{api_key}{recv_window}{payload}"),
        )?;

        let mut headers = HeaderMap::new();
        headers.insert("X-BAPI-API-KEY", HeaderValue::from_str(&api_key)?);
        headers.insert("X-BAPI-SIGN", HeaderValue::from_str(&signature)?);
        headers.insert("X-BAPI-TIMESTAMP", HeaderValue::from_str(&timestamp)?);
        headers.insert("X-BAPI-RECV-WINDOW", HeaderValue::from_str(&recv_window)?);
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        let url = if let Some(query) = query.as_ref() {
            format!("{}{}?{}", self.base_url, path, query)
        } else {
            format!("{}{}", self.base_url, path)
        };
        let request = self.client.request(method, url).headers(headers);
        let request = if let Some(body) = body {
            request.body(body)
        } else {
            request
        };

        request
            .send()
            .await
            .context("failed to send signed bybit request")?
            .error_for_status()
            .context("bybit private endpoint returned non-success status")
    }

    async fn server_timestamp_ms(&self) -> Result<i64> {
        if let Some(offset_ms) = *self.time_offset_ms.lock().expect("lock") {
            return Ok(now_ms() + offset_ms);
        }

        let response = self
            .client
            .get(format!("{}/v5/market/time", self.base_url))
            .send()
            .await
            .context("failed to request bybit server time")?
            .error_for_status()
            .context("bybit server time returned non-success status")?
            .json::<BybitApiResponse<BybitServerTimeResult>>()
            .await
            .context("failed to decode bybit server time")?;
        if response.ret_code != 0 {
            return Err(anyhow!("bybit server time failed: {}", response.ret_msg));
        }
        let server_time = response
            .time
            .or_else(|| {
                response
                    .result
                    .and_then(|result| parse_i64(&result.time_nano).ok().map(|ns| ns / 1_000_000))
            })
            .ok_or_else(|| anyhow!("bybit server time missing"))?;
        let offset_ms = server_time - now_ms();
        self.time_offset_ms.lock().expect("lock").replace(offset_ms);
        Ok(now_ms() + offset_ms)
    }
}

#[async_trait]
impl VenueAdapter for BybitLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Bybit
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut quotes = Vec::new();
        let mut observed_at_ms = 0_i64;
        let allow_direct_fallback = symbols.len() == 1;
        for symbol in symbols.iter().filter(|symbol| self.supports_symbol(symbol)) {
            if let Some((snapshot, snapshot_observed_at_ms)) = self.cached_snapshot(symbol) {
                observed_at_ms = observed_at_ms.max(snapshot_observed_at_ms);
                quotes.push(snapshot);
            } else if allow_direct_fallback {
                let snapshot = self.fetch_symbol_snapshot(symbol).await?;
                observed_at_ms = observed_at_ms.max(snapshot.funding_timestamp_ms.min(now_ms()));
                quotes.push(snapshot);
            }
        }
        if quotes.is_empty() {
            return Err(anyhow!(
                "bybit market snapshot unavailable for requested symbols"
            ));
        }

        Ok(VenueMarketSnapshot {
            venue: Venue::Bybit,
            observed_at_ms: now_ms().max(observed_at_ms),
            symbols: quotes,
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        let meta = self.symbol_meta(&request.symbol).await?;
        let quantity = floor_to_step(request.quantity, meta.qty_step);
        if quantity <= 0.0 {
            return Err(anyhow!(
                "bybit order quantity rounded to zero for {}",
                request.symbol
            ));
        }

        let body = serde_json::json!({
            "category": "linear",
            "symbol": venue_symbol(&self.config, &request.symbol),
            "side": match request.side {
                Side::Buy => "Buy",
                Side::Sell => "Sell",
            },
            "orderType": "Market",
            "qty": format_decimal(quantity, meta.qty_step),
            "reduceOnly": request.reduce_only,
            "positionIdx": 0,
            "orderLinkId": request.client_order_id,
        })
        .to_string();
        let response = self
            .signed_request(reqwest::Method::POST, "/v5/order/create", None, Some(body))
            .await?
            .json::<BybitApiResponse<BybitOrderResult>>()
            .await
            .context("failed to decode bybit order response")?;
        if response.ret_code != 0 {
            return Err(anyhow!("bybit order failed: {}", response.ret_msg));
        }
        let order_id = response
            .result
            .and_then(|result| result.order_id)
            .unwrap_or_else(|| "bybit-unknown".to_string());

        let (average_price, filled_at_ms) = if let Some(fill) = hinted_fill(&request) {
            fill
        } else {
            let snapshot = self
                .fetch_market_snapshot(&[request.symbol.clone()])
                .await?;
            quote_fill(&snapshot, &request.symbol, request.side)?
        };

        let mut fill = OrderFill {
            venue: Venue::Bybit,
            symbol: request.symbol,
            side: request.side,
            quantity,
            average_price,
            fee_quote: estimate_fee_quote(average_price, quantity, self.config.taker_fee_bps),
            order_id: order_id.clone(),
            filled_at_ms,
            timing: None,
        };
        if let Some(private_fill) = lookup_or_wait_private_order(
            &self.private_ws,
            Some(&request.client_order_id),
            Some(order_id.as_str()),
            self.config.live.post_ack_private_fill_wait_ms,
        )
        .await
        {
            fill = enrich_fill_from_private(fill, &private_fill);
        }
        Ok(fill)
    }

    fn cached_position(&self, symbol: &str) -> Option<PositionSnapshot> {
        self.private_ws
            .position_if_fresh(symbol, self.runtime.private_position_max_age_ms, now_ms())
            .map(|position| PositionSnapshot {
                venue: Venue::Bybit,
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
                venue: Venue::Bybit,
                symbol: symbol.to_string(),
                size: position.size,
                updated_at_ms: position.updated_at_ms,
            });
        }

        let query = build_query(&[
            ("category", "linear".to_string()),
            ("symbol", venue_symbol(&self.config, symbol)),
        ]);
        let mut last_error = None;
        let mut response = None;
        for attempt in 0..3 {
            match self
                .signed_request(
                    reqwest::Method::GET,
                    "/v5/position/list",
                    Some(query.clone()),
                    None,
                )
                .await
            {
                Ok(result) => {
                    response = Some(
                        result
                            .json::<BybitApiResponse<BybitPositionList>>()
                            .await
                            .context("failed to decode bybit positions")?,
                    );
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 2 {
                        sleep(Duration::from_millis(150 * (attempt + 1) as u64)).await;
                    }
                }
            }
        }
        let response = response
            .ok_or_else(|| last_error.unwrap_or_else(|| anyhow!("bybit position query failed")))?;
        if response.ret_code != 0 {
            return Err(anyhow!("bybit position query failed: {}", response.ret_msg));
        }
        let size = response
            .result
            .map(|result| {
                result.list.into_iter().try_fold(0.0, |acc, row| {
                    let quantity = parse_f64(&row.size)?;
                    let signed = match row.side.as_deref() {
                        Some("Buy") => quantity.abs(),
                        Some("Sell") => -quantity.abs(),
                        _ => 0.0,
                    };
                    Ok::<f64, anyhow::Error>(acc + signed)
                })
            })
            .transpose()?
            .unwrap_or_default();

        Ok(PositionSnapshot {
            venue: Venue::Bybit,
            symbol: symbol.to_string(),
            size,
            updated_at_ms: now_ms(),
        })
    }

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        let meta = self.symbol_meta(symbol).await?;
        Ok(floor_to_step(quantity, meta.qty_step))
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

        let response = self
            .signed_request(
                reqwest::Method::GET,
                "/v5/asset/coin/query-info",
                None,
                None,
            )
            .await?
            .json::<BybitApiResponse<BybitCoinInfoResult>>()
            .await
            .context("failed to decode bybit coin info")?;
        if response.ret_code != 0 {
            return Err(anyhow!("bybit coin info failed: {}", response.ret_msg));
        }

        let cache = build_bybit_transfer_status_cache(
            response.result.map(|result| result.rows).unwrap_or_default(),
            observed_at_ms,
        );
        let statuses = filter_bybit_transfer_statuses(&cache, &wanted);
        store_json_cache("bybit-transfer-status.json", &cache);
        self.transfer_status_cache
            .lock()
            .expect("lock")
            .replace(cache);
        Ok(statuses)
    }

    async fn shutdown(&self) -> Result<()> {
        self.market_ws.abort_worker();
        self.private_ws.abort_workers();
        Ok(())
    }
}

fn bybit_public_ws_url(base_url: &str) -> &'static str {
    if base_url.contains("testnet") {
        "wss://stream-testnet.bybit.com/v5/public/linear"
    } else {
        "wss://stream.bybit.com/v5/public/linear"
    }
}

fn bybit_private_ws_url(base_url: &str) -> &'static str {
    if base_url.contains("testnet") {
        "wss://stream-testnet.bybit.com/v5/private"
    } else {
        "wss://stream.bybit.com/v5/private"
    }
}

fn build_bybit_subscribe_messages(topics: &[String]) -> Vec<String> {
    topics
        .chunks(BYBIT_MAX_SUBSCRIBE_TOPICS_PER_MESSAGE)
        .map(|chunk| {
            serde_json::json!({
                "op": "subscribe",
                "args": chunk,
            })
            .to_string()
        })
        .collect()
}

fn handle_bybit_private_message(
    private_state: &Arc<WsPrivateState>,
    symbol_map: &HashMap<String, String>,
    raw: &str,
    subscribed: &mut bool,
) -> Result<Option<String>> {
    let payload = parse_text_message(raw)?;
    if payload
        .get("op")
        .and_then(|value| value.as_str())
        .is_some_and(|op| op == "auth")
        && payload
            .get("success")
            .and_then(|value| value.as_bool())
            .unwrap_or_default()
    {
        *subscribed = true;
        return Ok(Some(
            serde_json::json!({
                "op": "subscribe",
                "args": ["position", "order", "execution"],
            })
            .to_string(),
        ));
    }
    if payload
        .get("op")
        .and_then(|value| value.as_str())
        .is_some_and(|op| op == "pong" || op == "subscribe")
    {
        return Ok(None);
    }
    if !*subscribed {
        return Ok(None);
    }

    let topic = payload
        .get("topic")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let data = match payload.get("data").and_then(|value| value.as_array()) {
        Some(data) => data,
        None => return Ok(None),
    };
    match topic {
        "order" => {
            for row in data {
                let venue_symbol = row
                    .get("symbol")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow!("bybit order update missing symbol"))?;
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    continue;
                };
                private_state.record_order(PrivateOrderUpdate {
                    symbol: symbol.clone(),
                    order_id: row
                        .get("orderId")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    client_order_id: row
                        .get("orderLinkId")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    filled_quantity: row
                        .get("cumExecQty")
                        .and_then(|value| value.as_str())
                        .map(parse_f64)
                        .transpose()?,
                    average_price: row
                        .get("avgPrice")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty() && *value != "0")
                        .map(parse_f64)
                        .transpose()?,
                    fee_quote: row
                        .get("cumExecFee")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(parse_f64)
                        .transpose()?
                        .or_else(|| extract_bybit_quote_fee(row.get("cumFeeDetail"))),
                    updated_at_ms: row
                        .get("updatedTime")
                        .and_then(|value| value.as_str())
                        .map(parse_i64)
                        .transpose()?
                        .unwrap_or_else(now_ms),
                });
            }
        }
        "execution" => {
            for row in data {
                let venue_symbol = row
                    .get("symbol")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow!("bybit execution update missing symbol"))?;
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    continue;
                };
                private_state.record_order(PrivateOrderUpdate {
                    symbol: symbol.clone(),
                    order_id: row
                        .get("orderId")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    client_order_id: row
                        .get("orderLinkId")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    filled_quantity: row
                        .get("execQty")
                        .and_then(|value| value.as_str())
                        .map(parse_f64)
                        .transpose()?,
                    average_price: row
                        .get("execPrice")
                        .and_then(|value| value.as_str())
                        .map(parse_f64)
                        .transpose()?,
                    fee_quote: row
                        .get("execFee")
                        .and_then(|value| value.as_str())
                        .map(parse_f64)
                        .transpose()?,
                    updated_at_ms: row
                        .get("execTime")
                        .and_then(|value| value.as_str())
                        .map(parse_i64)
                        .transpose()?
                        .unwrap_or_else(now_ms),
                });
            }
        }
        "position" => {
            let mut net_positions = HashMap::<String, f64>::new();
            let updated_at_ms = payload
                .get("creationTime")
                .and_then(|value| value.as_i64())
                .unwrap_or_else(now_ms);
            for row in data {
                let Some(venue_symbol) = row.get("symbol").and_then(|value| value.as_str()) else {
                    continue;
                };
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    continue;
                };
                let size = row
                    .get("size")
                    .and_then(|value| value.as_str())
                    .map(parse_f64)
                    .transpose()?
                    .unwrap_or_default();
                let signed = match row.get("side").and_then(|value| value.as_str()) {
                    Some("Buy") => size.abs(),
                    Some("Sell") => -size.abs(),
                    _ => 0.0,
                };
                *net_positions.entry(symbol.clone()).or_default() += signed;
            }
            for (symbol, size) in net_positions {
                private_state.update_position(&symbol, size, updated_at_ms);
            }
        }
        _ => {}
    }
    Ok(None)
}

fn extract_bybit_quote_fee(value: Option<&serde_json::Value>) -> Option<f64> {
    let object = value?.as_object()?;
    object.iter().find_map(|(asset, fee)| match asset.as_str() {
        "USDT" | "USDC" => fee.as_str().and_then(|raw| parse_f64(raw).ok()),
        _ => None,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BybitInstrumentMeta {
    qty_step: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BybitSymbolCatalogCache {
    updated_at_ms: i64,
    supported_symbols: Vec<String>,
    metadata: HashMap<String, BybitInstrumentMeta>,
}

#[derive(Debug, Deserialize)]
struct BybitApiResponse<T> {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<T>,
    time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct BybitServerTimeResult {
    #[serde(rename = "timeNano")]
    time_nano: String,
}

#[derive(Debug, Deserialize, Default)]
struct BybitInstrumentList {
    #[serde(default)]
    list: Vec<BybitInstrument>,
    #[serde(rename = "nextPageCursor")]
    next_page_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BybitInstrument {
    symbol: String,
    status: Option<String>,
    #[serde(rename = "lotSizeFilter")]
    lot_size_filter: BybitLotSizeFilter,
}

#[derive(Debug, Deserialize)]
struct BybitLotSizeFilter {
    #[serde(rename = "qtyStep")]
    qty_step: String,
}

#[derive(Debug, Deserialize)]
struct BybitTickerList {
    #[serde(default)]
    list: Vec<BybitTicker>,
}

#[derive(Debug, Deserialize)]
struct BybitTicker {
    #[serde(rename = "bid1Price")]
    bid1_price: String,
    #[serde(rename = "bid1Size")]
    bid1_size: String,
    #[serde(rename = "ask1Price")]
    ask1_price: String,
    #[serde(rename = "ask1Size")]
    ask1_size: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: String,
}

#[derive(Debug, Deserialize)]
struct BybitOrderResult {
    #[serde(rename = "orderId")]
    order_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BybitPositionList {
    #[serde(default)]
    list: Vec<BybitPosition>,
}

#[derive(Debug, Deserialize)]
struct BybitPosition {
    size: String,
    side: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BybitCoinInfoResult {
    #[serde(default)]
    rows: Vec<BybitCoinInfo>,
}

#[derive(Debug, Deserialize)]
struct BybitCoinInfo {
    coin: String,
    #[serde(default)]
    chains: Vec<BybitCoinChain>,
}

#[derive(Debug, Deserialize)]
struct BybitCoinChain {
    #[serde(rename = "chainDeposit")]
    chain_deposit: String,
    #[serde(rename = "chainWithdraw")]
    chain_withdraw: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BybitTransferStatusCache {
    observed_at_ms: i64,
    by_asset: HashMap<String, AssetTransferStatus>,
}

fn bybit_instrument_is_supported(instrument: &BybitInstrument) -> bool {
    instrument
        .status
        .as_deref()
        .map_or(true, |status| status.eq_ignore_ascii_case("Trading"))
}

fn build_bybit_transfer_status_cache(
    rows: Vec<BybitCoinInfo>,
    observed_at_ms: i64,
) -> BybitTransferStatusCache {
    let mut by_asset = HashMap::new();
    for row in rows {
        let asset = base_asset(&row.coin);
        let entry = by_asset.entry(asset.clone()).or_insert(AssetTransferStatus {
            venue: Venue::Bybit,
            asset: asset.clone(),
            deposit_enabled: false,
            withdraw_enabled: false,
            observed_at_ms,
            source: "bybit".to_string(),
        });
        entry.deposit_enabled |= row
            .chains
            .iter()
            .any(|chain| parse_bool_flag(&chain.chain_deposit));
        entry.withdraw_enabled |= row
            .chains
            .iter()
            .any(|chain| parse_bool_flag(&chain.chain_withdraw));
    }

    BybitTransferStatusCache {
        observed_at_ms,
        by_asset,
    }
}

fn filter_bybit_transfer_statuses(
    cache: &BybitTransferStatusCache,
    wanted: &BTreeSet<String>,
) -> Vec<AssetTransferStatus> {
    wanted
        .iter()
        .map(|asset| {
            cache
                .by_asset
                .get(asset)
                .cloned()
                .unwrap_or_else(|| AssetTransferStatus {
                    venue: Venue::Bybit,
                    asset: asset.clone(),
                    deposit_enabled: false,
                    withdraw_enabled: false,
                    observed_at_ms: cache.observed_at_ms,
                    source: "bybit".to_string(),
                })
        })
        .collect()
}

fn bybit_transfer_status_cache_is_fresh(
    cache: &BybitTransferStatusCache,
    now_ms: i64,
    ttl_ms: u64,
) -> bool {
    let ttl_ms = ttl_ms.min(i64::MAX as u64) as i64;
    now_ms.saturating_sub(cache.observed_at_ms) <= ttl_ms
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::models::Venue;

    use super::{
        build_bybit_transfer_status_cache, bybit_transfer_status_cache_is_fresh,
        filter_bybit_transfer_statuses, BybitCoinChain, BybitCoinInfo,
    };

    #[test]
    fn transfer_status_cache_filters_requested_assets_and_marks_missing_assets_closed() {
        let cache = build_bybit_transfer_status_cache(
            vec![
                BybitCoinInfo {
                    coin: "BTC".to_string(),
                    chains: vec![BybitCoinChain {
                        chain_deposit: "1".to_string(),
                        chain_withdraw: "1".to_string(),
                    }],
                },
                BybitCoinInfo {
                    coin: "ETH".to_string(),
                    chains: vec![BybitCoinChain {
                        chain_deposit: "0".to_string(),
                        chain_withdraw: "1".to_string(),
                    }],
                },
            ],
            12_345,
        );

        let wanted = ["BTC".to_string(), "DOGE".to_string()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let statuses = filter_bybit_transfer_statuses(&cache, &wanted);

        assert_eq!(statuses.len(), 2);
        let btc = statuses.iter().find(|item| item.asset == "BTC").unwrap();
        assert_eq!(btc.venue, Venue::Bybit);
        assert!(btc.deposit_enabled);
        assert!(btc.withdraw_enabled);

        let doge = statuses.iter().find(|item| item.asset == "DOGE").unwrap();
        assert_eq!(doge.venue, Venue::Bybit);
        assert!(!doge.deposit_enabled);
        assert!(!doge.withdraw_enabled);
        assert_eq!(doge.observed_at_ms, 12_345);
    }

    #[test]
    fn transfer_status_cache_expires_after_ttl() {
        let cache = build_bybit_transfer_status_cache(Vec::new(), 10_000);

        assert!(bybit_transfer_status_cache_is_fresh(&cache, 10_250, 500));
        assert!(!bybit_transfer_status_cache_is_fresh(&cache, 10_501, 500));
    }
}
