use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client,
};
use serde::{Deserialize, Deserializer, Serialize};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        AccountBalanceSnapshot, AssetTransferStatus, OrderExecutionTiming, OrderFill,
        OrderFillReconciliation, OrderRequest, PerpLiquiditySnapshot, PositionSnapshot, Side,
        SymbolMarketSnapshot, Venue, VenueMarketSnapshot,
    },
    resilience::FailureBackoff,
    venue::VenueAdapter,
};

use super::{
    base_asset, build_http_client, build_query, cache_is_fresh, enrich_fill_from_private,
    estimate_fee_quote, filter_transfer_statuses, floor_to_step, format_decimal, hinted_fill,
    hmac_sha256_hex, load_json_cache, lookup_or_wait_private_order, now_ms, parse_f64, parse_i64,
    parse_text_message, quote_fill, spawn_ws_loop, store_json_cache, transfer_cache_ttl_ms,
    venue_symbol, PrivateOrderUpdate, VenueTransferStatusCache, WsMarketState, WsPrivateState,
    SYMBOL_CACHE_TTL_MS,
};

const BINANCE_MAX_SUBSCRIBE_STREAMS_PER_MESSAGE: usize = 150;
const BINANCE_RECV_WINDOW_MS: &str = "10000";
const BINANCE_PERP_LIQUIDITY_CACHE_TTL_MS: i64 = 60 * 1_000;

pub struct BinanceLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    client: Client,
    base_url: String,
    wallet_base_url: String,
    metadata: Mutex<HashMap<String, BinanceSymbolMeta>>,
    supported_symbols: Mutex<HashSet<String>>,
    position_mode: Mutex<Option<BinancePositionMode>>,
    time_offset_ms: Mutex<Option<i64>>,
    market_ws: Arc<WsMarketState>,
    market_subscription_symbols: Mutex<Vec<String>>,
    private_ws: Arc<WsPrivateState>,
    transfer_status_cache: Mutex<Option<VenueTransferStatusCache>>,
    perp_liquidity_cache: Mutex<HashMap<String, PerpLiquiditySnapshot>>,
}

impl BinanceLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        runtime: &RuntimeConfig,
        symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Binance {
            return Err(anyhow!("binance live adapter requires binance config"));
        }

        let base_url = config
            .live
            .base_url
            .clone()
            .unwrap_or_else(|| "https://fapi.binance.com".to_string());
        let wallet_base_url = config
            .live
            .wallet_base_url
            .clone()
            .unwrap_or_else(|| "https://api.binance.com".to_string());

        let market_ws = WsMarketState::new();
        let persisted_catalog =
            load_json_cache::<BinanceSymbolCatalogCache>("binance-symbols.json");
        let persisted_transfer_cache =
            load_json_cache::<VenueTransferStatusCache>("binance-transfer-status.json");
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        if let Some(cache) = persisted_catalog {
            if !cache_is_fresh(cache.updated_at_ms, now_ms(), SYMBOL_CACHE_TTL_MS) {
                debug!("binance symbol catalog cache is stale; using as fallback seed");
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
            base_url,
            wallet_base_url,
            metadata: Mutex::new(metadata),
            supported_symbols: Mutex::new(supported_symbols),
            position_mode: Mutex::new(None),
            time_offset_ms: Mutex::new(None),
            market_ws,
            market_subscription_symbols: Mutex::new(Vec::new()),
            private_ws: WsPrivateState::new(),
            transfer_status_cache: Mutex::new(transfer_status_cache),
            perp_liquidity_cache: Mutex::new(HashMap::new()),
        };
        if let Err(error) = adapter.refresh_symbol_catalog().await {
            if adapter.supported_symbols.lock().expect("lock").is_empty() {
                return Err(error);
            }
            warn!(
                ?error,
                "binance symbol catalog refresh failed; using persisted cache"
            );
        }
        let tracked_symbols = adapter.tracked_symbols(symbols);
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
            BINANCE_PERP_LIQUIDITY_CACHE_TTL_MS,
        ) {
            return None;
        }
        Some(snapshot)
    }

    fn start_market_ws(&self, symbols: &[String]) {
        let stream_names = symbols
            .iter()
            .flat_map(|symbol| {
                let venue_symbol = venue_symbol(&self.config, symbol).to_ascii_lowercase();
                [
                    format!("{venue_symbol}@bookTicker"),
                    format!("{venue_symbol}@markPrice@1s"),
                ]
            })
            .collect::<Vec<_>>();
        if stream_names.is_empty() {
            return;
        }

        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let subscribe_messages = build_binance_subscribe_messages(&stream_names);
        let url = format!("{}/ws", binance_ws_base_url(&self.base_url));
        let state = self.market_ws.clone();
        spawn_ws_loop(
            "binance",
            url,
            subscribe_messages,
            state,
            self.runtime.ws_reconnect_initial_ms,
            self.runtime.ws_reconnect_max_ms,
            self.runtime.ws_unhealthy_after_failures,
            move |cache, raw| {
                let payload = parse_text_message(raw)?;
                let data = payload.get("data").unwrap_or(&payload);
                let event = data
                    .get("e")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                let venue_symbol = data
                    .get("s")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow!("binance ws payload missing symbol"))?;
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    return Ok(());
                };

                match event {
                    "bookTicker" => {
                        cache.update_quote(
                            symbol,
                            parse_f64(
                                data.get("b")
                                    .and_then(|value| value.as_str())
                                    .ok_or_else(|| anyhow!("binance ws missing bid price"))?,
                            )?,
                            parse_f64(
                                data.get("a")
                                    .and_then(|value| value.as_str())
                                    .ok_or_else(|| anyhow!("binance ws missing ask price"))?,
                            )?,
                            parse_f64(
                                data.get("B")
                                    .and_then(|value| value.as_str())
                                    .ok_or_else(|| anyhow!("binance ws missing bid size"))?,
                            )?,
                            parse_f64(
                                data.get("A")
                                    .and_then(|value| value.as_str())
                                    .ok_or_else(|| anyhow!("binance ws missing ask size"))?,
                            )?,
                            data.get("E")
                                .and_then(|value| value.as_i64())
                                .unwrap_or_else(now_ms),
                        );
                    }
                    "markPriceUpdate" => {
                        cache.update_mark_price(
                            symbol,
                            parse_f64(
                                data.get("p")
                                    .and_then(|value| value.as_str())
                                    .ok_or_else(|| anyhow!("binance ws missing mark price"))?,
                            )?,
                        );
                        cache.update_funding(
                            symbol,
                            parse_f64(
                                data.get("r")
                                    .and_then(|value| value.as_str())
                                    .ok_or_else(|| anyhow!("binance ws missing funding rate"))?,
                            )?,
                            data.get("T")
                                .and_then(|value| value.as_i64())
                                .ok_or_else(|| anyhow!("binance ws missing next funding time"))?,
                        );
                    }
                    _ => {}
                }

                Ok(())
            },
        );
    }

    fn cached_snapshot(&self, symbol: &str) -> Option<SymbolMarketSnapshot> {
        self.market_ws.snapshot(symbol)
    }

    fn start_private_ws(&self, symbols: &[String]) {
        let Some(api_key) = self.config.live.resolved_api_key() else {
            return;
        };
        if symbols.is_empty() {
            return;
        }

        let client = self.client.clone();
        let base_url = self.base_url.clone();
        let ws_base_url = binance_ws_base_url(&self.base_url).to_string();
        let private_state = self.private_ws.clone();
        let reconnect_initial_ms = self.runtime.ws_reconnect_initial_ms;
        let reconnect_max_ms = self.runtime.ws_reconnect_max_ms;
        let unhealthy_after_failures = self.runtime.ws_unhealthy_after_failures;
        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let task = tokio::spawn(async move {
            let mut reconnect_backoff = FailureBackoff::new(
                reconnect_initial_ms,
                reconnect_max_ms,
                Venue::Binance as u64,
            );
            loop {
                let listen_key =
                    match start_binance_listen_key(&client, &base_url, api_key.as_str()).await {
                        Ok(listen_key) => listen_key,
                        Err(error) => {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                error.to_string(),
                            );
                            warn!(?error, "binance private listenKey start failed");
                            sleep(Duration::from_millis(
                                reconnect_backoff.on_failure_with_jitter(),
                            ))
                            .await;
                            continue;
                        }
                    };
                let keepalive_client = client.clone();
                let keepalive_base_url = base_url.clone();
                let keepalive_api_key = api_key.clone();
                let keepalive_listen_key = listen_key.clone();
                let keepalive = tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_secs(30 * 60)).await;
                        if let Err(error) = keepalive_binance_listen_key(
                            &keepalive_client,
                            &keepalive_base_url,
                            keepalive_api_key.as_str(),
                            keepalive_listen_key.as_str(),
                        )
                        .await
                        {
                            warn!(?error, "binance private listenKey keepalive failed");
                            break;
                        }
                    }
                });

                let url = format!("{ws_base_url}/ws/{listen_key}");
                match connect_async(url.as_str()).await {
                    Ok((mut socket, _)) => {
                        reconnect_backoff.on_success();
                        private_state.record_connection_success(now_ms());
                        debug!("binance private websocket connected");
                        while let Some(message) = socket.next().await {
                            match message {
                                Ok(Message::Text(text)) => {
                                    if let Err(error) = handle_binance_private_message(
                                        &private_state,
                                        &symbol_map,
                                        text.as_ref(),
                                    ) {
                                        debug!(?error, "binance private websocket message ignored");
                                    }
                                }
                                Ok(Message::Ping(payload)) => {
                                    if let Err(error) = socket.send(Message::Pong(payload)).await {
                                        private_state.record_connection_failure(
                                            now_ms(),
                                            unhealthy_after_failures,
                                            error.to_string(),
                                        );
                                        warn!(?error, "binance private websocket pong failed");
                                        break;
                                    }
                                }
                                Ok(Message::Close(frame)) => {
                                    private_state.record_connection_failure(
                                        now_ms(),
                                        unhealthy_after_failures,
                                        format!("closed:{frame:?}"),
                                    );
                                    debug!(?frame, "binance private websocket closed");
                                    break;
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    private_state.record_connection_failure(
                                        now_ms(),
                                        unhealthy_after_failures,
                                        error.to_string(),
                                    );
                                    warn!(?error, "binance private websocket receive failed");
                                    break;
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
                        warn!(?error, "binance private websocket connect failed");
                    }
                }

                keepalive.abort();
                let _ = close_binance_listen_key(&client, &base_url, api_key.as_str(), &listen_key)
                    .await;
                sleep(Duration::from_millis(
                    reconnect_backoff.on_failure_with_jitter(),
                ))
                .await;
            }
        });
        self.private_ws.push_worker(task);
    }

    async fn fetch_symbol_snapshot(&self, symbol: &str) -> Result<SymbolMarketSnapshot> {
        if !self.supports_symbol(symbol) {
            return Err(anyhow!("binance symbol not supported for {}", symbol));
        }
        let venue_symbol = venue_symbol(&self.config, symbol);
        let book = self
            .client
            .get(format!("{}/fapi/v1/ticker/bookTicker", self.base_url))
            .query(&[("symbol", venue_symbol.as_str())])
            .send()
            .await
            .context("failed to request binance book ticker")?
            .error_for_status()
            .context("binance book ticker returned non-success status")?
            .json::<BinanceBookTicker>()
            .await
            .context("failed to decode binance book ticker")?;
        let premium = self
            .client
            .get(format!("{}/fapi/v1/premiumIndex", self.base_url))
            .query(&[("symbol", venue_symbol.as_str())])
            .send()
            .await
            .context("failed to request binance premium index")?
            .error_for_status()
            .context("binance premium index returned non-success status")?
            .json::<BinancePremiumIndex>()
            .await
            .context("failed to decode binance premium index")?;

        Ok(SymbolMarketSnapshot {
            symbol: symbol.to_string(),
            best_bid: parse_f64(&book.bid_price)?,
            best_ask: parse_f64(&book.ask_price)?,
            bid_size: parse_f64(&book.bid_qty)?,
            ask_size: parse_f64(&book.ask_qty)?,
            mark_price: Some(parse_f64(&premium.mark_price)?),
            funding_rate: parse_f64(&premium.last_funding_rate)?,
            funding_timestamp_ms: parse_i64(&premium.next_funding_time)?,
        })
    }

    async fn symbol_meta(&self, symbol: &str) -> Result<BinanceSymbolMeta> {
        if let Some(meta) = self.metadata.lock().expect("lock").get(symbol).cloned() {
            return Ok(meta);
        }
        self.refresh_symbol_catalog().await?;
        self.metadata
            .lock()
            .expect("lock")
            .get(symbol)
            .cloned()
            .with_context(|| {
                format!(
                    "binance exchange info missing symbol {}",
                    venue_symbol(&self.config, symbol)
                )
            })
    }

    async fn refresh_symbol_catalog(&self) -> Result<()> {
        let response = self
            .client
            .get(format!("{}/fapi/v1/exchangeInfo", self.base_url))
            .send()
            .await
            .context("failed to request binance exchange info")?
            .error_for_status()
            .context("binance exchange info returned non-success status")?
            .json::<BinanceExchangeInfo>()
            .await
            .context("failed to decode binance exchange info")?;

        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        for symbol_info in response.symbols {
            if !binance_symbol_is_supported(&symbol_info) {
                continue;
            }
            let Some(meta) = binance_symbol_meta_from_exchange_symbol(&symbol_info)? else {
                continue;
            };
            supported_symbols.insert(symbol_info.symbol.clone());
            metadata.insert(symbol_info.symbol, meta);
        }

        store_json_cache(
            "binance-symbols.json",
            &BinanceSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols: supported_symbols.iter().cloned().collect(),
                metadata: metadata.clone(),
            },
        );
        *self.metadata.lock().expect("lock") = metadata;
        *self.supported_symbols.lock().expect("lock") = supported_symbols;
        Ok(())
    }

    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        params: Vec<(&str, String)>,
        body: Option<String>,
        base_url: &str,
    ) -> Result<reqwest::Response> {
        let api_key = self
            .config
            .live
            .resolved_api_key()
            .ok_or_else(|| anyhow!("binance api key is not configured"))?;
        let api_secret = self
            .config
            .live
            .resolved_api_secret()
            .ok_or_else(|| anyhow!("binance api secret is not configured"))?;

        let query = build_query(&params);
        let signature = hmac_sha256_hex(&api_secret, &query)?;
        let url = format!("{base_url}{path}?{query}&signature={signature}");
        let mut headers = HeaderMap::new();
        headers.insert("X-MBX-APIKEY", HeaderValue::from_str(&api_key)?);

        let request = self.client.request(method, url).headers(headers);
        let request = if let Some(body) = body {
            request
                .header("Content-Type", "application/json")
                .body(body)
        } else {
            request
        };

        let response = request
            .send()
            .await
            .context("failed to send signed binance request")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format_binance_http_error(status, &body));
        }
        Ok(response)
    }

    async fn server_timestamp_ms(&self) -> Result<i64> {
        if let Some(offset_ms) = *self.time_offset_ms.lock().expect("lock") {
            return Ok(now_ms() + offset_ms);
        }

        let response = self
            .client
            .get(format!("{}/fapi/v1/time", self.base_url))
            .send()
            .await
            .context("failed to request binance server time")?
            .error_for_status()
            .context("binance server time returned non-success status")?
            .json::<BinanceServerTime>()
            .await
            .context("failed to decode binance server time")?;
        let offset_ms = response.server_time - now_ms();
        self.time_offset_ms.lock().expect("lock").replace(offset_ms);
        Ok(now_ms() + offset_ms)
    }

    async fn position_mode(&self) -> Result<BinancePositionMode> {
        if let Some(mode) = self.position_mode.lock().expect("lock").clone() {
            return Ok(mode);
        }

        let timestamp = self.server_timestamp_ms().await?.to_string();
        let response = self
            .signed_request(
                reqwest::Method::GET,
                "/fapi/v1/positionSide/dual",
                vec![("timestamp", timestamp)],
                None,
                &self.base_url,
            )
            .await?
            .json::<BinancePositionModeResponse>()
            .await
            .context("failed to decode binance position mode")?;
        let mode = if response.dual_side_position {
            BinancePositionMode::Hedge
        } else {
            BinancePositionMode::OneWay
        };
        self.position_mode
            .lock()
            .expect("lock")
            .replace(mode.clone());
        Ok(mode)
    }

    async fn submit_market_order_once(
        &self,
        request: &OrderRequest,
        quantity: f64,
        step_size: f64,
        position_mode: BinancePositionMode,
    ) -> Result<BinanceOrderResponseWithTiming> {
        let timestamp = self.server_timestamp_ms().await?.to_string();
        let sign_started_at = Instant::now();
        let mut params = vec![
            ("symbol", venue_symbol(&self.config, &request.symbol)),
            (
                "side",
                match request.side {
                    Side::Buy => "BUY".to_string(),
                    Side::Sell => "SELL".to_string(),
                },
            ),
            ("type", "MARKET".to_string()),
            ("quantity", format_decimal(quantity, step_size)),
            ("newClientOrderId", request.client_order_id.clone()),
            ("newOrderRespType", "RESULT".to_string()),
            ("recvWindow", BINANCE_RECV_WINDOW_MS.to_string()),
            ("timestamp", timestamp),
        ];
        if matches!(position_mode, BinancePositionMode::Hedge) {
            params.push((
                "positionSide",
                binance_position_side(position_mode, request.side, request.reduce_only).to_string(),
            ));
        } else {
            params.push(("reduceOnly", request.reduce_only.to_string()));
        }
        let api_key = self
            .config
            .live
            .resolved_api_key()
            .ok_or_else(|| anyhow!("binance api key is not configured"))?;
        let api_secret = self
            .config
            .live
            .resolved_api_secret()
            .ok_or_else(|| anyhow!("binance api secret is not configured"))?;
        let query = build_query(&params);
        let signature = hmac_sha256_hex(&api_secret, &query)?;
        let url = format!(
            "{}/fapi/v1/order?{query}&signature={signature}",
            self.base_url
        );
        let mut headers = HeaderMap::new();
        headers.insert("X-MBX-APIKEY", HeaderValue::from_str(&api_key)?);
        let request_builder = self
            .client
            .request(reqwest::Method::POST, url)
            .headers(headers);
        let request_sign_ms = elapsed_ms(sign_started_at);

        let submit_started_at = Instant::now();
        let response = request_builder
            .send()
            .await
            .context("failed to send signed binance request")?;
        let submit_http_ms = elapsed_ms(submit_started_at);
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format_binance_http_error(status, &body));
        }
        let decode_started_at = Instant::now();
        let response = response
            .json::<BinanceOrderResponse>()
            .await
            .context("failed to decode binance order response")?;
        let response_decode_ms = elapsed_ms(decode_started_at);
        Ok(BinanceOrderResponseWithTiming {
            response,
            request_sign_ms,
            submit_http_ms,
            response_decode_ms,
        })
    }

    fn clear_server_time_offset(&self) {
        self.time_offset_ms.lock().expect("lock").take();
    }

    fn clear_position_mode(&self) {
        self.position_mode.lock().expect("lock").take();
    }
}

fn format_binance_http_error(status: reqwest::StatusCode, body: &str) -> anyhow::Error {
    let trimmed = body.trim();
    if let Ok(payload) = serde_json::from_str::<BinanceApiErrorPayload>(trimmed) {
        return anyhow!(
            "binance private endpoint returned non-success status: status={} code={} msg={}",
            status,
            payload.code,
            payload.msg
        );
    }

    if trimmed.is_empty() {
        anyhow!(
            "binance private endpoint returned non-success status: status={}",
            status
        )
    } else {
        anyhow!(
            "binance private endpoint returned non-success status: status={} body={}",
            status,
            trimmed
        )
    }
}

fn should_retry_binance_order_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("code=-1021")
        || message.contains("recvwindow")
        || message.contains("timestamp")
        || (message.contains("position side") && message.contains("setting"))
        || message.contains("positionside")
        || message.contains("status=500")
        || message.contains("status=502")
        || message.contains("status=503")
        || message.contains("status=504")
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn binance_private_fill_wait_ms(
    average_price: f64,
    executed_quantity: f64,
    configured_wait_ms: u64,
) -> u64 {
    if average_price.is_finite()
        && average_price > 0.0
        && executed_quantity.is_finite()
        && executed_quantity > 0.0
    {
        0
    } else {
        configured_wait_ms
    }
}

#[async_trait]
impl VenueAdapter for BinanceLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Binance
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut quotes = Vec::new();
        let mut observed_at_ms = 0_i64;
        let allow_direct_fallback = symbols.len() == 1;
        for symbol in symbols.iter().filter(|symbol| self.supports_symbol(symbol)) {
            if let Some(snapshot) = self.cached_snapshot(symbol) {
                observed_at_ms = observed_at_ms.max(
                    self.market_ws
                        .quote(symbol)
                        .map(|quote| quote.observed_at_ms)
                        .unwrap_or_default(),
                );
                quotes.push(snapshot);
            } else if allow_direct_fallback {
                let snapshot = self.fetch_symbol_snapshot(symbol).await?;
                observed_at_ms = observed_at_ms.max(snapshot.funding_timestamp_ms.min(now_ms()));
                quotes.push(snapshot);
            }
        }
        if quotes.is_empty() {
            return Err(anyhow!(
                "binance market snapshot unavailable for requested symbols"
            ));
        }

        Ok(VenueMarketSnapshot {
            venue: Venue::Binance,
            observed_at_ms: now_ms().max(observed_at_ms),
            symbols: quotes,
        })
    }

    async fn refresh_market_snapshot(&self, symbol: &str) -> Result<VenueMarketSnapshot> {
        let snapshot = self.fetch_symbol_snapshot(symbol).await?;
        Ok(VenueMarketSnapshot {
            venue: Venue::Binance,
            observed_at_ms: now_ms(),
            symbols: vec![snapshot],
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        ensure_binance_client_order_id(&request.client_order_id)?;
        let order_prepare_started_at = Instant::now();
        let meta = self.symbol_meta(&request.symbol).await?;
        let position_mode = self.position_mode().await?;
        let quantity = floor_to_step(request.quantity, meta.step_size);
        validate_binance_order_request(
            &meta,
            &request.symbol,
            quantity,
            request.price_hint,
            request.mark_price_hint,
        )?;
        let order_prepare_ms = elapsed_ms(order_prepare_started_at);

        let submit_started_at = Instant::now();
        let response = match self
            .submit_market_order_once(&request, quantity, meta.step_size, position_mode.clone())
            .await
        {
            Ok(response) => response,
            Err(error) if should_retry_binance_order_error(&error) => {
                self.clear_server_time_offset();
                self.clear_position_mode();
                sleep(Duration::from_millis(100)).await;
                let retry_position_mode = self.position_mode().await?;
                self.submit_market_order_once(
                    &request,
                    quantity,
                    meta.step_size,
                    retry_position_mode,
                )
                .await?
            }
            Err(error) => return Err(error),
        };
        let submit_ack_ms = elapsed_ms(submit_started_at);

        let (fallback_price, fallback_ts) = if let Some(fill) = hinted_fill(&request) {
            fill
        } else {
            let snapshot = self
                .fetch_market_snapshot(&[request.symbol.clone()])
                .await?;
            quote_fill(&snapshot, &request.symbol, request.side)?
        };
        let average_price = parse_f64(&response.response.avg_price).unwrap_or(0.0);
        let average_price = if average_price > 0.0 {
            average_price
        } else {
            fallback_price
        };
        let executed_qty = parse_f64(&response.response.executed_qty).unwrap_or(quantity);
        let order_id = response.response.order_id.to_string();

        let mut fill = OrderFill {
            venue: Venue::Binance,
            symbol: request.symbol,
            side: request.side,
            quantity: executed_qty,
            average_price,
            fee_quote: estimate_fee_quote(average_price, executed_qty, self.config.taker_fee_bps),
            order_id: order_id.clone(),
            filled_at_ms: response.response.update_time.unwrap_or(fallback_ts),
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
        let private_fill_wait_ms = binance_private_fill_wait_ms(
            average_price,
            executed_qty,
            self.config.live.post_ack_private_fill_wait_ms,
        );
        let private_fill_wait_started_at = Instant::now();
        if let Some(private_fill) = lookup_or_wait_private_order(
            &self.private_ws,
            Some(&request.client_order_id),
            Some(order_id.as_str()),
            private_fill_wait_ms,
        )
        .await
        {
            fill = enrich_fill_from_private(fill, &private_fill);
        }
        if let Some(timing) = fill.timing.as_mut() {
            timing.private_fill_wait_ms = Some(elapsed_ms(private_fill_wait_started_at));
        }
        Ok(fill)
    }

    fn cached_position(&self, symbol: &str) -> Option<PositionSnapshot> {
        self.private_ws
            .position_if_fresh(symbol, self.runtime.private_position_max_age_ms, now_ms())
            .map(|position| PositionSnapshot {
                venue: Venue::Binance,
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
                venue: Venue::Binance,
                symbol: symbol.to_string(),
                size: position.size,
                updated_at_ms: position.updated_at_ms,
            });
        }

        let params = vec![
            ("symbol", venue_symbol(&self.config, symbol)),
            ("recvWindow", BINANCE_RECV_WINDOW_MS.to_string()),
            ("timestamp", self.server_timestamp_ms().await?.to_string()),
        ];
        let positions = self
            .signed_request(
                reqwest::Method::GET,
                "/fapi/v2/positionRisk",
                params,
                None,
                &self.base_url,
            )
            .await?
            .json::<Vec<BinancePositionRisk>>()
            .await
            .context("failed to decode binance position risk")?;

        let size = positions.into_iter().try_fold(0.0, |acc, position| {
            let qty = parse_f64(&position.position_amt)?;
            let signed = match position.position_side.as_deref() {
                Some("LONG") => qty.abs(),
                Some("SHORT") => -qty.abs(),
                _ => qty,
            };
            Ok::<f64, anyhow::Error>(acc + signed)
        })?;

        Ok(PositionSnapshot {
            venue: Venue::Binance,
            symbol: symbol.to_string(),
            size,
            updated_at_ms: now_ms(),
        })
    }

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        let params = vec![
            ("recvWindow", BINANCE_RECV_WINDOW_MS.to_string()),
            ("timestamp", self.server_timestamp_ms().await?.to_string()),
        ];
        let account = self
            .signed_request(
                reqwest::Method::GET,
                "/fapi/v2/account",
                params,
                None,
                &self.base_url,
            )
            .await?
            .json::<BinanceAccountInfo>()
            .await
            .context("failed to decode binance account balance")?;

        Ok(Some(AccountBalanceSnapshot {
            venue: Venue::Binance,
            equity_quote: parse_f64(&account.total_margin_balance)?,
            wallet_balance_quote: Some(parse_f64(&account.total_wallet_balance)?),
            available_balance_quote: Some(parse_f64(&account.available_balance)?),
            observed_at_ms: now_ms(),
        }))
    }

    async fn fetch_order_fill_reconciliation(
        &self,
        symbol: &str,
        order_id: &str,
        _client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        let parsed_order_id = order_id
            .parse::<i64>()
            .with_context(|| format!("invalid binance order id {order_id}"))?;
        let params = vec![
            ("symbol", venue_symbol(&self.config, symbol)),
            ("orderId", parsed_order_id.to_string()),
            ("recvWindow", BINANCE_RECV_WINDOW_MS.to_string()),
            ("timestamp", self.server_timestamp_ms().await?.to_string()),
        ];
        let order = self
            .signed_request(
                reqwest::Method::GET,
                "/fapi/v1/order",
                params,
                None,
                &self.base_url,
            )
            .await?
            .json::<BinanceOrderResponse>()
            .await
            .context("failed to decode binance order reconciliation")?;
        let quantity = parse_f64(&order.executed_qty)?;
        if quantity <= 0.0 {
            return Ok(None);
        }

        let trade_params = vec![
            ("symbol", venue_symbol(&self.config, symbol)),
            ("orderId", parsed_order_id.to_string()),
            ("recvWindow", BINANCE_RECV_WINDOW_MS.to_string()),
            ("timestamp", self.server_timestamp_ms().await?.to_string()),
        ];
        let trades = self
            .signed_request(
                reqwest::Method::GET,
                "/fapi/v1/userTrades",
                trade_params,
                None,
                &self.base_url,
            )
            .await?
            .json::<Vec<BinanceUserTrade>>()
            .await
            .context("failed to decode binance user trades")?;
        let mut total_quantity = 0.0;
        let mut weighted_notional = 0.0;
        let mut total_fee_quote = 0.0;
        let mut latest_fill_ms = order.update_time.unwrap_or_else(now_ms);
        for trade in trades
            .into_iter()
            .filter(|trade| trade.order_id == parsed_order_id)
        {
            let trade_quantity = parse_f64(&trade.qty)?;
            let trade_price = parse_f64(&trade.price)?;
            total_quantity += trade_quantity;
            weighted_notional += trade_price * trade_quantity;
            total_fee_quote += parse_f64(&trade.commission)?;
            latest_fill_ms = latest_fill_ms.max(trade.time);
        }
        let average_price = if total_quantity > 0.0 {
            weighted_notional / total_quantity
        } else {
            parse_f64(&order.avg_price)?
        };

        Ok(Some(OrderFillReconciliation {
            order_id: order.order_id.to_string(),
            client_order_id: None,
            quantity: if total_quantity > 0.0 {
                total_quantity
            } else {
                quantity
            },
            average_price,
            fee_quote: Some(total_fee_quote).filter(|value| *value > 0.0),
            filled_at_ms: latest_fill_ms,
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

    fn min_entry_notional_quote_hint(&self, symbol: &str, _price_hint: Option<f64>) -> Option<f64> {
        self.metadata
            .lock()
            .expect("lock")
            .get(symbol)
            .and_then(|meta| meta.min_notional)
    }

    async fn fetch_transfer_statuses(&self, assets: &[String]) -> Result<Vec<AssetTransferStatus>> {
        if self.wallet_base_url.contains("testnet") {
            return Ok(Vec::new());
        }

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

        let params = vec![
            ("recvWindow", BINANCE_RECV_WINDOW_MS.to_string()),
            ("timestamp", self.server_timestamp_ms().await?.to_string()),
        ];
        let coins = self
            .signed_request(
                reqwest::Method::GET,
                "/sapi/v1/capital/config/getall",
                params,
                None,
                &self.wallet_base_url,
            )
            .await?
            .json::<Vec<BinanceCapitalCoin>>()
            .await
            .context("failed to decode binance capital config")?;
        let statuses = coins
            .into_iter()
            .filter_map(|coin| {
                let asset = base_asset(&coin.coin);
                if !wanted.is_empty() && !wanted.contains(&asset) {
                    return None;
                }
                let deposit_enabled = coin
                    .network_list
                    .iter()
                    .any(|network| network.deposit_enable);
                let withdraw_enabled = coin
                    .network_list
                    .iter()
                    .any(|network| network.withdraw_enable);
                Some(AssetTransferStatus {
                    venue: Venue::Binance,
                    asset,
                    deposit_enabled,
                    withdraw_enabled,
                    observed_at_ms,
                    source: "binance".to_string(),
                })
            })
            .collect::<Vec<_>>();
        let cache = VenueTransferStatusCache {
            observed_at_ms,
            statuses: statuses.clone(),
        };
        store_json_cache("binance-transfer-status.json", &cache);
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
        let _ = self.server_timestamp_ms().await?;
        let _ = self.position_mode().await?;
        Ok(())
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

        let venue_symbol = venue_symbol(&self.config, symbol);
        let ticker_response = self
            .client
            .get(format!("{}/fapi/v1/ticker/24hr", self.base_url))
            .query(&[("symbol", venue_symbol.as_str())])
            .send()
            .await
            .context("failed to request binance 24h ticker")?;
        let status = ticker_response.status();
        if !status.is_success() {
            let body = ticker_response.text().await.unwrap_or_default();
            return Err(format_binance_http_error(status, &body));
        }
        let ticker = ticker_response
            .json::<BinanceTicker24h>()
            .await
            .context("failed to decode binance 24h ticker")?;

        let open_interest_response = self
            .client
            .get(format!("{}/fapi/v1/openInterest", self.base_url))
            .query(&[("symbol", venue_symbol.as_str())])
            .send()
            .await
            .context("failed to request binance open interest")?;
        let status = open_interest_response.status();
        if !status.is_success() {
            let body = open_interest_response.text().await.unwrap_or_default();
            return Err(format_binance_http_error(status, &body));
        }
        let open_interest = open_interest_response
            .json::<BinanceOpenInterest>()
            .await
            .context("failed to decode binance open interest")?;

        let premium = self
            .client
            .get(format!("{}/fapi/v1/premiumIndex", self.base_url))
            .query(&[("symbol", venue_symbol.as_str())])
            .send()
            .await
            .context("failed to request binance premium index for liquidity")?;
        let status = premium.status();
        if !status.is_success() {
            let body = premium.text().await.unwrap_or_default();
            return Err(format_binance_http_error(status, &body));
        }
        let premium = premium
            .json::<BinancePremiumIndex>()
            .await
            .context("failed to decode binance premium index for liquidity")?;

        let snapshot = PerpLiquiditySnapshot {
            venue: Venue::Binance,
            symbol: symbol.to_string(),
            volume_24h_quote: parse_f64(&ticker.quote_volume)?,
            open_interest_quote: parse_f64(&open_interest.open_interest)?
                * parse_f64(&premium.mark_price)?,
            observed_at_ms,
        };
        self.perp_liquidity_cache
            .lock()
            .expect("lock")
            .insert(symbol.to_string(), snapshot.clone());
        Ok(Some(snapshot))
    }

    async fn shutdown(&self) -> Result<()> {
        self.market_ws.abort_worker();
        self.private_ws.abort_workers();
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BinanceSymbolMeta {
    min_qty: f64,
    step_size: f64,
    max_qty: Option<f64>,
    min_notional: Option<f64>,
    market_take_bound: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BinanceSymbolCatalogCache {
    updated_at_ms: i64,
    supported_symbols: Vec<String>,
    metadata: HashMap<String, BinanceSymbolMeta>,
}

#[derive(Clone, Debug)]
enum BinancePositionMode {
    OneWay,
    Hedge,
}

#[derive(Debug, Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "bidPrice")]
    bid_price: String,
    #[serde(rename = "bidQty")]
    bid_qty: String,
    #[serde(rename = "askPrice")]
    ask_price: String,
    #[serde(rename = "askQty")]
    ask_qty: String,
}

#[derive(Debug, Deserialize)]
struct BinancePremiumIndex {
    #[serde(
        rename = "markPrice",
        deserialize_with = "deserialize_string_or_number"
    )]
    mark_price: String,
    #[serde(
        rename = "lastFundingRate",
        deserialize_with = "deserialize_string_or_number"
    )]
    last_funding_rate: String,
    #[serde(
        rename = "nextFundingTime",
        deserialize_with = "deserialize_string_or_number"
    )]
    next_funding_time: String,
}

#[derive(Debug, Deserialize)]
struct BinanceTicker24h {
    #[serde(
        rename = "quoteVolume",
        deserialize_with = "deserialize_string_or_number"
    )]
    quote_volume: String,
}

#[derive(Debug, Deserialize)]
struct BinanceOpenInterest {
    #[serde(
        rename = "openInterest",
        deserialize_with = "deserialize_string_or_number"
    )]
    open_interest: String,
}

#[derive(Debug, Deserialize)]
struct BinanceServerTime {
    #[serde(rename = "serverTime")]
    server_time: i64,
}

#[derive(Debug, Deserialize)]
struct BinancePositionModeResponse {
    #[serde(rename = "dualSidePosition")]
    dual_side_position: bool,
}

fn deserialize_string_or_number<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber {
        String(String),
        Integer(i64),
        Float(f64),
    }

    let value = StringOrNumber::deserialize(deserializer)?;
    Ok(match value {
        StringOrNumber::String(value) => value,
        StringOrNumber::Integer(value) => value.to_string(),
        StringOrNumber::Float(value) => value.to_string(),
    })
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceExchangeSymbol>,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeSymbol {
    symbol: String,
    status: Option<String>,
    #[serde(rename = "contractType")]
    contract_type: Option<String>,
    #[serde(rename = "quoteAsset")]
    quote_asset: Option<String>,
    #[serde(rename = "marketTakeBound", default)]
    market_take_bound: Option<String>,
    filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
struct BinanceFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "minQty", default)]
    min_qty: Option<String>,
    #[serde(rename = "maxQty", default)]
    max_qty: Option<String>,
    #[serde(rename = "stepSize", default)]
    step_size: Option<String>,
    #[serde(rename = "notional", default)]
    notional: Option<String>,
    #[serde(rename = "minNotional", default)]
    min_notional: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BinanceOrderResponse {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "avgPrice")]
    avg_price: String,
    #[serde(rename = "executedQty")]
    executed_qty: String,
    #[serde(rename = "updateTime")]
    update_time: Option<i64>,
}

#[derive(Debug)]
struct BinanceOrderResponseWithTiming {
    response: BinanceOrderResponse,
    request_sign_ms: u64,
    submit_http_ms: u64,
    response_decode_ms: u64,
}

#[derive(Debug, Deserialize)]
struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

fn binance_symbol_is_supported(symbol: &BinanceExchangeSymbol) -> bool {
    let is_trading = symbol
        .status
        .as_deref()
        .is_none_or(|status| status.eq_ignore_ascii_case("TRADING"));
    let is_perpetual = symbol
        .contract_type
        .as_deref()
        .is_none_or(|value| value.eq_ignore_ascii_case("PERPETUAL"));
    let is_quote_supported = symbol
        .quote_asset
        .as_deref()
        .is_none_or(|quote| matches!(quote, "USDT" | "USDC"));
    is_trading && is_perpetual && is_quote_supported
}

fn binance_symbol_meta_from_exchange_symbol(
    symbol_info: &BinanceExchangeSymbol,
) -> Result<Option<BinanceSymbolMeta>> {
    let lot_size = symbol_info
        .filters
        .iter()
        .find(|filter| filter.filter_type == "MARKET_LOT_SIZE")
        .or_else(|| {
            symbol_info
                .filters
                .iter()
                .find(|filter| filter.filter_type == "LOT_SIZE")
        });
    let Some(lot_size) = lot_size else {
        return Ok(None);
    };
    let min_notional_filter = symbol_info
        .filters
        .iter()
        .find(|filter| matches!(filter.filter_type.as_str(), "MIN_NOTIONAL" | "NOTIONAL"));
    Ok(Some(BinanceSymbolMeta {
        min_qty: parse_f64(
            lot_size
                .min_qty
                .as_deref()
                .ok_or_else(|| anyhow!("binance minQty missing for {}", symbol_info.symbol))?,
        )?,
        step_size: parse_f64(
            lot_size
                .step_size
                .as_deref()
                .ok_or_else(|| anyhow!("binance stepSize missing for {}", symbol_info.symbol))?,
        )?,
        max_qty: lot_size
            .max_qty
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(parse_f64)
            .transpose()?,
        min_notional: min_notional_filter
            .and_then(|filter| {
                filter
                    .notional
                    .as_deref()
                    .or(filter.min_notional.as_deref())
            })
            .filter(|value| !value.is_empty())
            .map(parse_f64)
            .transpose()?,
        market_take_bound: symbol_info
            .market_take_bound
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(parse_f64)
            .transpose()?,
    }))
}

fn binance_position_side(mode: BinancePositionMode, side: Side, reduce_only: bool) -> &'static str {
    match mode {
        BinancePositionMode::OneWay => "BOTH",
        BinancePositionMode::Hedge => match (side, reduce_only) {
            (Side::Buy, false) => "LONG",
            (Side::Sell, false) => "SHORT",
            (Side::Sell, true) => "LONG",
            (Side::Buy, true) => "SHORT",
        },
    }
}

fn binance_ws_base_url(base_url: &str) -> &'static str {
    if base_url.contains("testnet") {
        "wss://stream.binancefuture.com"
    } else {
        "wss://fstream.binance.com"
    }
}

fn build_binance_subscribe_messages(stream_names: &[String]) -> Vec<String> {
    stream_names
        .chunks(BINANCE_MAX_SUBSCRIBE_STREAMS_PER_MESSAGE)
        .enumerate()
        .map(|(index, chunk)| {
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": index + 1,
            })
            .to_string()
        })
        .collect()
}

async fn start_binance_listen_key(
    client: &Client,
    base_url: &str,
    api_key: &str,
) -> Result<String> {
    let response = client
        .post(format!("{base_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .context("failed to start binance listenKey")?
        .error_for_status()
        .context("binance listenKey start returned non-success status")?
        .json::<BinanceListenKeyResponse>()
        .await
        .context("failed to decode binance listenKey response")?;
    Ok(response.listen_key)
}

async fn keepalive_binance_listen_key(
    client: &Client,
    base_url: &str,
    api_key: &str,
    listen_key: &str,
) -> Result<()> {
    client
        .put(format!("{base_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", api_key)
        .query(&[("listenKey", listen_key)])
        .send()
        .await
        .context("failed to keepalive binance listenKey")?
        .error_for_status()
        .context("binance listenKey keepalive returned non-success status")?;
    Ok(())
}

async fn close_binance_listen_key(
    client: &Client,
    base_url: &str,
    api_key: &str,
    listen_key: &str,
) -> Result<()> {
    client
        .delete(format!("{base_url}/fapi/v1/listenKey"))
        .header("X-MBX-APIKEY", api_key)
        .query(&[("listenKey", listen_key)])
        .send()
        .await
        .context("failed to close binance listenKey")?
        .error_for_status()
        .context("binance listenKey close returned non-success status")?;
    Ok(())
}

fn handle_binance_private_message(
    private_state: &Arc<WsPrivateState>,
    symbol_map: &HashMap<String, String>,
    raw: &str,
) -> Result<()> {
    let payload = parse_text_message(raw)?;
    match payload
        .get("e")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
    {
        "TRADE_LITE" => {
            let venue_symbol = payload
                .get("s")
                .and_then(|value| value.as_str())
                .ok_or_else(|| anyhow!("binance TRADE_LITE missing symbol"))?;
            let Some(symbol) = symbol_map.get(venue_symbol) else {
                return Ok(());
            };
            private_state.record_order(PrivateOrderUpdate {
                symbol: symbol.clone(),
                order_id: payload
                    .get("i")
                    .map(|value| value.to_string().trim_matches('"').to_string())
                    .unwrap_or_default(),
                client_order_id: payload
                    .get("c")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
                filled_quantity: payload
                    .get("l")
                    .and_then(|value| value.as_str())
                    .map(parse_f64)
                    .transpose()?,
                average_price: payload
                    .get("L")
                    .and_then(|value| value.as_str())
                    .filter(|value| *value != "0")
                    .map(parse_f64)
                    .transpose()?,
                fee_quote: None,
                updated_at_ms: payload
                    .get("T")
                    .and_then(|value| value.as_i64())
                    .or_else(|| payload.get("E").and_then(|value| value.as_i64()))
                    .unwrap_or_else(now_ms),
            });
        }
        "ORDER_TRADE_UPDATE" => {
            let order = payload
                .get("o")
                .ok_or_else(|| anyhow!("binance ORDER_TRADE_UPDATE missing order"))?;
            let venue_symbol = order
                .get("s")
                .and_then(|value| value.as_str())
                .ok_or_else(|| anyhow!("binance order update missing symbol"))?;
            let Some(symbol) = symbol_map.get(venue_symbol) else {
                return Ok(());
            };
            let fee_quote = match (
                order.get("N").and_then(|value| value.as_str()),
                order.get("n").and_then(|value| value.as_str()),
            ) {
                (Some("USDT" | "USDC"), Some(fee)) => Some(parse_f64(fee)?),
                _ => None,
            };
            private_state.record_order(PrivateOrderUpdate {
                symbol: symbol.clone(),
                order_id: order
                    .get("i")
                    .map(|value| value.to_string().trim_matches('"').to_string())
                    .unwrap_or_default(),
                client_order_id: order
                    .get("c")
                    .and_then(|value| value.as_str())
                    .map(str::to_string),
                filled_quantity: order
                    .get("z")
                    .and_then(|value| value.as_str())
                    .map(parse_f64)
                    .transpose()?,
                average_price: order
                    .get("ap")
                    .and_then(|value| value.as_str())
                    .filter(|value| *value != "0")
                    .map(parse_f64)
                    .transpose()?,
                fee_quote,
                updated_at_ms: order
                    .get("T")
                    .and_then(|value| value.as_i64())
                    .or_else(|| payload.get("E").and_then(|value| value.as_i64()))
                    .unwrap_or_else(now_ms),
            });
        }
        "ACCOUNT_UPDATE" => {
            let event_time = payload
                .get("E")
                .and_then(|value| value.as_i64())
                .unwrap_or_else(now_ms);
            let mut net_positions = HashMap::<String, f64>::new();
            let positions = payload
                .get("a")
                .and_then(|value| value.get("P"))
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default();
            for position in positions {
                let Some(venue_symbol) = position.get("s").and_then(|value| value.as_str()) else {
                    continue;
                };
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    continue;
                };
                let quantity = position
                    .get("pa")
                    .and_then(|value| value.as_str())
                    .map(parse_f64)
                    .transpose()?
                    .unwrap_or_default();
                let signed = match position.get("ps").and_then(|value| value.as_str()) {
                    Some("LONG") => quantity.abs(),
                    Some("SHORT") => -quantity.abs(),
                    _ => quantity,
                };
                *net_positions.entry(symbol.clone()).or_default() += signed;
            }
            for (symbol, size) in net_positions {
                private_state.update_position(&symbol, size, event_time);
            }
        }
        _ => {}
    }
    Ok(())
}

fn ensure_binance_client_order_id(client_order_id: &str) -> Result<()> {
    if client_order_id.is_empty() {
        return Err(anyhow!("binance client order id must not be empty"));
    }
    if client_order_id.len() > 36 {
        return Err(anyhow!(
            "binance client order id too long: {} > 36",
            client_order_id.len()
        ));
    }
    if !client_order_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | ':' | '/' | '_' | '-'))
    {
        return Err(anyhow!(
            "binance client order id contains unsupported characters: {client_order_id}"
        ));
    }
    Ok(())
}

fn validate_binance_order_request(
    meta: &BinanceSymbolMeta,
    symbol: &str,
    quantity: f64,
    price_hint: Option<f64>,
    mark_price_hint: Option<f64>,
) -> Result<()> {
    if quantity < meta.min_qty {
        return Err(anyhow!(
            "binance order quantity {} below min qty {} for {}",
            quantity,
            meta.min_qty,
            symbol
        ));
    }
    if let Some(max_qty) = meta.max_qty {
        if quantity > max_qty {
            return Err(anyhow!(
                "binance order quantity {} above max qty {} for {}",
                quantity,
                max_qty,
                symbol
            ));
        }
    }
    if let (Some(min_notional), Some(price_hint)) = (
        meta.min_notional,
        price_hint.filter(|price| price.is_finite() && *price > 0.0),
    ) {
        let notional = quantity * price_hint;
        if notional < min_notional {
            return Err(anyhow!(
                "binance order notional {} below min notional {} for {}",
                notional,
                min_notional,
                symbol
            ));
        }
    }
    if let (Some(market_take_bound), Some(price_hint), Some(mark_price_hint)) = (
        meta.market_take_bound,
        price_hint.filter(|price| price.is_finite() && *price > 0.0),
        mark_price_hint.filter(|price| price.is_finite() && *price > 0.0),
    ) {
        let max_price = mark_price_hint * (1.0 + market_take_bound);
        let min_price = mark_price_hint * (1.0 - market_take_bound);
        if price_hint > max_price || price_hint < min_price {
            return Err(anyhow!(
                "binance order price hint {} exceeds marketTakeBound {} around mark price {} for {}",
                price_hint,
                market_take_bound,
                mark_price_hint,
                symbol
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        binance_position_side, binance_private_fill_wait_ms, build_binance_subscribe_messages,
        ensure_binance_client_order_id, format_binance_http_error,
        should_retry_binance_order_error, validate_binance_order_request, BinancePositionMode,
        BinancePremiumIndex, BinanceSymbolMeta, BINANCE_MAX_SUBSCRIBE_STREAMS_PER_MESSAGE,
    };
    use crate::models::Side;
    use anyhow::anyhow;

    #[test]
    fn premium_index_accepts_integer_next_funding_time() {
        let payload = r#"{
            "markPrice":"0.0",
            "lastFundingRate":"0.0001",
            "nextFundingTime":1773964800000
        }"#;
        let decoded: BinancePremiumIndex = serde_json::from_str(payload).expect("decode");
        assert_eq!(decoded.mark_price, "0.0");
        assert_eq!(decoded.last_funding_rate, "0.0001");
        assert_eq!(decoded.next_funding_time, "1773964800000");
    }

    #[test]
    fn hedge_mode_maps_position_side_correctly() {
        assert_eq!(
            binance_position_side(BinancePositionMode::Hedge, Side::Buy, false),
            "LONG"
        );
        assert_eq!(
            binance_position_side(BinancePositionMode::Hedge, Side::Sell, false),
            "SHORT"
        );
        assert_eq!(
            binance_position_side(BinancePositionMode::Hedge, Side::Sell, true),
            "LONG"
        );
        assert_eq!(
            binance_position_side(BinancePositionMode::Hedge, Side::Buy, true),
            "SHORT"
        );
    }

    #[test]
    fn market_subscribe_messages_are_chunked_for_large_symbol_sets() {
        let streams = (0..400)
            .map(|index| format!("symbol{index}@bookTicker"))
            .collect::<Vec<_>>();
        let messages = build_binance_subscribe_messages(&streams);
        assert!(messages.len() > 1);
        for message in messages {
            let payload: serde_json::Value =
                serde_json::from_str(&message).expect("decode subscribe payload");
            let params = payload["params"].as_array().expect("params");
            assert!(params.len() <= BINANCE_MAX_SUBSCRIBE_STREAMS_PER_MESSAGE);
        }
    }

    #[test]
    fn private_http_error_includes_status_code_and_exchange_message() {
        let error = format_binance_http_error(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}"#,
        );
        let rendered = error.to_string();
        assert!(rendered.contains("status=400 Bad Request"));
        assert!(rendered.contains("code=-1021"));
        assert!(rendered.contains("recvWindow"));
    }

    #[test]
    fn private_http_error_falls_back_to_trimmed_body_when_json_decode_fails() {
        let error =
            format_binance_http_error(reqwest::StatusCode::TOO_MANY_REQUESTS, "  rate limited  ");
        let rendered = error.to_string();
        assert!(rendered.contains("status=429 Too Many Requests"));
        assert!(rendered.contains("body=rate limited"));
    }

    #[test]
    fn retryable_order_errors_include_timestamp_and_server_failures() {
        assert!(should_retry_binance_order_error(&anyhow!(
            "binance private endpoint returned non-success status: status=400 Bad Request code=-1021 msg=Timestamp for this request is outside of the recvWindow."
        )));
        assert!(should_retry_binance_order_error(&anyhow!(
            "binance private endpoint returned non-success status: status=503 Service Unavailable body=busy"
        )));
        assert!(!should_retry_binance_order_error(&anyhow!(
            "binance private endpoint returned non-success status: status=400 Bad Request code=-4164 msg=Order's notional must be no smaller than 5"
        )));
        assert!(should_retry_binance_order_error(&anyhow!(
            "binance private endpoint returned non-success status: status=400 Bad Request code=-4061 msg=Order's position side does not match user's setting."
        )));
    }

    #[test]
    fn result_response_skips_private_fill_wait_when_fill_is_already_known() {
        assert_eq!(binance_private_fill_wait_ms(2134.5, 0.011, 120), 0);
        assert_eq!(binance_private_fill_wait_ms(0.0, 0.011, 120), 120);
        assert_eq!(binance_private_fill_wait_ms(2134.5, 0.0, 120), 120);
    }

    #[test]
    fn client_order_id_validation_rejects_overlong_values() {
        let error = ensure_binance_client_order_id(&"a".repeat(37)).expect_err("reject long id");
        assert!(error.to_string().contains("too long"));
    }

    #[test]
    fn client_order_id_validation_rejects_unsupported_characters() {
        let error = ensure_binance_client_order_id("bad id").expect_err("reject invalid chars");
        assert!(error.to_string().contains("unsupported characters"));
    }

    #[test]
    fn order_validation_applies_max_qty_and_min_notional_constraints() {
        let meta = BinanceSymbolMeta {
            min_qty: 0.001,
            step_size: 0.001,
            max_qty: Some(5.0),
            min_notional: Some(10.0),
            market_take_bound: Some(0.1),
        };
        assert!(
            validate_binance_order_request(&meta, "ETHUSDT", 1.0, Some(20.0), Some(20.0)).is_ok()
        );
        assert!(
            validate_binance_order_request(&meta, "ETHUSDT", 6.0, Some(20.0), Some(20.0))
                .expect_err("above max")
                .to_string()
                .contains("above max qty")
        );
        assert!(
            validate_binance_order_request(&meta, "ETHUSDT", 0.2, Some(20.0), Some(20.0))
                .expect_err("below min notional")
                .to_string()
                .contains("below min notional")
        );
        assert!(
            validate_binance_order_request(&meta, "ETHUSDT", 1.0, Some(25.0), Some(20.0))
                .expect_err("outside market take bound")
                .to_string()
                .contains("marketTakeBound")
        );
    }
}

#[derive(Debug, Deserialize)]
struct BinancePositionRisk {
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "positionSide")]
    position_side: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BinanceAccountInfo {
    #[serde(rename = "totalMarginBalance")]
    total_margin_balance: String,
    #[serde(rename = "totalWalletBalance")]
    total_wallet_balance: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
}

#[derive(Debug, Deserialize)]
struct BinanceCapitalCoin {
    coin: String,
    #[serde(rename = "networkList", default)]
    network_list: Vec<BinanceCapitalNetwork>,
}

#[derive(Debug, Deserialize)]
struct BinanceCapitalNetwork {
    #[serde(rename = "depositEnable")]
    deposit_enable: bool,
    #[serde(rename = "withdrawEnable")]
    withdraw_enable: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceUserTrade {
    #[serde(rename = "orderId")]
    order_id: i64,
    price: String,
    qty: String,
    commission: String,
    time: i64,
}

#[derive(Debug, Deserialize)]
struct BinanceApiErrorPayload {
    code: i64,
    msg: String,
}
