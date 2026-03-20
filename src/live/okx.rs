use std::{
    collections::{HashMap, HashSet},
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
    estimate_fee_quote, floor_to_step, format_decimal, hinted_fill, hmac_sha256_base64,
    iso8601_from_ms, load_json_cache, lookup_or_wait_private_order, merged_quote_snapshot,
    now_ms, parse_f64, parse_i64, parse_text_message, quote_fill, spawn_ws_loop,
    store_json_cache, venue_symbol, PrivateOrderUpdate, WsMarketState, WsPrivateState,
    SYMBOL_CACHE_TTL_MS,
};

const OKX_MAX_SUBSCRIBE_ARGS_PER_MESSAGE: usize = 100;

pub struct OkxLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    client: Client,
    base_url: String,
    metadata: Mutex<HashMap<String, OkxInstrumentMeta>>,
    supported_symbols: Mutex<HashSet<String>>,
    position_mode: Mutex<Option<OkxPositionMode>>,
    time_offset_ms: Mutex<Option<i64>>,
    market_ws: Arc<WsMarketState>,
    private_ws: Arc<WsPrivateState>,
}

impl OkxLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        runtime: &RuntimeConfig,
        symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Okx {
            return Err(anyhow!("okx live adapter requires okx config"));
        }

        let market_ws = WsMarketState::new();
        let persisted_catalog = load_json_cache::<OkxSymbolCatalogCache>("okx-symbols.json");
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        if let Some(cache) = persisted_catalog {
            if !cache_is_fresh(cache.updated_at_ms, now_ms(), SYMBOL_CACHE_TTL_MS) {
                debug!("okx symbol catalog cache is stale; using as fallback seed");
            }
            metadata.extend(cache.metadata);
            supported_symbols.extend(cache.supported_symbols);
        }
        let adapter = Self {
            config: config.clone(),
            runtime: runtime.clone(),
            client: build_http_client(runtime.exchange_http_timeout_ms)?,
            base_url: config
                .live
                .base_url
                .clone()
                .unwrap_or_else(|| "https://www.okx.com".to_string()),
            metadata: Mutex::new(metadata),
            supported_symbols: Mutex::new(supported_symbols),
            position_mode: Mutex::new(None),
            time_offset_ms: Mutex::new(None),
            market_ws,
            private_ws: WsPrivateState::new(),
        };
        if let Err(error) = adapter.refresh_symbol_catalog().await {
            if adapter.supported_symbols.lock().expect("lock").is_empty() {
                return Err(error);
            }
            warn!(?error, "okx symbol catalog refresh failed; using persisted cache");
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

        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let mut args = Vec::new();
        for symbol in symbols {
            let inst_id = venue_symbol(&self.config, symbol);
            args.push(serde_json::json!({
                "channel": "tickers",
                "instId": inst_id,
            }));
            args.push(serde_json::json!({
                "channel": "funding-rate",
                "instId": inst_id,
            }));
        }
        let subscribe_messages = build_okx_subscribe_messages(&args);
        let state = self.market_ws.clone();
        spawn_ws_loop(
            "okx",
            okx_public_ws_url(&self.base_url).to_string(),
            subscribe_messages,
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
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                let data = match payload.get("data").and_then(|value| value.as_array()) {
                    Some(data) => data,
                    None => return Ok(()),
                };
                for row in data {
                    let inst_id = row
                        .get("instId")
                        .and_then(|value| value.as_str())
                        .or_else(|| arg.get("instId").and_then(|value| value.as_str()))
                        .ok_or_else(|| anyhow!("okx ws payload missing instId"))?;
                    let Some(symbol) = symbol_map.get(inst_id) else {
                        continue;
                    };
                    match channel {
                        "tickers" => {
                            cache.update_quote(
                                symbol,
                                parse_f64(
                                    row.get("bidPx")
                                        .and_then(|value| value.as_str())
                                        .ok_or_else(|| anyhow!("okx ws missing bidPx"))?,
                                )?,
                                parse_f64(
                                    row.get("askPx")
                                        .and_then(|value| value.as_str())
                                        .ok_or_else(|| anyhow!("okx ws missing askPx"))?,
                                )?,
                                parse_f64(
                                    row.get("bidSz")
                                        .and_then(|value| value.as_str())
                                        .ok_or_else(|| anyhow!("okx ws missing bidSz"))?,
                                )?,
                                parse_f64(
                                    row.get("askSz")
                                        .and_then(|value| value.as_str())
                                        .ok_or_else(|| anyhow!("okx ws missing askSz"))?,
                                )?,
                                parse_i64(
                                    row.get("ts")
                                        .and_then(|value| value.as_str())
                                        .ok_or_else(|| anyhow!("okx ws missing ts"))?,
                                )?,
                            );
                        }
                        "funding-rate" => {
                            cache.update_funding(
                                symbol,
                                parse_f64(
                                    row.get("fundingRate")
                                        .and_then(|value| value.as_str())
                                        .ok_or_else(|| anyhow!("okx ws missing fundingRate"))?,
                                )?,
                                parse_i64(
                                    row.get("nextFundingTime")
                                        .and_then(|value| value.as_str())
                                        .ok_or_else(|| anyhow!("okx ws missing nextFundingTime"))?,
                                )?,
                            );
                        }
                        _ => {}
                    }
                }
                Ok(())
            },
        );
    }

    async fn cached_snapshot(&self, symbol: &str) -> Result<Option<(SymbolMarketSnapshot, i64)>> {
        let Some(quote) = self.market_ws.quote(symbol) else {
            return Ok(None);
        };
        let Some((funding_rate, funding_timestamp_ms)) = self.market_ws.funding(symbol) else {
            return Ok(None);
        };
        let meta = match self.symbol_meta(symbol).await {
            Ok(meta) => meta,
            Err(error) if is_missing_okx_instrument_meta(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(Some((
            merged_quote_snapshot(
                symbol,
                super::WsBookQuote {
                    best_bid: quote.best_bid,
                    best_ask: quote.best_ask,
                    bid_size: quote.bid_size * meta.ct_val,
                    ask_size: quote.ask_size * meta.ct_val,
                    observed_at_ms: quote.observed_at_ms,
                },
                funding_rate,
                funding_timestamp_ms,
            ),
            quote.observed_at_ms,
        )))
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

        let client = self.client.clone();
        let base_url = self.base_url.clone();
        let private_state = self.private_ws.clone();
        let reconnect_initial_ms = self.runtime.ws_reconnect_initial_ms;
        let reconnect_max_ms = self.runtime.ws_reconnect_max_ms;
        let unhealthy_after_failures = self.runtime.ws_unhealthy_after_failures;
        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let subscribe_messages = build_okx_private_subscribe_messages(&symbol_map);
        let url = okx_private_ws_url(&self.base_url).to_string();
        let ct_val_map = okx_ct_val_map_from_cached_metadata(
            &self.metadata.lock().expect("lock"),
            &symbol_map,
        );
        let task = tokio::spawn(async move {
            let mut reconnect_backoff =
                FailureBackoff::new(reconnect_initial_ms, reconnect_max_ms, Venue::Okx as u64);
            loop {
                match connect_async(url.as_str()).await {
                    Ok((mut socket, _)) => {
                        reconnect_backoff.on_success();
                        private_state.record_connection_success(now_ms());
                        let timestamp =
                            match fetch_okx_server_timestamp_ms(&client, &base_url).await {
                                Ok(server_timestamp_ms) => {
                                    format!("{:.3}", server_timestamp_ms as f64 / 1_000.0)
                                }
                                Err(error) => {
                                    private_state.record_connection_failure(
                                        now_ms(),
                                        unhealthy_after_failures,
                                        error.to_string(),
                                    );
                                    warn!(?error, "okx private websocket server time fetch failed");
                                    sleep(Duration::from_millis(
                                        reconnect_backoff.on_failure_with_jitter(),
                                    ))
                                    .await;
                                    continue;
                                }
                            };
                        let signature = match hmac_sha256_base64(
                            api_secret.as_str(),
                            &format!("{timestamp}GET/users/self/verify"),
                        ) {
                            Ok(signature) => signature,
                            Err(error) => {
                                private_state.record_connection_failure(
                                    now_ms(),
                                    unhealthy_after_failures,
                                    error.to_string(),
                                );
                                warn!(?error, "okx private websocket auth sign failed");
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
                                "sign": signature,
                            }],
                        })
                        .to_string();
                        if let Err(error) = socket.send(Message::Text(login.into())).await {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                error.to_string(),
                            );
                            warn!(?error, "okx private websocket login send failed");
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
                                            match handle_okx_private_message(
                                                &private_state,
                                                &symbol_map,
                                                &ct_val_map,
                                                &subscribe_messages,
                                                text.as_ref(),
                                                &mut subscribed,
                                            ) {
                                                Ok(Some(subscribe_payloads)) => {
                                                    for subscribe_payload in subscribe_payloads {
                                                        if let Err(error) = socket.send(Message::Text(subscribe_payload.into())).await {
                                                            private_state.record_connection_failure(
                                                                now_ms(),
                                                                unhealthy_after_failures,
                                                                error.to_string(),
                                                            );
                                                            warn!(?error, "okx private websocket subscribe send failed");
                                                            break;
                                                        }
                                                    }
                                                }
                                                Ok(None) => {}
                                                Err(error) => {
                                                    debug!(?error, "okx private websocket message ignored");
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
                                            debug!(?frame, "okx private websocket closed");
                                            break;
                                        }
                                        Some(Ok(_)) => {}
                                        Some(Err(error)) => {
                                            private_state.record_connection_failure(
                                                now_ms(),
                                                unhealthy_after_failures,
                                                error.to_string(),
                                            );
                                            warn!(?error, "okx private websocket receive failed");
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
                        warn!(?error, "okx private websocket connect failed");
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

    async fn symbol_meta(&self, symbol: &str) -> Result<OkxInstrumentMeta> {
        if let Some(meta) = self.metadata.lock().expect("lock").get(symbol).cloned() {
            return Ok(meta);
        }
        self.refresh_symbol_catalog().await?;
        self.metadata
            .lock()
            .expect("lock")
            .get(symbol)
            .cloned()
            .with_context(|| format!("okx instrument metadata missing for {}", venue_symbol(&self.config, symbol)))
    }

    async fn refresh_symbol_catalog(&self) -> Result<()> {
        let instrument_map = fetch_okx_instrument_meta_map(&self.client, &self.base_url).await?;
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        for (instrument_id, meta) in instrument_map {
            let symbol_key = okx_symbol_key(&instrument_id);
            supported_symbols.insert(symbol_key.clone());
            metadata.insert(symbol_key, meta);
        }
        store_json_cache(
            "okx-symbols.json",
            &OkxSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols: supported_symbols.iter().cloned().collect(),
                metadata: metadata.clone(),
            },
        );
        *self.metadata.lock().expect("lock") = metadata;
        *self.supported_symbols.lock().expect("lock") = supported_symbols;
        Ok(())
    }

    async fn position_mode(&self) -> Result<OkxPositionMode> {
        if let Some(mode) = self.position_mode.lock().expect("lock").clone() {
            return Ok(mode);
        }

        let response = self
            .signed_request(reqwest::Method::GET, "/api/v5/account/config", None, None)
            .await?
            .json::<OkxApiResponse<OkxAccountConfig>>()
            .await
            .context("failed to decode okx account config")?;
        let mode = response
            .data
            .into_iter()
            .next()
            .map(|row| {
                if row.pos_mode.contains("long_short") {
                    OkxPositionMode::LongShort
                } else {
                    OkxPositionMode::Net
                }
            })
            .ok_or_else(|| anyhow!("okx account config missing row"))?;
        self.position_mode
            .lock()
            .expect("lock")
            .replace(mode.clone());
        Ok(mode)
    }

    async fn fetch_symbol_snapshot(&self, symbol: &str) -> Result<SymbolMarketSnapshot> {
        if !self.supports_symbol(symbol) {
            return Err(anyhow!("okx symbol not supported for {}", symbol));
        }
        let inst_id = venue_symbol(&self.config, symbol);
        let meta = self.symbol_meta(symbol).await?;
        let book = self
            .client
            .get(format!("{}/api/v5/market/books", self.base_url))
            .query(&[("instId", inst_id.as_str()), ("sz", "1")])
            .send()
            .await
            .context("failed to request okx order book")?
            .error_for_status()
            .context("okx order book returned non-success status")?
            .json::<OkxApiResponse<OkxBook>>()
            .await
            .context("failed to decode okx order book")?;
        let funding = self
            .client
            .get(format!("{}/api/v5/public/funding-rate", self.base_url))
            .query(&[("instId", inst_id.as_str())])
            .send()
            .await
            .context("failed to request okx funding rate")?
            .error_for_status()
            .context("okx funding rate returned non-success status")?
            .json::<OkxApiResponse<OkxFundingRate>>()
            .await
            .context("failed to decode okx funding rate")?;
        let book = book
            .data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("okx order book missing rows for {inst_id}"))?;
        let funding = funding
            .data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("okx funding response missing rows for {inst_id}"))?;
        let best_bid = book
            .bids
            .first()
            .ok_or_else(|| anyhow!("okx bids missing for {inst_id}"))?;
        let best_ask = book
            .asks
            .first()
            .ok_or_else(|| anyhow!("okx asks missing for {inst_id}"))?;
        Ok(SymbolMarketSnapshot {
            symbol: symbol.to_string(),
            best_bid: parse_f64(&best_bid[0])?,
            best_ask: parse_f64(&best_ask[0])?,
            bid_size: parse_f64(&best_bid[1])? * meta.ct_val,
            ask_size: parse_f64(&best_ask[1])? * meta.ct_val,
            funding_rate: parse_f64(&funding.funding_rate)?,
            funding_timestamp_ms: parse_i64(&funding.next_funding_time)?,
        })
    }

    async fn server_timestamp_ms(&self) -> Result<i64> {
        if let Some(offset_ms) = *self.time_offset_ms.lock().expect("lock") {
            return Ok(now_ms() + offset_ms);
        }

        let offset_ms =
            fetch_okx_server_timestamp_ms(&self.client, &self.base_url).await? - now_ms();
        self.time_offset_ms.lock().expect("lock").replace(offset_ms);
        Ok(now_ms() + offset_ms)
    }

    async fn server_timestamp_iso8601(&self) -> Result<String> {
        Ok(iso8601_from_ms(self.server_timestamp_ms().await?))
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
            .ok_or_else(|| anyhow!("okx api key is not configured"))?;
        let api_secret = self
            .config
            .live
            .resolved_api_secret()
            .ok_or_else(|| anyhow!("okx api secret is not configured"))?;
        let passphrase = self
            .config
            .live
            .resolved_api_passphrase()
            .ok_or_else(|| anyhow!("okx api passphrase is not configured"))?;
        let timestamp = self.server_timestamp_iso8601().await?;
        let request_path = if let Some(query) = query.as_ref() {
            format!("{path}?{query}")
        } else {
            path.to_string()
        };
        let body_payload = body.clone().unwrap_or_default();
        let sign_payload = format!(
            "{}{}{}{}",
            timestamp,
            method.as_str(),
            request_path,
            body_payload
        );
        let signature = hmac_sha256_base64(&api_secret, &sign_payload)?;

        let mut headers = HeaderMap::new();
        headers.insert("OK-ACCESS-KEY", HeaderValue::from_str(&api_key)?);
        headers.insert("OK-ACCESS-SIGN", HeaderValue::from_str(&signature)?);
        headers.insert("OK-ACCESS-TIMESTAMP", HeaderValue::from_str(&timestamp)?);
        headers.insert("OK-ACCESS-PASSPHRASE", HeaderValue::from_str(&passphrase)?);
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        let url = format!("{}{}", self.base_url, request_path);
        let request = self.client.request(method, url).headers(headers);
        let request = if let Some(body) = body {
            request.body(body)
        } else {
            request
        };

        request
            .send()
            .await
            .context("failed to send signed okx request")?
            .error_for_status()
            .context("okx private endpoint returned non-success status")
    }
}

fn is_missing_okx_instrument_meta(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains("okx instrument metadata missing"))
}

#[async_trait]
impl VenueAdapter for OkxLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Okx
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut quotes = Vec::new();
        let mut observed_at_ms = 0_i64;
        let allow_direct_fallback = symbols.len() == 1;
        for symbol in symbols.iter().filter(|symbol| self.supports_symbol(symbol)) {
            if let Some((snapshot, snapshot_observed_at_ms)) = self.cached_snapshot(symbol).await? {
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
                "okx market snapshot unavailable for requested symbols"
            ));
        }

        Ok(VenueMarketSnapshot {
            venue: Venue::Okx,
            observed_at_ms: now_ms().max(observed_at_ms),
            symbols: quotes,
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        let meta = self.symbol_meta(&request.symbol).await?;
        let position_mode = self.position_mode().await?;
        let contracts = floor_to_step(request.quantity / meta.ct_val, meta.lot_sz);
        if contracts <= 0.0 {
            return Err(anyhow!(
                "okx order contracts rounded to zero for {}",
                request.symbol
            ));
        }

        let body = serde_json::json!({
            "instId": venue_symbol(&self.config, &request.symbol),
            "tdMode": "cross",
            "side": match request.side {
                Side::Buy => "buy",
                Side::Sell => "sell",
            },
            "posSide": okx_pos_side(position_mode, request.side, request.reduce_only),
            "ordType": "market",
            "sz": format_decimal(contracts, meta.lot_sz),
            "clOrdId": request.client_order_id,
            "reduceOnly": request.reduce_only,
        })
        .to_string();
        let response = self
            .signed_request(
                reqwest::Method::POST,
                "/api/v5/trade/order",
                None,
                Some(body),
            )
            .await?
            .json::<OkxApiResponse<OkxOrderAck>>()
            .await
            .context("failed to decode okx order ack")?;
        let ack = response
            .data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("okx order ack missing row"))?;
        if ack.s_code.as_deref().unwrap_or("0") != "0" {
            return Err(anyhow!(
                "okx order rejected: {}",
                ack.s_msg.unwrap_or_else(|| "unknown error".to_string())
            ));
        }

        let (average_price, filled_at_ms) = if let Some(fill) = hinted_fill(&request) {
            fill
        } else {
            let snapshot = self
                .fetch_market_snapshot(&[request.symbol.clone()])
                .await?;
            quote_fill(&snapshot, &request.symbol, request.side)?
        };
        let quantity = contracts * meta.ct_val;

        let mut fill = OrderFill {
            venue: Venue::Okx,
            symbol: request.symbol,
            side: request.side,
            quantity,
            average_price,
            fee_quote: estimate_fee_quote(average_price, quantity, self.config.taker_fee_bps),
            order_id: ack.ord_id.unwrap_or_else(|| "okx-unknown".to_string()),
            filled_at_ms,
            timing: None,
        };
        if let Some(private_fill) = lookup_or_wait_private_order(
            &self.private_ws,
            Some(&request.client_order_id),
            Some(fill.order_id.as_str()),
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
                venue: Venue::Okx,
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
                venue: Venue::Okx,
                symbol: symbol.to_string(),
                size: position.size,
                updated_at_ms: position.updated_at_ms,
            });
        }

        let query = build_query(&[("instId", venue_symbol(&self.config, symbol))]);
        let response = self
            .signed_request(
                reqwest::Method::GET,
                "/api/v5/account/positions",
                Some(query),
                None,
            )
            .await?
            .json::<OkxApiResponse<OkxPosition>>()
            .await
            .context("failed to decode okx positions")?;
        let meta = self.symbol_meta(symbol).await?;
        let size = response.data.into_iter().try_fold(0.0, |acc, position| {
            let contracts = parse_f64(&position.pos)?;
            let signed = match position.pos_side.as_deref() {
                Some("long") => contracts.abs(),
                Some("short") => -contracts.abs(),
                _ => contracts,
            };
            Ok::<f64, anyhow::Error>(acc + signed * meta.ct_val)
        })?;

        Ok(PositionSnapshot {
            venue: Venue::Okx,
            symbol: symbol.to_string(),
            size,
            updated_at_ms: now_ms(),
        })
    }

    async fn fetch_all_positions(&self) -> Result<Option<Vec<PositionSnapshot>>> {
        let response = self
            .signed_request(reqwest::Method::GET, "/api/v5/account/positions", None, None)
            .await?
            .json::<OkxApiResponse<OkxPosition>>()
            .await
            .context("failed to decode okx positions")?;

        let metadata = self.metadata.lock().expect("lock").clone();
        let mut positions_by_symbol = HashMap::new();
        for position in response.data {
            let symbol = okx_symbol_key(&position.inst_id);
            let Some(meta) = metadata.get(&symbol) else {
                continue;
            };
            let contracts = parse_f64(&position.pos)?;
            let signed = match position.pos_side.as_deref() {
                Some("long") => contracts.abs(),
                Some("short") => -contracts.abs(),
                _ => contracts,
            };
            *positions_by_symbol.entry(symbol).or_insert(0.0) += signed * meta.ct_val;
        }

        Ok(Some(
            positions_by_symbol
                .into_iter()
                .map(|(symbol, size)| PositionSnapshot {
                    venue: Venue::Okx,
                    symbol,
                    size,
                    updated_at_ms: now_ms(),
                })
                .collect(),
        ))
    }

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        let meta = self.symbol_meta(symbol).await?;
        let contracts = floor_to_step(quantity / meta.ct_val, meta.lot_sz);
        if contracts <= 0.0 {
            return Ok(0.0);
        }
        Ok(contracts * meta.ct_val)
    }

    async fn fetch_transfer_statuses(&self, assets: &[String]) -> Result<Vec<AssetTransferStatus>> {
        let wanted = assets
            .iter()
            .map(|asset| base_asset(asset))
            .collect::<std::collections::BTreeSet<_>>();
        if wanted.is_empty() {
            return Ok(Vec::new());
        }

        let query = build_query(&[("ccy", wanted.iter().cloned().collect::<Vec<_>>().join(","))]);
        let response = self
            .signed_request(
                reqwest::Method::GET,
                "/api/v5/asset/currencies",
                Some(query),
                None,
            )
            .await?
            .json::<OkxApiResponse<OkxCurrency>>()
            .await
            .context("failed to decode okx currencies")?;
        let observed_at_ms = now_ms();

        Ok(response
            .data
            .into_iter()
            .fold(
                HashMap::<String, AssetTransferStatus>::new(),
                |mut acc, row| {
                    let asset = base_asset(&row.ccy);
                    let entry = acc.entry(asset.clone()).or_insert(AssetTransferStatus {
                        venue: Venue::Okx,
                        asset,
                        deposit_enabled: false,
                        withdraw_enabled: false,
                        observed_at_ms,
                        source: "okx".to_string(),
                    });
                    entry.deposit_enabled |= row.can_dep;
                    entry.withdraw_enabled |= row.can_wd;
                    acc
                },
            )
            .into_values()
            .collect())
    }

    async fn shutdown(&self) -> Result<()> {
        self.market_ws.abort_worker();
        self.private_ws.abort_workers();
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OkxInstrumentMeta {
    ct_val: f64,
    lot_sz: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OkxSymbolCatalogCache {
    updated_at_ms: i64,
    supported_symbols: Vec<String>,
    metadata: HashMap<String, OkxInstrumentMeta>,
}

#[derive(Debug, Deserialize)]
struct OkxApiResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct OkxInstrument {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "ctVal")]
    ct_val: String,
    #[serde(rename = "lotSz")]
    lot_sz: String,
}

#[derive(Debug, Deserialize)]
struct OkxBook {
    asks: Vec<Vec<String>>,
    bids: Vec<Vec<String>>,
    #[allow(dead_code)]
    ts: String,
}

#[derive(Debug, Deserialize)]
struct OkxFundingRate {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: String,
}

#[derive(Debug, Deserialize)]
struct OkxServerTime {
    ts: String,
}

#[derive(Debug, Deserialize)]
struct OkxOrderAck {
    #[serde(rename = "ordId")]
    ord_id: Option<String>,
    #[serde(rename = "sCode")]
    s_code: Option<String>,
    #[serde(rename = "sMsg")]
    s_msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OkxPosition {
    #[serde(rename = "instId")]
    inst_id: String,
    pos: String,
    #[serde(rename = "posSide")]
    pos_side: Option<String>,
}

#[derive(Clone, Debug)]
enum OkxPositionMode {
    Net,
    LongShort,
}

#[derive(Debug, Deserialize)]
struct OkxAccountConfig {
    #[serde(rename = "posMode")]
    pos_mode: String,
}

fn okx_pos_side(mode: OkxPositionMode, side: Side, reduce_only: bool) -> &'static str {
    match mode {
        OkxPositionMode::Net => "net",
        OkxPositionMode::LongShort => match (side, reduce_only) {
            (Side::Buy, false) => "long",
            (Side::Sell, false) => "short",
            (Side::Buy, true) => "short",
            (Side::Sell, true) => "long",
        },
    }
}

fn okx_public_ws_url(base_url: &str) -> &'static str {
    if base_url.contains("us.okx.com") {
        "wss://wsus.okx.com:8443/ws/v5/public"
    } else {
        "wss://ws.okx.com:8443/ws/v5/public"
    }
}

fn okx_private_ws_url(base_url: &str) -> &'static str {
    if base_url.contains("us.okx.com") {
        "wss://wsus.okx.com:8443/ws/v5/private"
    } else {
        "wss://ws.okx.com:8443/ws/v5/private"
    }
}

async fn fetch_okx_server_timestamp_ms(client: &Client, base_url: &str) -> Result<i64> {
    let response = client
        .get(format!("{base_url}/api/v5/public/time"))
        .send()
        .await
        .context("failed to request okx server time")?
        .error_for_status()
        .context("okx server time returned non-success status")?
        .json::<OkxApiResponse<OkxServerTime>>()
        .await
        .context("failed to decode okx server time")?;
    let server_time = response
        .data
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("okx server time missing row"))?
        .ts;
    parse_i64(&server_time)
}

fn okx_ct_val_map_from_cached_metadata(
    metadata: &HashMap<String, OkxInstrumentMeta>,
    symbol_map: &HashMap<String, String>,
) -> HashMap<String, f64> {
    let mut map = HashMap::new();
    for (inst_id, symbol) in symbol_map {
        if let Some(meta) = metadata.get(symbol) {
            map.insert(inst_id.clone(), meta.ct_val);
        }
    }
    for inst_id in symbol_map.keys() {
        map.entry(inst_id.clone()).or_insert(1.0);
    }
    map
}

async fn fetch_okx_instrument_meta_map(
    client: &Client,
    base_url: &str,
) -> Result<HashMap<String, OkxInstrumentMeta>> {
    let response = client
        .get(format!("{base_url}/api/v5/public/instruments"))
        .query(&[("instType", "SWAP")])
        .send()
        .await
        .context("failed to request okx instruments")?
        .error_for_status()
        .context("okx instruments returned non-success status")?
        .json::<OkxApiResponse<OkxInstrument>>()
        .await
        .context("failed to decode okx instruments")?;
    let mut map = HashMap::new();
    for instrument in response.data {
        map.insert(
            instrument.inst_id,
            OkxInstrumentMeta {
                ct_val: parse_f64(&instrument.ct_val)?,
                lot_sz: parse_f64(&instrument.lot_sz)?,
            },
        );
    }
    Ok(map)
}

fn okx_symbol_key(inst_id: &str) -> String {
    inst_id.replace('-', "").trim_end_matches("SWAP").to_string()
}

fn handle_okx_private_message(
    private_state: &Arc<WsPrivateState>,
    symbol_map: &HashMap<String, String>,
    ct_val_map: &HashMap<String, f64>,
    subscribe_messages: &[String],
    raw: &str,
    subscribed: &mut bool,
) -> Result<Option<Vec<String>>> {
    let payload = parse_text_message(raw)?;
    if payload
        .get("event")
        .and_then(|value| value.as_str())
        .is_some_and(|event| event == "login")
        && payload
            .get("code")
            .and_then(|value| value.as_str())
            .is_some_and(|code| code == "0")
    {
        *subscribed = true;
        return Ok(Some(subscribe_messages.to_vec()));
    }
    if payload
        .get("event")
        .and_then(|value| value.as_str())
        .is_some_and(|event| event == "subscribe")
    {
        return Ok(None);
    }
    if payload.get("data").is_none()
        && payload
            .get("msg")
            .and_then(|value| value.as_str())
            .is_some_and(|msg| msg.eq_ignore_ascii_case("pong"))
    {
        return Ok(None);
    }
    if !*subscribed {
        return Ok(None);
    }

    let arg = match payload.get("arg") {
        Some(arg) => arg,
        None => return Ok(None),
    };
    let channel = arg
        .get("channel")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let data = match payload.get("data").and_then(|value| value.as_array()) {
        Some(data) => data,
        None => return Ok(None),
    };
    match channel {
        "orders" => {
            for row in data {
                let venue_symbol = row
                    .get("instId")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow!("okx order update missing instId"))?;
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    continue;
                };
                let ct_val = ct_val_map.get(venue_symbol).copied().unwrap_or(1.0);
                let fee_quote = match (
                    row.get("fillFeeCcy").and_then(|value| value.as_str()),
                    row.get("fillFee").and_then(|value| value.as_str()),
                ) {
                    (Some("USDT" | "USDC"), Some(fee)) => Some(parse_f64(fee)?.abs()),
                    _ => None,
                };
                let filled_contracts = row
                    .get("accFillSz")
                    .and_then(|value| value.as_str())
                    .or_else(|| row.get("fillSz").and_then(|value| value.as_str()))
                    .map(parse_f64)
                    .transpose()?;
                private_state.record_order(PrivateOrderUpdate {
                    symbol: symbol.clone(),
                    order_id: row
                        .get("ordId")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    client_order_id: row
                        .get("clOrdId")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    filled_quantity: filled_contracts.map(|contracts| contracts * ct_val),
                    average_price: row
                        .get("avgPx")
                        .and_then(|value| value.as_str())
                        .or_else(|| row.get("fillPx").and_then(|value| value.as_str()))
                        .filter(|value| !value.is_empty() && *value != "0")
                        .map(parse_f64)
                        .transpose()?,
                    fee_quote,
                    updated_at_ms: row
                        .get("fillTime")
                        .and_then(|value| value.as_str())
                        .or_else(|| row.get("uTime").and_then(|value| value.as_str()))
                        .map(parse_i64)
                        .transpose()?
                        .unwrap_or_else(now_ms),
                });
            }
        }
        "positions" => {
            let mut net_positions = HashMap::<String, f64>::new();
            let updated_at_ms = payload
                .get("data")
                .and_then(|value| value.as_array())
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("uTime"))
                .and_then(|value| value.as_str())
                .map(parse_i64)
                .transpose()?
                .unwrap_or_else(now_ms);
            for row in data {
                let Some(venue_symbol) = row.get("instId").and_then(|value| value.as_str()) else {
                    continue;
                };
                let Some(symbol) = symbol_map.get(venue_symbol) else {
                    continue;
                };
                let ct_val = ct_val_map.get(venue_symbol).copied().unwrap_or(1.0);
                let contracts = row
                    .get("pos")
                    .and_then(|value| value.as_str())
                    .map(parse_f64)
                    .transpose()?
                    .unwrap_or_default();
                let signed_contracts = match row.get("posSide").and_then(|value| value.as_str()) {
                    Some("long") => contracts.abs(),
                    Some("short") => -contracts.abs(),
                    _ => contracts,
                };
                *net_positions.entry(symbol.clone()).or_default() += signed_contracts * ct_val;
            }
            for (symbol, size) in net_positions {
                private_state.update_position(&symbol, size, updated_at_ms);
            }
        }
        _ => {}
    }
    Ok(None)
}

fn build_okx_subscribe_messages(args: &[serde_json::Value]) -> Vec<String> {
    args.chunks(OKX_MAX_SUBSCRIBE_ARGS_PER_MESSAGE)
        .map(|chunk| {
            serde_json::json!({
                "op": "subscribe",
                "args": chunk,
            })
            .to_string()
        })
        .collect()
}

fn build_okx_private_subscribe_messages(symbol_map: &HashMap<String, String>) -> Vec<String> {
    let args = symbol_map
        .keys()
        .flat_map(|inst_id| {
            [
                serde_json::json!({ "channel": "orders", "instType": "SWAP", "instId": inst_id }),
                serde_json::json!({ "channel": "positions", "instType": "SWAP", "instId": inst_id }),
            ]
        })
        .collect::<Vec<_>>();
    build_okx_subscribe_messages(&args)
}

#[derive(Debug, Deserialize)]
struct OkxCurrency {
    ccy: String,
    #[serde(rename = "canDep")]
    can_dep: bool,
    #[serde(rename = "canWd")]
    can_wd: bool,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        build_okx_private_subscribe_messages, build_okx_subscribe_messages, okx_pos_side,
        OkxPositionMode, OKX_MAX_SUBSCRIBE_ARGS_PER_MESSAGE,
    };
    use crate::models::Side;

    #[test]
    fn pos_side_maps_long_short_mode_correctly() {
        assert_eq!(
            okx_pos_side(OkxPositionMode::LongShort, Side::Buy, false),
            "long"
        );
        assert_eq!(
            okx_pos_side(OkxPositionMode::LongShort, Side::Sell, false),
            "short"
        );
        assert_eq!(
            okx_pos_side(OkxPositionMode::LongShort, Side::Buy, true),
            "short"
        );
        assert_eq!(
            okx_pos_side(OkxPositionMode::LongShort, Side::Sell, true),
            "long"
        );
    }

    #[test]
    fn pos_side_maps_net_mode_to_net() {
        assert_eq!(okx_pos_side(OkxPositionMode::Net, Side::Buy, false), "net");
        assert_eq!(okx_pos_side(OkxPositionMode::Net, Side::Sell, true), "net");
    }

    #[test]
    fn okx_market_subscriptions_are_chunked() {
        let args = (0..220)
            .map(|index| serde_json::json!({"channel": "tickers", "instId": format!("COIN{index}-USDT-SWAP")}))
            .collect::<Vec<_>>();
        let messages = build_okx_subscribe_messages(&args);
        assert!(messages.len() > 1);
        for message in messages {
            let payload: serde_json::Value =
                serde_json::from_str(&message).expect("decode subscribe payload");
            let args = payload["args"].as_array().expect("args");
            assert!(args.len() <= OKX_MAX_SUBSCRIBE_ARGS_PER_MESSAGE);
        }
    }

    #[test]
    fn okx_private_subscriptions_are_chunked() {
        let symbol_map = (0..120)
            .map(|index| (format!("COIN{index}-USDT-SWAP"), format!("COIN{index}USDT")))
            .collect::<HashMap<_, _>>();
        let messages = build_okx_private_subscribe_messages(&symbol_map);
        assert!(messages.len() > 1);
    }
}
