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
use serde::{Deserialize, Serialize};
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        AccountBalanceSnapshot, AssetTransferStatus, OrderExecutionTiming, OrderFill,
        OrderFillReconciliation, OrderRequest, PositionSnapshot, Side, SymbolMarketSnapshot, Venue,
        VenueMarketSnapshot,
    },
    resilience::FailureBackoff,
    venue::VenueAdapter,
};

use super::{
    base_asset, build_http_client, build_query, cache_is_fresh, enrich_fill_from_private,
    estimate_fee_quote, filter_transfer_statuses, floor_to_step, format_decimal, hinted_fill,
    hmac_sha256_base64, iso8601_from_ms, load_json_cache, lookup_or_wait_private_order,
    merged_quote_snapshot, now_ms, parse_f64, parse_i64, parse_text_message, quote_fill,
    spawn_ws_loop, store_json_cache, transfer_cache_ttl_ms, venue_symbol, PrivateOrderUpdate,
    VenueTransferStatusCache, WsMarketState, WsPrivateState, SYMBOL_CACHE_TTL_MS,
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
    market_subscription_symbols: Mutex<Vec<String>>,
    private_ws: Arc<WsPrivateState>,
    transfer_status_cache: Mutex<Option<VenueTransferStatusCache>>,
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
        let persisted_transfer_cache =
            load_json_cache::<VenueTransferStatusCache>("okx-transfer-status.json");
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        if let Some(cache) = persisted_catalog {
            if !cache_is_fresh(cache.updated_at_ms, now_ms(), SYMBOL_CACHE_TTL_MS) {
                debug!("okx symbol catalog cache is stale; using as fallback seed");
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
                .unwrap_or_else(|| "https://www.okx.com".to_string()),
            metadata: Mutex::new(metadata),
            supported_symbols: Mutex::new(supported_symbols),
            position_mode: Mutex::new(None),
            time_offset_ms: Mutex::new(None),
            market_ws,
            market_subscription_symbols: Mutex::new(Vec::new()),
            private_ws: WsPrivateState::new(),
            transfer_status_cache: Mutex::new(transfer_status_cache),
        };
        if let Err(error) = adapter.refresh_symbol_catalog().await {
            if adapter.supported_symbols.lock().expect("lock").is_empty() {
                return Err(error);
            }
            warn!(
                ?error,
                "okx symbol catalog refresh failed; using persisted cache"
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

    fn prune_supported_symbol(&self, symbol: &str, reason: &str) {
        let removed = prune_okx_symbol_catalog_entry(
            &mut self.metadata.lock().expect("lock"),
            &mut self.supported_symbols.lock().expect("lock"),
            symbol,
        );
        if !removed {
            return;
        }
        self.persist_symbol_catalog();
        warn!(
            symbol,
            venue_symbol = %venue_symbol(&self.config, symbol),
            reason,
            "okx symbol removed from supported set"
        );
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
            "okx-symbols.json",
            &OkxSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols,
                metadata,
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
        if !cache_is_fresh(
            cache.observed_at_ms,
            now_ms,
            transfer_cache_ttl_ms(self.runtime.transfer_status_cache_ms),
        ) {
            return None;
        }
        Some(filter_transfer_statuses(cache, wanted))
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
        let ct_val_map =
            okx_ct_val_map_from_cached_metadata(&self.metadata.lock().expect("lock"), &symbol_map);
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
        let refresh_error = self.refresh_symbol_catalog().await.err();
        if let Some(meta) = self.metadata.lock().expect("lock").get(symbol).cloned() {
            return Ok(meta);
        }
        self.prune_supported_symbol(
            symbol,
            if refresh_error.is_some() {
                "metadata_missing_after_refresh_failure"
            } else {
                "metadata_missing_after_refresh"
            },
        );
        if let Some(error) = refresh_error {
            warn!(
                symbol,
                venue_symbol = %venue_symbol(&self.config, symbol),
                ?error,
                "okx symbol metadata refresh failed before pruning symbol"
            );
        }
        Err(anyhow!(
            "okx instrument metadata missing for {}",
            venue_symbol(&self.config, symbol)
        ))
    }

    async fn refresh_symbol_catalog(&self) -> Result<()> {
        let instrument_map = self.fetch_instrument_meta_map().await?;
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

    async fn fetch_instrument_meta_map(&self) -> Result<HashMap<String, OkxInstrumentMeta>> {
        if self.config.live.resolved_api_key().is_some()
            && self.config.live.resolved_api_secret().is_some()
            && self.config.live.resolved_api_passphrase().is_some()
        {
            let query = Some(build_query(&[("instType", "SWAP".to_string())]));
            match self
                .signed_request(
                    reqwest::Method::GET,
                    "/api/v5/account/instruments",
                    query,
                    None,
                )
                .await
            {
                Ok(response) => {
                    let response = response
                        .json::<OkxApiResponse<OkxInstrument>>()
                        .await
                        .context("failed to decode okx account instruments")?;
                    let response =
                        ensure_okx_api_success("okx account instruments failed", response)?;
                    return okx_instrument_meta_map_from_rows(response.data);
                }
                Err(error) => {
                    warn!(
                        ?error,
                        "okx account instruments fetch failed; falling back to public instruments"
                    );
                }
            }
        }

        fetch_okx_public_instrument_meta_map(&self.client, &self.base_url).await
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
        let response = ensure_okx_api_success("okx account config failed", response)?;
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
        let book = ensure_okx_api_success("okx order book failed", book)?;
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
        let funding = ensure_okx_api_success("okx funding rate failed", funding)?;
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
            mark_price: None,
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

    fn clear_position_mode(&self) {
        self.position_mode.lock().expect("lock").take();
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

        let response = request
            .send()
            .await
            .context("failed to send signed okx request")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format_okx_http_error(status, &body));
        }
        Ok(response)
    }

    async fn submit_order_once(
        &self,
        request: &OrderRequest,
        meta: &OkxInstrumentMeta,
        position_mode: OkxPositionMode,
        contracts: f64,
    ) -> Result<OkxOrderResponseWithTiming> {
        let sign_started_at = Instant::now();
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
        let timestamp = self.server_timestamp_iso8601().await?;
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
        let request_path = "/api/v5/trade/order";
        let sign_payload = format!(
            "{}{}{}{}",
            timestamp,
            reqwest::Method::POST.as_str(),
            request_path,
            body
        );
        let signature = hmac_sha256_base64(&api_secret, &sign_payload)?;
        let request_sign_ms = elapsed_ms(sign_started_at);

        let mut headers = HeaderMap::new();
        headers.insert("OK-ACCESS-KEY", HeaderValue::from_str(&api_key)?);
        headers.insert("OK-ACCESS-SIGN", HeaderValue::from_str(&signature)?);
        headers.insert("OK-ACCESS-TIMESTAMP", HeaderValue::from_str(&timestamp)?);
        headers.insert("OK-ACCESS-PASSPHRASE", HeaderValue::from_str(&passphrase)?);
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        let submit_started_at = Instant::now();
        let response = self
            .client
            .request(
                reqwest::Method::POST,
                format!("{}{}", self.base_url, request_path),
            )
            .headers(headers)
            .body(body)
            .send()
            .await
            .context("failed to send signed okx request")?;
        let submit_http_ms = elapsed_ms(submit_started_at);
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format_okx_http_error(status, &body));
        }
        let decode_started_at = Instant::now();
        let response = response
            .json::<OkxApiResponse<OkxOrderAck>>()
            .await
            .context("failed to decode okx order ack")?;
        let response_decode_ms = elapsed_ms(decode_started_at);
        Ok(OkxOrderResponseWithTiming {
            response,
            request_sign_ms,
            submit_http_ms,
            response_decode_ms,
        })
    }
}

fn format_okx_http_error(status: reqwest::StatusCode, body: &str) -> anyhow::Error {
    let trimmed = body.trim();
    if let Ok(payload) = serde_json::from_str::<OkxErrorPayload>(trimmed) {
        return anyhow!(
            "okx private endpoint returned non-success status: status={} code={} msg={}",
            status,
            payload.code,
            payload.msg
        );
    }
    if trimmed.is_empty() {
        anyhow!(
            "okx private endpoint returned non-success status: status={}",
            status
        )
    } else {
        anyhow!(
            "okx private endpoint returned non-success status: status={} body={}",
            status,
            trimmed
        )
    }
}

fn format_okx_order_ack_error(ack: &OkxOrderAck) -> anyhow::Error {
    anyhow!(
        "okx order rejected: s_code={} s_msg={} ord_id={} cl_ord_id={} tag={}",
        ack.s_code.as_deref().unwrap_or("unknown"),
        ack.s_msg.as_deref().unwrap_or("unknown"),
        ack.ord_id.as_deref().unwrap_or("unknown"),
        ack.cl_ord_id.as_deref().unwrap_or("unknown"),
        ack.tag.as_deref().unwrap_or("unknown")
    )
}

fn format_okx_order_response_error(
    context: &str,
    response: &OkxApiResponse<OkxOrderAck>,
) -> anyhow::Error {
    let top_code = response.code.as_deref().unwrap_or("unknown");
    let top_msg = response.msg.as_deref().unwrap_or("unknown");
    if let Some(ack) = response.data.first() {
        return anyhow!(
            "{context}: code={} msg={} s_code={} s_msg={} ord_id={} cl_ord_id={} tag={}",
            top_code,
            top_msg,
            ack.s_code.as_deref().unwrap_or("unknown"),
            ack.s_msg.as_deref().unwrap_or("unknown"),
            ack.ord_id.as_deref().unwrap_or("unknown"),
            ack.cl_ord_id.as_deref().unwrap_or("unknown"),
            ack.tag.as_deref().unwrap_or("unknown")
        );
    }

    anyhow!("{context}: code={} msg={}", top_code, top_msg)
}

fn should_retry_okx_order_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("posside")
        || message.contains("posside error")
        || message.contains("posmode")
        || message.contains("posmode error")
        || message.contains("position side")
        || message.contains("position mode")
}

fn ensure_okx_api_success<T>(
    context: &str,
    response: OkxApiResponse<T>,
) -> Result<OkxApiResponse<T>> {
    if response.code.as_deref().is_some_and(|code| code != "0") {
        return Err(anyhow!(
            "{context}: code={} msg={}",
            response.code.as_deref().unwrap_or("unknown"),
            response.msg.as_deref().unwrap_or("unknown")
        ));
    }
    Ok(response)
}

fn is_missing_okx_instrument_meta(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .to_string()
            .contains("okx instrument metadata missing")
    })
}

fn prune_okx_symbol_catalog_entry(
    metadata: &mut HashMap<String, OkxInstrumentMeta>,
    supported_symbols: &mut HashSet<String>,
    symbol: &str,
) -> bool {
    let removed_meta = metadata.remove(symbol).is_some();
    let removed_supported = supported_symbols.remove(symbol);
    removed_meta || removed_supported
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

    async fn refresh_market_snapshot(&self, symbol: &str) -> Result<VenueMarketSnapshot> {
        let snapshot = self.fetch_symbol_snapshot(symbol).await?;
        Ok(VenueMarketSnapshot {
            venue: Venue::Okx,
            observed_at_ms: now_ms(),
            symbols: vec![snapshot],
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        ensure_okx_client_order_id(&request.client_order_id)?;
        let order_prepare_started_at = Instant::now();
        let meta = self.symbol_meta(&request.symbol).await?;
        let contracts = floor_to_step(request.quantity / meta.ct_val, meta.lot_sz);
        validate_okx_order_request(&meta, &request.symbol, contracts)?;
        let order_prepare_ms = elapsed_ms(order_prepare_started_at);
        let submit_started_at = Instant::now();
        let mut attempted_mode_reset = false;
        let response = loop {
            let position_mode = self.position_mode().await?;
            let response = self
                .submit_order_once(&request, &meta, position_mode, contracts)
                .await;
            match response {
                Ok(response) => {
                    let top_level_error = response
                        .response
                        .code
                        .as_deref()
                        .filter(|code| *code != "0")
                        .map(|_| {
                            format_okx_order_response_error(
                                "okx order ack failed",
                                &response.response,
                            )
                        });
                    let row_error = response
                        .response
                        .data
                        .first()
                        .filter(|ack| ack.s_code.as_deref().unwrap_or("0") != "0")
                        .map(format_okx_order_ack_error);
                    if let Some(error) = top_level_error.or(row_error) {
                        if !attempted_mode_reset && should_retry_okx_order_error(&error) {
                            attempted_mode_reset = true;
                            self.clear_position_mode();
                            sleep(Duration::from_millis(100)).await;
                            let _ = self.position_mode().await?;
                            continue;
                        }
                        return Err(error);
                    }
                    break response;
                }
                Err(error) if !attempted_mode_reset && should_retry_okx_order_error(&error) => {
                    attempted_mode_reset = true;
                    self.clear_position_mode();
                    sleep(Duration::from_millis(100)).await;
                    let _ = self.position_mode().await?;
                }
                Err(error) => return Err(error),
            }
        };
        let submit_ack_ms = elapsed_ms(submit_started_at);
        let ack = response
            .response
            .data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("okx order ack missing row"))?;

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
            Some(fill.order_id.as_str()),
            self.config.live.post_ack_private_fill_wait_ms,
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
        let response = ensure_okx_api_success("okx positions failed", response)?;
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
            .signed_request(
                reqwest::Method::GET,
                "/api/v5/account/positions",
                None,
                None,
            )
            .await?
            .json::<OkxApiResponse<OkxPosition>>()
            .await
            .context("failed to decode okx positions")?;
        let response = ensure_okx_api_success("okx positions failed", response)?;

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

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        let response = self
            .signed_request(reqwest::Method::GET, "/api/v5/account/balance", None, None)
            .await?
            .json::<OkxApiResponse<OkxAccountBalance>>()
            .await
            .context("failed to decode okx account balance")?;
        let response = ensure_okx_api_success("okx account balance failed", response)?;
        let row = response
            .data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("okx account balance missing row"))?;

        Ok(Some(AccountBalanceSnapshot {
            venue: Venue::Okx,
            equity_quote: parse_f64(&row.total_eq)?,
            wallet_balance_quote: row.adj_eq.as_deref().map(parse_f64).transpose()?,
            available_balance_quote: row.avail_eq.as_deref().map(parse_f64).transpose()?,
            observed_at_ms: now_ms(),
        }))
    }

    async fn fetch_order_fill_reconciliation(
        &self,
        symbol: &str,
        order_id: &str,
        _client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        let meta = self.symbol_meta(symbol).await?;
        let query = build_query(&[
            ("instType", "SWAP".to_string()),
            ("instId", venue_symbol(&self.config, symbol)),
            ("ordId", order_id.to_string()),
        ]);
        let response = self
            .signed_request(
                reqwest::Method::GET,
                "/api/v5/trade/fills",
                Some(query),
                None,
            )
            .await?
            .json::<OkxApiResponse<OkxTradeFill>>()
            .await
            .context("failed to decode okx fill reconciliation")?;
        let response = ensure_okx_api_success("okx fill reconciliation failed", response)?;
        if response.data.is_empty() {
            return Ok(None);
        }

        let mut total_quantity = 0.0;
        let mut weighted_notional = 0.0;
        let mut total_fee_quote = 0.0;
        let mut latest_fill_ms = 0_i64;
        for fill in response.data {
            let contracts = parse_f64(&fill.fill_sz)?;
            let quantity = contracts * meta.ct_val;
            let price = parse_f64(&fill.fill_px)?;
            total_quantity += quantity;
            weighted_notional += price * quantity;
            total_fee_quote += parse_f64(&fill.fee)?.abs();
            latest_fill_ms = latest_fill_ms.max(parse_i64(&fill.ts)?);
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
        let contracts = floor_to_step(quantity / meta.ct_val, meta.lot_sz);
        if contracts <= 0.0 {
            return Ok(0.0);
        }
        Ok(contracts * meta.ct_val)
    }

    fn min_entry_notional_quote_hint(&self, symbol: &str, price_hint: Option<f64>) -> Option<f64> {
        let price_hint = price_hint.filter(|price| price.is_finite() && *price > 0.0)?;
        let metadata = self.metadata.lock().expect("lock");
        let meta = metadata.get(symbol)?;
        Some(meta.min_sz.unwrap_or(meta.lot_sz).max(meta.lot_sz) * meta.ct_val * price_hint)
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
        let response = ensure_okx_api_success("okx currencies failed", response)?;
        let statuses = response
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
            .collect::<Vec<_>>();
        let cache = VenueTransferStatusCache {
            observed_at_ms,
            statuses: statuses.clone(),
        };
        store_json_cache("okx-transfer-status.json", &cache);
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
    min_sz: Option<f64>,
    max_mkt_sz: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OkxSymbolCatalogCache {
    updated_at_ms: i64,
    supported_symbols: Vec<String>,
    metadata: HashMap<String, OkxInstrumentMeta>,
}

#[derive(Debug)]
struct OkxOrderResponseWithTiming {
    response: OkxApiResponse<OkxOrderAck>,
    request_sign_ms: u64,
    submit_http_ms: u64,
    response_decode_ms: u64,
}

#[derive(Debug, Deserialize)]
struct OkxApiResponse<T> {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct OkxErrorPayload {
    code: String,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct OkxInstrument {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "ctVal")]
    ct_val: String,
    #[serde(rename = "lotSz")]
    lot_sz: String,
    #[serde(rename = "minSz")]
    min_sz: Option<String>,
    #[serde(rename = "maxMktSz")]
    max_mkt_sz: Option<String>,
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
    #[serde(rename = "clOrdId")]
    cl_ord_id: Option<String>,
    #[serde(rename = "sCode")]
    s_code: Option<String>,
    #[serde(rename = "sMsg")]
    s_msg: Option<String>,
    tag: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OkxPosition {
    #[serde(rename = "instId")]
    inst_id: String,
    pos: String,
    #[serde(rename = "posSide")]
    pos_side: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OkxTradeFill {
    #[serde(rename = "fillPx")]
    fill_px: String,
    #[serde(rename = "fillSz")]
    fill_sz: String,
    fee: String,
    ts: String,
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

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
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

fn okx_instrument_meta_map_from_rows(
    instruments: Vec<OkxInstrument>,
) -> Result<HashMap<String, OkxInstrumentMeta>> {
    let mut map = HashMap::new();
    for instrument in instruments {
        map.insert(
            instrument.inst_id,
            OkxInstrumentMeta {
                ct_val: parse_f64(&instrument.ct_val)?,
                lot_sz: parse_f64(&instrument.lot_sz)?,
                min_sz: instrument
                    .min_sz
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map(parse_f64)
                    .transpose()?,
                max_mkt_sz: instrument
                    .max_mkt_sz
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map(parse_f64)
                    .transpose()?,
            },
        );
    }
    Ok(map)
}

async fn fetch_okx_public_instrument_meta_map(
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
    okx_instrument_meta_map_from_rows(response.data)
}

fn okx_symbol_key(inst_id: &str) -> String {
    inst_id
        .replace('-', "")
        .trim_end_matches("SWAP")
        .to_string()
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

#[derive(Debug, Deserialize)]
struct OkxAccountBalance {
    #[serde(rename = "totalEq")]
    total_eq: String,
    #[serde(rename = "adjEq")]
    adj_eq: Option<String>,
    #[serde(rename = "availEq")]
    avail_eq: Option<String>,
}

fn ensure_okx_client_order_id(client_order_id: &str) -> Result<()> {
    if client_order_id.is_empty() {
        return Err(anyhow!("okx client order id must not be empty"));
    }
    if client_order_id.len() > 32 {
        return Err(anyhow!(
            "okx client order id too long: {} > 32",
            client_order_id.len()
        ));
    }
    if !client_order_id.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return Err(anyhow!(
            "okx client order id must be alphanumeric: {client_order_id}"
        ));
    }
    Ok(())
}

fn validate_okx_order_request(
    meta: &OkxInstrumentMeta,
    symbol: &str,
    contracts: f64,
) -> Result<()> {
    if contracts <= 0.0 {
        return Err(anyhow!(
            "okx order contracts rounded to zero for {}",
            symbol
        ));
    }
    if let Some(min_sz) = meta.min_sz {
        if contracts < min_sz {
            return Err(anyhow!(
                "okx order contracts {} below min sz {} for {}",
                contracts,
                min_sz,
                symbol
            ));
        }
    }
    if let Some(max_mkt_sz) = meta.max_mkt_sz {
        if contracts > max_mkt_sz {
            return Err(anyhow!(
                "okx order contracts {} above max market sz {} for {}",
                contracts,
                max_mkt_sz,
                symbol
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::{
        build_okx_private_subscribe_messages, build_okx_subscribe_messages,
        ensure_okx_client_order_id, format_okx_http_error, format_okx_order_ack_error,
        format_okx_order_response_error, okx_instrument_meta_map_from_rows, okx_pos_side,
        prune_okx_symbol_catalog_entry, should_retry_okx_order_error, validate_okx_order_request,
        OkxApiResponse, OkxInstrument, OkxInstrumentMeta, OkxOrderAck, OkxPositionMode,
        OKX_MAX_SUBSCRIBE_ARGS_PER_MESSAGE,
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

    #[test]
    fn pruning_missing_symbol_removes_cached_metadata_and_support() {
        let mut metadata = HashMap::from([(
            "ARCUSDT".to_string(),
            OkxInstrumentMeta {
                ct_val: 1.0,
                lot_sz: 0.1,
                min_sz: None,
                max_mkt_sz: None,
            },
        )]);
        let mut supported_symbols = HashSet::from(["ARCUSDT".to_string(), "BTCUSDT".to_string()]);

        let removed =
            prune_okx_symbol_catalog_entry(&mut metadata, &mut supported_symbols, "ARCUSDT");

        assert!(removed);
        assert!(!metadata.contains_key("ARCUSDT"));
        assert!(!supported_symbols.contains("ARCUSDT"));
        assert!(supported_symbols.contains("BTCUSDT"));
    }

    #[test]
    fn client_order_id_validation_rejects_non_alphanumeric_values() {
        let error = ensure_okx_client_order_id("lf-el-123").expect_err("reject invalid chars");
        assert!(error.to_string().contains("alphanumeric"));
    }

    #[test]
    fn client_order_id_validation_rejects_overlong_values() {
        let error = ensure_okx_client_order_id(&"a".repeat(33)).expect_err("reject long id");
        assert!(error.to_string().contains("too long"));
    }

    #[test]
    fn order_validation_applies_contract_size_constraints() {
        let meta = OkxInstrumentMeta {
            ct_val: 1.0,
            lot_sz: 0.1,
            min_sz: Some(1.0),
            max_mkt_sz: Some(5.0),
        };
        assert!(validate_okx_order_request(&meta, "ETHUSDT", 2.0).is_ok());
        assert!(validate_okx_order_request(&meta, "ETHUSDT", 0.5)
            .expect_err("below min")
            .to_string()
            .contains("below min sz"));
        assert!(validate_okx_order_request(&meta, "ETHUSDT", 6.0)
            .expect_err("above max")
            .to_string()
            .contains("above max market sz"));
    }

    #[test]
    fn instrument_meta_map_parses_account_level_size_constraints() {
        let map = okx_instrument_meta_map_from_rows(vec![OkxInstrument {
            inst_id: "ETH-USDT-SWAP".to_string(),
            ct_val: "0.01".to_string(),
            lot_sz: "1".to_string(),
            min_sz: Some("2".to_string()),
            max_mkt_sz: Some("1000".to_string()),
        }])
        .expect("parse meta map");
        let meta = map.get("ETH-USDT-SWAP").expect("instrument");
        assert_eq!(meta.ct_val, 0.01);
        assert_eq!(meta.lot_sz, 1.0);
        assert_eq!(meta.min_sz, Some(2.0));
        assert_eq!(meta.max_mkt_sz, Some(1000.0));
    }

    #[test]
    fn private_http_error_includes_status_code_and_exchange_message() {
        let error = format_okx_http_error(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            r#"{"code":"50011","msg":"Requests too frequent."}"#,
        );
        let rendered = error.to_string();
        assert!(rendered.contains("status=429 Too Many Requests"));
        assert!(rendered.contains("code=50011"));
        assert!(rendered.contains("Requests too frequent"));
    }

    #[test]
    fn order_ack_error_includes_exchange_code_and_message() {
        let error = format_okx_order_ack_error(&OkxOrderAck {
            ord_id: Some("123".to_string()),
            cl_ord_id: Some("cid-123".to_string()),
            s_code: Some("51008".to_string()),
            s_msg: Some("Order failed due to margin".to_string()),
            tag: Some("alpha".to_string()),
        });
        let rendered = error.to_string();
        assert!(rendered.contains("s_code=51008"));
        assert!(rendered.contains("Order failed due to margin"));
        assert!(rendered.contains("ord_id=123"));
        assert!(rendered.contains("cl_ord_id=cid-123"));
        assert!(rendered.contains("tag=alpha"));
    }

    #[test]
    fn top_level_order_ack_failure_includes_row_level_details() {
        let error = format_okx_order_response_error(
            "okx order ack failed",
            &OkxApiResponse {
                code: Some("1".to_string()),
                msg: Some("All operations failed".to_string()),
                data: vec![OkxOrderAck {
                    ord_id: None,
                    cl_ord_id: Some("cid-abc".to_string()),
                    s_code: Some("51008".to_string()),
                    s_msg: Some("Insufficient margin".to_string()),
                    tag: Some("alpha".to_string()),
                }],
            },
        );
        let rendered = error.to_string();
        assert!(rendered.contains("code=1"));
        assert!(rendered.contains("All operations failed"));
        assert!(rendered.contains("s_code=51008"));
        assert!(rendered.contains("Insufficient margin"));
        assert!(rendered.contains("cl_ord_id=cid-abc"));
    }

    #[test]
    fn retryable_order_errors_include_position_mode_mismatch() {
        assert!(should_retry_okx_order_error(&anyhow::anyhow!(
            "okx order rejected: s_code=51000 s_msg=Parameter posSide error ord_id=unknown cl_ord_id=unknown tag=unknown"
        )));
        assert!(should_retry_okx_order_error(&anyhow::anyhow!(
            "okx order ack failed: code=1 msg=All operations failed s_code=51000 s_msg=Parameter posMode error ord_id=unknown cl_ord_id=unknown tag=unknown"
        )));
        assert!(!should_retry_okx_order_error(&anyhow::anyhow!(
            "okx order rejected: s_code=51008 s_msg=Insufficient margin ord_id=unknown cl_ord_id=unknown tag=unknown"
        )));
    }
}
