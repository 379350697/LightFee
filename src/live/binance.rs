use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client,
};
use serde::{Deserialize, Deserializer};
use tokio::time::{sleep, Duration};
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
    base_asset, build_http_client, build_query, enrich_fill_from_private, estimate_fee_quote,
    floor_to_step, format_decimal, hinted_fill, hmac_sha256_hex, lookup_or_wait_private_order,
    now_ms, parse_f64, parse_i64, parse_text_message, quote_fill, spawn_ws_loop, venue_symbol,
    PrivateOrderUpdate, WsMarketState, WsPrivateState,
};

pub struct BinanceLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    client: Client,
    base_url: String,
    wallet_base_url: String,
    metadata: Mutex<HashMap<String, BinanceSymbolMeta>>,
    position_mode: Mutex<Option<BinancePositionMode>>,
    time_offset_ms: Mutex<Option<i64>>,
    market_ws: Arc<WsMarketState>,
    private_ws: Arc<WsPrivateState>,
}

impl BinanceLiveAdapter {
    pub fn new(config: &VenueConfig, runtime: &RuntimeConfig, symbols: &[String]) -> Result<Self> {
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
        let adapter = Self {
            config: config.clone(),
            runtime: runtime.clone(),
            client: build_http_client(runtime.chillybot_timeout_ms)?,
            base_url,
            wallet_base_url,
            metadata: Mutex::new(HashMap::new()),
            position_mode: Mutex::new(None),
            time_offset_ms: Mutex::new(None),
            market_ws,
            private_ws: WsPrivateState::new(),
        };
        adapter.start_market_ws(symbols);
        adapter.start_private_ws(symbols);
        Ok(adapter)
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
        let url = format!(
            "{}/stream?streams={}",
            binance_ws_base_url(&self.base_url),
            stream_names.join("/")
        );
        let state = self.market_ws.clone();
        spawn_ws_loop(
            "binance",
            url,
            Vec::new(),
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
            funding_rate: parse_f64(&premium.last_funding_rate)?,
            funding_timestamp_ms: parse_i64(&premium.next_funding_time)?,
        })
    }

    async fn symbol_meta(&self, symbol: &str) -> Result<BinanceSymbolMeta> {
        if let Some(meta) = self.metadata.lock().expect("lock").get(symbol).cloned() {
            return Ok(meta);
        }

        let venue_symbol = venue_symbol(&self.config, symbol);
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
        let symbol_info = response
            .symbols
            .into_iter()
            .find(|item| item.symbol == venue_symbol)
            .with_context(|| format!("binance exchange info missing symbol {venue_symbol}"))?;

        let lot_size = symbol_info
            .filters
            .iter()
            .find(|filter| filter.filter_type == "MARKET_LOT_SIZE")
            .or_else(|| {
                symbol_info
                    .filters
                    .iter()
                    .find(|filter| filter.filter_type == "LOT_SIZE")
            })
            .ok_or_else(|| anyhow!("binance lot size filter missing for {venue_symbol}"))?;
        let meta = BinanceSymbolMeta {
            min_qty: parse_f64(
                lot_size
                    .min_qty
                    .as_deref()
                    .ok_or_else(|| anyhow!("binance minQty missing for {venue_symbol}"))?,
            )?,
            step_size: parse_f64(
                lot_size
                    .step_size
                    .as_deref()
                    .ok_or_else(|| anyhow!("binance stepSize missing for {venue_symbol}"))?,
            )?,
        };
        self.metadata
            .lock()
            .expect("lock")
            .insert(symbol.to_string(), meta.clone());
        Ok(meta)
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

        request
            .send()
            .await
            .context("failed to send signed binance request")?
            .error_for_status()
            .context("binance private endpoint returned non-success status")
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
}

#[async_trait]
impl VenueAdapter for BinanceLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Binance
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut quotes = Vec::new();
        let mut observed_at_ms = 0_i64;
        for symbol in symbols {
            if let Some(snapshot) = self.cached_snapshot(symbol) {
                observed_at_ms = observed_at_ms.max(
                    self.market_ws
                        .quote(symbol)
                        .map(|quote| quote.observed_at_ms)
                        .unwrap_or_default(),
                );
                quotes.push(snapshot);
            } else {
                let snapshot = self.fetch_symbol_snapshot(symbol).await?;
                observed_at_ms = observed_at_ms.max(snapshot.funding_timestamp_ms.min(now_ms()));
                quotes.push(snapshot);
            }
        }

        Ok(VenueMarketSnapshot {
            venue: Venue::Binance,
            observed_at_ms: now_ms().max(observed_at_ms),
            symbols: quotes,
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        let meta = self.symbol_meta(&request.symbol).await?;
        let position_mode = self.position_mode().await?;
        let quantity = floor_to_step(request.quantity, meta.step_size);
        if quantity < meta.min_qty {
            return Err(anyhow!(
                "binance order quantity {} below min qty {}",
                quantity,
                meta.min_qty
            ));
        }

        let timestamp = self.server_timestamp_ms().await?.to_string();
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
            ("quantity", format_decimal(quantity, meta.step_size)),
            ("newClientOrderId", request.client_order_id.clone()),
            ("newOrderRespType", "RESULT".to_string()),
            ("recvWindow", "5000".to_string()),
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
        let response = self
            .signed_request(
                reqwest::Method::POST,
                "/fapi/v1/order",
                params,
                None,
                &self.base_url,
            )
            .await?
            .json::<BinanceOrderResponse>()
            .await
            .context("failed to decode binance order response")?;

        let (fallback_price, fallback_ts) = if let Some(fill) = hinted_fill(&request) {
            fill
        } else {
            let snapshot = self
                .fetch_market_snapshot(&[request.symbol.clone()])
                .await?;
            quote_fill(&snapshot, &request.symbol, request.side)?
        };
        let average_price = parse_f64(&response.avg_price).unwrap_or(0.0);
        let average_price = if average_price > 0.0 {
            average_price
        } else {
            fallback_price
        };
        let executed_qty = parse_f64(&response.executed_qty).unwrap_or(quantity);
        let order_id = response.order_id.to_string();

        let mut fill = OrderFill {
            venue: Venue::Binance,
            symbol: request.symbol,
            side: request.side,
            quantity: executed_qty,
            average_price,
            fee_quote: estimate_fee_quote(average_price, executed_qty, self.config.taker_fee_bps),
            order_id: order_id.clone(),
            filled_at_ms: response.update_time.unwrap_or(fallback_ts),
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
            ("recvWindow", "5000".to_string()),
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

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        let meta = self.symbol_meta(symbol).await?;
        let quantity = floor_to_step(quantity, meta.step_size);
        if quantity < meta.min_qty {
            return Ok(0.0);
        }
        Ok(quantity)
    }

    async fn fetch_transfer_statuses(&self, assets: &[String]) -> Result<Vec<AssetTransferStatus>> {
        if self.wallet_base_url.contains("testnet") {
            return Ok(Vec::new());
        }

        let params = vec![
            ("recvWindow", "5000".to_string()),
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
        let wanted = assets
            .iter()
            .map(|asset| base_asset(asset))
            .collect::<std::collections::BTreeSet<_>>();
        let observed_at_ms = now_ms();

        Ok(coins
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
            .collect())
    }

    async fn shutdown(&self) -> Result<()> {
        self.market_ws.abort_worker();
        self.private_ws.abort_workers();
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct BinanceSymbolMeta {
    min_qty: f64,
    step_size: f64,
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
    filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
struct BinanceFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "minQty", default)]
    min_qty: Option<String>,
    #[serde(rename = "stepSize", default)]
    step_size: Option<String>,
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

#[derive(Debug, Deserialize)]
struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
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

#[cfg(test)]
mod tests {
    use super::{binance_position_side, BinancePositionMode, BinancePremiumIndex};
    use crate::models::Side;

    #[test]
    fn premium_index_accepts_integer_next_funding_time() {
        let payload = r#"{
            "lastFundingRate":"0.0001",
            "nextFundingTime":1773964800000
        }"#;
        let decoded: BinancePremiumIndex = serde_json::from_str(payload).expect("decode");
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
}

#[derive(Debug, Deserialize)]
struct BinancePositionRisk {
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "positionSide")]
    position_side: Option<String>,
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
