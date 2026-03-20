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
use serde::Deserialize;
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};

use crate::{
    config::VenueConfig,
    models::{
        AssetTransferStatus, OrderFill, OrderRequest, PositionSnapshot, Side, SymbolMarketSnapshot,
        Venue, VenueMarketSnapshot,
    },
    venue::VenueAdapter,
};

use super::{
    base_asset, build_http_client, build_query, enrich_fill_from_private, estimate_fee_quote,
    floor_to_step, format_decimal, hinted_fill, hmac_sha256_hex, lookup_or_wait_private_order,
    now_ms, parse_bool_flag, parse_f64, parse_i64, parse_text_message, quote_fill, spawn_ws_loop,
    venue_symbol, PrivateOrderUpdate, WsMarketState, WsPrivateState,
};

pub struct BybitLiveAdapter {
    config: VenueConfig,
    client: Client,
    base_url: String,
    metadata: Mutex<HashMap<String, BybitInstrumentMeta>>,
    time_offset_ms: Mutex<Option<i64>>,
    market_ws: Arc<WsMarketState>,
    private_ws: Arc<WsPrivateState>,
}

impl BybitLiveAdapter {
    pub fn new(config: &VenueConfig, timeout_ms: u64, symbols: &[String]) -> Result<Self> {
        if config.venue != Venue::Bybit {
            return Err(anyhow!("bybit live adapter requires bybit config"));
        }

        let market_ws = WsMarketState::new();
        let adapter = Self {
            config: config.clone(),
            client: build_http_client(timeout_ms)?,
            base_url: config
                .live
                .base_url
                .clone()
                .unwrap_or_else(|| "https://api.bybit.com".to_string()),
            metadata: Mutex::new(HashMap::new()),
            time_offset_ms: Mutex::new(None),
            market_ws,
            private_ws: WsPrivateState::new(),
        };
        adapter.start_market_ws(symbols);
        adapter.start_private_ws(symbols);
        Ok(adapter)
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
        let subscribe = serde_json::json!({
            "op": "subscribe",
            "args": topics,
        })
        .to_string();
        let state = self.market_ws.clone();
        spawn_ws_loop(
            "bybit",
            bybit_public_ws_url(&self.base_url).to_string(),
            vec![subscribe],
            state,
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
        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let url = bybit_private_ws_url(&self.base_url).to_string();
        let task = tokio::spawn(async move {
            loop {
                match connect_async(url.as_str()).await {
                    Ok((mut socket, _)) => {
                        let expires = now_ms() + 10_000;
                        let signature = match hmac_sha256_hex(
                            api_secret.as_str(),
                            &format!("GET/realtime{expires}"),
                        ) {
                            Ok(signature) => signature,
                            Err(error) => {
                                warn!(?error, "bybit private websocket auth sign failed");
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                        };
                        let auth = serde_json::json!({
                            "op": "auth",
                            "args": [api_key, expires, signature],
                        })
                        .to_string();
                        if let Err(error) = socket.send(Message::Text(auth.into())).await {
                            warn!(?error, "bybit private websocket auth send failed");
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }

                        let mut subscribed = false;
                        let mut ping_interval = interval(Duration::from_secs(20));
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        loop {
                            tokio::select! {
                                _ = ping_interval.tick() => {
                                    if socket
                                        .send(Message::Text(serde_json::json!({ "op": "ping" }).to_string().into()))
                                        .await
                                        .is_err()
                                    {
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
                                            if socket.send(Message::Pong(payload)).await.is_err() {
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Close(frame))) => {
                                            debug!(?frame, "bybit private websocket closed");
                                            break;
                                        }
                                        Some(Ok(_)) => {}
                                        Some(Err(error)) => {
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
                        warn!(?error, "bybit private websocket connect failed");
                    }
                }

                sleep(Duration::from_secs(1)).await;
            }
        });
        self.private_ws.push_worker(task);
    }

    async fn symbol_meta(&self, symbol: &str) -> Result<BybitInstrumentMeta> {
        if let Some(meta) = self.metadata.lock().expect("lock").get(symbol).cloned() {
            return Ok(meta);
        }

        let venue_symbol = venue_symbol(&self.config, symbol);
        let response = self
            .client
            .get(format!("{}/v5/market/instruments-info", self.base_url))
            .query(&[("category", "linear"), ("symbol", venue_symbol.as_str())])
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
        let instrument = response
            .result
            .and_then(|result| result.list.into_iter().next())
            .with_context(|| format!("bybit instrument metadata missing for {venue_symbol}"))?;
        let meta = BybitInstrumentMeta {
            qty_step: parse_f64(&instrument.lot_size_filter.qty_step)?,
        };
        self.metadata
            .lock()
            .expect("lock")
            .insert(symbol.to_string(), meta.clone());
        Ok(meta)
    }

    async fn fetch_symbol_snapshot(&self, symbol: &str) -> Result<SymbolMarketSnapshot> {
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
        for symbol in symbols {
            if let Some((snapshot, snapshot_observed_at_ms)) = self.cached_snapshot(symbol) {
                observed_at_ms = observed_at_ms.max(snapshot_observed_at_ms);
                quotes.push(snapshot);
            } else {
                let snapshot = self.fetch_symbol_snapshot(symbol).await?;
                observed_at_ms = observed_at_ms.max(snapshot.funding_timestamp_ms.min(now_ms()));
                quotes.push(snapshot);
            }
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
            .position(symbol)
            .map(|position| PositionSnapshot {
                venue: Venue::Bybit,
                symbol: symbol.to_string(),
                size: position.size,
                updated_at_ms: position.updated_at_ms,
            })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<PositionSnapshot> {
        if let Some(position) = self.private_ws.position(symbol) {
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
        let response = self
            .signed_request(reqwest::Method::GET, "/v5/position/list", Some(query), None)
            .await?
            .json::<BybitApiResponse<BybitPositionList>>()
            .await
            .context("failed to decode bybit positions")?;
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
            .collect::<std::collections::BTreeSet<_>>();
        let observed_at_ms = now_ms();
        let mut statuses = Vec::new();

        for asset in wanted {
            let query = build_query(&[("coin", asset.clone())]);
            let response = self
                .signed_request(
                    reqwest::Method::GET,
                    "/v5/asset/coin/query-info",
                    Some(query),
                    None,
                )
                .await?
                .json::<BybitApiResponse<BybitCoinInfoResult>>()
                .await
                .context("failed to decode bybit coin info")?;
            if response.ret_code != 0 {
                return Err(anyhow!("bybit coin info failed: {}", response.ret_msg));
            }

            let mut deposit_enabled = false;
            let mut withdraw_enabled = false;
            if let Some(result) = response.result {
                for row in result.rows {
                    if base_asset(&row.coin) != asset {
                        continue;
                    }
                    deposit_enabled |= row
                        .chains
                        .iter()
                        .any(|chain| parse_bool_flag(&chain.chain_deposit));
                    withdraw_enabled |= row
                        .chains
                        .iter()
                        .any(|chain| parse_bool_flag(&chain.chain_withdraw));
                }
            }

            statuses.push(AssetTransferStatus {
                venue: Venue::Bybit,
                asset,
                deposit_enabled,
                withdraw_enabled,
                observed_at_ms,
                source: "bybit".to_string(),
            });
        }

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

#[derive(Clone, Debug)]
struct BybitInstrumentMeta {
    qty_step: f64,
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

#[derive(Debug, Deserialize)]
struct BybitInstrumentList {
    #[serde(default)]
    list: Vec<BybitInstrument>,
}

#[derive(Debug, Deserialize)]
struct BybitInstrument {
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
