use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use ethers::{
    contract::{Eip712, EthAbiType},
    core::k256::ecdsa::signature::digest::{
        FixedOutput, FixedOutputReset, HashMarker, Reset, Update,
    },
    prelude::k256::{
        elliptic_curve::{generic_array::GenericArray, FieldBytes},
        sha2::{
            self,
            digest::{Output, OutputSizeUser},
            Digest,
        },
        Secp256k1,
    },
    signers::{LocalWallet, Signer},
    types::transaction::eip712::Eip712 as _,
    types::{Signature, H160, H256, U256},
};
use futures::{SinkExt, StreamExt};
use hyperliquid_rust_sdk::{
    BaseUrl, ExchangeClient, ExchangeDataStatus, ExchangeResponseStatus, InfoClient,
    Message as HyperliquidMessage, Subscription, UserData,
};
use serde_json::json;
use tokio::sync::mpsc::unbounded_channel;
use tokio::{net::TcpStream, sync::Mutex as AsyncMutex, time::Instant as TokioInstant};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        OrderExecutionTiming, OrderFill, OrderRequest, PositionSnapshot, Side,
        SymbolMarketSnapshot, Venue, VenueMarketSnapshot,
    },
    venue::VenueAdapter,
};

use super::{
    enrich_fill_from_private, estimate_fee_quote, hinted_fill, lookup_or_wait_private_order,
    now_ms, parse_f64, quote_fill, venue_symbol, PrivateOrderUpdate, WsMarketState, WsPrivateState,
};

pub struct HyperliquidLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    info_client: InfoClient,
    exchange_client: ExchangeClient,
    account_address: H160,
    meta_cache: Mutex<HashMap<String, HyperliquidAssetMeta>>,
    market_ws: Arc<WsMarketState>,
    private_ws: Arc<WsPrivateState>,
    order_ws: HyperliquidWsPostClient,
}

impl HyperliquidLiveAdapter {
    pub async fn new(
        config: &VenueConfig,
        runtime: &RuntimeConfig,
        symbols: &[String],
    ) -> Result<Self> {
        if config.venue != Venue::Hyperliquid {
            return Err(anyhow!(
                "hyperliquid live adapter requires hyperliquid config"
            ));
        }

        let wallet: LocalWallet = config
            .live
            .resolved_wallet_private_key()
            .ok_or_else(|| anyhow!("hyperliquid wallet private key is not configured"))?
            .parse()
            .context("failed to parse hyperliquid private key")?;
        let account_address = config
            .live
            .resolved_account_address()
            .map(|value| value.parse::<H160>())
            .transpose()
            .context("failed to parse hyperliquid account address")?
            .unwrap_or_else(|| wallet.address());
        let base_url = if config.live.is_testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };
        let info_client = InfoClient::new(None, Some(base_url))
            .await
            .context("failed to build hyperliquid info client")?;
        let exchange_client = ExchangeClient::new(None, wallet, Some(base_url), None, None)
            .await
            .context("failed to build hyperliquid exchange client")?;

        let market_ws = WsMarketState::new();
        let adapter = Self {
            config: config.clone(),
            runtime: runtime.clone(),
            info_client,
            exchange_client,
            account_address,
            meta_cache: Mutex::new(HashMap::new()),
            market_ws,
            private_ws: WsPrivateState::new(),
            order_ws: HyperliquidWsPostClient::new(hyperliquid_ws_url(base_url)),
        };
        adapter.start_market_ws(symbols, base_url).await?;
        adapter.start_private_ws(symbols, base_url).await?;
        Ok(adapter)
    }

    async fn start_market_ws(&self, symbols: &[String], base_url: BaseUrl) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let (sender, mut receiver) = unbounded_channel();
        let mut info_client = InfoClient::new(None, Some(base_url))
            .await
            .context("failed to build hyperliquid market websocket client")?;
        for symbol in symbols {
            let asset = venue_symbol(&self.config, symbol);
            info_client
                .subscribe(
                    Subscription::L2Book {
                        coin: asset.clone(),
                    },
                    sender.clone(),
                )
                .await
                .with_context(|| format!("failed to subscribe hyperliquid l2Book for {asset}"))?;
            info_client
                .subscribe(
                    Subscription::ActiveAssetCtx {
                        coin: asset.clone(),
                    },
                    sender.clone(),
                )
                .await
                .with_context(|| {
                    format!("failed to subscribe hyperliquid activeAssetCtx for {asset}")
                })?;
        }

        let cache = self.market_ws.clone();
        let task = tokio::spawn(async move {
            let _info_client = info_client;
            while let Some(message) = receiver.recv().await {
                match message {
                    HyperliquidMessage::L2Book(book) => {
                        let Some(symbol) = symbol_map.get(&book.data.coin) else {
                            continue;
                        };
                        let Some(best_bid) =
                            book.data.levels.first().and_then(|levels| levels.first())
                        else {
                            continue;
                        };
                        let Some(best_ask) =
                            book.data.levels.get(1).and_then(|levels| levels.first())
                        else {
                            continue;
                        };
                        let best_bid_px = parse_f64(&best_bid.px).ok();
                        let best_ask_px = parse_f64(&best_ask.px).ok();
                        let bid_sz = parse_f64(&best_bid.sz).ok();
                        let ask_sz = parse_f64(&best_ask.sz).ok();
                        if let (Some(best_bid_px), Some(best_ask_px), Some(bid_sz), Some(ask_sz)) =
                            (best_bid_px, best_ask_px, bid_sz, ask_sz)
                        {
                            cache.update_quote(
                                symbol,
                                best_bid_px,
                                best_ask_px,
                                bid_sz,
                                ask_sz,
                                book.data.time as i64,
                            );
                        }
                    }
                    HyperliquidMessage::ActiveAssetCtx(ctx) => {
                        let Some(symbol) = symbol_map.get(&ctx.data.coin) else {
                            continue;
                        };
                        let funding_rate = match ctx.data.ctx {
                            hyperliquid_rust_sdk::AssetCtx::Perps(ref perps) => {
                                parse_f64(&perps.funding).ok()
                            }
                            hyperliquid_rust_sdk::AssetCtx::Spot(_) => None,
                        };
                        if let Some(funding_rate) = funding_rate {
                            cache.update_funding(
                                symbol,
                                funding_rate,
                                next_hour_boundary(now_ms()),
                            );
                        }
                    }
                    _ => {}
                }
            }
        });
        self.market_ws.set_worker(task);
        Ok(())
    }

    async fn start_private_ws(&self, symbols: &[String], base_url: BaseUrl) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let (sender, mut receiver) = unbounded_channel();
        let mut info_client = InfoClient::new(None, Some(base_url))
            .await
            .context("failed to build hyperliquid private websocket client")?;
        if let Ok(user_state) = info_client.user_state(self.account_address).await {
            for position in user_state.asset_positions {
                if let Some(symbol) = symbol_map.get(&position.position.coin) {
                    if let Ok(size) = parse_f64(&position.position.szi) {
                        self.private_ws.update_position(symbol, size, now_ms());
                    }
                }
            }
        }
        info_client
            .subscribe(
                Subscription::UserEvents {
                    user: self.account_address,
                },
                sender.clone(),
            )
            .await
            .context("failed to subscribe hyperliquid user events")?;
        info_client
            .subscribe(
                Subscription::OrderUpdates {
                    user: self.account_address,
                },
                sender.clone(),
            )
            .await
            .context("failed to subscribe hyperliquid order updates")?;

        let private_state = self.private_ws.clone();
        let task = tokio::spawn(async move {
            let _info_client = info_client;
            while let Some(message) = receiver.recv().await {
                match message {
                    HyperliquidMessage::User(user_event) => {
                        if let UserData::Fills(fills) = user_event.data {
                            for fill in fills {
                                let Some(symbol) = symbol_map.get(&fill.coin) else {
                                    continue;
                                };
                                let quantity = match parse_f64(&fill.sz) {
                                    Ok(quantity) => quantity,
                                    Err(_) => continue,
                                };
                                let average_price = parse_f64(&fill.px).ok();
                                let fee_quote = match fill.fee_token.to_ascii_uppercase().as_str() {
                                    "USDC" | "USDT" => parse_f64(&fill.fee).ok(),
                                    _ => None,
                                };
                                private_state.record_order(PrivateOrderUpdate {
                                    symbol: symbol.clone(),
                                    order_id: fill.oid.to_string(),
                                    client_order_id: fill.cloid.clone(),
                                    filled_quantity: Some(quantity),
                                    average_price,
                                    fee_quote,
                                    updated_at_ms: fill.time as i64,
                                });
                                let signed_quantity = hyperliquid_side_sign(&fill.side) * quantity;
                                let current_size = private_state
                                    .position(symbol)
                                    .map(|position| position.size)
                                    .unwrap_or_default();
                                private_state.update_position(
                                    symbol,
                                    current_size + signed_quantity,
                                    fill.time as i64,
                                );
                            }
                        }
                    }
                    HyperliquidMessage::OrderUpdates(order_updates) => {
                        for update in order_updates.data {
                            let Some(symbol) = symbol_map.get(&update.order.coin) else {
                                continue;
                            };
                            private_state.record_order(PrivateOrderUpdate {
                                symbol: symbol.clone(),
                                order_id: update.order.oid.to_string(),
                                client_order_id: update.order.cloid.clone(),
                                filled_quantity: None,
                                average_price: None,
                                fee_quote: None,
                                updated_at_ms: update.status_timestamp as i64,
                            });
                        }
                    }
                    _ => {}
                }
            }
        });
        self.private_ws.push_worker(task);
        Ok(())
    }

    fn cached_snapshot(&self, symbol: &str) -> Option<(SymbolMarketSnapshot, i64)> {
        let snapshot = self.market_ws.snapshot(symbol)?;
        let observed_at_ms = self.market_ws.quote(symbol)?.observed_at_ms;
        Some((snapshot, observed_at_ms))
    }

    async fn fetch_symbol_snapshot(&self, symbol: &str) -> Result<SymbolMarketSnapshot> {
        let asset = venue_symbol(&self.config, symbol);
        let book = self
            .info_client
            .l2_snapshot(asset.clone())
            .await
            .context("failed to request hyperliquid l2 book")?;
        let funding_history = self
            .info_client
            .funding_history(
                asset.clone(),
                (now_ms().saturating_sub(2 * 60 * 60 * 1_000)) as u64,
                None,
            )
            .await
            .context("failed to request hyperliquid funding history")?;
        let funding_rate = funding_history
            .last()
            .map(|row| parse_f64(&row.funding_rate))
            .transpose()?
            .unwrap_or_default();
        let best_bid = book
            .levels
            .first()
            .and_then(|levels| levels.first())
            .ok_or_else(|| anyhow!("hyperliquid bid book missing for {asset}"))?;
        let best_ask = book
            .levels
            .get(1)
            .and_then(|levels| levels.first())
            .ok_or_else(|| anyhow!("hyperliquid ask book missing for {asset}"))?;

        Ok(SymbolMarketSnapshot {
            symbol: symbol.to_string(),
            best_bid: parse_f64(&best_bid.px)?,
            best_ask: parse_f64(&best_ask.px)?,
            bid_size: parse_f64(&best_bid.sz)?,
            ask_size: parse_f64(&best_ask.sz)?,
            funding_rate,
            funding_timestamp_ms: next_hour_boundary(book.time as i64),
        })
    }

    fn decode_response(
        &self,
        response: ExchangeResponseStatus,
        fallback_price: f64,
        fallback_qty: f64,
    ) -> Result<HyperliquidOrderOutcome> {
        match response {
            ExchangeResponseStatus::Err(error) => Err(anyhow!("hyperliquid order failed: {error}")),
            ExchangeResponseStatus::Ok(response) => {
                let status = response
                    .data
                    .and_then(|data| data.statuses.into_iter().next())
                    .ok_or_else(|| anyhow!("hyperliquid order response missing status"))?;
                match status {
                    ExchangeDataStatus::Filled(order) => Ok(HyperliquidOrderOutcome::Resolved(
                        order.oid.to_string(),
                        parse_f64(&order.avg_px).unwrap_or(fallback_price),
                        parse_f64(&order.total_sz).unwrap_or(fallback_qty),
                    )),
                    ExchangeDataStatus::Resting(order) => Ok(HyperliquidOrderOutcome::Resolved(
                        order.oid.to_string(),
                        fallback_price,
                        fallback_qty,
                    )),
                    ExchangeDataStatus::Success
                    | ExchangeDataStatus::WaitingForFill
                    | ExchangeDataStatus::WaitingForTrigger => Ok(HyperliquidOrderOutcome::Pending),
                    ExchangeDataStatus::Error(error) => {
                        Err(anyhow!("hyperliquid order error: {error}"))
                    }
                }
            }
        }
    }

    async fn asset_meta(&self, asset: &str) -> Result<HyperliquidAssetMeta> {
        if let Some(meta) = self.meta_cache.lock().expect("lock").get(asset).cloned() {
            return Ok(meta);
        }

        let meta = self
            .info_client
            .meta()
            .await
            .context("failed to request hyperliquid meta")?;
        let cache = meta
            .universe
            .into_iter()
            .map(|row| {
                (
                    row.name.clone(),
                    HyperliquidAssetMeta {
                        sz_decimals: row.sz_decimals,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let asset_meta = cache
            .get(asset)
            .cloned()
            .ok_or_else(|| anyhow!("hyperliquid asset metadata missing for {asset}"))?;
        self.meta_cache.lock().expect("lock").extend(cache);
        Ok(asset_meta)
    }

    async fn ioc_order_params(
        &self,
        asset: &str,
        side: Side,
        quantity: f64,
        fallback_price: f64,
        reference_price_hint: Option<f64>,
    ) -> Result<(f64, f64)> {
        let asset_meta = self.asset_meta(asset).await?;
        let sz_decimals = asset_meta.sz_decimals;
        let asset_index = self
            .exchange_client
            .coin_to_asset
            .get(asset)
            .copied()
            .ok_or_else(|| anyhow!("hyperliquid asset index missing for {asset}"))?;
        let max_decimals: u32 = if asset_index < 10_000 { 6 } else { 8 };
        let price_decimals = max_decimals.saturating_sub(sz_decimals);
        Ok(ioc_order_params_from_reference_price(
            side,
            quantity,
            sz_decimals,
            price_decimals,
            reference_price_hint,
            fallback_price,
        ))
    }

    async fn submit_ws_order(
        &self,
        asset: &str,
        is_buy: bool,
        reduce_only: bool,
        limit_px: f64,
        quantity: f64,
        cloid: Option<String>,
    ) -> Result<ExchangeResponseStatus> {
        let asset_index = self
            .exchange_client
            .coin_to_asset
            .get(asset)
            .copied()
            .ok_or_else(|| anyhow!("hyperliquid asset index missing for {asset}"))?;
        let action = HyperliquidAction::Order(HyperliquidBulkOrder {
            orders: vec![HyperliquidWireOrderRequest {
                asset: asset_index,
                is_buy,
                limit_px: float_to_wire_string(limit_px),
                sz: float_to_wire_string(quantity),
                reduce_only,
                order_type: HyperliquidWireOrderType::Limit(HyperliquidWireLimit {
                    tif: "Ioc".to_string(),
                }),
                cloid,
            }],
            grouping: "na".to_string(),
            builder: None,
        });
        let nonce = next_hyperliquid_nonce();
        let connection_id =
            hyperliquid_action_connection_id(&action, nonce, self.exchange_client.vault_address)?;
        let signature = sign_hyperliquid_l1_action(
            &self.exchange_client.wallet,
            connection_id,
            !self.config.live.is_testnet,
        )?;

        self.order_ws
            .post_action(
                serde_json::to_value(action).context("failed to serialize hyperliquid action")?,
                nonce,
                signature,
                self.exchange_client.vault_address,
            )
            .await
    }
}

#[async_trait]
impl VenueAdapter for HyperliquidLiveAdapter {
    fn venue(&self) -> Venue {
        Venue::Hyperliquid
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut quotes = Vec::new();
        let mut observed_at_ms = 0_i64;
        for symbol in symbols {
            if let Some((snapshot, snapshot_observed_at_ms)) = self.cached_snapshot(symbol) {
                observed_at_ms = observed_at_ms.max(snapshot_observed_at_ms);
                quotes.push(snapshot);
            } else {
                let quote = self.fetch_symbol_snapshot(symbol).await?;
                observed_at_ms = observed_at_ms.max(quote.funding_timestamp_ms.min(now_ms()));
                quotes.push(quote);
            }
        }

        Ok(VenueMarketSnapshot {
            venue: Venue::Hyperliquid,
            observed_at_ms: now_ms().max(observed_at_ms),
            symbols: quotes,
        })
    }

    async fn place_order(&self, request: OrderRequest) -> Result<OrderFill> {
        let asset = venue_symbol(&self.config, &request.symbol);
        let quote_resolve_started_at = Instant::now();
        let (fallback_price, filled_at_ms) = if let Some(fill) = hinted_fill(&request) {
            fill
        } else {
            let snapshot = self
                .fetch_market_snapshot(&[request.symbol.clone()])
                .await?;
            quote_fill(&snapshot, &request.symbol, request.side)?
        };
        let quote_resolve_ms = elapsed_ms(quote_resolve_started_at);
        let order_prepare_started_at = Instant::now();
        let (limit_px, quantity) = self
            .ioc_order_params(
                &asset,
                request.side,
                request.quantity,
                fallback_price,
                request.price_hint,
            )
            .await?;
        let order_prepare_ms = elapsed_ms(order_prepare_started_at);
        let wire_cloid = hyperliquid_cloid_for_client_order(&request.client_order_id);
        let submit_started_at = Instant::now();
        let response = self
            .submit_ws_order(
                &asset,
                request.side == Side::Buy,
                request.reduce_only,
                limit_px,
                quantity,
                Some(wire_cloid.clone()),
            )
            .await
            .with_context(|| {
                if request.reduce_only {
                    "failed to submit hyperliquid reduce-only order"
                } else {
                    "failed to submit hyperliquid open order"
                }
            })?;
        let submit_ack_ms = elapsed_ms(submit_started_at);
        let timing = Some(OrderExecutionTiming {
            quote_resolve_ms: Some(quote_resolve_ms),
            order_prepare_ms: Some(order_prepare_ms),
            submit_ack_ms: Some(submit_ack_ms),
        });
        let mut fill = OrderFill {
            venue: Venue::Hyperliquid,
            symbol: request.symbol.clone(),
            side: request.side,
            quantity,
            average_price: fallback_price,
            fee_quote: estimate_fee_quote(fallback_price, quantity, self.config.taker_fee_bps),
            order_id: "hyperliquid-pending".to_string(),
            filled_at_ms,
            timing,
        };
        match self.decode_response(response, fallback_price, request.quantity)? {
            HyperliquidOrderOutcome::Resolved(order_id, average_price, quantity) => {
                fill.order_id = order_id.clone();
                fill.average_price = average_price;
                fill.quantity = quantity;
                fill.fee_quote =
                    estimate_fee_quote(average_price, quantity, self.config.taker_fee_bps);
                if let Some(private_fill) = lookup_or_wait_private_order(
                    &self.private_ws,
                    Some(wire_cloid.as_str()),
                    Some(order_id.as_str()),
                    self.config.live.post_ack_private_fill_wait_ms,
                )
                .await
                {
                    fill = enrich_fill_from_private(fill, &private_fill);
                }
                Ok(fill)
            }
            HyperliquidOrderOutcome::Pending => {
                if let Some(private_fill) = lookup_or_wait_private_order(
                    &self.private_ws,
                    Some(wire_cloid.as_str()),
                    None,
                    self.config.live.post_ack_private_fill_wait_ms,
                )
                .await
                {
                    return Ok(enrich_fill_from_private(fill, &private_fill));
                }
                Err(anyhow!(
                    "hyperliquid order status uncertain after pending ack"
                ))
            }
        }
    }

    fn cached_position(&self, symbol: &str) -> Option<PositionSnapshot> {
        self.private_ws
            .position_if_fresh(symbol, self.runtime.private_position_max_age_ms, now_ms())
            .map(|position| PositionSnapshot {
                venue: Venue::Hyperliquid,
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
                venue: Venue::Hyperliquid,
                symbol: symbol.to_string(),
                size: position.size,
                updated_at_ms: position.updated_at_ms,
            });
        }

        let asset = venue_symbol(&self.config, symbol);
        let user_state = self
            .info_client
            .user_state(self.account_address)
            .await
            .context("failed to query hyperliquid user state")?;
        let size = user_state
            .asset_positions
            .iter()
            .find(|position| position.position.coin == asset)
            .map(|position| parse_f64(&position.position.szi))
            .transpose()?
            .unwrap_or_default();

        Ok(PositionSnapshot {
            venue: Venue::Hyperliquid,
            symbol: symbol.to_string(),
            size,
            updated_at_ms: now_ms(),
        })
    }

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        let asset = venue_symbol(&self.config, symbol);
        let asset_meta = self.asset_meta(&asset).await?;
        Ok(round_to_decimals(quantity, asset_meta.sz_decimals))
    }

    async fn shutdown(&self) -> Result<()> {
        self.market_ws.abort_worker();
        self.private_ws.abort_workers();
        self.order_ws.shutdown().await;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct HyperliquidAssetMeta {
    sz_decimals: u32,
}

enum HyperliquidOrderOutcome {
    Resolved(String, f64, f64),
    Pending,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
enum HyperliquidAction {
    Order(HyperliquidBulkOrder),
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidBulkOrder {
    orders: Vec<HyperliquidWireOrderRequest>,
    grouping: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    builder: Option<serde_json::Value>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct HyperliquidWireOrderRequest {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    limit_px: String,
    #[serde(rename = "s")]
    sz: String,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: HyperliquidWireOrderType,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    cloid: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
enum HyperliquidWireOrderType {
    Limit(HyperliquidWireLimit),
}

#[derive(Clone, Debug, serde::Serialize)]
struct HyperliquidWireLimit {
    tif: String,
}

#[derive(Debug, Eip712, Clone, EthAbiType)]
#[eip712(
    name = "Exchange",
    version = "1",
    chain_id = 1337,
    verifying_contract = "0x0000000000000000000000000000000000000000"
)]
struct Agent {
    source: String,
    connection_id: H256,
}

struct HyperliquidWsPostClient {
    url: String,
    state: AsyncMutex<HyperliquidWsPostState>,
    next_request_id: AtomicU64,
}

struct HyperliquidWsPostState {
    socket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    last_used_at: Option<TokioInstant>,
}

impl HyperliquidWsPostClient {
    fn new(url: String) -> Self {
        Self {
            url,
            state: AsyncMutex::new(HyperliquidWsPostState {
                socket: None,
                last_used_at: None,
            }),
            next_request_id: AtomicU64::new(1),
        }
    }

    async fn post_action(
        &self,
        action: serde_json::Value,
        nonce: u64,
        signature: Signature,
        vault_address: Option<H160>,
    ) -> Result<ExchangeResponseStatus> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let payload = encode_ws_post_order_request(
            request_id,
            nonce,
            action,
            serde_json::to_value(signature).context("failed to serialize hyperliquid signature")?,
            vault_address.map(|value| format!("{value:#x}")),
        );
        let request_text =
            serde_json::to_string(&payload).context("failed to encode hyperliquid ws request")?;

        for attempt in 0..2 {
            let mut state = self.state.lock().await;
            if should_refresh_ws_connection(state.last_used_at) {
                state.socket = None;
                state.last_used_at = None;
            }
            if state.socket.is_none() {
                state.socket = Some(connect_hyperliquid_ws(&self.url).await?);
            }

            let send_result = async {
                let socket = state
                    .socket
                    .as_mut()
                    .ok_or_else(|| anyhow!("hyperliquid ws socket unavailable"))?;
                socket
                    .send(Message::Text(request_text.clone().into()))
                    .await
                    .context("failed to send hyperliquid ws post request")?;
                read_hyperliquid_ws_post_response(socket, request_id).await
            }
            .await;

            match send_result {
                Ok(response) => {
                    state.last_used_at = Some(TokioInstant::now());
                    return Ok(response);
                }
                Err(_error) if attempt == 0 => {
                    state.socket = None;
                    state.last_used_at = None;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        Err(anyhow!("hyperliquid ws post request exhausted retries"))
    }

    async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        if let Some(mut socket) = state.socket.take() {
            let _ = socket.close(None).await;
        }
        state.last_used_at = None;
    }
}

fn hyperliquid_ws_url(base_url: BaseUrl) -> String {
    match base_url {
        BaseUrl::Mainnet => "wss://api.hyperliquid.xyz/ws".to_string(),
        BaseUrl::Testnet => "wss://api.hyperliquid-testnet.xyz/ws".to_string(),
        BaseUrl::Localhost => "ws://localhost:3001/ws".to_string(),
    }
}

async fn connect_hyperliquid_ws(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    Ok(connect_async(url)
        .await
        .with_context(|| format!("failed to connect hyperliquid ws post {url}"))?
        .0)
}

async fn read_hyperliquid_ws_post_response(
    socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    request_id: u64,
) -> Result<ExchangeResponseStatus> {
    while let Some(message) = socket.next().await {
        match message.context("failed to read hyperliquid ws post response")? {
            Message::Text(text) => {
                let payload: serde_json::Value =
                    serde_json::from_str(&text).context("failed to decode hyperliquid ws json")?;
                if payload
                    .get("channel")
                    .and_then(|value| value.as_str())
                    .is_some_and(|channel| channel == "post")
                {
                    let response_id = payload
                        .get("data")
                        .and_then(|value| value.get("id"))
                        .and_then(|value| value.as_u64());
                    if response_id == Some(request_id) {
                        return decode_ws_post_action_response(request_id, &payload);
                    }
                }
            }
            Message::Ping(frame) => {
                socket
                    .send(Message::Pong(frame))
                    .await
                    .context("failed to respond to hyperliquid ws ping")?;
            }
            Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            Message::Close(frame) => {
                return Err(anyhow!(
                    "hyperliquid ws closed before post response: {frame:?}"
                ));
            }
        }
    }

    Err(anyhow!(
        "hyperliquid ws stream ended before post response for request id {request_id}"
    ))
}

fn should_refresh_ws_connection(last_used_at: Option<TokioInstant>) -> bool {
    last_used_at
        .map(|instant| instant.elapsed().as_secs() >= 50)
        .unwrap_or(true)
}

fn encode_ws_post_order_request(
    request_id: u64,
    nonce: u64,
    action: serde_json::Value,
    signature: serde_json::Value,
    vault_address: Option<String>,
) -> serde_json::Value {
    json!({
        "method": "post",
        "id": request_id,
        "request": {
            "type": "action",
            "payload": {
                "action": action,
                "nonce": nonce,
                "signature": signature,
                "vaultAddress": vault_address,
            }
        }
    })
}

fn decode_ws_post_action_response(
    request_id: u64,
    response: &serde_json::Value,
) -> Result<ExchangeResponseStatus> {
    let response_id = response
        .get("data")
        .and_then(|value| value.get("id"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("hyperliquid ws post response missing request id"))?;
    if response_id != request_id {
        return Err(anyhow!(
            "hyperliquid ws post response request id mismatch: expected {request_id}, got {response_id}"
        ));
    }

    let response_type = response
        .get("data")
        .and_then(|value| value.get("response"))
        .and_then(|value| value.get("type"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("hyperliquid ws post response missing type"))?;
    let payload = response
        .get("data")
        .and_then(|value| value.get("response"))
        .and_then(|value| value.get("payload"))
        .ok_or_else(|| anyhow!("hyperliquid ws post response missing payload"))?;

    match response_type {
        "action" => serde_json::from_value(payload.clone())
            .context("failed to decode hyperliquid ws action payload"),
        "error" => Err(anyhow!(
            "hyperliquid ws post error: {}",
            payload.as_str().unwrap_or("unknown")
        )),
        other => Err(anyhow!(
            "unexpected hyperliquid ws post response type {other}"
        )),
    }
}

fn float_to_wire_string(value: f64) -> String {
    let mut value = format!("{value:.8}");
    while value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
    if value == "-0" {
        "0".to_string()
    } else {
        value
    }
}

fn hyperliquid_cloid_for_client_order(client_order_id: &str) -> String {
    let digest = sha2::Sha256::digest(client_order_id.as_bytes());
    format!("0x{}", hex::encode(&digest[..16]))
}

fn next_hyperliquid_nonce() -> u64 {
    static NEXT_NONCE: AtomicU64 = AtomicU64::new(0);

    loop {
        let current = NEXT_NONCE.load(Ordering::Relaxed);
        let now = now_ms().max(0) as u64;
        let next = current.max(now);
        if NEXT_NONCE
            .compare_exchange(
                current,
                next.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            return next;
        }
    }
}

fn hyperliquid_action_connection_id(
    action: &HyperliquidAction,
    nonce: u64,
    vault_address: Option<H160>,
) -> Result<H256> {
    let mut bytes =
        rmp_serde::to_vec_named(action).context("failed to msgpack hyperliquid action")?;
    bytes.extend(nonce.to_be_bytes());
    if let Some(vault_address) = vault_address {
        bytes.push(1);
        bytes.extend(vault_address.to_fixed_bytes());
    } else {
        bytes.push(0);
    }
    Ok(H256(ethers::utils::keccak256(bytes)))
}

#[derive(Clone)]
enum HyperliquidProxyDigest<D: Digest> {
    Proxy(Output<D>),
    Digest(D),
}

impl<D: Digest + Clone> From<H256> for HyperliquidProxyDigest<D>
where
    GenericArray<u8, <D as OutputSizeUser>::OutputSize>: Copy,
{
    fn from(src: H256) -> Self {
        Self::Proxy(*GenericArray::from_slice(src.as_bytes()))
    }
}

impl<D: Digest> Default for HyperliquidProxyDigest<D> {
    fn default() -> Self {
        Self::Digest(D::new())
    }
}

impl<D: Digest> Update for HyperliquidProxyDigest<D> {
    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Digest(digest) => digest.update(data),
            Self::Proxy(_) => unreachable!("proxy digest should not be updated"),
        }
    }
}

impl<D: Digest> HashMarker for HyperliquidProxyDigest<D> {}

impl<D: Digest> Reset for HyperliquidProxyDigest<D> {
    fn reset(&mut self) {
        *self = Self::default();
    }
}

impl<D: Digest> OutputSizeUser for HyperliquidProxyDigest<D> {
    type OutputSize = <D as OutputSizeUser>::OutputSize;
}

impl<D: Digest> FixedOutput for HyperliquidProxyDigest<D> {
    fn finalize_into(self, out: &mut GenericArray<u8, Self::OutputSize>) {
        match self {
            Self::Digest(digest) => *out = digest.finalize(),
            Self::Proxy(proxy) => *out = proxy,
        }
    }
}

impl<D: Digest> FixedOutputReset for HyperliquidProxyDigest<D> {
    fn finalize_into_reset(&mut self, out: &mut Output<Self>) {
        let digest = std::mem::take(self);
        Digest::finalize_into(digest, out)
    }
}

fn sign_hyperliquid_hash(hash: H256, wallet: &LocalWallet) -> Result<Signature> {
    let (signature, recovery_id) = wallet
        .signer()
        .sign_digest_recoverable(HyperliquidProxyDigest::<sha2::Sha256>::from(hash))
        .context("failed to sign hyperliquid digest")?;
    let v = u8::from(recovery_id) as u64 + 27;
    let r_bytes: FieldBytes<Secp256k1> = signature.r().into();
    let s_bytes: FieldBytes<Secp256k1> = signature.s().into();
    let r = U256::from_big_endian(r_bytes.as_slice());
    let s = U256::from_big_endian(s_bytes.as_slice());
    Ok(Signature { r, s, v })
}

fn sign_hyperliquid_l1_action(
    wallet: &LocalWallet,
    connection_id: H256,
    is_mainnet: bool,
) -> Result<Signature> {
    let digest = Agent {
        source: if is_mainnet { "a" } else { "b" }.to_string(),
        connection_id,
    }
    .encode_eip712()
    .context("failed to encode hyperliquid ws typed data")?;
    sign_hyperliquid_hash(H256::from(digest), wallet)
}

fn next_hour_boundary(now_ms: i64) -> i64 {
    let hour_ms = 60 * 60 * 1_000;
    ((now_ms / hour_ms) + 1) * hour_ms
}

fn round_to_decimals(value: f64, decimals: u32) -> f64 {
    let factor = 10f64.powi(decimals as i32);
    (value * factor).round() / factor
}

fn round_to_significant_and_decimal(value: f64, sig_figs: u32, max_decimals: u32) -> f64 {
    let abs_value = value.abs();
    if abs_value <= f64::EPSILON {
        return 0.0;
    }
    let magnitude = abs_value.log10().floor() as i32;
    let scale = 10f64.powi(sig_figs as i32 - magnitude - 1);
    let rounded = (abs_value * scale).round() / scale;
    round_to_decimals(rounded.copysign(value), max_decimals)
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn hyperliquid_side_sign(side: &str) -> f64 {
    let normalized = side.trim().to_ascii_lowercase();
    if normalized.starts_with('b') {
        1.0
    } else if normalized.starts_with('a') || normalized.starts_with('s') {
        -1.0
    } else {
        0.0
    }
}

fn ioc_order_params_from_reference_price(
    side: Side,
    quantity: f64,
    size_decimals: u32,
    price_decimals: u32,
    reference_price_hint: Option<f64>,
    fallback_price: f64,
) -> (f64, f64) {
    let reference_price = reference_price_hint
        .filter(|price| price.is_finite() && *price > 0.0)
        .unwrap_or(fallback_price);
    let slippage_factor = match side {
        Side::Buy => 1.01,
        Side::Sell => 0.99,
    };
    let limit_px =
        round_to_significant_and_decimal(reference_price * slippage_factor, 5, price_decimals);
    let quantity = round_to_decimals(quantity, size_decimals);
    (limit_px, quantity)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ethers::types::H256;
    use hyperliquid_rust_sdk::ExchangeResponseStatus;
    use serde_json::json;

    use crate::models::Side;

    use super::{
        decode_ws_post_action_response, encode_ws_post_order_request,
        hyperliquid_cloid_for_client_order, ioc_order_params_from_reference_price,
        sign_hyperliquid_l1_action,
    };

    #[test]
    fn ioc_order_params_prefers_valid_hint_for_buy_orders() {
        let (limit_px, quantity) =
            ioc_order_params_from_reference_price(Side::Buy, 0.011, 3, 4, Some(2142.89), 2138.0);
        assert_eq!(limit_px, 2164.3);
        assert_eq!(quantity, 0.011);
    }

    #[test]
    fn ioc_order_params_falls_back_when_hint_missing_or_invalid() {
        let (limit_px, quantity) =
            ioc_order_params_from_reference_price(Side::Sell, 0.011, 3, 4, Some(0.0), 2142.89);
        assert_eq!(limit_px, 2121.5);
        assert_eq!(quantity, 0.011);
    }

    #[test]
    fn ws_post_order_request_wraps_signed_action_payload() {
        let payload = encode_ws_post_order_request(
            256,
            1713825891591,
            json!({
                "type": "order",
                "orders": [{"a": 4, "b": true, "p": "1100", "s": "0.2", "r": false, "t": {"limit": {"tif": "Ioc"}}}],
                "grouping": "na",
            }),
            json!({
                "r": "0x1",
                "s": "0x2",
                "v": 27,
            }),
            Some("0x0000000000000000000000000000000000000001".to_string()),
        );

        assert_eq!(
            payload,
            json!({
                "method": "post",
                "id": 256,
                "request": {
                    "type": "action",
                    "payload": {
                        "action": {
                            "type": "order",
                            "orders": [{"a": 4, "b": true, "p": "1100", "s": "0.2", "r": false, "t": {"limit": {"tif": "Ioc"}}}],
                            "grouping": "na",
                        },
                        "nonce": 1713825891591_u64,
                        "signature": {
                            "r": "0x1",
                            "s": "0x2",
                            "v": 27,
                        },
                        "vaultAddress": "0x0000000000000000000000000000000000000001",
                    }
                }
            })
        );
    }

    #[test]
    fn ws_post_action_response_decodes_wrapped_exchange_status() {
        let response = json!({
            "channel": "post",
            "data": {
                "id": 256,
                "response": {
                    "type": "action",
                    "payload": {
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": {
                                "statuses": [{"filled": {"oid": 88383, "totalSz": "0.2", "avgPx": "1100"}}]
                            }
                        }
                    }
                }
            }
        });

        let decoded = decode_ws_post_action_response(256, &response).expect("decode response");
        assert!(matches!(decoded, ExchangeResponseStatus::Ok(_)));
    }

    #[test]
    fn ws_post_action_response_rejects_mismatched_request_id() {
        let response = json!({
            "channel": "post",
            "data": {
                "id": 99,
                "response": {
                    "type": "action",
                    "payload": {
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": {
                                "statuses": []
                            }
                        }
                    }
                }
            }
        });

        let error = decode_ws_post_action_response(256, &response).expect_err("mismatched id");
        let message = error.to_string();
        assert!(message.contains("request id"));
        assert!(message.contains("256"));
    }

    #[test]
    fn sign_hyperliquid_l1_action_matches_sdk_mainnet_vector() {
        let wallet = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
            .parse()
            .expect("wallet");
        let connection_id =
            H256::from_str("0xde6c4037798a4434ca03cd05f00e3b803126221375cd1e7eaaaf041768be06eb")
                .expect("connection id");

        let signature =
            sign_hyperliquid_l1_action(&wallet, connection_id, true).expect("signature");

        assert_eq!(
            signature.to_string(),
            "fa8a41f6a3fa728206df80801a83bcbfbab08649cd34d9c0bfba7c7b2f99340f53a00226604567b98a1492803190d65a201d6805e5831b7044f17fd530aec7841c"
        );
    }

    #[test]
    fn hyperliquid_cloid_is_stable_hex_uuid_like() {
        let cloid = hyperliquid_cloid_for_client_order("pos-1-entry-long");
        assert!(cloid.starts_with("0x"));
        assert_eq!(cloid.len(), 34);
        assert_eq!(
            cloid,
            hyperliquid_cloid_for_client_order("pos-1-entry-long")
        );
    }
}
