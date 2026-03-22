use std::{
    collections::{HashMap, HashSet},
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
use tokio::{
    net::TcpStream,
    sync::Mutex as AsyncMutex,
    time::{interval, Duration, Instant as TokioInstant, MissedTickBehavior},
};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, warn};

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        AccountBalanceSnapshot, AccountFeeSnapshot, OrderExecutionTiming, OrderFill,
        OrderFillReconciliation, OrderRequest, PerpLiquiditySnapshot, PositionSnapshot, Side,
        SymbolMarketSnapshot, Venue, VenueMarketSnapshot,
    },
    resilience::FailureBackoff,
    venue::VenueAdapter,
};

use super::{
    cache_is_fresh, enrich_fill_from_private, estimate_fee_quote, hinted_fill,
    load_account_fee_snapshot_cache, load_json_cache, lookup_or_wait_private_order, now_ms,
    parse_f64, quote_fill, store_account_fee_snapshot_cache, store_json_cache, venue_symbol,
    PrivateOrderUpdate, WsMarketState, WsPrivateState, SYMBOL_CACHE_TTL_MS,
};

const HYPERLIQUID_MIN_NOTIONAL_QUOTE: f64 = 10.0;
const HYPERLIQUID_PERP_LIQUIDITY_CACHE_TTL_MS: i64 = 60 * 1_000;
const HYPERLIQUID_MARKET_WS_IDLE_TIMEOUT_MS: i64 = 45 * 1_000;
#[cfg(test)]
const HYPERLIQUID_PRIVATE_WS_IDLE_TIMEOUT_MS: i64 = 0;
const HYPERLIQUID_WS_WATCHDOG_TICK_MS: u64 = 5 * 1_000;

pub struct HyperliquidLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    info_client: InfoClient,
    exchange_client: ExchangeClient,
    account_address: H160,
    meta_cache: Mutex<HashMap<String, HyperliquidAssetMeta>>,
    supported_symbols: Mutex<HashSet<String>>,
    market_ws: Arc<WsMarketState>,
    market_subscription_symbols: Mutex<Vec<String>>,
    private_ws: Arc<WsPrivateState>,
    account_fee_snapshot: Mutex<Option<AccountFeeSnapshot>>,
    perp_liquidity_cache: Arc<Mutex<HashMap<String, PerpLiquiditySnapshot>>>,
    configured_leverage: Mutex<HashMap<String, u32>>,
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

        let persisted_catalog =
            load_json_cache::<HyperliquidSymbolCatalogCache>("hyperliquid-symbols.json");
        let account_fee_snapshot =
            load_account_fee_snapshot_cache(Venue::Hyperliquid, "hyperliquid-fees.json");
        let mut meta_cache = HashMap::new();
        let mut supported_symbols = HashSet::new();
        if let Some(cache) = persisted_catalog {
            if !cache_is_fresh(cache.updated_at_ms, now_ms(), SYMBOL_CACHE_TTL_MS) {
                tracing::debug!(
                    "hyperliquid symbol catalog cache is stale; using as fallback seed"
                );
            }
            meta_cache.extend(cache.metadata);
            supported_symbols.extend(cache.supported_symbols);
        }

        let market_ws = WsMarketState::new();
        let adapter = Self {
            config: config.clone(),
            runtime: runtime.clone(),
            info_client,
            exchange_client,
            account_address,
            meta_cache: Mutex::new(meta_cache),
            supported_symbols: Mutex::new(supported_symbols),
            market_ws,
            market_subscription_symbols: Mutex::new(Vec::new()),
            private_ws: WsPrivateState::new(),
            account_fee_snapshot: Mutex::new(account_fee_snapshot),
            perp_liquidity_cache: Arc::new(Mutex::new(HashMap::new())),
            configured_leverage: Mutex::new(HashMap::new()),
            order_ws: HyperliquidWsPostClient::new(hyperliquid_ws_url(base_url)),
        };
        if let Err(error) = adapter.refresh_symbol_catalog().await {
            if adapter.supported_symbols.lock().expect("lock").is_empty() {
                return Err(error);
            }
            tracing::warn!(
                ?error,
                "hyperliquid symbol catalog refresh failed; using persisted cache"
            );
        }
        let tracked_symbols = adapter.tracked_symbols(symbols);
        *adapter.market_subscription_symbols.lock().expect("lock") = tracked_symbols.clone();
        adapter.start_market_ws(&tracked_symbols, base_url).await?;
        adapter.start_private_ws(&tracked_symbols, base_url).await?;
        Ok(adapter)
    }

    fn tracked_symbols(&self, symbols: &[String]) -> Vec<String> {
        symbols
            .iter()
            .filter(|symbol| {
                self.supported_symbols
                    .lock()
                    .expect("lock")
                    .contains(venue_symbol(&self.config, symbol).as_str())
            })
            .cloned()
            .collect()
    }

    fn supports_symbol(&self, symbol: &str) -> bool {
        self.supported_symbols
            .lock()
            .expect("lock")
            .contains(venue_symbol(&self.config, symbol).as_str())
    }

    fn prune_supported_symbol(&self, symbol: &str, reason: &str) {
        let removed = prune_hyperliquid_symbol_catalog_entry(
            &mut self.meta_cache.lock().expect("lock"),
            &mut self.supported_symbols.lock().expect("lock"),
            &venue_symbol(&self.config, symbol),
        );
        if removed {
            self.persist_symbol_catalog();
            warn!(
                symbol,
                venue_symbol = %venue_symbol(&self.config, symbol),
                reason,
                "hyperliquid pruned stale symbol from local catalog"
            );
        }
    }

    fn persist_symbol_catalog(&self) {
        let metadata = self.meta_cache.lock().expect("lock").clone();
        let supported_symbols = self
            .supported_symbols
            .lock()
            .expect("lock")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        store_json_cache(
            "hyperliquid-symbols.json",
            &HyperliquidSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols,
                metadata,
            },
        );
    }

    fn store_account_fee_snapshot(&self, snapshot: &AccountFeeSnapshot) {
        self.account_fee_snapshot
            .lock()
            .expect("lock")
            .replace(snapshot.clone());
        store_account_fee_snapshot_cache("hyperliquid-fees.json", snapshot);
    }

    async fn refresh_account_fee_snapshot(&self) -> Result<Option<AccountFeeSnapshot>> {
        let fees = self
            .info_client
            .user_fees(self.account_address)
            .await
            .context("failed to request hyperliquid user fees")?;
        let snapshot = AccountFeeSnapshot {
            venue: Venue::Hyperliquid,
            taker_fee_bps: parse_f64(&fees.user_cross_rate)? * 10_000.0,
            maker_fee_bps: parse_f64(&fees.user_add_rate)? * 10_000.0,
            observed_at_ms: now_ms(),
            source: "hyperliquid_user_fees".to_string(),
        };
        self.store_account_fee_snapshot(&snapshot);
        Ok(Some(snapshot))
    }

    async fn refresh_symbol_catalog(&self) -> Result<()> {
        let meta = self
            .info_client
            .meta()
            .await
            .context("failed to request hyperliquid meta")?;
        let mut metadata = HashMap::new();
        let mut supported_symbols = HashSet::new();
        for row in meta.universe {
            supported_symbols.insert(row.name.clone());
            metadata.insert(
                row.name.clone(),
                HyperliquidAssetMeta {
                    sz_decimals: row.sz_decimals,
                },
            );
        }
        store_json_cache(
            "hyperliquid-symbols.json",
            &HyperliquidSymbolCatalogCache {
                updated_at_ms: now_ms(),
                supported_symbols: supported_symbols.iter().cloned().collect(),
                metadata: metadata.clone(),
            },
        );
        *self.meta_cache.lock().expect("lock") = metadata;
        *self.supported_symbols.lock().expect("lock") = supported_symbols;
        Ok(())
    }

    async fn start_market_ws(&self, symbols: &[String], base_url: BaseUrl) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let symbol_map = symbols
            .iter()
            .map(|symbol| (venue_symbol(&self.config, symbol), symbol.clone()))
            .collect::<HashMap<_, _>>();
        let cache = self.market_ws.clone();
        let perp_liquidity_cache = self.perp_liquidity_cache.clone();
        let unhealthy_after_failures = self.runtime.ws_unhealthy_after_failures;
        let reconnect_initial_ms = self.runtime.ws_reconnect_initial_ms;
        let reconnect_max_ms = self.runtime.ws_reconnect_max_ms;
        let task = tokio::spawn(async move {
            let mut reconnect_backoff = FailureBackoff::new(
                reconnect_initial_ms,
                reconnect_max_ms,
                Venue::Hyperliquid as u64 + 11,
            );
            loop {
                let (sender, mut receiver) = unbounded_channel();
                let mut info_client = match InfoClient::with_reconnect(None, Some(base_url)).await {
                    Ok(client) => client,
                    Err(error) => {
                        cache.record_connection_failure(
                            now_ms(),
                            unhealthy_after_failures,
                            error.to_string(),
                        );
                        warn!(
                            ?error,
                            "failed to build hyperliquid market websocket client"
                        );
                        tokio::time::sleep(Duration::from_millis(
                            reconnect_backoff.on_failure_with_jitter(),
                        ))
                        .await;
                        continue;
                    }
                };
                let mut subscribe_failed = false;
                for asset in symbol_map.keys() {
                    if let Err(error) = info_client
                        .subscribe(
                            Subscription::L2Book {
                                coin: asset.clone(),
                            },
                            sender.clone(),
                        )
                        .await
                    {
                        cache.record_connection_failure(
                            now_ms(),
                            unhealthy_after_failures,
                            error.to_string(),
                        );
                        warn!(asset, ?error, "failed to subscribe hyperliquid l2Book");
                        subscribe_failed = true;
                        break;
                    }
                    if let Err(error) = info_client
                        .subscribe(
                            Subscription::ActiveAssetCtx {
                                coin: asset.clone(),
                            },
                            sender.clone(),
                        )
                        .await
                    {
                        cache.record_connection_failure(
                            now_ms(),
                            unhealthy_after_failures,
                            error.to_string(),
                        );
                        warn!(
                            asset,
                            ?error,
                            "failed to subscribe hyperliquid activeAssetCtx"
                        );
                        subscribe_failed = true;
                        break;
                    }
                }
                if subscribe_failed {
                    tokio::time::sleep(Duration::from_millis(
                        reconnect_backoff.on_failure_with_jitter(),
                    ))
                    .await;
                    continue;
                }

                reconnect_backoff.on_success();
                cache.record_connection_success(now_ms());
                let _info_client = info_client;
                let mut last_message_ms = now_ms();
                let mut watchdog = interval(Duration::from_millis(HYPERLIQUID_WS_WATCHDOG_TICK_MS));
                watchdog.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut should_rebuild = false;
                while !should_rebuild {
                    tokio::select! {
                        _ = watchdog.tick() => {
                            let current_now_ms = now_ms();
                            if hyperliquid_ws_watchdog_timed_out(
                                last_message_ms,
                                current_now_ms,
                                HYPERLIQUID_MARKET_WS_IDLE_TIMEOUT_MS,
                            ) {
                                cache.record_connection_failure(
                                    current_now_ms,
                                    unhealthy_after_failures,
                                    "hyperliquid market websocket idle timeout".to_string(),
                                );
                                warn!(
                                    idle_ms = current_now_ms.saturating_sub(last_message_ms),
                                    "hyperliquid market websocket idle watchdog timed out"
                                );
                                should_rebuild = true;
                            }
                        }
                        message = receiver.recv() => {
                            match message {
                                Some(HyperliquidMessage::L2Book(book)) => {
                                    last_message_ms = now_ms();
                                    cache.record_connection_success(last_message_ms);
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
                                Some(HyperliquidMessage::ActiveAssetCtx(ctx)) => {
                                    last_message_ms = now_ms();
                                    cache.record_connection_success(last_message_ms);
                                    let Some(symbol) = symbol_map.get(&ctx.data.coin) else {
                                        continue;
                                    };
                                    let funding_rate = match ctx.data.ctx {
                                        hyperliquid_rust_sdk::AssetCtx::Perps(ref perps) => {
                                            if let (Ok(volume_24h_quote), Ok(open_interest), Ok(mark_price)) = (
                                                parse_f64(&perps.shared.day_ntl_vlm),
                                                parse_f64(&perps.open_interest),
                                                parse_f64(&perps.shared.mark_px),
                                            ) {
                                                perp_liquidity_cache.lock().expect("lock").insert(
                                                    symbol.clone(),
                                                    PerpLiquiditySnapshot {
                                                        venue: Venue::Hyperliquid,
                                                        symbol: symbol.clone(),
                                                        volume_24h_quote,
                                                        open_interest_quote: open_interest * mark_price,
                                                        observed_at_ms: last_message_ms,
                                                    },
                                                );
                                            }
                                            parse_f64(&perps.funding).ok()
                                        }
                                        hyperliquid_rust_sdk::AssetCtx::Spot(_) => None,
                                    };
                                    if let Some(funding_rate) = funding_rate {
                                        cache.update_funding(
                                            symbol,
                                            funding_rate,
                                            next_hour_boundary(last_message_ms),
                                        );
                                    }
                                }
                                Some(HyperliquidMessage::HyperliquidError(error)) => {
                                    cache.record_connection_failure(
                                        now_ms(),
                                        unhealthy_after_failures,
                                        error.clone(),
                                    );
                                    warn!(?error, "hyperliquid market websocket reported error");
                                }
                                Some(HyperliquidMessage::NoData) => {
                                    cache.record_connection_failure(
                                        now_ms(),
                                        unhealthy_after_failures,
                                        "hyperliquid market websocket disconnected".to_string(),
                                    );
                                    debug!("hyperliquid market websocket disconnected");
                                    should_rebuild = true;
                                }
                                Some(_) => {}
                                None => {
                                    cache.record_connection_failure(
                                        now_ms(),
                                        unhealthy_after_failures,
                                        "hyperliquid market websocket receiver closed".to_string(),
                                    );
                                    debug!("hyperliquid market websocket receiver closed");
                                    should_rebuild = true;
                                }
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(
                    reconnect_backoff.on_failure_with_jitter(),
                ))
                .await;
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
        let private_state = self.private_ws.clone();
        let unhealthy_after_failures = self.runtime.ws_unhealthy_after_failures;
        let reconnect_initial_ms = self.runtime.ws_reconnect_initial_ms;
        let reconnect_max_ms = self.runtime.ws_reconnect_max_ms;
        let account_address = self.account_address;
        let task = tokio::spawn(async move {
            let mut reconnect_backoff = FailureBackoff::new(
                reconnect_initial_ms,
                reconnect_max_ms,
                Venue::Hyperliquid as u64 + 29,
            );
            loop {
                let (sender, mut receiver) = unbounded_channel();
                let mut info_client = match InfoClient::with_reconnect(None, Some(base_url)).await {
                    Ok(client) => client,
                    Err(error) => {
                        private_state.record_connection_failure(
                            now_ms(),
                            unhealthy_after_failures,
                            error.to_string(),
                        );
                        warn!(
                            ?error,
                            "failed to build hyperliquid private websocket client"
                        );
                        tokio::time::sleep(Duration::from_millis(
                            reconnect_backoff.on_failure_with_jitter(),
                        ))
                        .await;
                        continue;
                    }
                };
                if let Ok(user_state) = info_client.user_state(account_address).await {
                    for position in user_state.asset_positions {
                        if let Some(symbol) = symbol_map.get(&position.position.coin) {
                            if let Ok(size) = parse_f64(&position.position.szi) {
                                private_state.update_position(symbol, size, now_ms());
                            }
                        }
                    }
                }
                if let Err(error) = info_client
                    .subscribe(
                        Subscription::UserEvents {
                            user: account_address,
                        },
                        sender.clone(),
                    )
                    .await
                {
                    private_state.record_connection_failure(
                        now_ms(),
                        unhealthy_after_failures,
                        error.to_string(),
                    );
                    warn!(?error, "failed to subscribe hyperliquid user events");
                    tokio::time::sleep(Duration::from_millis(
                        reconnect_backoff.on_failure_with_jitter(),
                    ))
                    .await;
                    continue;
                }
                if let Err(error) = info_client
                    .subscribe(
                        Subscription::OrderUpdates {
                            user: account_address,
                        },
                        sender.clone(),
                    )
                    .await
                {
                    private_state.record_connection_failure(
                        now_ms(),
                        unhealthy_after_failures,
                        error.to_string(),
                    );
                    warn!(?error, "failed to subscribe hyperliquid order updates");
                    tokio::time::sleep(Duration::from_millis(
                        reconnect_backoff.on_failure_with_jitter(),
                    ))
                    .await;
                    continue;
                }

                reconnect_backoff.on_success();
                private_state.record_connection_success(now_ms());
                let _info_client = info_client;
                let mut should_rebuild = false;
                while !should_rebuild {
                    match receiver.recv().await {
                        Some(HyperliquidMessage::User(user_event)) => {
                            private_state.record_connection_success(now_ms());
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
                                    let fee_quote =
                                        match fill.fee_token.to_ascii_uppercase().as_str() {
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
                                    let signed_quantity =
                                        hyperliquid_side_sign(&fill.side) * quantity;
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
                        Some(HyperliquidMessage::OrderUpdates(order_updates)) => {
                            private_state.record_connection_success(now_ms());
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
                        Some(HyperliquidMessage::HyperliquidError(error)) => {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                error.clone(),
                            );
                            warn!(?error, "hyperliquid private websocket reported error");
                        }
                        Some(HyperliquidMessage::NoData) => {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                "hyperliquid private websocket disconnected".to_string(),
                            );
                            debug!("hyperliquid private websocket disconnected");
                            should_rebuild = true;
                        }
                        Some(_) => {}
                        None => {
                            private_state.record_connection_failure(
                                now_ms(),
                                unhealthy_after_failures,
                                "hyperliquid private websocket receiver closed".to_string(),
                            );
                            debug!("hyperliquid private websocket receiver closed");
                            should_rebuild = true;
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(
                    reconnect_backoff.on_failure_with_jitter(),
                ))
                .await;
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
        if !self.supports_symbol(symbol) {
            return Err(anyhow!("hyperliquid symbol not supported for {}", symbol));
        }
        let asset = venue_symbol(&self.config, symbol);
        let book = match self.info_client.l2_snapshot(asset.clone()).await {
            Ok(book) => book,
            Err(error) => {
                let refresh_error = self.refresh_symbol_catalog().await.err();
                if !self.supports_symbol(symbol) {
                    if let Some(refresh_error) = refresh_error {
                        warn!(
                            symbol,
                            venue_symbol = %asset,
                            ?refresh_error,
                            "hyperliquid symbol refresh failed before marking symbol unsupported"
                        );
                    }
                    self.prune_supported_symbol(symbol, "l2_snapshot_failed_symbol_missing");
                    return Err(anyhow!("hyperliquid symbol not supported for {}", symbol));
                }
                return Err(error).context("failed to request hyperliquid l2 book");
            }
        };
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
            mark_price: None,
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
                    ExchangeDataStatus::Filled(order) => Ok(HyperliquidOrderOutcome::Resolved {
                        order_id: order.oid.to_string(),
                        average_price: parse_f64(&order.avg_px).unwrap_or(fallback_price),
                        quantity: parse_f64(&order.total_sz).unwrap_or(fallback_qty),
                        private_fill_wait_optional: true,
                    }),
                    ExchangeDataStatus::Resting(order) => Ok(HyperliquidOrderOutcome::Resolved {
                        order_id: order.oid.to_string(),
                        average_price: fallback_price,
                        quantity: fallback_qty,
                        private_fill_wait_optional: false,
                    }),
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
        self.refresh_symbol_catalog().await?;
        self.meta_cache
            .lock()
            .expect("lock")
            .get(asset)
            .cloned()
            .ok_or_else(|| anyhow!("hyperliquid asset metadata missing for {asset}"))
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
            HYPERLIQUID_PERP_LIQUIDITY_CACHE_TTL_MS,
        ) {
            return None;
        }
        Some(snapshot)
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
    ) -> Result<HyperliquidOrderResponseWithTiming> {
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
        let sign_started_at = Instant::now();
        let nonce = next_hyperliquid_nonce();
        let connection_id =
            hyperliquid_action_connection_id(&action, nonce, self.exchange_client.vault_address)?;
        let signature = sign_hyperliquid_l1_action(
            &self.exchange_client.wallet,
            connection_id,
            !self.config.live.is_testnet,
        )?;
        let action_payload = serde_json::to_value(action.clone())
            .context("failed to serialize hyperliquid action")?;
        let request_sign_ms = elapsed_ms(sign_started_at);

        match self
            .order_ws
            .post_action(
                action_payload.clone(),
                nonce,
                signature,
                self.exchange_client.vault_address,
            )
            .await
        {
            Ok(response) => Ok(HyperliquidOrderResponseWithTiming {
                response,
                request_sign_ms,
            }),
            Err(error) if should_retry_hyperliquid_duplicate_nonce(&error) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                let retry_sign_started_at = Instant::now();
                let retry_nonce = next_hyperliquid_nonce();
                let retry_connection_id = hyperliquid_action_connection_id(
                    &action,
                    retry_nonce,
                    self.exchange_client.vault_address,
                )?;
                let retry_signature = sign_hyperliquid_l1_action(
                    &self.exchange_client.wallet,
                    retry_connection_id,
                    !self.config.live.is_testnet,
                )?;
                self.order_ws
                    .post_action(
                        action_payload,
                        retry_nonce,
                        retry_signature,
                        self.exchange_client.vault_address,
                    )
                    .await
                    .map(|response| HyperliquidOrderResponseWithTiming {
                        response,
                        request_sign_ms: elapsed_ms(retry_sign_started_at),
                    })
            }
            Err(error) => Err(error),
        }
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
        let allow_direct_fallback = symbols.len() == 1;
        for symbol in symbols.iter().filter(|symbol| self.supports_symbol(symbol)) {
            if let Some((snapshot, snapshot_observed_at_ms)) = self.cached_snapshot(symbol) {
                observed_at_ms = observed_at_ms.max(snapshot_observed_at_ms);
                quotes.push(snapshot);
            } else if allow_direct_fallback {
                let quote = self.fetch_symbol_snapshot(symbol).await?;
                observed_at_ms = observed_at_ms.max(quote.funding_timestamp_ms.min(now_ms()));
                quotes.push(quote);
            }
        }
        if quotes.is_empty() {
            return Err(anyhow!(
                "hyperliquid market snapshot unavailable for requested symbols"
            ));
        }

        Ok(VenueMarketSnapshot {
            venue: Venue::Hyperliquid,
            observed_at_ms: now_ms().max(observed_at_ms),
            symbols: quotes,
        })
    }

    async fn refresh_market_snapshot(&self, symbol: &str) -> Result<VenueMarketSnapshot> {
        let snapshot = self.fetch_symbol_snapshot(symbol).await?;
        Ok(VenueMarketSnapshot {
            venue: Venue::Hyperliquid,
            observed_at_ms: now_ms(),
            symbols: vec![snapshot],
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
            request_sign_ms: Some(response.request_sign_ms),
            submit_http_ms: None,
            response_decode_ms: None,
            private_fill_wait_ms: None,
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
        let outcome = self.decode_response(response.response, fallback_price, request.quantity)?;
        match outcome {
            HyperliquidOrderOutcome::Resolved {
                order_id,
                average_price,
                quantity,
                private_fill_wait_optional,
            } => {
                fill.order_id = order_id.clone();
                fill.average_price = average_price;
                fill.quantity = quantity;
                fill.fee_quote =
                    estimate_fee_quote(average_price, quantity, self.config.taker_fee_bps);
                let private_fill_wait_ms = hyperliquid_private_fill_wait_ms(
                    &HyperliquidOrderOutcome::Resolved {
                        order_id: order_id.clone(),
                        average_price,
                        quantity,
                        private_fill_wait_optional,
                    },
                    self.config.live.post_ack_private_fill_wait_ms,
                );
                let private_fill_wait_started_at = Instant::now();
                if let Some(private_fill) = lookup_or_wait_private_order(
                    &self.private_ws,
                    Some(wire_cloid.as_str()),
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
            HyperliquidOrderOutcome::Pending => {
                let private_fill_wait_started_at = Instant::now();
                if let Some(private_fill) = lookup_or_wait_private_order(
                    &self.private_ws,
                    Some(wire_cloid.as_str()),
                    None,
                    self.config.live.post_ack_private_fill_wait_ms,
                )
                .await
                {
                    fill = enrich_fill_from_private(fill, &private_fill);
                    if let Some(timing) = fill.timing.as_mut() {
                        timing.private_fill_wait_ms =
                            Some(elapsed_ms(private_fill_wait_started_at));
                    }
                    return Ok(fill);
                }
                if let Some(timing) = fill.timing.as_mut() {
                    timing.private_fill_wait_ms = Some(elapsed_ms(private_fill_wait_started_at));
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

    async fn fetch_account_balance_snapshot(&self) -> Result<Option<AccountBalanceSnapshot>> {
        let user_state = self
            .info_client
            .user_state(self.account_address)
            .await
            .context("failed to query hyperliquid user state")?;
        Ok(Some(AccountBalanceSnapshot {
            venue: Venue::Hyperliquid,
            equity_quote: parse_f64(&user_state.margin_summary.account_value)?,
            wallet_balance_quote: Some(parse_f64(&user_state.cross_margin_summary.account_value)?),
            available_balance_quote: Some(parse_f64(&user_state.withdrawable)?),
            observed_at_ms: now_ms(),
        }))
    }

    fn cached_account_fee_snapshot(&self) -> Option<AccountFeeSnapshot> {
        self.account_fee_snapshot.lock().expect("lock").clone()
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

        let response = self
            .exchange_client
            .update_leverage(leverage, &venue_symbol(&self.config, symbol), true, None)
            .await
            .context("failed to submit hyperliquid leverage update")?;
        match response {
            ExchangeResponseStatus::Ok(_) => {
                self.configured_leverage
                    .lock()
                    .expect("lock")
                    .insert(symbol.to_string(), leverage);
                Ok(())
            }
            ExchangeResponseStatus::Err(error) => {
                Err(anyhow!("hyperliquid set leverage failed: {error}"))
            }
        }
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
        let parsed_order_id = order_id
            .parse::<u64>()
            .with_context(|| format!("invalid hyperliquid order id {order_id}"))?;
        let asset = venue_symbol(&self.config, symbol);
        let fills = self
            .info_client
            .user_fills(self.account_address)
            .await
            .context("failed to query hyperliquid user fills")?;
        let matched = fills
            .into_iter()
            .filter(|fill| fill.oid == parsed_order_id && fill.coin == asset)
            .collect::<Vec<_>>();
        if matched.is_empty() {
            return Ok(None);
        }

        let mut total_quantity = 0.0;
        let mut weighted_notional = 0.0;
        let mut total_fee_quote = 0.0;
        let mut latest_fill_ms = 0_i64;
        for fill in matched {
            let quantity = parse_f64(&fill.sz)?;
            let price = parse_f64(&fill.px)?;
            total_quantity += quantity;
            weighted_notional += price * quantity;
            total_fee_quote += parse_f64(&fill.fee)?.abs();
            latest_fill_ms = latest_fill_ms.max(fill.time.min(i64::MAX as u64) as i64);
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
        let asset = venue_symbol(&self.config, symbol);
        let asset_meta = self.asset_meta(&asset).await?;
        Ok(round_to_decimals(quantity, asset_meta.sz_decimals))
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<PerpLiquiditySnapshot>> {
        if !self.supports_symbol(symbol) {
            return Ok(None);
        }
        let observed_at_ms = now_ms();
        self.cached_perp_liquidity_snapshot(symbol, observed_at_ms)
            .map(Some)
            .ok_or_else(|| {
                anyhow!(
                    "hyperliquid active asset ctx unavailable for {}",
                    venue_symbol(&self.config, symbol)
                )
            })
    }

    fn min_entry_notional_quote_hint(
        &self,
        _symbol: &str,
        _price_hint: Option<f64>,
    ) -> Option<f64> {
        Some(HYPERLIQUID_MIN_NOTIONAL_QUOTE)
    }

    async fn live_startup_prewarm(&self) -> Result<()> {
        self.order_ws.prewarm().await?;
        if let Err(error) = self.refresh_account_fee_snapshot().await {
            warn!(?error, "hyperliquid account fee snapshot prewarm failed");
        }
        Ok(())
    }

    fn supports_market_data_activity_control(&self) -> bool {
        true
    }

    async fn set_market_data_active(&self, active: bool, symbols: &[String]) -> Result<()> {
        let tracked_symbols = self.tracked_symbols(symbols);
        let current_symbols = self
            .market_subscription_symbols
            .lock()
            .expect("lock")
            .clone();
        if !active || tracked_symbols.is_empty() {
            if self.market_ws.has_worker() || !current_symbols.is_empty() {
                self.market_ws.abort_worker();
                self.market_ws.clear();
                self.market_subscription_symbols
                    .lock()
                    .expect("lock")
                    .clear();
            }
            return Ok(());
        }
        if self.market_ws.has_worker() && current_symbols == tracked_symbols {
            return Ok(());
        }
        self.market_ws.abort_worker();
        self.market_ws.clear();
        let base_url = if self.config.live.is_testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };
        self.start_market_ws(&tracked_symbols, base_url).await?;
        *self.market_subscription_symbols.lock().expect("lock") = tracked_symbols;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.market_ws.abort_worker();
        self.private_ws.abort_workers();
        self.order_ws.shutdown().await;
        Ok(())
    }

    fn supported_symbols(&self, requested_symbols: &[String]) -> Option<Vec<String>> {
        Some(self.tracked_symbols(requested_symbols))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct HyperliquidAssetMeta {
    sz_decimals: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct HyperliquidSymbolCatalogCache {
    updated_at_ms: i64,
    supported_symbols: Vec<String>,
    metadata: HashMap<String, HyperliquidAssetMeta>,
}

fn prune_hyperliquid_symbol_catalog_entry(
    metadata: &mut HashMap<String, HyperliquidAssetMeta>,
    supported_symbols: &mut HashSet<String>,
    asset: &str,
) -> bool {
    let removed_meta = metadata.remove(asset).is_some();
    let removed_supported = supported_symbols.remove(asset);
    removed_meta || removed_supported
}

fn hyperliquid_ws_watchdog_timed_out(last_message_ms: i64, now_ms: i64, timeout_ms: i64) -> bool {
    timeout_ms > 0 && now_ms.saturating_sub(last_message_ms) > timeout_ms
}

enum HyperliquidOrderOutcome {
    Resolved {
        order_id: String,
        average_price: f64,
        quantity: f64,
        private_fill_wait_optional: bool,
    },
    Pending,
}

fn hyperliquid_private_fill_wait_ms(
    outcome: &HyperliquidOrderOutcome,
    configured_wait_ms: u64,
) -> u64 {
    match outcome {
        HyperliquidOrderOutcome::Resolved {
            private_fill_wait_optional: true,
            ..
        } => 0,
        _ => configured_wait_ms,
    }
}

struct HyperliquidOrderResponseWithTiming {
    response: ExchangeResponseStatus,
    request_sign_ms: u64,
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

    async fn prewarm(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if should_refresh_ws_connection(state.last_used_at) {
            state.socket = None;
            state.last_used_at = None;
        }
        if state.socket.is_none() {
            state.socket = Some(connect_hyperliquid_ws(&self.url).await?);
        }
        state.last_used_at = Some(TokioInstant::now());
        Ok(())
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
            payload
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| payload.to_string())
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

fn should_retry_hyperliquid_duplicate_nonce(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("invalid nonce") && message.contains("duplicate nonce")
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
    use std::collections::{HashMap, HashSet};
    use std::str::FromStr;

    use anyhow::anyhow;
    use ethers::types::H256;
    use hyperliquid_rust_sdk::ExchangeResponseStatus;
    use serde_json::json;

    use crate::models::Side;

    use super::{
        decode_ws_post_action_response, encode_ws_post_order_request,
        hyperliquid_cloid_for_client_order, hyperliquid_private_fill_wait_ms,
        hyperliquid_ws_watchdog_timed_out, ioc_order_params_from_reference_price,
        prune_hyperliquid_symbol_catalog_entry, should_retry_hyperliquid_duplicate_nonce,
        sign_hyperliquid_l1_action, HyperliquidAssetMeta, HyperliquidOrderOutcome,
        HYPERLIQUID_PRIVATE_WS_IDLE_TIMEOUT_MS,
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
    fn private_fill_wait_is_skipped_only_for_ack_filled_orders() {
        let filled = HyperliquidOrderOutcome::Resolved {
            order_id: "1".to_string(),
            average_price: 1100.0,
            quantity: 0.2,
            private_fill_wait_optional: true,
        };
        let resting = HyperliquidOrderOutcome::Resolved {
            order_id: "2".to_string(),
            average_price: 1100.0,
            quantity: 0.2,
            private_fill_wait_optional: false,
        };

        assert_eq!(hyperliquid_private_fill_wait_ms(&filled, 120), 0);
        assert_eq!(hyperliquid_private_fill_wait_ms(&resting, 120), 120);
        assert_eq!(
            hyperliquid_private_fill_wait_ms(&HyperliquidOrderOutcome::Pending, 120),
            120
        );
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
    fn ws_post_action_response_error_includes_raw_payload() {
        let response = json!({
            "channel": "post",
            "data": {
                "id": 256,
                "response": {
                    "type": "error",
                    "payload": {
                        "code": "BadAloPx",
                        "message": "Post only order would cross"
                    }
                }
            }
        });

        let error = decode_ws_post_action_response(256, &response).expect_err("error payload");
        let message = error.to_string();
        assert!(message.contains("hyperliquid ws post error"));
        assert!(message.contains("BadAloPx"));
        assert!(message.contains("Post only order would cross"));
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

    #[test]
    fn duplicate_nonce_errors_are_retryable() {
        assert!(should_retry_hyperliquid_duplicate_nonce(&anyhow!(
            "hyperliquid order error: Invalid nonce: duplicate nonce"
        )));
        assert!(!should_retry_hyperliquid_duplicate_nonce(&anyhow!(
            "hyperliquid order error: Order must have minimum value of $10. asset=103"
        )));
    }

    #[test]
    fn watchdog_timeout_only_trips_after_threshold() {
        assert!(!hyperliquid_ws_watchdog_timed_out(1_000, 1_000, 45_000));
        assert!(!hyperliquid_ws_watchdog_timed_out(1_000, 46_000, 45_000));
        assert!(hyperliquid_ws_watchdog_timed_out(1_000, 46_001, 45_000));
    }

    #[test]
    fn private_idle_watchdog_is_disabled_for_sparse_event_streams() {
        assert_eq!(HYPERLIQUID_PRIVATE_WS_IDLE_TIMEOUT_MS, 0);
    }

    #[test]
    fn pruning_missing_symbol_removes_cached_metadata_and_support() {
        let mut metadata =
            HashMap::from([("FET".to_string(), HyperliquidAssetMeta { sz_decimals: 3 })]);
        let mut supported_symbols = HashSet::from(["FET".to_string(), "BTC".to_string()]);

        let removed =
            prune_hyperliquid_symbol_catalog_entry(&mut metadata, &mut supported_symbols, "FET");

        assert!(removed);
        assert!(!metadata.contains_key("FET"));
        assert!(!supported_symbols.contains("FET"));
        assert!(supported_symbols.contains("BTC"));
    }
}
