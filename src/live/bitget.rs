use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Mutex,
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Client, Method, StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};

use crate::{
    config::{RuntimeConfig, VenueConfig},
    models::{
        AccountBalanceSnapshot, AssetTransferStatus, OrderExecutionTiming, OrderFill,
        OrderFillReconciliation, OrderRequest, PerpLiquiditySnapshot, PositionSnapshot, Side,
        SymbolMarketSnapshot, Venue, VenueMarketSnapshot,
    },
    venue::VenueAdapter,
};

use super::{
    base_asset, build_http_client, build_query, cache_is_fresh, estimate_fee_quote,
    filter_transfer_statuses, floor_to_step, format_decimal, hinted_fill, hmac_sha256_base64,
    load_json_cache, now_ms, parse_f64, parse_i64, quote_fill, store_json_cache,
    transfer_cache_ttl_ms, venue_symbol, VenueTransferStatusCache, SYMBOL_CACHE_TTL_MS,
};

const BITGET_PRODUCT_TYPE: &str = "USDT-FUTURES";
const BITGET_MARGIN_COIN: &str = "USDT";
const BITGET_PERP_LIQUIDITY_CACHE_TTL_MS: i64 = 60 * 1_000;

pub struct BitgetLiveAdapter {
    config: VenueConfig,
    runtime: RuntimeConfig,
    client: Client,
    base_url: String,
    wallet_base_url: String,
    metadata: Mutex<HashMap<String, BitgetSymbolMeta>>,
    supported_symbols: Mutex<HashSet<String>>,
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
    ) -> Result<BitgetOrderResponseWithTiming> {
        let sign_started_at = Instant::now();
        let body = serde_json::json!({
            "symbol": venue_symbol(&self.config, &request.symbol),
            "productType": BITGET_PRODUCT_TYPE,
            "marginMode": "crossed",
            "marginCoin": BITGET_MARGIN_COIN,
            "size": format_decimal(quantity, step_size),
            "side": match request.side {
                Side::Buy => "buy",
                Side::Sell => "sell",
            },
            "orderType": "market",
            "force": "ioc",
            "clientOid": request.client_order_id,
            "reduceOnly": if request.reduce_only { "YES" } else { "NO" },
        })
        .to_string();
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
        let rows = self.fetch_tickers_map().await?;
        let observed_at_ms = now_ms();
        let mut snapshots = Vec::new();
        for symbol in tracked {
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
        let order_prepare_started_at = Instant::now();
        let meta = self.symbol_meta(&request.symbol).await?;
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
        let response = self
            .submit_order_once(&request, quantity, meta.step_size)
            .await?;
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
            timing.private_fill_wait_ms = Some(elapsed_ms(reconciliation_started_at));
        }
        Ok(fill)
    }

    async fn fetch_position(&self, symbol: &str) -> Result<PositionSnapshot> {
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BitgetSymbolMeta {
    min_qty: f64,
    step_size: f64,
    min_notional: Option<f64>,
    max_qty: Option<f64>,
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
        },
    ))
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

    use super::{parse_bitget_contract_meta, parse_bitget_liquidity_snapshot};

    #[test]
    fn parses_bitget_contract_meta_from_v2_contracts_row() {
        let row = json!({
            "symbol": "BTCUSDT",
            "symbolStatus": "normal",
            "sizeMultiplier": "0.001",
            "minTradeNum": "0.001",
            "minTradeUSDT": "5",
            "maxMarketOrderQty": "1000"
        });

        let (symbol, meta) = parse_bitget_contract_meta(&row).expect("parse contract row");
        assert_eq!(symbol, "BTCUSDT");
        assert!((meta.step_size - 0.001).abs() < 1e-9);
        assert!((meta.min_qty - 0.001).abs() < 1e-9);
        assert_eq!(meta.min_notional, Some(5.0));
        assert_eq!(meta.max_qty, Some(1000.0));
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
}
