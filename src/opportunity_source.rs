use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::DateTime;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::models::{AssetTransferStatus, Venue};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpportunityHint {
    pub symbol: String,
    pub long_venue: Venue,
    pub short_venue: Venue,
    pub price_diff_pct: f64,
    pub funding_diff_pct_per_hour: f64,
    pub direction_consistent: bool,
    pub interval_aligned: bool,
    pub source: String,
}

impl OpportunityHint {
    pub fn matches_symbol(&self, symbol: &str) -> bool {
        normalize_symbol_key(&self.symbol) == normalize_symbol_key(symbol)
    }
}

#[async_trait]
pub trait OpportunityHintSource: Send + Sync {
    async fn fetch_hints(&self, symbols: &[String]) -> Result<Vec<OpportunityHint>>;
}

#[async_trait]
pub trait TransferStatusSource: Send + Sync {
    async fn fetch_transfer_statuses(
        &self,
        assets: &[String],
        venues: &[Venue],
    ) -> Result<Vec<AssetTransferStatus>>;
}

#[derive(Clone)]
pub struct ChillybotOpportunitySource {
    base_url: String,
    client: reqwest::Client,
}

impl ChillybotOpportunitySource {
    pub fn new(base_url: impl Into<String>, timeout_ms: u64) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .context("failed to build chillybot http client")?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client,
        })
    }
}

#[derive(Clone)]
pub struct FeedgrabChillybotSource {
    base_url: String,
    client: reqwest::Client,
}

impl FeedgrabChillybotSource {
    pub fn new(base_url: impl Into<String>, timeout_ms: u64) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .context("failed to build feedgrab-jina http client")?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client,
        })
    }

    async fn fetch_jina_json<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let endpoint = endpoint.trim_start_matches('/');
        let target = format!("{}/{}", self.base_url, endpoint);
        let jina_url = format!("https://r.jina.ai/http://{}", target.trim_start_matches("https://"));
        let body = self
            .client
            .get(jina_url)
            .send()
            .await
            .context("failed to request feedgrab-jina endpoint")?
            .error_for_status()
            .context("feedgrab-jina returned non-success status")?
            .text()
            .await
            .context("failed to read feedgrab-jina payload")?;

        let start = body.find('{').context("feedgrab-jina payload missing json start")?;
        let end = body.rfind('}').context("feedgrab-jina payload missing json end")?;
        let json_slice = &body[start..=end];
        serde_json::from_str::<T>(json_slice).context("failed to decode feedgrab-jina json payload")
    }
}

#[async_trait]
impl OpportunityHintSource for ChillybotOpportunitySource {
    async fn fetch_hints(&self, symbols: &[String]) -> Result<Vec<OpportunityHint>> {
        let response = self
            .client
            .get(format!("{}/api/data", self.base_url))
            .send()
            .await
            .context("failed to request chillybot funding data")?
            .error_for_status()
            .context("chillybot returned non-success status")?;
        let payload = response
            .json::<ChillybotResponse>()
            .await
            .context("failed to decode chillybot funding payload")?;

        let wanted = symbols
            .iter()
            .map(|symbol| normalize_symbol_key(symbol))
            .collect::<BTreeSet<_>>();
        let mut grouped = BTreeMap::<String, Vec<ChillybotFundingRow>>::new();
        for row in payload.data {
            let symbol_key = normalize_symbol_key(&row.symbol);
            if !wanted.is_empty() && !wanted.contains(&symbol_key) {
                continue;
            }
            grouped.entry(symbol_key).or_default().push(row);
        }

        let mut seen_pairs = BTreeSet::new();
        let mut hints = Vec::new();
        for (symbol, rows) in grouped {
            if rows.len() < 2 {
                continue;
            }

            for left_index in 0..rows.len() {
                for right_index in (left_index + 1)..rows.len() {
                    let left = &rows[left_index];
                    let right = &rows[right_index];
                    let Some(left_venue) = map_exchange(&left.exchange) else {
                        continue;
                    };
                    let Some(right_venue) = map_exchange(&right.exchange) else {
                        continue;
                    };
                    if left_venue == right_venue {
                        continue;
                    }

                    let left_interval = normalize_interval_hours(left.funding_interval);
                    let right_interval = normalize_interval_hours(right.funding_interval);
                    let left_funding_per_hour = left.funding_rate / left_interval;
                    let right_funding_per_hour = right.funding_rate / right_interval;
                    let funding_diff_pct_per_hour =
                        (left_funding_per_hour - right_funding_per_hour).abs();
                    if funding_diff_pct_per_hour <= 0.0 {
                        continue;
                    }

                    let left_price = left.price;
                    let right_price = right.price;
                    if left_price <= 0.0 || right_price <= 0.0 {
                        continue;
                    }
                    let price_diff_pct = ((left_price.max(right_price)
                        - left_price.min(right_price))
                        / left_price.min(right_price))
                        * 100.0;

                    let (long_venue, short_venue, funding_higher_venue) =
                        if left_funding_per_hour >= right_funding_per_hour {
                            (right_venue, left_venue, left_venue)
                        } else {
                            (left_venue, right_venue, right_venue)
                        };
                    let price_higher_venue = if left_price >= right_price {
                        left_venue
                    } else {
                        right_venue
                    };
                    let pair_key = format!("{symbol}:{}->{}", long_venue, short_venue);
                    if !seen_pairs.insert(pair_key) {
                        continue;
                    }

                    hints.push(OpportunityHint {
                        symbol: symbol.clone(),
                        long_venue,
                        short_venue,
                        price_diff_pct,
                        funding_diff_pct_per_hour,
                        direction_consistent: price_higher_venue == funding_higher_venue,
                        interval_aligned: (left.funding_interval - right.funding_interval).abs()
                            < f64::EPSILON,
                        source: "chillybot".to_string(),
                    });
                }
            }
        }

        hints.sort_by(|left, right| {
            opportunity_score(right)
                .total_cmp(&opportunity_score(left))
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        Ok(hints)
    }
}

#[async_trait]
impl TransferStatusSource for ChillybotOpportunitySource {
    async fn fetch_transfer_statuses(
        &self,
        assets: &[String],
        venues: &[Venue],
    ) -> Result<Vec<AssetTransferStatus>> {
        let response = self
            .client
            .get(format!("{}/cex/api/status", self.base_url))
            .send()
            .await
            .context("failed to request chillybot cex status data")?
            .error_for_status()
            .context("chillybot cex status returned non-success status")?;
        let payload = response
            .json::<ChillybotCexStatusResponse>()
            .await
            .context("failed to decode chillybot cex status payload")?;

        let wanted_assets = assets
            .iter()
            .map(|asset| normalize_symbol_key(asset))
            .collect::<BTreeSet<_>>();
        let wanted_venues = venues.iter().copied().collect::<BTreeSet<_>>();
        let default_ts = payload
            .meta
            .as_ref()
            .and_then(|meta| meta.updated_at.as_deref())
            .and_then(parse_rfc3339_ms)
            .unwrap_or_default();

        let mut aggregated = BTreeMap::<(Venue, String), AssetTransferStatus>::new();
        for row in payload.data {
            let Some(venue) = map_exchange(&row.exchange_id) else {
                continue;
            };
            if !wanted_venues.is_empty() && !wanted_venues.contains(&venue) {
                continue;
            }

            let asset = normalize_symbol_key(&row.asset);
            if !wanted_assets.is_empty() && !wanted_assets.contains(&asset) {
                continue;
            }

            let observed_at_ms = row
                .updated_at
                .as_deref()
                .and_then(parse_rfc3339_ms)
                .unwrap_or(default_ts);
            aggregated
                .entry((venue, asset.clone()))
                .and_modify(|current| {
                    current.deposit_enabled |= row.deposit_enabled;
                    current.withdraw_enabled |= row.withdraw_enabled;
                    current.observed_at_ms = current.observed_at_ms.max(observed_at_ms);
                })
                .or_insert(AssetTransferStatus {
                    venue,
                    asset,
                    deposit_enabled: row.deposit_enabled,
                    withdraw_enabled: row.withdraw_enabled,
                    observed_at_ms,
                    source: "chillybot".to_string(),
                });
        }

        Ok(aggregated.into_values().collect())
    }
}

#[async_trait]
impl OpportunityHintSource for FeedgrabChillybotSource {
    async fn fetch_hints(&self, symbols: &[String]) -> Result<Vec<OpportunityHint>> {
        let payload = self
            .fetch_jina_json::<ChillybotResponse>("api/data")
            .await
            .context("failed to fetch chillybot funding via feedgrab-jina")?;

        let wanted = symbols
            .iter()
            .map(|symbol| normalize_symbol_key(symbol))
            .collect::<BTreeSet<_>>();
        let mut grouped = BTreeMap::<String, Vec<ChillybotFundingRow>>::new();
        for row in payload.data {
            let symbol_key = normalize_symbol_key(&row.symbol);
            if !wanted.is_empty() && !wanted.contains(&symbol_key) {
                continue;
            }
            grouped.entry(symbol_key).or_default().push(row);
        }

        let mut seen_pairs = BTreeSet::new();
        let mut hints = Vec::new();
        for (symbol, rows) in grouped {
            if rows.len() < 2 {
                continue;
            }

            for left_index in 0..rows.len() {
                for right_index in (left_index + 1)..rows.len() {
                    let left = &rows[left_index];
                    let right = &rows[right_index];
                    let Some(left_venue) = map_exchange(&left.exchange) else {
                        continue;
                    };
                    let Some(right_venue) = map_exchange(&right.exchange) else {
                        continue;
                    };
                    if left_venue == right_venue {
                        continue;
                    }

                    let left_interval = normalize_interval_hours(left.funding_interval);
                    let right_interval = normalize_interval_hours(right.funding_interval);
                    let left_funding_per_hour = left.funding_rate / left_interval;
                    let right_funding_per_hour = right.funding_rate / right_interval;
                    let funding_diff_pct_per_hour =
                        (left_funding_per_hour - right_funding_per_hour).abs();
                    if funding_diff_pct_per_hour <= 0.0 {
                        continue;
                    }

                    let left_price = left.price;
                    let right_price = right.price;
                    if left_price <= 0.0 || right_price <= 0.0 {
                        continue;
                    }
                    let price_diff_pct = ((left_price.max(right_price)
                        - left_price.min(right_price))
                        / left_price.min(right_price))
                        * 100.0;

                    let (long_venue, short_venue, funding_higher_venue) =
                        if left_funding_per_hour >= right_funding_per_hour {
                            (right_venue, left_venue, left_venue)
                        } else {
                            (left_venue, right_venue, right_venue)
                        };
                    let price_higher_venue = if left_price >= right_price {
                        left_venue
                    } else {
                        right_venue
                    };
                    let pair_key = format!("{symbol}:{}->{}", long_venue, short_venue);
                    if !seen_pairs.insert(pair_key) {
                        continue;
                    }

                    hints.push(OpportunityHint {
                        symbol: symbol.clone(),
                        long_venue,
                        short_venue,
                        price_diff_pct,
                        funding_diff_pct_per_hour,
                        direction_consistent: price_higher_venue == funding_higher_venue,
                        interval_aligned: (left.funding_interval - right.funding_interval).abs()
                            < f64::EPSILON,
                        source: "feedgrab_jina".to_string(),
                    });
                }
            }
        }

        hints.sort_by(|left, right| {
            opportunity_score(right)
                .total_cmp(&opportunity_score(left))
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        Ok(hints)
    }
}

#[async_trait]
impl TransferStatusSource for FeedgrabChillybotSource {
    async fn fetch_transfer_statuses(
        &self,
        assets: &[String],
        venues: &[Venue],
    ) -> Result<Vec<AssetTransferStatus>> {
        let payload = self
            .fetch_jina_json::<ChillybotCexStatusResponse>("cex/api/status")
            .await
            .context("failed to fetch chillybot cex status via feedgrab-jina")?;

        let wanted_assets = assets
            .iter()
            .map(|asset| normalize_symbol_key(asset))
            .collect::<BTreeSet<_>>();
        let wanted_venues = venues.iter().copied().collect::<BTreeSet<_>>();
        let default_ts = payload
            .meta
            .as_ref()
            .and_then(|meta| meta.updated_at.as_deref())
            .and_then(parse_rfc3339_ms)
            .unwrap_or_default();

        let mut aggregated = BTreeMap::<(Venue, String), AssetTransferStatus>::new();
        for row in payload.data {
            let Some(venue) = map_exchange(&row.exchange_id) else {
                continue;
            };
            if !wanted_venues.is_empty() && !wanted_venues.contains(&venue) {
                continue;
            }

            let asset = normalize_symbol_key(&row.asset);
            if !wanted_assets.is_empty() && !wanted_assets.contains(&asset) {
                continue;
            }

            let observed_at_ms = row
                .updated_at
                .as_deref()
                .and_then(parse_rfc3339_ms)
                .unwrap_or(default_ts);
            aggregated
                .entry((venue, asset.clone()))
                .and_modify(|current| {
                    current.deposit_enabled |= row.deposit_enabled;
                    current.withdraw_enabled |= row.withdraw_enabled;
                    current.observed_at_ms = current.observed_at_ms.max(observed_at_ms);
                })
                .or_insert(AssetTransferStatus {
                    venue,
                    asset,
                    deposit_enabled: row.deposit_enabled,
                    withdraw_enabled: row.withdraw_enabled,
                    observed_at_ms,
                    source: "feedgrab_jina".to_string(),
                });
        }

        Ok(aggregated.into_values().collect())
    }
}

#[derive(Debug, Deserialize)]
struct ChillybotResponse {
    #[serde(default)]
    data: Vec<ChillybotFundingRow>,
}

#[derive(Debug, Deserialize)]
struct ChillybotCexStatusResponse {
    #[serde(default)]
    meta: Option<ChillybotCexStatusMeta>,
    #[serde(default)]
    data: Vec<ChillybotCexStatusRow>,
}

#[derive(Debug, Deserialize)]
struct ChillybotCexStatusMeta {
    #[serde(rename = "updatedAt")]
    updated_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ChillybotCexStatusRow {
    #[serde(rename = "exchangeId")]
    exchange_id: String,
    #[serde(rename = "asset")]
    asset: String,
    #[serde(rename = "depositEnabled")]
    deposit_enabled: bool,
    #[serde(rename = "withdrawEnabled")]
    withdraw_enabled: bool,
    #[serde(rename = "updatedAt")]
    updated_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ChillybotFundingRow {
    #[serde(rename = "Symbol")]
    symbol: String,
    #[serde(rename = "Price")]
    price: f64,
    #[serde(rename = "FundingRate")]
    funding_rate: f64,
    #[serde(rename = "FundingInterval")]
    funding_interval: f64,
    #[serde(rename = "Exchange")]
    exchange: String,
}

fn map_exchange(exchange: &str) -> Option<Venue> {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "binance" => Some(Venue::Binance),
        "okx" => Some(Venue::Okx),
        "bybit" => Some(Venue::Bybit),
        "hyperliquid" => Some(Venue::Hyperliquid),
        _ => None,
    }
}

fn normalize_interval_hours(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        1.0
    }
}

fn opportunity_score(hint: &OpportunityHint) -> f64 {
    hint.price_diff_pct + (hint.funding_diff_pct_per_hour.abs() * 100.0)
}

fn parse_rfc3339_ms(raw: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

pub fn normalize_symbol_key(symbol: &str) -> String {
    let uppercase = symbol.trim().to_ascii_uppercase();
    let sanitized = uppercase
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    for suffix in ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR"] {
        if sanitized.ends_with(suffix) && sanitized.len() > suffix.len() {
            return sanitized.trim_end_matches(suffix).to_string();
        }
    }
    sanitized
}
