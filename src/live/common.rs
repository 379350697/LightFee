use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{SecondsFormat, TimeZone, Utc};
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;
use url::form_urlencoded;

use crate::{
    config::VenueConfig,
    models::{OrderRequest, Side, Venue, VenueMarketSnapshot},
    opportunity_source::normalize_symbol_key,
};

type HmacSha256 = Hmac<Sha256>;

pub fn build_http_client(timeout_ms: u64) -> Result<Client> {
    let timeout = Duration::from_millis(timeout_ms.max(250));
    let connect_timeout = Duration::from_millis((timeout_ms / 2).clamp(250, 3_000));
    Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .tcp_keepalive(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(4)
        .build()
        .context("failed to build live http client")
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

pub fn iso8601_from_ms(timestamp_ms: i64) -> String {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn hmac_sha256_hex(secret: &str, payload: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .context("failed to initialize hmac sha256")?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn hmac_sha256_base64(secret: &str, payload: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .context("failed to initialize hmac sha256")?;
    mac.update(payload.as_bytes());
    Ok(BASE64_STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn build_query(params: &[(&str, String)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

pub fn parse_f64(raw: &str) -> Result<f64> {
    raw.parse::<f64>()
        .with_context(|| format!("failed to parse float: {raw}"))
}

pub fn parse_i64(raw: &str) -> Result<i64> {
    raw.parse::<i64>()
        .with_context(|| format!("failed to parse integer: {raw}"))
}

pub fn parse_bool_flag(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "open"
    )
}

pub fn floor_to_step(value: f64, step: f64) -> f64 {
    if !value.is_finite() || !step.is_finite() || value <= 0.0 || step <= 0.0 {
        return 0.0;
    }

    let factor = 1.0 / step;
    let scaled = (value * factor + 1e-9).floor();
    if scaled <= 0.0 {
        0.0
    } else {
        scaled / factor
    }
}

pub fn format_decimal(value: f64, scale_hint: f64) -> String {
    let decimals = decimals_from_step(scale_hint);
    format!("{value:.decimals$}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

pub fn estimate_fee_quote(price: f64, quantity: f64, taker_fee_bps: f64) -> f64 {
    price * quantity * taker_fee_bps / 10_000.0
}

pub fn quote_fill(snapshot: &VenueMarketSnapshot, symbol: &str, side: Side) -> Result<(f64, i64)> {
    let quote = snapshot
        .symbols
        .iter()
        .find(|item| item.symbol == symbol)
        .ok_or_else(|| anyhow!("missing live market quote for {symbol}"))?;
    let price = match side {
        Side::Buy => quote.best_ask,
        Side::Sell => quote.best_bid,
    };
    Ok((price, snapshot.observed_at_ms))
}

pub fn hinted_fill(request: &OrderRequest) -> Option<(f64, i64)> {
    request
        .price_hint
        .zip(request.observed_at_ms)
        .filter(|(price, ts_ms)| price.is_finite() && *price > 0.0 && *ts_ms > 0)
}

pub fn base_asset(symbol: &str) -> String {
    normalize_symbol_key(symbol)
}

pub fn venue_symbol(config: &VenueConfig, symbol: &str) -> String {
    if let Some(mapped) = config.live.resolve_symbol(symbol) {
        return mapped;
    }

    let (base, quote) = split_symbol(symbol);
    match config.venue {
        Venue::Binance | Venue::Bybit | Venue::Aster => format!("{base}{quote}"),
        Venue::Okx => format!("{base}-{quote}-SWAP"),
        Venue::Bitget => format!("{base}{quote}_UMCBL"),
        Venue::Gate => format!("{base}_{quote}"),
        Venue::Hyperliquid => base,
    }
}

fn split_symbol(symbol: &str) -> (String, String) {
    let raw = symbol.trim().to_ascii_uppercase().replace('-', "");
    for quote in ["USDT", "USDC", "BUSD", "USD"] {
        if raw.ends_with(quote) && raw.len() > quote.len() {
            return (raw.trim_end_matches(quote).to_string(), quote.to_string());
        }
    }

    (normalize_symbol_key(&raw), "USDT".to_string())
}

fn decimals_from_step(step: f64) -> usize {
    if step <= 0.0 {
        return 8;
    }

    let rendered = format!("{step:.12}");
    rendered
        .split('.')
        .nth(1)
        .map(|fraction| fraction.trim_end_matches('0').len())
        .unwrap_or_default()
        .min(12)
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{LiveVenueConfig, VenueConfig},
        models::{OrderRequest, Side, Venue},
    };

    use super::{floor_to_step, hinted_fill, venue_symbol};

    #[test]
    fn floor_to_step_keeps_boundary_values() {
        assert!((floor_to_step(0.01, 0.01) - 0.01).abs() <= 1e-9);
        assert!((floor_to_step(0.1, 0.01) - 0.1).abs() <= 1e-9);
    }

    #[test]
    fn hinted_fill_prefers_request_hint() {
        let request = OrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            quantity: 1.0,
            reduce_only: false,
            client_order_id: "test".to_string(),
            price_hint: Some(123.45),
            mark_price_hint: None,
            observed_at_ms: Some(9_999),
        };

        assert_eq!(hinted_fill(&request), Some((123.45, 9_999)));
    }

    #[test]
    fn venue_symbol_formats_bitget_and_gate_contracts() {
        let bitget = VenueConfig {
            venue: Venue::Bitget,
            enabled: true,
            taker_fee_bps: 1.0,
            max_notional: 100.0,
            market_data_file: None,
            live: LiveVenueConfig::default(),
        };
        let gate = VenueConfig {
            venue: Venue::Gate,
            enabled: true,
            taker_fee_bps: 1.0,
            max_notional: 100.0,
            market_data_file: None,
            live: LiveVenueConfig::default(),
        };

        assert_eq!(venue_symbol(&bitget, "BTCUSDT"), "BTCUSDT_UMCBL");
        assert_eq!(venue_symbol(&gate, "BTCUSDT"), "BTC_USDT");
    }
}
