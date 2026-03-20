use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    #[default]
    Binance,
    Okx,
    Bybit,
    Hyperliquid,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Okx => "okx",
            Self::Bybit => "bybit",
            Self::Hyperliquid => "hyperliquid",
        }
    }
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Venue {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "binance" => Ok(Self::Binance),
            "okx" => Ok(Self::Okx),
            "bybit" => Ok(Self::Bybit),
            "hyperliquid" => Ok(Self::Hyperliquid),
            other => Err(format!("unsupported venue: {other}")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    pub fn signed_qty(self, quantity: f64) -> f64 {
        match self {
            Self::Buy => quantity,
            Self::Sell => -quantity,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingOpportunityType {
    #[default]
    Aligned,
    Staggered,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingLeg {
    #[default]
    Long,
    Short,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SymbolMarketSnapshot {
    pub symbol: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub funding_rate: f64,
    pub funding_timestamp_ms: i64,
}

impl SymbolMarketSnapshot {
    pub fn mid_price(&self) -> f64 {
        (self.best_bid + self.best_ask) / 2.0
    }

    pub fn spread_bps(&self) -> f64 {
        let mid = self.mid_price();
        if mid <= 0.0 {
            return 0.0;
        }

        ((self.best_ask - self.best_bid) / mid) * 10_000.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VenueMarketSnapshot {
    pub venue: Venue,
    pub observed_at_ms: i64,
    pub symbols: Vec<SymbolMarketSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub venue: Venue,
    pub symbol: String,
    pub size: f64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssetTransferStatus {
    pub venue: Venue,
    pub asset: String,
    pub deposit_enabled: bool,
    pub withdraw_enabled: bool,
    pub observed_at_ms: i64,
    pub source: String,
}

impl AssetTransferStatus {
    pub fn healthy(&self) -> bool {
        self.deposit_enabled && self.withdraw_enabled
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: Side,
    pub quantity: f64,
    pub reduce_only: bool,
    pub client_order_id: String,
    #[serde(default)]
    pub price_hint: Option<f64>,
    #[serde(default)]
    pub observed_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderFill {
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub quantity: f64,
    pub average_price: f64,
    pub fee_quote: f64,
    pub order_id: String,
    pub filled_at_ms: i64,
    #[serde(default)]
    pub timing: Option<OrderExecutionTiming>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderExecutionTiming {
    #[serde(default)]
    pub quote_resolve_ms: Option<u64>,
    #[serde(default)]
    pub order_prepare_ms: Option<u64>,
    #[serde(default)]
    pub submit_ack_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CandidateOpportunity {
    pub pair_id: String,
    pub symbol: String,
    pub long_venue: Venue,
    pub short_venue: Venue,
    pub quantity: f64,
    pub entry_notional_quote: f64,
    pub funding_timestamp_ms: i64,
    #[serde(default)]
    pub long_funding_timestamp_ms: i64,
    #[serde(default)]
    pub short_funding_timestamp_ms: i64,
    #[serde(default)]
    pub opportunity_type: FundingOpportunityType,
    #[serde(default)]
    pub first_funding_leg: FundingLeg,
    #[serde(default)]
    pub first_funding_timestamp_ms: i64,
    #[serde(default)]
    pub second_funding_timestamp_ms: i64,
    pub funding_edge_bps: f64,
    #[serde(default)]
    pub total_funding_edge_bps: f64,
    #[serde(default)]
    pub first_stage_funding_edge_bps: f64,
    #[serde(default)]
    pub first_stage_expected_edge_bps: f64,
    #[serde(default)]
    pub second_stage_incremental_funding_edge_bps: f64,
    #[serde(default)]
    pub stagger_gap_ms: i64,
    pub entry_cross_bps: f64,
    pub fee_bps: f64,
    pub entry_slippage_bps: f64,
    pub expected_edge_bps: f64,
    pub worst_case_edge_bps: f64,
    #[serde(default)]
    pub ranking_edge_bps: f64,
    #[serde(default)]
    pub transfer_bias_bps: f64,
    #[serde(default)]
    pub transfer_state: Option<String>,
    #[serde(default)]
    pub advisories: Vec<String>,
    pub blocked_reasons: Vec<String>,
}

impl CandidateOpportunity {
    pub fn is_tradeable(&self) -> bool {
        self.blocked_reasons.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{OrderExecutionTiming, OrderFill, Side, Venue};

    #[test]
    fn order_fill_serializes_optional_timing() {
        let fill = OrderFill {
            venue: Venue::Hyperliquid,
            symbol: "ETHUSDT".to_string(),
            side: Side::Buy,
            quantity: 0.011,
            average_price: 2145.9,
            fee_quote: 0.0007,
            order_id: "123".to_string(),
            filled_at_ms: 42,
            timing: Some(OrderExecutionTiming {
                quote_resolve_ms: Some(3),
                order_prepare_ms: Some(5),
                submit_ack_ms: Some(1200),
            }),
        };

        let value = serde_json::to_value(fill).expect("serialize order fill");
        assert_eq!(
            value["timing"],
            json!({
                "quote_resolve_ms": 3,
                "order_prepare_ms": 5,
                "submit_ack_ms": 1200,
            })
        );
    }
}
