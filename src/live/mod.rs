mod binance;
mod bybit;
mod common;
mod hyperliquid;
mod okx;
mod private_ws;
mod ws;

pub use binance::BinanceLiveAdapter;
pub use bybit::BybitLiveAdapter;
pub use hyperliquid::HyperliquidLiveAdapter;
pub use okx::OkxLiveAdapter;

pub(crate) use common::{
    base_asset, build_http_client, build_query, estimate_fee_quote, floor_to_step, format_decimal,
    hinted_fill, hmac_sha256_base64, hmac_sha256_hex, iso8601_from_ms, now_ms, parse_bool_flag,
    parse_f64, parse_i64, quote_fill, venue_symbol,
};
pub(crate) use private_ws::{
    enrich_fill_from_private, lookup_or_wait_private_order, PrivateOrderUpdate, WsPrivateState,
};
pub(crate) use ws::{
    merged_quote_snapshot, parse_text_message, spawn_ws_loop, WsBookQuote, WsMarketState,
};
