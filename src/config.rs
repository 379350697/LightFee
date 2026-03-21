use std::{collections::BTreeMap, env, fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::models::Venue;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub venues: Vec<VenueConfig>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub directed_pairs: Vec<DirectedPairConfig>,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let config = toml::from_str::<Self>(&raw)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        Ok(config)
    }

    pub fn enabled_venues(&self) -> impl Iterator<Item = &VenueConfig> {
        self.venues.iter().filter(|venue| venue.enabled)
    }

    pub fn venue(&self, venue: Venue) -> Option<&VenueConfig> {
        self.venues
            .iter()
            .find(|item| item.venue == venue && item.enabled)
    }

    pub fn directed_pairs_or_all(&self) -> Vec<DirectedPairConfig> {
        if !self.directed_pairs.is_empty() {
            return self.directed_pairs.clone();
        }

        let venues = self
            .enabled_venues()
            .map(|venue| venue.venue)
            .collect::<Vec<_>>();
        let mut pairs = Vec::new();
        for long in &venues {
            for short in &venues {
                if long != short {
                    pairs.push(DirectedPairConfig {
                        long: *long,
                        short: *short,
                        symbols: Vec::new(),
                    });
                }
            }
        }
        pairs
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    Paper,
    Live,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_runtime_mode")]
    pub mode: RuntimeMode,
    #[serde(default = "default_opportunity_source")]
    pub opportunity_source: OpportunitySourceMode,
    #[serde(default = "default_chillybot_api_base")]
    pub chillybot_api_base: String,
    #[serde(default = "default_chillybot_timeout_ms")]
    pub chillybot_timeout_ms: u64,
    #[serde(default = "default_exchange_http_timeout_ms")]
    pub exchange_http_timeout_ms: u64,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_market_age_ms")]
    pub max_market_age_ms: i64,
    #[serde(default = "default_private_position_max_age_ms")]
    pub private_position_max_age_ms: i64,
    #[serde(default = "default_order_quote_age_ms")]
    pub max_order_quote_age_ms: i64,
    #[serde(default = "default_uncertain_order_cooldown_ms")]
    pub uncertain_order_cooldown_ms: i64,
    #[serde(default = "default_transfer_status_cache_ms")]
    pub transfer_status_cache_ms: u64,
    #[serde(default = "default_tick_failure_backoff_initial_ms")]
    pub tick_failure_backoff_initial_ms: u64,
    #[serde(default = "default_tick_failure_backoff_max_ms")]
    pub tick_failure_backoff_max_ms: u64,
    #[serde(default = "default_ws_reconnect_initial_ms")]
    pub ws_reconnect_initial_ms: u64,
    #[serde(default = "default_ws_reconnect_max_ms")]
    pub ws_reconnect_max_ms: u64,
    #[serde(default = "default_ws_unhealthy_after_failures")]
    pub ws_unhealthy_after_failures: usize,
    #[serde(default = "default_journal_async_queue_capacity")]
    pub journal_async_queue_capacity: usize,
    #[serde(default = "default_true")]
    pub auto_trade_enabled: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            mode: default_runtime_mode(),
            opportunity_source: default_opportunity_source(),
            chillybot_api_base: default_chillybot_api_base(),
            chillybot_timeout_ms: default_chillybot_timeout_ms(),
            exchange_http_timeout_ms: default_exchange_http_timeout_ms(),
            poll_interval_ms: default_poll_interval_ms(),
            max_market_age_ms: default_market_age_ms(),
            private_position_max_age_ms: default_private_position_max_age_ms(),
            max_order_quote_age_ms: default_order_quote_age_ms(),
            uncertain_order_cooldown_ms: default_uncertain_order_cooldown_ms(),
            transfer_status_cache_ms: default_transfer_status_cache_ms(),
            tick_failure_backoff_initial_ms: default_tick_failure_backoff_initial_ms(),
            tick_failure_backoff_max_ms: default_tick_failure_backoff_max_ms(),
            ws_reconnect_initial_ms: default_ws_reconnect_initial_ms(),
            ws_reconnect_max_ms: default_ws_reconnect_max_ms(),
            ws_unhealthy_after_failures: default_ws_unhealthy_after_failures(),
            journal_async_queue_capacity: default_journal_async_queue_capacity(),
            auto_trade_enabled: default_true(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunitySourceMode {
    ExchangeOnly,
    ChillybotFirst,
    ChillybotViaFeedgrab,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaggeredExitMode {
    AfterFirstStage,
    EvaluateSecondStage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyConfig {
    #[serde(default = "default_entry_window_secs")]
    pub entry_window_secs: i64,
    #[serde(default = "default_post_funding_hold_secs")]
    pub post_funding_hold_secs: i64,
    #[serde(default = "default_max_entry_notional")]
    pub max_entry_notional: f64,
    #[serde(default = "default_live_max_entry_notional")]
    pub live_max_entry_notional: f64,
    #[serde(default = "default_forced_live_entry_notional_quote")]
    pub forced_live_entry_notional_quote: f64,
    #[serde(default = "default_min_entry_leg_notional_quote")]
    pub min_entry_leg_notional_quote: f64,
    #[serde(default = "default_max_concurrent_positions")]
    pub max_concurrent_positions: usize,
    #[serde(default = "default_live_target_leverage")]
    pub live_target_leverage: u32,
    #[serde(default = "default_max_scan_minutes_before_funding")]
    pub max_scan_minutes_before_funding: i64,
    #[serde(default = "default_min_scan_minutes_before_funding")]
    pub min_scan_minutes_before_funding: i64,
    #[serde(default = "default_max_stagger_gap_minutes")]
    pub max_stagger_gap_minutes: i64,
    #[serde(default = "default_max_top_book_usage_ratio")]
    pub max_top_book_usage_ratio: f64,
    #[serde(default = "default_staggered_exit_mode")]
    pub staggered_exit_mode: StaggeredExitMode,
    #[serde(default = "default_min_funding_edge_bps")]
    pub min_funding_edge_bps: f64,
    #[serde(default = "default_min_expected_edge_bps")]
    pub min_expected_edge_bps: f64,
    #[serde(default = "default_min_worst_case_edge_bps")]
    pub min_worst_case_edge_bps: f64,
    #[serde(default = "default_exit_slippage_reserve_bps")]
    pub exit_slippage_reserve_bps: f64,
    #[serde(default = "default_execution_buffer_bps")]
    pub execution_buffer_bps: f64,
    #[serde(default = "default_capital_buffer_bps")]
    pub capital_buffer_bps: f64,
    #[serde(default = "default_transfer_healthy_bias_bps")]
    pub transfer_healthy_bias_bps: f64,
    #[serde(default = "default_transfer_unknown_bias_bps")]
    pub transfer_unknown_bias_bps: f64,
    #[serde(default = "default_transfer_degraded_bias_bps")]
    pub transfer_degraded_bias_bps: f64,
    #[serde(default = "default_profit_take_quote")]
    pub profit_take_quote: f64,
    #[serde(default = "default_stop_loss_quote")]
    pub stop_loss_quote: f64,
    #[serde(default = "default_trailing_drawdown_quote")]
    pub trailing_drawdown_quote: f64,
    #[serde(default = "default_hyperliquid_max_submit_ack_p95_ms")]
    pub hyperliquid_max_submit_ack_p95_ms: u64,
    #[serde(default = "default_hyperliquid_submit_ack_window_size")]
    pub hyperliquid_submit_ack_window_size: usize,
    #[serde(default = "default_hyperliquid_submit_ack_min_samples")]
    pub hyperliquid_submit_ack_min_samples: usize,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            entry_window_secs: default_entry_window_secs(),
            post_funding_hold_secs: default_post_funding_hold_secs(),
            max_entry_notional: default_max_entry_notional(),
            live_max_entry_notional: default_live_max_entry_notional(),
            forced_live_entry_notional_quote: default_forced_live_entry_notional_quote(),
            min_entry_leg_notional_quote: default_min_entry_leg_notional_quote(),
            max_concurrent_positions: default_max_concurrent_positions(),
            live_target_leverage: default_live_target_leverage(),
            max_scan_minutes_before_funding: default_max_scan_minutes_before_funding(),
            min_scan_minutes_before_funding: default_min_scan_minutes_before_funding(),
            max_stagger_gap_minutes: default_max_stagger_gap_minutes(),
            max_top_book_usage_ratio: default_max_top_book_usage_ratio(),
            staggered_exit_mode: default_staggered_exit_mode(),
            min_funding_edge_bps: default_min_funding_edge_bps(),
            min_expected_edge_bps: default_min_expected_edge_bps(),
            min_worst_case_edge_bps: default_min_worst_case_edge_bps(),
            exit_slippage_reserve_bps: default_exit_slippage_reserve_bps(),
            execution_buffer_bps: default_execution_buffer_bps(),
            capital_buffer_bps: default_capital_buffer_bps(),
            transfer_healthy_bias_bps: default_transfer_healthy_bias_bps(),
            transfer_unknown_bias_bps: default_transfer_unknown_bias_bps(),
            transfer_degraded_bias_bps: default_transfer_degraded_bias_bps(),
            profit_take_quote: default_profit_take_quote(),
            stop_loss_quote: default_stop_loss_quote(),
            trailing_drawdown_quote: default_trailing_drawdown_quote(),
            hyperliquid_max_submit_ack_p95_ms: default_hyperliquid_max_submit_ack_p95_ms(),
            hyperliquid_submit_ack_window_size: default_hyperliquid_submit_ack_window_size(),
            hyperliquid_submit_ack_min_samples: default_hyperliquid_submit_ack_min_samples(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistenceConfig {
    #[serde(default = "default_event_log_path")]
    pub event_log_path: String,
    #[serde(default = "default_snapshot_path")]
    pub snapshot_path: String,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            event_log_path: default_event_log_path(),
            snapshot_path: default_snapshot_path(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VenueConfig {
    pub venue: Venue,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub taker_fee_bps: f64,
    #[serde(default = "default_max_entry_notional")]
    pub max_notional: f64,
    #[serde(default)]
    pub market_data_file: Option<String>,
    #[serde(default)]
    pub live: LiveVenueConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LiveVenueConfig {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub wallet_base_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub api_secret: Option<String>,
    #[serde(default)]
    pub api_secret_env: Option<String>,
    #[serde(default)]
    pub api_passphrase: Option<String>,
    #[serde(default)]
    pub api_passphrase_env: Option<String>,
    #[serde(default)]
    pub wallet_private_key: Option<String>,
    #[serde(default)]
    pub wallet_private_key_env: Option<String>,
    #[serde(default)]
    pub account_address: Option<String>,
    #[serde(default)]
    pub account_address_env: Option<String>,
    #[serde(default)]
    pub is_testnet: bool,
    #[serde(default)]
    pub post_ack_private_fill_wait_ms: u64,
    #[serde(default)]
    pub symbol_overrides: BTreeMap<String, String>,
}

impl LiveVenueConfig {
    pub fn resolved_api_key(&self) -> Option<String> {
        resolve_secret(&self.api_key, &self.api_key_env)
    }

    pub fn resolved_api_secret(&self) -> Option<String> {
        resolve_secret(&self.api_secret, &self.api_secret_env)
    }

    pub fn resolved_api_passphrase(&self) -> Option<String> {
        resolve_secret(&self.api_passphrase, &self.api_passphrase_env)
    }

    pub fn resolved_wallet_private_key(&self) -> Option<String> {
        resolve_secret(&self.wallet_private_key, &self.wallet_private_key_env)
    }

    pub fn resolved_account_address(&self) -> Option<String> {
        resolve_secret(&self.account_address, &self.account_address_env)
    }

    pub fn resolve_symbol(&self, symbol: &str) -> Option<String> {
        self.symbol_overrides.get(symbol).cloned()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectedPairConfig {
    pub long: Venue,
    pub short: Venue,
    #[serde(default)]
    pub symbols: Vec<String>,
}

impl DirectedPairConfig {
    pub fn allows_symbol(&self, symbol: &str) -> bool {
        self.symbols.is_empty() || self.symbols.iter().any(|item| item == symbol)
    }
}

fn default_runtime_mode() -> RuntimeMode {
    RuntimeMode::Paper
}

fn default_opportunity_source() -> OpportunitySourceMode {
    OpportunitySourceMode::ExchangeOnly
}

fn default_chillybot_api_base() -> String {
    "https://api.chillybot.xyz".to_string()
}

fn default_chillybot_timeout_ms() -> u64 {
    2_000
}

fn default_exchange_http_timeout_ms() -> u64 {
    10_000
}

fn default_poll_interval_ms() -> u64 {
    1_000
}

fn default_market_age_ms() -> i64 {
    5_000
}

fn default_private_position_max_age_ms() -> i64 {
    15_000
}

fn default_order_quote_age_ms() -> i64 {
    6_000
}

fn default_uncertain_order_cooldown_ms() -> i64 {
    30_000
}

fn default_transfer_status_cache_ms() -> u64 {
    300_000
}

fn default_tick_failure_backoff_initial_ms() -> u64 {
    1_000
}

fn default_tick_failure_backoff_max_ms() -> u64 {
    30_000
}

fn default_ws_reconnect_initial_ms() -> u64 {
    1_000
}

fn default_ws_reconnect_max_ms() -> u64 {
    30_000
}

fn default_ws_unhealthy_after_failures() -> usize {
    5
}

fn default_journal_async_queue_capacity() -> usize {
    4_096
}

fn default_entry_window_secs() -> i64 {
    900
}

fn default_post_funding_hold_secs() -> i64 {
    30
}

fn default_max_entry_notional() -> f64 {
    1_000.0
}

fn default_live_max_entry_notional() -> f64 {
    50.0
}

fn default_forced_live_entry_notional_quote() -> f64 {
    50.0
}

fn default_min_entry_leg_notional_quote() -> f64 {
    8.0
}

fn default_max_concurrent_positions() -> usize {
    6
}

fn default_live_target_leverage() -> u32 {
    4
}

fn default_max_scan_minutes_before_funding() -> i64 {
    25
}

fn default_min_scan_minutes_before_funding() -> i64 {
    5
}

fn default_max_stagger_gap_minutes() -> i64 {
    480
}

fn default_max_top_book_usage_ratio() -> f64 {
    0.85
}

fn default_staggered_exit_mode() -> StaggeredExitMode {
    StaggeredExitMode::AfterFirstStage
}

fn default_min_funding_edge_bps() -> f64 {
    8.0
}

fn default_min_expected_edge_bps() -> f64 {
    3.0
}

fn default_min_worst_case_edge_bps() -> f64 {
    1.0
}

fn default_exit_slippage_reserve_bps() -> f64 {
    4.0
}

fn default_execution_buffer_bps() -> f64 {
    2.0
}

fn default_capital_buffer_bps() -> f64 {
    1.0
}

fn default_transfer_healthy_bias_bps() -> f64 {
    0.25
}

fn default_transfer_unknown_bias_bps() -> f64 {
    0.0
}

fn default_transfer_degraded_bias_bps() -> f64 {
    -0.5
}

fn default_profit_take_quote() -> f64 {
    40.0
}

fn default_stop_loss_quote() -> f64 {
    30.0
}

fn default_trailing_drawdown_quote() -> f64 {
    20.0
}

fn default_hyperliquid_max_submit_ack_p95_ms() -> u64 {
    1_200
}

fn default_hyperliquid_submit_ack_window_size() -> usize {
    5
}

fn default_hyperliquid_submit_ack_min_samples() -> usize {
    3
}

fn default_event_log_path() -> String {
    "runtime/events.jsonl".to_string()
}

fn default_snapshot_path() -> String {
    "runtime/state.json".to_string()
}

fn default_true() -> bool {
    true
}

fn resolve_secret(value: &Option<String>, env_name: &Option<String>) -> Option<String> {
    normalize_secret(value.clone()).or_else(|| {
        env_name
            .as_ref()
            .and_then(|name| {
                env::var(name)
                    .ok()
                    .or_else(|| env_alias(name).and_then(|alias| env::var(alias).ok()))
            })
            .and_then(|item| normalize_secret(Some(item)))
    })
}

fn env_alias(name: &str) -> Option<String> {
    if name == "LIGHTFEE_HYPERLIQUID_PRIVATE_KEY" {
        return Some("FEETL_HYPERLIQUID_API_SECRET".to_string());
    }
    name.strip_prefix("LIGHTFEE_")
        .map(|suffix| format!("FEETL_{suffix}"))
}

fn normalize_secret(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::resolve_secret;

    #[test]
    fn resolve_secret_trims_inline_values() {
        let resolved = resolve_secret(&Some(" 0xabc123\r\n".to_string()), &None);
        assert_eq!(resolved.as_deref(), Some("0xabc123"));
    }

    #[test]
    fn resolve_secret_trims_env_values() {
        let env_name = "LIGHTFEE_TEST_SECRET_TRIM";
        // SAFETY: test scope controls this temporary variable and restores it before exit.
        unsafe { env::set_var(env_name, " 0xdef456\r\n") };
        let resolved = resolve_secret(&None, &Some(env_name.to_string()));
        // SAFETY: paired with the temporary set_var above.
        unsafe { env::remove_var(env_name) };
        assert_eq!(resolved.as_deref(), Some("0xdef456"));
    }

    #[test]
    fn resolve_secret_falls_back_to_feetl_alias() {
        let env_name = "LIGHTFEE_TEST_ALIAS_SECRET";
        let alias_name = "FEETL_TEST_ALIAS_SECRET";
        // SAFETY: test scope controls these temporary variables and restores them before exit.
        unsafe {
            env::remove_var(env_name);
            env::set_var(alias_name, " 0xfeedbeef\r\n");
        }
        let resolved = resolve_secret(&None, &Some(env_name.to_string()));
        // SAFETY: paired with the temporary set_var above.
        unsafe {
            env::remove_var(env_name);
            env::remove_var(alias_name);
        }
        assert_eq!(resolved.as_deref(), Some("0xfeedbeef"));
    }

    #[test]
    fn resolve_secret_maps_hyperliquid_private_key_to_legacy_feetl_name() {
        let env_name = "LIGHTFEE_HYPERLIQUID_PRIVATE_KEY";
        let alias_name = "FEETL_HYPERLIQUID_API_SECRET";
        // SAFETY: test scope controls these temporary variables and restores them before exit.
        unsafe {
            env::remove_var(env_name);
            env::set_var(alias_name, " 0xhyper\r\n");
        }
        let resolved = resolve_secret(&None, &Some(env_name.to_string()));
        // SAFETY: paired with the temporary set_var above.
        unsafe {
            env::remove_var(env_name);
            env::remove_var(alias_name);
        }
        assert_eq!(resolved.as_deref(), Some("0xhyper"));
    }
}
