use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::Instant,
};

use anyhow::{anyhow, Result};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    config::{AppConfig, RuntimeMode, StaggeredExitMode},
    journal::JsonlJournal,
    live::{
        cache_is_fresh, load_json_cache, now_ms, quote_fill, store_json_cache, SYMBOL_CACHE_TTL_MS,
    },
    market::MarketView,
    models::{
        AccountFeeSnapshot, CandidateOpportunity, FundingLeg, FundingOpportunityType,
        OrderFillReconciliation, OrderRequest, PerpLiquiditySnapshot, Side, Venue,
    },
    opportunity_source::{
        normalize_symbol_key, OpportunityHint, OpportunityHintSource, TransferStatusSource,
    },
    store::FileStateStore,
    strategy::{
        discover_candidates, has_near_term_settlement_leg, is_within_funding_scan_window_ms,
        sort_candidates,
    },
    transfer::TransferStatusView,
    venue::VenueAdapter,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineMode {
    Running,
    Recovering,
    FailClosed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanSnapshot {
    pub candidate_count: usize,
    pub tradeable_count: usize,
    pub best_candidate: Option<CandidateOpportunity>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenPosition {
    pub position_id: String,
    pub symbol: String,
    pub long_venue: Venue,
    pub short_venue: Venue,
    pub quantity: f64,
    #[serde(default)]
    pub initial_quantity: f64,
    pub long_entry_price: f64,
    pub short_entry_price: f64,
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
    pub second_funding_timestamp_ms: i64,
    pub funding_edge_bps_entry: f64,
    #[serde(default)]
    pub total_funding_edge_bps_entry: f64,
    pub expected_edge_bps_entry: f64,
    #[serde(default)]
    pub worst_case_edge_bps_entry: f64,
    #[serde(default)]
    pub entry_cross_bps_entry: f64,
    #[serde(default)]
    pub fee_bps_entry: f64,
    #[serde(default)]
    pub entry_slippage_bps_entry: f64,
    #[serde(default)]
    pub entry_depth_capped_at_entry: bool,
    pub total_entry_fee_quote: f64,
    #[serde(default)]
    pub realized_price_pnl_quote: f64,
    #[serde(default)]
    pub realized_exit_fee_quote: f64,
    pub entered_at_ms: i64,
    pub current_net_quote: f64,
    pub peak_net_quote: f64,
    pub funding_captured: bool,
    #[serde(default)]
    pub second_stage_funding_captured: bool,
    pub captured_funding_quote: f64,
    #[serde(default)]
    pub second_stage_funding_quote: f64,
    #[serde(default)]
    pub settlement_half_closed_quantity: f64,
    #[serde(default)]
    pub settlement_half_closed_at_ms: i64,
    #[serde(default = "default_exit_after_first_stage")]
    pub exit_after_first_stage: bool,
    #[serde(default)]
    pub second_stage_enabled_at_entry: bool,
    pub exit_reason: Option<String>,
}

const ENTRY_SELECTION_BUFFER_MULTIPLIER: usize = 4;
const ENTRY_SELECTION_BUFFER_MAX: usize = 8;
const ENTRY_LIQUIDITY_SCREEN_MULTIPLIER: usize = 3;
const ENTRY_LIQUIDITY_SCREEN_MAX: usize = 24;
const SETTLEMENT_HALF_CLOSE_RATIO: f64 = 0.5;
const SETTLEMENT_REMAINDER_CLOSE_DELAY_MS: i64 = 5 * 60 * 1_000;
const SETTLEMENT_FORCE_CLOSE_DELAY_MS: i64 = 20 * 60 * 1_000;
const CLOSE_RECONCILIATION_RETRY_BASE_MS: i64 = 30 * 1_000;
const MARKET_DATA_WINDOW_PREWARM_MS: i64 = 2 * 60 * 1_000;
const MARKET_DATA_WINDOW_LINGER_MS: i64 = 3 * 60 * 1_000;
const CLOSE_RECONCILIATION_RETRY_MAX_MS: i64 = 5 * 60 * 1_000;
const CEX_MIN_PERP_VOLUME_24H_QUOTE: f64 = 5_000_000.0;
const HYPERLIQUID_MIN_PERP_VOLUME_24H_QUOTE: f64 = 1_000_000.0;
const MIN_PERP_OPEN_INTEREST_QUOTE: f64 = 1_000_000.0;

pub fn apply_cached_fee_snapshots_to_candidates(
    config: &AppConfig,
    candidates: &mut [CandidateOpportunity],
    fee_snapshots: &BTreeMap<Venue, AccountFeeSnapshot>,
) -> BTreeMap<Venue, f64> {
    let mut fallback_venues = BTreeMap::new();

    for candidate in candidates {
        let (long_fee_bps, long_fallback) =
            resolved_taker_fee_bps(config, fee_snapshots, candidate.long_venue);
        if long_fallback {
            fallback_venues
                .entry(candidate.long_venue)
                .or_insert(long_fee_bps);
        }
        let (short_fee_bps, short_fallback) =
            resolved_taker_fee_bps(config, fee_snapshots, candidate.short_venue);
        if short_fallback {
            fallback_venues
                .entry(candidate.short_venue)
                .or_insert(short_fee_bps);
        }

        candidate.fee_bps = long_fee_bps + short_fee_bps;
        candidate.expected_edge_bps = candidate.funding_edge_bps
            - candidate.fee_bps
            - candidate.entry_slippage_bps
            - config.strategy.exit_slippage_reserve_bps
            - config.strategy.capital_buffer_bps;
        candidate.worst_case_edge_bps =
            candidate.expected_edge_bps - config.strategy.execution_buffer_bps;
        candidate.first_stage_expected_edge_bps = candidate.first_stage_funding_edge_bps
            - candidate.fee_bps
            - candidate.entry_slippage_bps
            - config.strategy.exit_slippage_reserve_bps
            - config.strategy.capital_buffer_bps;
        candidate.ranking_edge_bps = candidate.worst_case_edge_bps + candidate.transfer_bias_bps;

        candidate.blocked_reasons.retain(|reason| {
            reason != "expected_edge_below_floor" && reason != "worst_case_edge_below_floor"
        });
        if candidate.expected_edge_bps < config.strategy.min_expected_edge_bps {
            candidate
                .blocked_reasons
                .push("expected_edge_below_floor".to_string());
        }
        if candidate.worst_case_edge_bps < config.strategy.min_worst_case_edge_bps {
            candidate
                .blocked_reasons
                .push("worst_case_edge_below_floor".to_string());
        }
        candidate.blocked_reasons.sort();
        candidate.blocked_reasons.dedup();
    }

    fallback_venues
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct FeeResolutionLogEntry {
    venue: Venue,
    taker_fee_bps: f64,
    source: &'static str,
    used_fallback: bool,
}

fn resolved_taker_fee_bps(
    config: &AppConfig,
    fee_snapshots: &BTreeMap<Venue, AccountFeeSnapshot>,
    venue: Venue,
) -> (f64, bool) {
    if let Some(snapshot) = fee_snapshots.get(&venue) {
        return (snapshot.taker_fee_bps, false);
    }

    (
        config
            .venue(venue)
            .map(|venue_config| venue_config.taker_fee_bps)
            .unwrap_or_default(),
        true,
    )
}

fn resolved_fee_log_entry(
    config: &AppConfig,
    fee_snapshots: &BTreeMap<Venue, AccountFeeSnapshot>,
    venue: Venue,
) -> FeeResolutionLogEntry {
    if let Some(snapshot) = fee_snapshots.get(&venue) {
        return FeeResolutionLogEntry {
            venue,
            taker_fee_bps: snapshot.taker_fee_bps,
            source: "account_fee_snapshot",
            used_fallback: false,
        };
    }

    FeeResolutionLogEntry {
        venue,
        taker_fee_bps: config
            .venue(venue)
            .map(|venue_config| venue_config.taker_fee_bps)
            .unwrap_or_default(),
        source: "configured_taker_fee_bps",
        used_fallback: true,
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngineState {
    pub cycle: u64,
    pub mode: EngineMode,
    pub last_market_ts_ms: Option<i64>,
    pub last_scan: Option<ScanSnapshot>,
    #[serde(default)]
    pub open_positions: Vec<OpenPosition>,
    #[serde(default)]
    pub open_position: Option<OpenPosition>,
    pub last_error: Option<String>,
    #[serde(default)]
    pub venue_health: BTreeMap<Venue, PersistedVenueHealthState>,
    #[serde(default)]
    pending_close_reconciliations: Vec<PendingCloseReconciliation>,
}

impl Default for EngineState {
    fn default() -> Self {
        Self {
            cycle: 0,
            mode: EngineMode::Running,
            last_market_ts_ms: None,
            last_scan: None,
            open_positions: Vec::new(),
            open_position: None,
            last_error: None,
            venue_health: BTreeMap::new(),
            pending_close_reconciliations: Vec::new(),
        }
    }
}

pub struct Engine {
    config: AppConfig,
    adapters: BTreeMap<Venue, Arc<dyn VenueAdapter>>,
    opportunity_source: Option<Arc<dyn OpportunityHintSource>>,
    transfer_status_source: Option<Arc<dyn TransferStatusSource>>,
    journal: JsonlJournal,
    store: FileStateStore,
    state: EngineState,
    last_live_recovery_probe_ms: Option<i64>,
    last_running_slot_reconcile_probe_ms: Option<i64>,
    last_persisted_state: Option<EngineState>,
    recent_submit_ack_ms: BTreeMap<Venue, VecDeque<u64>>,
    venue_entry_cooldowns: BTreeMap<Venue, VenueEntryCooldown>,
    recent_order_health: BTreeMap<Venue, VecDeque<VenueOrderHealthSample>>,
    venue_health_updated_at_ms: BTreeMap<Venue, i64>,
    cached_transfer_status_view: Option<CachedTransferStatusView>,
    scan_symbols: Vec<String>,
    market_data_active: bool,
    market_data_activated_at_ms: Option<i64>,
    last_no_entry_diagnostics: Option<NoEntryDiagnosticsLogState>,
    repeated_failure_logs: BTreeMap<String, RepeatedFailureLogState>,
    cached_live_entry_venue_filter: Option<CachedLiveEntryVenueFilter>,
}

#[derive(Clone, Debug, Serialize)]
struct QuantityPlan {
    requested_quantity: f64,
    long_requested_quantity: f64,
    short_requested_quantity: f64,
    common_requested_quantity: f64,
    long_executable_quantity: f64,
    short_executable_quantity: f64,
    executable_quantity: f64,
}

struct CloseExecution {
    short_fill: crate::models::OrderFill,
    short_client_order_id: String,
    short_submit_started_at_ms: i64,
    short_latency_ms: u64,
    long_fill: crate::models::OrderFill,
    long_client_order_id: String,
    long_submit_started_at_ms: i64,
    long_latency_ms: u64,
    realized_price_pnl_quote: f64,
    total_exit_fee_quote: f64,
}

struct EntryQuoteRefreshLeg {
    venue: Venue,
    side: Side,
    stage: &'static str,
    expired_reason: String,
    age_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
struct EntryQuoteVerificationLeg {
    venue: Venue,
    side: Side,
    stage: &'static str,
    expired_reason: String,
    previous_observed_at_ms: Option<i64>,
    pre_refresh_age_ms: Option<i64>,
    refresh_succeeded: bool,
    refresh_error: Option<String>,
    refreshed_observed_at_ms: Option<i64>,
    post_refresh_age_ms: Option<i64>,
    refreshed_price_hint: Option<f64>,
}

#[derive(Default)]
struct EntryQuoteRefreshResult {
    block_reason: Option<String>,
    refreshed_leg_count: usize,
    max_pre_refresh_age_ms: Option<i64>,
    legs: Vec<EntryQuoteVerificationLeg>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CloseReconciliationKind {
    Partial,
    Final,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct CloseLegRecord {
    venue: Venue,
    side: Side,
    order_id: String,
    client_order_id: String,
    quantity: f64,
    average_price: f64,
    fee_quote: f64,
    filled_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PendingCloseReconciliation {
    position_id: String,
    symbol: String,
    kind: CloseReconciliationKind,
    reason: String,
    position_snapshot: OpenPosition,
    original_payload: serde_json::Value,
    long_leg: CloseLegRecord,
    short_leg: CloseLegRecord,
    closed_at_ms: i64,
    created_cycle: u64,
    next_attempt_ms: i64,
    attempt_count: u32,
}

struct PartialClosePlan {
    quantity: f64,
    promoted_to_full_close: bool,
    min_notional_violation: Option<(Venue, f64, f64)>,
}

#[derive(Clone, Copy)]
struct OrderLegContext<'a> {
    stage: &'a str,
    position_id: &'a str,
    pair_id: Option<&'a str>,
}

#[derive(Clone, Debug)]
struct VenueEntryCooldown {
    until_wall_clock_ms: i64,
    reason: String,
}

#[derive(Clone, Debug)]
struct CachedTransferStatusView {
    observed_at_ms: i64,
    view: TransferStatusView,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
struct VenueOrderHealthSample {
    failed: bool,
    uncertain: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersistedVenueHealthState {
    #[serde(default)]
    updated_at_ms: i64,
    #[serde(default)]
    recent_submit_ack_ms: Vec<u64>,
    #[serde(default)]
    recent_order_health: Vec<VenueOrderHealthSample>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedScanPair {
    long: Venue,
    short: Venue,
    symbols: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedScanSymbolCache {
    updated_at_ms: i64,
    requested_symbols: Vec<String>,
    pairs: Vec<PersistedScanPair>,
    scan_symbols: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HedgeLeg {
    Long,
    Short,
}

impl HedgeLeg {
    fn label(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }

    fn entry_stage(self) -> &'static str {
        match self {
            Self::Long => "entry_long",
            Self::Short => "entry_short",
        }
    }

    fn cleanup_stage(self) -> &'static str {
        match self {
            Self::Long => "entry_cleanup_long",
            Self::Short => "entry_cleanup_short",
        }
    }

    fn compensate_stage(self) -> &'static str {
        match self {
            Self::Long => "entry_compensate_long",
            Self::Short => "entry_compensate_short",
        }
    }
}

#[derive(Clone, Debug)]
struct EntryLegPlan {
    leg: HedgeLeg,
    venue: Venue,
    request: OrderRequest,
}

#[derive(Clone, Debug, Serialize)]
struct EntryLegRisk {
    venue: Venue,
    leg: &'static str,
    total_score: f64,
    health_score: f64,
    submit_ack_score: f64,
    depth_score: f64,
}

#[derive(Clone, Debug, Serialize)]
struct OpportunityChecklistItem {
    key: &'static str,
    ok: bool,
    detail: String,
}

#[derive(Clone, Debug, Serialize)]
struct OpportunityCandidateMetrics {
    quantity: f64,
    entry_notional_quote: f64,
    funding_edge_bps: f64,
    expected_edge_bps: f64,
    worst_case_edge_bps: f64,
    ranking_edge_bps: f64,
    entry_cross_bps: f64,
    fee_bps: f64,
    entry_slippage_bps: f64,
    first_stage_expected_edge_bps: f64,
    second_stage_incremental_funding_edge_bps: f64,
}

#[derive(Clone, Debug, Serialize)]
struct OpportunityCandidateChecklist {
    pair_id: String,
    symbol: String,
    long_venue: Venue,
    short_venue: Venue,
    tradeable: bool,
    selected_for_entry: bool,
    selection_score: f64,
    selection_blocker: Option<String>,
    long_leg_risk: EntryLegRisk,
    short_leg_risk: EntryLegRisk,
    metrics: OpportunityCandidateMetrics,
    blocked_reasons: Vec<String>,
    advisories: Vec<String>,
    checklist: Vec<OpportunityChecklistItem>,
}

#[derive(Clone, Debug, Serialize)]
struct NoEntryDiagnostics {
    reason: String,
    candidate_count: usize,
    tradeable_count: usize,
    selected_candidate_count: usize,
    active_position_count: usize,
    remaining_slots: usize,
    candidate_detail_count: usize,
    omitted_candidate_count: usize,
    suppressed_repeat_count: usize,
    blocked_reason_counts: BTreeMap<String, usize>,
    advisory_counts: BTreeMap<String, usize>,
    candidates: Vec<OpportunityCandidateChecklist>,
}

#[derive(Clone, Debug, Default)]
struct NoEntryDiagnosticsLogState {
    fingerprint: String,
    last_emitted_cycle: u64,
    suppressed_count: usize,
}

fn remap_no_entry_diagnostic_reason(reason: &str) -> (Option<String>, Option<String>) {
    if reason.starts_with("entry_quote_stale:") {
        return (
            None,
            Some(reason.replacen("entry_quote_stale:", "entry_quote_refresh_needed:", 1)),
        );
    }
    (Some(reason.to_string()), None)
}

#[derive(Clone, Debug, Default)]
struct RepeatedFailureLogState {
    last_emitted_cycle: u64,
    suppressed_count: usize,
}

#[derive(Clone, Debug)]
struct LiveEntryVenueStatus {
    eligible: bool,
    reason: String,
    effective_balance_quote: Option<f64>,
    equity_quote: Option<f64>,
    available_balance_quote: Option<f64>,
    error: Option<String>,
}

#[derive(Clone, Debug)]
struct CachedLiveEntryVenueFilter {
    observed_at_ms: i64,
    eligible_venues: BTreeSet<Venue>,
}

const FLAT_RECOVERY_PROBE_INTERVAL_MS: i64 = 30_000;
const RUNNING_SLOT_RECONCILE_INTERVAL_MS: i64 = 10_000;
const ORDER_HEALTH_WINDOW_SIZE: usize = 20;
const NO_ENTRY_DIAGNOSTIC_CANDIDATE_LIMIT: usize = 8;
const PROACTIVE_ENTRY_QUOTE_REFRESH_MULTIPLIER: i64 = 2;
const NO_ENTRY_DIAGNOSTIC_SAMPLE_INTERVAL_CYCLES: u64 = 10;
const REPEATED_FAILURE_LOG_SAMPLE_INTERVAL_CYCLES: u64 = 20;
const REPEATED_FAILURE_LOG_RETENTION_CYCLES: u64 = 200;
const RECONCILIATION_SUMMARY_ONLY_DELTA_QUOTE: f64 = 0.05;
const LIVE_ENTRY_VENUE_FILTER_TTL_MS: i64 = 60_000;
const MIN_LIVE_ENTRY_VENUE_BALANCE_QUOTE: f64 = 50.0;

fn default_exit_after_first_stage() -> bool {
    true
}

fn entry_selection_target(remaining_slots: usize) -> usize {
    remaining_slots
        .saturating_mul(ENTRY_SELECTION_BUFFER_MULTIPLIER)
        .clamp(remaining_slots, ENTRY_SELECTION_BUFFER_MAX)
}

fn perp_liquidity_thresholds(venue: Venue) -> (f64, f64) {
    let min_volume_24h_quote = match venue {
        Venue::Hyperliquid | Venue::Aster => HYPERLIQUID_MIN_PERP_VOLUME_24H_QUOTE,
        Venue::Binance | Venue::Okx | Venue::Bybit | Venue::Bitget | Venue::Gate => {
            CEX_MIN_PERP_VOLUME_24H_QUOTE
        }
    };
    (min_volume_24h_quote, MIN_PERP_OPEN_INTEREST_QUOTE)
}

impl Engine {
    pub async fn new(config: AppConfig, adapters: Vec<Arc<dyn VenueAdapter>>) -> Result<Self> {
        Self::with_sources(config, adapters, None, None).await
    }

    pub async fn with_opportunity_source(
        config: AppConfig,
        adapters: Vec<Arc<dyn VenueAdapter>>,
        opportunity_source: Option<Arc<dyn OpportunityHintSource>>,
    ) -> Result<Self> {
        Self::with_sources(config, adapters, opportunity_source, None).await
    }

    pub async fn with_sources(
        config: AppConfig,
        adapters: Vec<Arc<dyn VenueAdapter>>,
        opportunity_source: Option<Arc<dyn OpportunityHintSource>>,
        transfer_status_source: Option<Arc<dyn TransferStatusSource>>,
    ) -> Result<Self> {
        let adapters = adapters
            .into_iter()
            .map(|adapter| (adapter.venue(), adapter))
            .collect::<BTreeMap<_, _>>();
        let store = FileStateStore::new(&config.persistence.snapshot_path);
        let loaded_state = store.load::<EngineState>()?;
        let mut state = loaded_state.clone().unwrap_or_default();
        let journal = JsonlJournal::with_capacity(
            &config.persistence.event_log_path,
            config.runtime.journal_async_queue_capacity,
        );
        normalize_engine_state_positions(&mut state);
        if state.open_positions.is_empty() {
            state.open_positions = recover_open_positions_from_journal(&journal)?;
            normalize_engine_state_positions(&mut state);
        }
        let (recent_submit_ack_ms, recent_order_health, venue_health_updated_at_ms) =
            restore_venue_health_from_state(&state);
        let cached_scan_symbol_state = load_persisted_scan_symbol_cache(&config);
        let initial_scan_symbols = cached_scan_symbol_state
            .as_ref()
            .map(|cache| cache.scan_symbols.clone())
            .filter(|symbols| !symbols.is_empty())
            .unwrap_or_else(|| config.symbols.clone());

        let mut engine = Self {
            journal,
            store,
            config,
            adapters,
            opportunity_source,
            transfer_status_source,
            state,
            last_live_recovery_probe_ms: None,
            last_running_slot_reconcile_probe_ms: None,
            last_persisted_state: loaded_state.as_ref().map(persistent_state_view),
            recent_submit_ack_ms,
            venue_entry_cooldowns: BTreeMap::new(),
            recent_order_health,
            venue_health_updated_at_ms,
            cached_transfer_status_view: None,
            scan_symbols: initial_scan_symbols,
            market_data_active: false,
            market_data_activated_at_ms: None,
            last_no_entry_diagnostics: None,
            repeated_failure_logs: BTreeMap::new(),
            cached_live_entry_venue_filter: None,
        };
        engine.finalize_startup_position_recovery().await?;
        engine.initialize_scan_symbols(cached_scan_symbol_state.as_ref());
        if matches!(engine.config.runtime.mode, crate::config::RuntimeMode::Live)
            && engine.supports_windowed_market_data_control()
        {
            engine.market_data_active = true;
            engine
                .sync_market_data_activity(engine.current_journal_ts_ms())
                .await?;
        }
        Ok(engine)
    }

    pub fn state(&self) -> &EngineState {
        &self.state
    }

    pub fn market_data_active(&self) -> bool {
        self.market_data_active
    }

    fn active_positions(&self) -> &[OpenPosition] {
        &self.state.open_positions
    }

    fn active_positions_mut(&mut self) -> &mut Vec<OpenPosition> {
        &mut self.state.open_positions
    }

    fn sync_open_position_mirror(&mut self) {
        self.state.open_position = self.state.open_positions.first().cloned();
    }

    fn active_scan_symbols(&self) -> &[String] {
        if self.scan_symbols.is_empty() {
            &self.config.symbols
        } else {
            &self.scan_symbols
        }
    }

    fn initialize_scan_symbols(&mut self, cached: Option<&PersistedScanSymbolCache>) {
        if let Some(cache) = cached.filter(|cache| !cache.scan_symbols.is_empty()) {
            self.scan_symbols = cache.scan_symbols.clone();
            self.log_event(
                "runtime.scan_symbol_cache.used",
                &json!({
                    "source": "disk",
                    "symbol_count": self.scan_symbols.len(),
                }),
            );
        }

        let Some(refreshed) = build_scan_symbol_cache(&self.config, &self.adapters) else {
            if self.scan_symbols.is_empty() {
                self.scan_symbols = self.config.symbols.clone();
            }
            return;
        };
        if refreshed.scan_symbols.is_empty() {
            if self.scan_symbols.is_empty() {
                self.scan_symbols = self.config.symbols.clone();
            }
            return;
        }

        self.scan_symbols = refreshed.scan_symbols.clone();
        store_json_cache(&scan_symbol_cache_filename(&self.config), &refreshed);
        self.log_event(
            "runtime.scan_symbol_cache.refreshed",
            &json!({
                "symbol_count": self.scan_symbols.len(),
            }),
        );
    }

    fn supports_windowed_market_data_control(&self) -> bool {
        self.adapters
            .values()
            .any(|adapter| adapter.supports_market_data_activity_control())
    }

    fn should_activate_market_data(&self, now_ms: i64) -> bool {
        if !matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live) {
            return false;
        }
        if !self.supports_windowed_market_data_control() {
            return true;
        }
        if !self.active_positions().is_empty() {
            return true;
        }
        if self.active_scan_symbols().is_empty() {
            return false;
        }
        let hour_ms = 60 * 60 * 1_000_i64;
        let elapsed_in_hour_ms = now_ms.rem_euclid(hour_ms);
        let remaining_ms = hour_ms.saturating_sub(elapsed_in_hour_ms);
        should_activate_windowed_market_data_with_hysteresis(
            &self.config,
            remaining_ms,
            self.market_data_active,
        )
    }

    fn market_data_warmup_pending(&self, now_ms: i64) -> bool {
        self.supports_windowed_market_data_control()
            && self.market_data_active
            && self.active_positions().is_empty()
            && self
                .market_data_activated_at_ms
                .is_some_and(|activated_at_ms| {
                    now_ms.saturating_sub(activated_at_ms)
                        < self.config.runtime.poll_interval_ms.min(i64::MAX as u64) as i64
                })
    }

    async fn sync_market_data_activity(&mut self, now_ms: i64) -> Result<bool> {
        let should_be_active = self.should_activate_market_data(now_ms);
        let symbols = self.active_scan_symbols().to_vec();
        if should_be_active {
            for adapter in self.adapters.values() {
                adapter.set_market_data_active(true, &symbols).await?;
            }
            if !self.market_data_active {
                self.market_data_active = true;
                self.market_data_activated_at_ms = Some(now_ms);
                self.log_event(
                    "market_data.activated",
                    &json!({
                        "symbol_count": symbols.len(),
                    }),
                );
            }
        } else if self.market_data_active {
            for adapter in self.adapters.values() {
                adapter.set_market_data_active(false, &[]).await?;
            }
            self.market_data_active = false;
            self.market_data_activated_at_ms = None;
            self.log_event("market_data.deactivated", &json!({}));
        }
        Ok(should_be_active)
    }

    fn active_position_count(&self) -> usize {
        self.state.open_positions.len()
    }

    fn has_active_symbol(&self, symbol: &str) -> bool {
        self.state
            .open_positions
            .iter()
            .any(|position| position.symbol == symbol)
    }

    fn add_open_position(&mut self, position: OpenPosition) {
        self.state
            .open_positions
            .retain(|existing| existing.position_id != position.position_id);
        self.state.open_positions.push(position);
        self.state
            .open_positions
            .sort_by(|left, right| left.position_id.cmp(&right.position_id));
        self.sync_open_position_mirror();
    }

    fn remove_open_position(&mut self, position_id: &str) -> Option<OpenPosition> {
        let index = self
            .state
            .open_positions
            .iter()
            .position(|position| position.position_id == position_id)?;
        let removed = self.state.open_positions.remove(index);
        self.sync_open_position_mirror();
        Some(removed)
    }

    async fn finalize_startup_position_recovery(&mut self) -> Result<()> {
        if self.active_positions().is_empty() {
            self.state.mode = EngineMode::Running;
            self.state.last_error = None;
            self.persist_state()?;
            return Ok(());
        }

        match self.reconcile_open_positions_internal(true).await {
            Ok(()) => {}
            Err(error) => {
                self.state.mode = EngineMode::FailClosed;
                self.state.last_error =
                    Some(format!("startup recovery validation failed: {error:#}"));
                self.log_critical_event(
                    "recovery.blocked",
                    &json!({
                        "reason": "startup_recovery_validation_failed",
                        "error": error.to_string(),
                    }),
                )?;
                self.persist_state()?;
                return Ok(());
            }
        }

        let max_positions = self.config.strategy.max_concurrent_positions.max(1);
        if self.active_position_count() > max_positions {
            self.state.mode = EngineMode::FailClosed;
            self.state.last_error = Some(format!(
                "open positions exceed configured max: {}>{}",
                self.active_position_count(),
                max_positions
            ));
            self.log_critical_event(
                "recovery.blocked",
                &json!({
                    "reason": "open_positions_exceed_configured_max",
                    "open_position_count": self.active_position_count(),
                    "max_concurrent_positions": max_positions,
                }),
            )?;
        }

        self.persist_state()?;
        Ok(())
    }

    pub async fn tick(&mut self) -> Result<()> {
        self.state.cycle += 1;
        let tick_started_at_ms = wall_clock_now_ms();
        let windowed_market_data =
            matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live)
                && self.supports_windowed_market_data_control();
        if windowed_market_data {
            let market_data_active = self.sync_market_data_activity(tick_started_at_ms).await?;
            if !market_data_active && self.active_positions().is_empty() {
                let market = MarketView::empty(tick_started_at_ms);
                self.state.last_market_ts_ms = Some(tick_started_at_ms);
                self.expire_entry_cooldowns();
                if self.state.mode == EngineMode::Running
                    && should_probe_live_recovery(
                        self.last_live_recovery_probe_ms,
                        tick_started_at_ms,
                        FLAT_RECOVERY_PROBE_INTERVAL_MS,
                    )
                {
                    self.discover_live_open_position(&market).await?;
                    self.last_live_recovery_probe_ms = Some(tick_started_at_ms);
                }
                if self.active_positions().is_empty() {
                    self.process_pending_close_reconciliations(tick_started_at_ms, &market)
                        .await?;
                    return Ok(());
                }
            }
            if self.market_data_warmup_pending(tick_started_at_ms) {
                self.state.last_market_ts_ms = Some(tick_started_at_ms);
                self.log_event(
                    "market_data.warmup_pending",
                    &json!({
                        "activated_at_ms": self.market_data_activated_at_ms,
                        "warmup_ms": self.config.runtime.poll_interval_ms,
                    }),
                );
                return Ok(());
            }
        }
        let live_entry_venue_filter =
            if matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live)
                && self.active_positions().is_empty()
            {
                Some(self.live_entry_venue_filter(tick_started_at_ms).await)
            } else {
                None
            };
        let market = match self
            .fetch_market_view(
                live_entry_venue_filter
                    .as_ref()
                    .map(|item| &item.eligible_venues),
            )
            .await
        {
            Ok(market) => market,
            Err(error) => {
                if !self.active_positions().is_empty() {
                    self.state.mode = EngineMode::FailClosed;
                    self.state.last_error = Some(format!(
                        "market fetch failed with open positions: {error:#}"
                    ));
                    self.log_critical_event(
                        "execution.guard_failed",
                        &json!({
                            "reason": "market_fetch_failed_with_open_positions",
                            "error": error.to_string(),
                            "open_position_count": self.active_position_count(),
                        }),
                    )?;
                    self.persist_state()?;
                }
                return Err(error);
            }
        };
        let now_ms = market.now_ms();
        if now_ms > 0 {
            self.state.last_market_ts_ms = Some(now_ms);
        }
        self.expire_entry_cooldowns();

        if self.active_positions().is_empty()
            && self.state.mode == EngineMode::Running
            && should_probe_live_recovery(
                self.last_live_recovery_probe_ms,
                now_ms,
                FLAT_RECOVERY_PROBE_INTERVAL_MS,
            )
        {
            self.discover_live_open_position(&market).await?;
            self.last_live_recovery_probe_ms = Some(now_ms.max(0));
        }

        if !self.active_positions().is_empty() && self.state.mode != EngineMode::Running {
            if self.active_position_count() > self.config.strategy.max_concurrent_positions.max(1) {
                self.state.mode = EngineMode::FailClosed;
                self.state.last_error = Some(format!(
                    "open positions exceed configured max: {}>{}",
                    self.active_position_count(),
                    self.config.strategy.max_concurrent_positions.max(1)
                ));
                self.log_critical_event(
                    "recovery.blocked",
                    &json!({
                        "reason": "open_positions_exceed_configured_max",
                        "open_position_count": self.active_position_count(),
                        "max_concurrent_positions": self.config.strategy.max_concurrent_positions.max(1),
                    }),
                )?;
                self.persist_state()?;
                return Ok(());
            }
            self.reconcile_open_positions_internal(true).await?;
            self.persist_state()?;
            if self.state.mode != EngineMode::Running {
                return Ok(());
            }
        }

        if !self.active_positions().is_empty() {
            self.process_pending_close_reconciliations(now_ms, &market)
                .await?;
            self.manage_open_positions(&market).await?;
        }

        let should_scan = self.should_scan_entries(&market);
        if self.active_positions().is_empty() && !should_scan {
            self.process_pending_close_reconciliations(now_ms, &market)
                .await?;
        }
        let selected_candidates = if should_scan {
            let hints = self.fetch_hints().await;
            let transfer_statuses = self.fetch_transfer_status_view().await;
            let mut scan_config = self.config.clone();
            scan_config.symbols = self.active_scan_symbols().to_vec();
            if let Some(filter) = live_entry_venue_filter.as_ref() {
                self.apply_live_entry_venue_filter_to_scan_config(
                    &mut scan_config,
                    &filter.eligible_venues,
                );
            }
            let mut candidates = discover_candidates(
                &scan_config,
                &market,
                hints.as_deref(),
                transfer_statuses.as_ref(),
            );
            if matches!(self.config.runtime.mode, RuntimeMode::Live) {
                let fee_snapshots = self.cached_live_fee_snapshots();
                let fallback_venues = apply_cached_fee_snapshots_to_candidates(
                    &scan_config,
                    &mut candidates,
                    &fee_snapshots,
                );
                for (venue, configured_taker_fee_bps) in fallback_venues {
                    self.log_repeated_failure_event(
                        "runtime.account_fee_snapshot_fallback",
                        format!("account_fee_snapshot_fallback:{venue}"),
                        &json!({
                            "venue": venue,
                            "configured_taker_fee_bps": configured_taker_fee_bps,
                            "reason": "cached_account_fee_snapshot_unavailable",
                        }),
                    );
                }
            }
            self.apply_runtime_entry_guards(&market, &mut candidates);
            sort_candidates(&mut candidates);
            self.reconcile_running_slots_if_needed(now_ms, &candidates)
                .await?;
            if self.state.mode != EngineMode::Running {
                self.persist_state()?;
                return Ok(());
            }
            self.apply_perp_liquidity_guards(&market, &mut candidates)
                .await?;
            self.apply_live_entry_leverage_guards(&mut candidates)
                .await?;
            sort_candidates(&mut candidates);
            let tradeable_count = candidates
                .iter()
                .filter(|candidate| candidate.is_tradeable())
                .count();
            self.state.last_scan = Some(ScanSnapshot {
                candidate_count: candidates.len(),
                tradeable_count,
                best_candidate: candidates.first().cloned(),
            });
            self.log_event_at(
                now_ms,
                "scan.completed",
                self.state.last_scan.as_ref().unwrap(),
            );
            let selected_candidates = if self.state.mode == EngineMode::Running {
                let selected = if self.config.runtime.auto_trade_enabled {
                    self.select_entry_candidates(&market, &candidates)
                } else {
                    Vec::new()
                };
                if selected.is_empty() {
                    self.log_no_entry_diagnostics(&market, &candidates, &selected);
                } else {
                    self.last_no_entry_diagnostics = None;
                }
                selected
            } else {
                Vec::new()
            };
            selected_candidates
        } else {
            self.state.last_scan = None;
            Vec::new()
        };

        if self.state.mode == EngineMode::Running && self.config.runtime.auto_trade_enabled {
            for candidate in selected_candidates {
                if self.active_position_count()
                    >= self.config.strategy.max_concurrent_positions.max(1)
                {
                    break;
                }
                self.try_open_position(candidate, &market).await?;
            }
        }

        self.persist_state()?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        for adapter in self.adapters.values() {
            adapter.shutdown().await?;
        }
        self.journal.shutdown()?;
        Ok(())
    }

    fn current_journal_ts_ms(&self) -> i64 {
        self.normalize_journal_ts_ms(self.state.last_market_ts_ms.unwrap_or_default())
    }

    fn normalize_journal_ts_ms(&self, ts_ms: i64) -> i64 {
        if ts_ms > 0 {
            ts_ms
        } else {
            chrono::Utc::now().timestamp_millis()
        }
    }

    fn log_event<T: Serialize>(&self, kind: &str, payload: &T) {
        self.log_event_at(self.current_journal_ts_ms(), kind, payload);
    }

    fn log_event_at<T: Serialize>(&self, ts_ms: i64, kind: &str, payload: &T) {
        let _ = self
            .journal
            .append(self.normalize_journal_ts_ms(ts_ms), kind, payload);
    }

    fn log_critical_event<T: Serialize>(&self, kind: &str, payload: &T) -> Result<()> {
        self.log_critical_event_at(self.current_journal_ts_ms(), kind, payload)
    }

    fn log_critical_event_at<T: Serialize>(
        &self,
        ts_ms: i64,
        kind: &str,
        payload: &T,
    ) -> Result<()> {
        self.journal
            .append_critical(self.normalize_journal_ts_ms(ts_ms), kind, payload)
    }

    async fn fetch_market_view(
        &mut self,
        allowed_venues: Option<&BTreeSet<Venue>>,
    ) -> Result<MarketView> {
        let symbols = self.active_scan_symbols().to_vec();
        let futures = self
            .adapters
            .iter()
            .filter(|(venue, _)| {
                allowed_venues
                    .map(|allowed| allowed.contains(venue))
                    .unwrap_or(true)
            })
            .map(|(venue, adapter)| {
                let adapter = Arc::clone(adapter);
                let symbols = symbols.clone();
                async move { (*venue, adapter.fetch_market_snapshot(&symbols).await) }
            });

        let mut snapshots = Vec::new();
        let mut saw_hard_failure = false;
        for (venue, result) in join_all(futures).await {
            match result {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => {
                    let soft_failure = is_soft_market_fetch_failure(&error);
                    if !soft_failure {
                        saw_hard_failure = true;
                    }
                    self.state.last_error =
                        Some(format!("market fetch failed on {venue}: {error:#}"));
                    self.log_repeated_failure_event(
                        "market.fetch_failed",
                        format!("market.fetch_failed:{venue}:{soft_failure}:{}", error),
                        &json!({
                            "venue": venue,
                            "error": error.to_string(),
                            "soft": soft_failure,
                        }),
                    );
                }
            }
        }
        if snapshots.is_empty() {
            if saw_hard_failure {
                return Err(anyhow!("market fetch failed on all venues"));
            }
            return Ok(MarketView::empty(now_ms()));
        }
        Ok(MarketView::from_snapshots(snapshots))
    }

    fn apply_live_entry_venue_filter_to_scan_config(
        &self,
        scan_config: &mut AppConfig,
        eligible_venues: &BTreeSet<Venue>,
    ) {
        scan_config
            .venues
            .retain(|venue| eligible_venues.contains(&venue.venue));
        scan_config.directed_pairs.retain(|pair| {
            eligible_venues.contains(&pair.long) && eligible_venues.contains(&pair.short)
        });
    }

    fn cached_live_fee_snapshots(&self) -> BTreeMap<Venue, AccountFeeSnapshot> {
        self.adapters
            .iter()
            .filter_map(|(venue, adapter)| {
                adapter
                    .cached_account_fee_snapshot()
                    .map(|snapshot| (*venue, snapshot))
            })
            .collect()
    }

    async fn live_entry_venue_filter(&mut self, now_ms: i64) -> CachedLiveEntryVenueFilter {
        if let Some(cache) = self.cached_live_entry_venue_filter.as_ref() {
            if now_ms.saturating_sub(cache.observed_at_ms) <= LIVE_ENTRY_VENUE_FILTER_TTL_MS {
                return cache.clone();
            }
        }

        let mut statuses = BTreeMap::new();
        let mut eligible_venues = BTreeSet::new();
        for (venue, adapter) in &self.adapters {
            if !adapter.enforces_entry_balance_gate() {
                eligible_venues.insert(*venue);
                statuses.insert(
                    *venue,
                    LiveEntryVenueStatus {
                        eligible: true,
                        reason: "gate_not_enforced".to_string(),
                        effective_balance_quote: None,
                        equity_quote: None,
                        available_balance_quote: None,
                        error: None,
                    },
                );
                continue;
            }

            let status = match adapter.fetch_account_balance_snapshot().await {
                Ok(Some(snapshot)) => {
                    let effective_balance_quote = snapshot
                        .available_balance_quote
                        .unwrap_or(snapshot.equity_quote);
                    let eligible = effective_balance_quote >= MIN_LIVE_ENTRY_VENUE_BALANCE_QUOTE;
                    if eligible {
                        eligible_venues.insert(*venue);
                    }
                    LiveEntryVenueStatus {
                        eligible,
                        reason: if eligible {
                            "eligible".to_string()
                        } else {
                            "balance_below_minimum".to_string()
                        },
                        effective_balance_quote: Some(effective_balance_quote),
                        equity_quote: Some(snapshot.equity_quote),
                        available_balance_quote: snapshot.available_balance_quote,
                        error: None,
                    }
                }
                Ok(None) => LiveEntryVenueStatus {
                    eligible: false,
                    reason: "account_balance_snapshot_unavailable".to_string(),
                    effective_balance_quote: None,
                    equity_quote: None,
                    available_balance_quote: None,
                    error: None,
                },
                Err(error) => LiveEntryVenueStatus {
                    eligible: false,
                    reason: "account_balance_fetch_failed".to_string(),
                    effective_balance_quote: None,
                    equity_quote: None,
                    available_balance_quote: None,
                    error: Some(error.to_string()),
                },
            };
            statuses.insert(*venue, status);
        }

        let payload_statuses = statuses
            .iter()
            .map(|(venue, status)| {
                json!({
                    "venue": venue,
                    "eligible": status.eligible,
                    "reason": status.reason,
                    "effective_balance_quote": status.effective_balance_quote,
                    "equity_quote": status.equity_quote,
                    "available_balance_quote": status.available_balance_quote,
                    "error": status.error,
                })
            })
            .collect::<Vec<_>>();
        let _ = self.log_critical_event_at(
            now_ms,
            "runtime.entry_venue_filter.refreshed",
            &json!({
                "minimum_balance_quote": MIN_LIVE_ENTRY_VENUE_BALANCE_QUOTE,
                "eligible_venues": eligible_venues.iter().copied().collect::<Vec<_>>(),
                "statuses": payload_statuses,
            }),
        );

        let cache = CachedLiveEntryVenueFilter {
            observed_at_ms: now_ms,
            eligible_venues,
        };
        self.cached_live_entry_venue_filter = Some(cache.clone());
        cache
    }

    async fn fetch_hints(&mut self) -> Option<Vec<OpportunityHint>> {
        let Some(source) = self.opportunity_source.as_ref().cloned() else {
            return None;
        };

        match source.fetch_hints(self.active_scan_symbols()).await {
            Ok(hints) if !hints.is_empty() => {
                self.log_event(
                    "hint_source.used",
                    &json!({
                        "source": "external",
                        "count": hints.len(),
                    }),
                );
                Some(hints)
            }
            Ok(_) => None,
            Err(error) => {
                self.state.last_error = Some(format!("opportunity source failed: {error:#}"));
                self.log_repeated_failure_event(
                    "hint_source.failed",
                    format!("hint_source.failed:{error}"),
                    &json!({
                        "error": error.to_string(),
                    }),
                );
                None
            }
        }
    }

    async fn fetch_transfer_status_view(&mut self) -> Option<TransferStatusView> {
        let cached_at_ms = now_ms();
        if let Some((cached, observed_at_ms)) = self.cached_transfer_status_view(cached_at_ms) {
            self.log_event(
                "transfer_cache.used",
                &json!({
                    "source": "engine",
                    "observed_at_ms": observed_at_ms,
                }),
            );
            return Some(cached);
        }

        let assets = self
            .active_scan_symbols()
            .iter()
            .map(|symbol| normalize_symbol_key(symbol))
            .filter(|asset| !asset.is_empty())
            .collect::<Vec<_>>();
        if assets.is_empty() {
            return None;
        }

        let venues = self.adapters.keys().copied().collect::<Vec<_>>();
        if let Some(source) = self.transfer_status_source.as_ref().cloned() {
            match source.fetch_transfer_statuses(&assets, &venues).await {
                Ok(statuses) if !statuses.is_empty() => {
                    let status_count = statuses.len();
                    let view = TransferStatusView::from_statuses(statuses);
                    self.cached_transfer_status_view = Some(CachedTransferStatusView {
                        observed_at_ms: cached_at_ms,
                        view: view.clone(),
                    });
                    self.log_event(
                        "transfer_source.used",
                        &json!({
                            "source": "external",
                            "count": status_count,
                        }),
                    );
                    return Some(view);
                }
                Ok(_) => {}
                Err(error) => {
                    self.state.last_error = Some(format!("transfer source failed: {error:#}"));
                    self.log_repeated_failure_event(
                        "transfer_source.failed",
                        format!("transfer_source.failed:{error}"),
                        &json!({
                            "error": error.to_string(),
                        }),
                    );
                }
            }
        }

        let futures = self.adapters.iter().map(|(venue, adapter)| {
            let adapter = Arc::clone(adapter);
            let assets = assets.clone();
            async move { (*venue, adapter.fetch_transfer_statuses(&assets).await) }
        });

        let mut statuses = Vec::new();
        for (venue, result) in join_all(futures).await {
            match result {
                Ok(mut venue_statuses) => statuses.append(&mut venue_statuses),
                Err(error) => {
                    self.state.last_error = Some(format!(
                        "transfer status fetch failed on {venue}: {error:#}"
                    ));
                    self.log_repeated_failure_event(
                        "transfer.fetch_failed",
                        format!("transfer.fetch_failed:{venue}:{error}"),
                        &json!({
                            "venue": venue,
                            "error": error.to_string(),
                        }),
                    );
                }
            }
        }

        if statuses.is_empty() {
            None
        } else {
            let view = TransferStatusView::from_statuses(statuses);
            self.cached_transfer_status_view = Some(CachedTransferStatusView {
                observed_at_ms: cached_at_ms,
                view: view.clone(),
            });
            Some(view)
        }
    }

    fn cached_transfer_status_view(&self, now_ms: i64) -> Option<(TransferStatusView, i64)> {
        let ttl_ms = self
            .config
            .runtime
            .transfer_status_cache_ms
            .min(i64::MAX as u64) as i64;
        if ttl_ms <= 0 {
            return None;
        }
        let cache = self.cached_transfer_status_view.as_ref()?;
        if now_ms.saturating_sub(cache.observed_at_ms) > ttl_ms {
            return None;
        }
        Some((cache.view.clone(), cache.observed_at_ms))
    }

    fn should_scan_entries(&self, market: &MarketView) -> bool {
        if market.is_empty() {
            return true;
        }
        if self.config.strategy.max_scan_minutes_before_funding <= 0 {
            return true;
        }

        self.active_scan_symbols().iter().any(|symbol| {
            self.adapters.keys().copied().any(|venue| {
                market
                    .symbol(venue, symbol)
                    .map(|quote| {
                        is_within_funding_scan_window_ms(
                            &self.config,
                            quote.funding_timestamp_ms.saturating_sub(market.now_ms()),
                        )
                    })
                    .unwrap_or(false)
            })
        })
    }

    async fn process_pending_close_reconciliations(
        &mut self,
        now_ms: i64,
        market: &MarketView,
    ) -> Result<()> {
        if self.state.pending_close_reconciliations.is_empty()
            || !matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live)
        {
            return Ok(());
        }

        let mut changed = false;
        let allow_final_reconciliations = self.active_positions().is_empty();
        while let Some(task) = self
            .state
            .pending_close_reconciliations
            .iter()
            .cloned()
            .filter(|item| {
                item.created_cycle < self.state.cycle
                    && now_ms >= item.next_attempt_ms
                    && (matches!(item.kind, CloseReconciliationKind::Partial)
                        || allow_final_reconciliations)
            })
            .min_by(|left, right| {
                left.closed_at_ms
                    .cmp(&right.closed_at_ms)
                    .then_with(|| {
                        close_reconciliation_kind_rank(left.kind)
                            .cmp(&close_reconciliation_kind_rank(right.kind))
                    })
                    .then_with(|| left.position_id.cmp(&right.position_id))
            })
        {
            match self.reconcile_pending_close(task.clone(), market).await {
                Ok(Some((event_kind, payload))) => {
                    let compacted_payload =
                        self.compact_reconciliation_payload(event_kind, &payload);
                    self.log_critical_event(event_kind, &compacted_payload)?;
                    self.state.pending_close_reconciliations.retain(|item| {
                        !(item.position_id == task.position_id
                            && item.kind == task.kind
                            && item.closed_at_ms == task.closed_at_ms)
                    });
                    if matches!(task.kind, CloseReconciliationKind::Partial) {
                        self.propagate_partial_close_reconciliation(&task, &payload);
                    }
                    changed = true;
                }
                Ok(None) => {
                    self.reschedule_pending_close_reconciliation(
                        &task,
                        "close fill reconciliation not yet available",
                    );
                    changed = true;
                }
                Err(error) => {
                    self.reschedule_pending_close_reconciliation(&task, &error.to_string());
                    changed = true;
                }
            }
        }

        if changed {
            self.persist_state()?;
        }
        Ok(())
    }

    async fn reconcile_pending_close(
        &self,
        task: PendingCloseReconciliation,
        market: &MarketView,
    ) -> Result<Option<(&'static str, serde_json::Value)>> {
        let long_leg = self
            .fetch_close_leg_reconciliation(&task.symbol, &task.long_leg)
            .await?;
        let short_leg = self
            .fetch_close_leg_reconciliation(&task.symbol, &task.short_leg)
            .await?;
        let (Some(long_leg), Some(short_leg)) = (long_leg, short_leg) else {
            return Ok(None);
        };

        let payload = match task.kind {
            CloseReconciliationKind::Partial => {
                self.build_partial_close_reconciled_payload(&task, market, &long_leg, &short_leg)?
            }
            CloseReconciliationKind::Final => {
                self.build_final_close_reconciled_payload(&task, market, &long_leg, &short_leg)?
            }
        };
        let event_kind = match task.kind {
            CloseReconciliationKind::Partial => "exit.partial_reconciled",
            CloseReconciliationKind::Final => "exit.reconciled",
        };
        Ok(Some((event_kind, payload)))
    }

    async fn fetch_close_leg_reconciliation(
        &self,
        symbol: &str,
        leg: &CloseLegRecord,
    ) -> Result<Option<OrderFillReconciliation>> {
        let adapter = self.adapter(leg.venue)?;
        adapter
            .fetch_order_fill_reconciliation(
                symbol,
                &leg.order_id,
                Some(leg.client_order_id.as_str()).filter(|value| !value.is_empty()),
            )
            .await
    }

    fn reschedule_pending_close_reconciliation(
        &mut self,
        task: &PendingCloseReconciliation,
        error: &str,
    ) {
        let log_payload = if let Some(pending) = self
            .state
            .pending_close_reconciliations
            .iter_mut()
            .find(|item| {
                item.position_id == task.position_id
                    && item.kind == task.kind
                    && item.closed_at_ms == task.closed_at_ms
            }) {
            pending.attempt_count = pending.attempt_count.saturating_add(1);
            let base_ts_ms = self
                .state
                .last_market_ts_ms
                .unwrap_or_else(wall_clock_now_ms);
            pending.next_attempt_ms = base_ts_ms
                .saturating_add(close_reconciliation_retry_delay_ms(pending.attempt_count));
            let position_id = pending.position_id.clone();
            let symbol = pending.symbol.clone();
            let kind = pending.kind;
            let attempt_count = pending.attempt_count;
            let next_attempt_ms = pending.next_attempt_ms;
            let closed_at_ms = pending.closed_at_ms;
            Some(json!({
                "position_id": position_id,
                "symbol": symbol,
                "kind": kind,
                "attempt_count": attempt_count,
                "next_attempt_ms": next_attempt_ms,
                "closed_at_ms": closed_at_ms,
                "error": error,
            }))
        } else {
            None
        };
        if let Some(payload) = log_payload {
            let key = format!(
                "exit.reconciliation_failed:{}:{:?}:{}",
                task.position_id, task.kind, error
            );
            self.log_repeated_failure_event("exit.reconciliation_failed", key, &payload);
        }
    }

    fn propagate_partial_close_reconciliation(
        &mut self,
        task: &PendingCloseReconciliation,
        payload: &serde_json::Value,
    ) {
        let reconciled_quantity = payload.get("quantity").and_then(|value| value.as_f64());
        let reconciled_settlement_half_closed_quantity = payload
            .get("settlement_half_closed_quantity")
            .and_then(|value| value.as_f64());
        let reconciled_settlement_half_closed_at_ms = payload
            .get("settlement_half_closed_at_ms")
            .and_then(|value| value.as_i64());
        let reconciled_realized_price_pnl_quote = payload
            .get("reconciled_realized_price_pnl_quote")
            .and_then(|value| value.as_f64());
        let reconciled_realized_exit_fee_quote = payload
            .get("reconciled_realized_exit_fee_quote")
            .and_then(|value| value.as_f64());
        let settlement_half_outcome_diagnostics = payload.get("outcome_diagnostics").cloned();

        for pending in &mut self.state.pending_close_reconciliations {
            if pending.position_id != task.position_id
                || !matches!(pending.kind, CloseReconciliationKind::Final)
            {
                continue;
            }
            if let Some(value) = reconciled_quantity {
                pending.position_snapshot.quantity = value;
            }
            if let Some(value) = reconciled_settlement_half_closed_quantity {
                pending.position_snapshot.settlement_half_closed_quantity = value;
            }
            if let Some(value) = reconciled_settlement_half_closed_at_ms {
                pending.position_snapshot.settlement_half_closed_at_ms = value;
            }
            if let Some(value) = reconciled_realized_price_pnl_quote {
                pending.position_snapshot.realized_price_pnl_quote = value;
            }
            if let Some(value) = reconciled_realized_exit_fee_quote {
                pending.position_snapshot.realized_exit_fee_quote = value;
            }
            if let Some(diagnostics) = settlement_half_outcome_diagnostics.clone() {
                if let Some(object) = pending.original_payload.as_object_mut() {
                    object.insert(
                        "settlement_half_outcome_diagnostics".to_string(),
                        diagnostics,
                    );
                }
            }
        }
        for position in &mut self.state.open_positions {
            if position.position_id != task.position_id {
                continue;
            }
            if let Some(value) = reconciled_quantity {
                position.quantity = value;
            }
            if let Some(value) = reconciled_settlement_half_closed_quantity {
                position.settlement_half_closed_quantity = value;
            }
            if let Some(value) = reconciled_settlement_half_closed_at_ms {
                position.settlement_half_closed_at_ms = value;
            }
            if let Some(value) = reconciled_realized_price_pnl_quote {
                position.realized_price_pnl_quote = value;
            }
            if let Some(value) = reconciled_realized_exit_fee_quote {
                position.realized_exit_fee_quote = value;
            }
            if let Some(value) = payload
                .get("current_net_quote")
                .and_then(|value| value.as_f64())
            {
                position.current_net_quote = value;
            }
            if let Some(value) = payload
                .get("peak_net_quote")
                .and_then(|value| value.as_f64())
            {
                position.peak_net_quote = value;
            }
        }
        self.sync_open_position_mirror();
    }

    fn select_entry_candidates(
        &self,
        market: &MarketView,
        candidates: &[CandidateOpportunity],
    ) -> Vec<CandidateOpportunity> {
        let capacity = self.config.strategy.max_concurrent_positions.max(1);
        let remaining_slots = capacity.saturating_sub(self.active_position_count());
        if remaining_slots == 0 {
            return Vec::new();
        }
        let selection_target = entry_selection_target(remaining_slots);

        let mut ranked = candidates
            .iter()
            .filter(|item| item.is_tradeable())
            .cloned()
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            self.runtime_candidate_selection_score(market, right)
                .total_cmp(&self.runtime_candidate_selection_score(market, left))
                .then_with(|| right.ranking_edge_bps.total_cmp(&left.ranking_edge_bps))
                .then_with(|| {
                    right
                        .worst_case_edge_bps
                        .total_cmp(&left.worst_case_edge_bps)
                })
                .then_with(|| left.pair_id.cmp(&right.pair_id))
        });

        let mut selected = Vec::new();
        for candidate in ranked {
            if self.has_active_symbol(&candidate.symbol)
                || selected
                    .iter()
                    .any(|item: &CandidateOpportunity| item.symbol == candidate.symbol)
            {
                continue;
            }
            selected.push(candidate);
            if selected.len() >= selection_target {
                break;
            }
        }
        selected
    }

    async fn apply_perp_liquidity_guards(
        &mut self,
        market: &MarketView,
        candidates: &mut [CandidateOpportunity],
    ) -> Result<()> {
        if !matches!(self.config.runtime.mode, RuntimeMode::Live) {
            return Ok(());
        }

        let capacity = self.config.strategy.max_concurrent_positions.max(1);
        let remaining_slots = capacity.saturating_sub(self.active_position_count());
        if remaining_slots == 0 {
            return Ok(());
        }

        let selection_target = entry_selection_target(remaining_slots);
        let evaluation_budget = selection_target
            .saturating_mul(ENTRY_LIQUIDITY_SCREEN_MULTIPLIER)
            .clamp(selection_target, ENTRY_LIQUIDITY_SCREEN_MAX);

        let mut ranked_indices = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.is_tradeable())
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        ranked_indices.sort_by(|left, right| {
            self.runtime_candidate_selection_score(market, &candidates[*right])
                .total_cmp(&self.runtime_candidate_selection_score(market, &candidates[*left]))
                .then_with(|| {
                    candidates[*right]
                        .ranking_edge_bps
                        .total_cmp(&candidates[*left].ranking_edge_bps)
                })
                .then_with(|| {
                    candidates[*right]
                        .worst_case_edge_bps
                        .total_cmp(&candidates[*left].worst_case_edge_bps)
                })
                .then_with(|| candidates[*left].pair_id.cmp(&candidates[*right].pair_id))
        });

        let mut liquidity_cache = HashMap::<
            (Venue, String),
            std::result::Result<Option<PerpLiquiditySnapshot>, String>,
        >::new();
        let mut passing_symbols = HashSet::new();
        let mut evaluated = 0usize;

        for index in ranked_indices {
            if evaluated >= evaluation_budget || passing_symbols.len() >= selection_target {
                break;
            }
            let symbol = candidates[index].symbol.clone();
            if self.has_active_symbol(&symbol) || passing_symbols.contains(symbol.as_str()) {
                continue;
            }
            evaluated = evaluated.saturating_add(1);
            self.apply_candidate_perp_liquidity_guard(&mut candidates[index], &mut liquidity_cache)
                .await?;
            if candidates[index].is_tradeable() {
                passing_symbols.insert(symbol);
            }
        }

        Ok(())
    }

    async fn apply_candidate_perp_liquidity_guard(
        &mut self,
        candidate: &mut CandidateOpportunity,
        cache: &mut HashMap<
            (Venue, String),
            std::result::Result<Option<PerpLiquiditySnapshot>, String>,
        >,
    ) -> Result<()> {
        for venue in [candidate.long_venue, candidate.short_venue] {
            let snapshot = match self
                .fetch_perp_liquidity_with_cache(cache, venue, &candidate.symbol)
                .await
            {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) | Err(_) => {
                    self.push_candidate_block_reason(
                        candidate,
                        format!("perp_liquidity_unavailable:{}", venue.as_str()),
                    );
                    continue;
                }
            };
            let (min_volume_24h_quote, min_open_interest_quote) = perp_liquidity_thresholds(venue);
            if snapshot.volume_24h_quote < min_volume_24h_quote {
                self.push_candidate_block_reason(
                    candidate,
                    format!("perp_volume_below_floor:{}", venue.as_str()),
                );
                self.log_event(
                    "execution.entry_liquidity_blocked",
                    &json!({
                        "position_symbol": candidate.symbol,
                        "pair_id": candidate.pair_id,
                        "venue": venue,
                        "metric": "volume_24h_quote",
                        "observed_value": snapshot.volume_24h_quote,
                        "minimum_required": min_volume_24h_quote,
                    }),
                );
            }
            if snapshot.open_interest_quote < min_open_interest_quote {
                self.push_candidate_block_reason(
                    candidate,
                    format!("perp_open_interest_below_floor:{}", venue.as_str()),
                );
                self.log_event(
                    "execution.entry_liquidity_blocked",
                    &json!({
                        "position_symbol": candidate.symbol,
                        "pair_id": candidate.pair_id,
                        "venue": venue,
                        "metric": "open_interest_quote",
                        "observed_value": snapshot.open_interest_quote,
                        "minimum_required": min_open_interest_quote,
                    }),
                );
            }
        }

        Ok(())
    }

    async fn fetch_perp_liquidity_with_cache(
        &mut self,
        cache: &mut HashMap<
            (Venue, String),
            std::result::Result<Option<PerpLiquiditySnapshot>, String>,
        >,
        venue: Venue,
        symbol: &str,
    ) -> std::result::Result<Option<PerpLiquiditySnapshot>, String> {
        let key = (venue, symbol.to_string());
        if let Some(result) = cache.get(&key) {
            return result.clone();
        }

        let adapter = match self.adapter(venue) {
            Ok(adapter) => adapter,
            Err(error) => return Err(error.to_string()),
        };
        let result = match adapter.fetch_perp_liquidity_snapshot(symbol).await {
            Ok(snapshot) => {
                if snapshot.is_none() {
                    let _ = self.log_critical_event(
                        "execution.entry_liquidity_unavailable",
                        &json!({
                            "venue": venue,
                            "symbol": symbol,
                            "error": "perp liquidity snapshot unavailable",
                        }),
                    );
                }
                Ok(snapshot)
            }
            Err(error) => {
                let error_message = error.to_string();
                let _ = self.log_critical_event(
                    "execution.entry_liquidity_unavailable",
                    &json!({
                        "venue": venue,
                        "symbol": symbol,
                        "error": error_message,
                    }),
                );
                Err(error_message)
            }
        };
        cache.insert(key, result.clone());
        result
    }

    fn push_candidate_block_reason(&self, candidate: &mut CandidateOpportunity, reason: String) {
        if !candidate.blocked_reasons.iter().any(|item| item == &reason) {
            candidate.blocked_reasons.push(reason);
        }
    }

    fn log_no_entry_diagnostics(
        &mut self,
        market: &MarketView,
        candidates: &[CandidateOpportunity],
        selected_candidates: &[CandidateOpportunity],
    ) {
        let capacity = self.config.strategy.max_concurrent_positions.max(1);
        let remaining_slots = capacity.saturating_sub(self.active_position_count());
        let tradeable_count = candidates
            .iter()
            .filter(|candidate| candidate.is_tradeable())
            .count();
        let selected_ids = selected_candidates
            .iter()
            .map(|candidate| candidate.pair_id.as_str())
            .collect::<Vec<_>>();
        let mut diagnostic_blocked_reasons = Vec::new();
        let mut diagnostic_advisories = Vec::new();
        for candidate in candidates {
            for reason in &candidate.blocked_reasons {
                let (blocked_reason, advisory_reason) = remap_no_entry_diagnostic_reason(reason);
                if let Some(reason) = blocked_reason {
                    diagnostic_blocked_reasons.push(reason);
                }
                if let Some(reason) = advisory_reason {
                    diagnostic_advisories.push(reason);
                }
            }
            diagnostic_advisories.extend(candidate.advisories.iter().cloned());
        }
        let blocked_reason_counts = count_strings(diagnostic_blocked_reasons.into_iter());
        let advisory_counts = count_strings(diagnostic_advisories.into_iter());
        let reason = self.no_entry_reason(candidates, selected_candidates, remaining_slots);
        let fingerprint = no_entry_diagnostics_fingerprint(
            &reason,
            candidates.len(),
            tradeable_count,
            selected_candidates.len(),
            self.active_position_count(),
            remaining_slots,
            &blocked_reason_counts,
            &advisory_counts,
        );
        let suppressed_repeat_count = match self.last_no_entry_diagnostics.as_mut() {
            Some(state)
                if state.fingerprint == fingerprint
                    && self.state.cycle.saturating_sub(state.last_emitted_cycle)
                        < NO_ENTRY_DIAGNOSTIC_SAMPLE_INTERVAL_CYCLES =>
            {
                state.suppressed_count = state.suppressed_count.saturating_add(1);
                return;
            }
            Some(state) if state.fingerprint == fingerprint => {
                let suppressed = state.suppressed_count;
                state.last_emitted_cycle = self.state.cycle;
                state.suppressed_count = 0;
                suppressed
            }
            Some(state) => {
                state.fingerprint = fingerprint.clone();
                state.last_emitted_cycle = self.state.cycle;
                state.suppressed_count = 0;
                0
            }
            None => {
                self.last_no_entry_diagnostics = Some(NoEntryDiagnosticsLogState {
                    fingerprint: fingerprint.clone(),
                    last_emitted_cycle: self.state.cycle,
                    suppressed_count: 0,
                });
                0
            }
        };
        let candidate_detail_count = candidates.len().min(NO_ENTRY_DIAGNOSTIC_CANDIDATE_LIMIT);
        let diagnostics = NoEntryDiagnostics {
            reason,
            candidate_count: candidates.len(),
            tradeable_count,
            selected_candidate_count: selected_candidates.len(),
            active_position_count: self.active_position_count(),
            remaining_slots,
            candidate_detail_count,
            omitted_candidate_count: candidates.len().saturating_sub(candidate_detail_count),
            suppressed_repeat_count,
            blocked_reason_counts,
            advisory_counts,
            candidates: candidates
                .iter()
                .take(NO_ENTRY_DIAGNOSTIC_CANDIDATE_LIMIT)
                .map(|candidate| {
                    self.build_candidate_checklist(
                        market,
                        candidate,
                        remaining_slots,
                        &selected_ids,
                    )
                })
                .collect(),
        };
        self.log_event("scan.no_entry_diagnostics", &diagnostics);
    }

    fn log_repeated_failure_event(
        &mut self,
        kind: &'static str,
        key: String,
        payload: &serde_json::Value,
    ) {
        self.prune_repeated_failure_logs();
        let suppressed_repeat_count = match self.repeated_failure_logs.get_mut(key.as_str()) {
            Some(state)
                if self.state.cycle.saturating_sub(state.last_emitted_cycle)
                    < REPEATED_FAILURE_LOG_SAMPLE_INTERVAL_CYCLES =>
            {
                state.suppressed_count = state.suppressed_count.saturating_add(1);
                return;
            }
            Some(state) => {
                let suppressed = state.suppressed_count;
                state.last_emitted_cycle = self.state.cycle;
                state.suppressed_count = 0;
                suppressed
            }
            None => {
                self.repeated_failure_logs.insert(
                    key,
                    RepeatedFailureLogState {
                        last_emitted_cycle: self.state.cycle,
                        suppressed_count: 0,
                    },
                );
                0
            }
        };
        let payload = append_object_field(
            payload,
            "suppressed_repeat_count",
            json!(suppressed_repeat_count),
        );
        self.log_event(kind, &payload);
    }

    fn prune_repeated_failure_logs(&mut self) {
        let current_cycle = self.state.cycle;
        self.repeated_failure_logs.retain(|_, state| {
            current_cycle.saturating_sub(state.last_emitted_cycle)
                <= REPEATED_FAILURE_LOG_RETENTION_CYCLES
        });
    }

    fn compact_reconciliation_payload(
        &self,
        event_kind: &'static str,
        payload: &serde_json::Value,
    ) -> serde_json::Value {
        if !should_compact_reconciliation_payload(payload) {
            return payload.clone();
        }

        let mut summary = serde_json::Map::new();
        summary.insert("summary_only".to_string(), json!(true));
        summary.insert("event_kind".to_string(), json!(event_kind));
        for key in [
            "position_id",
            "symbol",
            "reason",
            "closed_at_ms",
            "reconciled_at_ms",
            "net_quote",
            "realized_price_pnl_quote",
            "total_exit_fee_quote",
            "partial_realized_price_pnl_quote",
            "partial_exit_fee_quote",
            "closed_quantity",
            "long_exit_average_price",
            "short_exit_average_price",
            "long_exit_quantity",
            "short_exit_quantity",
            "reconciliation_delta_quote",
            "remaining_reconciliation_delta_quote",
        ] {
            if let Some(value) = payload.get(key) {
                summary.insert(key.to_string(), value.clone());
            }
        }
        if let Some(value) = payload.get("outcome_diagnostics") {
            summary.insert(
                "outcome_summary".to_string(),
                compact_outcome_diagnostics(Some(value)).unwrap_or_else(|| value.clone()),
            );
        }
        if let Some(value) = payload.get("remaining_outcome_diagnostics") {
            summary.insert(
                "remaining_outcome_summary".to_string(),
                compact_outcome_diagnostics(Some(value)).unwrap_or_else(|| value.clone()),
            );
        }
        if let Some(value) = payload.get("settlement_half_outcome_diagnostics") {
            summary.insert(
                "settlement_half_outcome_summary".to_string(),
                compact_outcome_diagnostics(Some(value)).unwrap_or_else(|| value.clone()),
            );
        }
        serde_json::Value::Object(summary)
    }

    fn no_entry_reason(
        &self,
        candidates: &[CandidateOpportunity],
        selected_candidates: &[CandidateOpportunity],
        remaining_slots: usize,
    ) -> String {
        if !self.config.runtime.auto_trade_enabled {
            return "auto_trade_disabled".to_string();
        }
        if remaining_slots == 0 {
            return "no_remaining_slots".to_string();
        }
        if candidates.is_empty() {
            return "no_candidates".to_string();
        }
        if !selected_candidates.is_empty() {
            return "candidates_selected".to_string();
        }
        let tradeable_count = candidates
            .iter()
            .filter(|candidate| candidate.is_tradeable())
            .count();
        if tradeable_count == 0 {
            return "no_tradeable_candidates".to_string();
        }
        if candidates
            .iter()
            .any(|candidate| candidate.is_tradeable() && self.has_active_symbol(&candidate.symbol))
        {
            return "tradeable_candidates_conflict_with_open_symbols".to_string();
        }
        "tradeable_candidates_not_selected".to_string()
    }

    fn build_candidate_checklist(
        &self,
        market: &MarketView,
        candidate: &CandidateOpportunity,
        remaining_slots: usize,
        selected_ids: &[&str],
    ) -> OpportunityCandidateChecklist {
        let remaining_ms = candidate
            .first_funding_timestamp_ms
            .saturating_sub(market.now_ms());
        let market_fresh_long =
            market.is_fresh(candidate.long_venue, self.config.runtime.max_market_age_ms);
        let market_fresh_short =
            market.is_fresh(candidate.short_venue, self.config.runtime.max_market_age_ms);
        let funding_not_passed = remaining_ms > 0;
        let near_term_settlement_leg =
            has_near_term_settlement_leg(candidate.first_funding_timestamp_ms, market);
        let inside_entry_window = funding_not_passed
            && near_term_settlement_leg
            && is_within_funding_scan_window_ms(&self.config, remaining_ms);
        let stagger_gap_limit_ms = self
            .config
            .strategy
            .max_stagger_gap_minutes
            .saturating_mul(60_000);
        let stagger_gap_ok = candidate.opportunity_type != FundingOpportunityType::Staggered
            || self.config.strategy.max_stagger_gap_minutes <= 0
            || candidate.stagger_gap_ms <= stagger_gap_limit_ms;
        let order_size_positive = candidate.quantity > 0.0;
        let funding_edge_ok =
            candidate.funding_edge_bps >= self.config.strategy.min_funding_edge_bps;
        let expected_edge_ok =
            candidate.expected_edge_bps >= self.config.strategy.min_expected_edge_bps;
        let worst_case_edge_ok =
            candidate.worst_case_edge_bps >= self.config.strategy.min_worst_case_edge_bps;
        let hyperliquid_gate_reason = if candidate.long_venue == Venue::Hyperliquid
            || candidate.short_venue == Venue::Hyperliquid
        {
            self.recent_submit_ack_ms
                .get(&Venue::Hyperliquid)
                .and_then(|samples| hyperliquid_entry_gate_reason(&self.config.strategy, samples))
        } else {
            None
        };
        let long_cooldown_reason = self.venue_entry_cooldown_reason(candidate.long_venue);
        let short_cooldown_reason = self.venue_entry_cooldown_reason(candidate.short_venue);
        let cached_balance_reason = self.cached_entry_balance_reason(candidate);
        let active_symbol_clear = !self.has_active_symbol(&candidate.symbol);
        let slot_available = remaining_slots > 0;
        let selected_for_entry = selected_ids
            .iter()
            .any(|pair_id| *pair_id == candidate.pair_id);
        let selection_blocker = if selected_for_entry {
            None
        } else if !self.config.runtime.auto_trade_enabled {
            Some("auto_trade_disabled".to_string())
        } else if remaining_slots == 0 {
            Some("no_remaining_slots".to_string())
        } else if !candidate.is_tradeable() {
            Some("candidate_blocked".to_string())
        } else if !active_symbol_clear {
            Some("symbol_already_active".to_string())
        } else {
            Some("not_selected_by_ranking".to_string())
        };

        let long_leg_risk = self.build_entry_leg_risk(
            market,
            HedgeLeg::Long,
            candidate.long_venue,
            &candidate.symbol,
            Side::Buy,
            candidate.quantity,
        );
        let short_leg_risk = self.build_entry_leg_risk(
            market,
            HedgeLeg::Short,
            candidate.short_venue,
            &candidate.symbol,
            Side::Sell,
            candidate.quantity,
        );
        let selection_score = self.runtime_candidate_selection_score(market, candidate);
        let mut blocked_reasons = Vec::new();
        let mut advisories = candidate.advisories.clone();
        for reason in &candidate.blocked_reasons {
            let (blocked_reason, advisory_reason) = remap_no_entry_diagnostic_reason(reason);
            if let Some(reason) = blocked_reason {
                blocked_reasons.push(reason);
            }
            if let Some(reason) = advisory_reason {
                advisories.push(reason);
            }
        }

        OpportunityCandidateChecklist {
            pair_id: candidate.pair_id.clone(),
            symbol: candidate.symbol.clone(),
            long_venue: candidate.long_venue,
            short_venue: candidate.short_venue,
            tradeable: candidate.is_tradeable(),
            selected_for_entry,
            selection_score,
            selection_blocker,
            long_leg_risk,
            short_leg_risk,
            metrics: OpportunityCandidateMetrics {
                quantity: candidate.quantity,
                entry_notional_quote: candidate.entry_notional_quote,
                funding_edge_bps: candidate.funding_edge_bps,
                expected_edge_bps: candidate.expected_edge_bps,
                worst_case_edge_bps: candidate.worst_case_edge_bps,
                ranking_edge_bps: candidate.ranking_edge_bps,
                entry_cross_bps: candidate.entry_cross_bps,
                fee_bps: candidate.fee_bps,
                entry_slippage_bps: candidate.entry_slippage_bps,
                first_stage_expected_edge_bps: candidate.first_stage_expected_edge_bps,
                second_stage_incremental_funding_edge_bps: candidate
                    .second_stage_incremental_funding_edge_bps,
            },
            blocked_reasons,
            advisories,
            checklist: vec![
                checklist_item(
                    "market_fresh_long",
                    market_fresh_long,
                    format!("{} age_ok={market_fresh_long}", candidate.long_venue),
                ),
                checklist_item(
                    "market_fresh_short",
                    market_fresh_short,
                    format!("{} age_ok={market_fresh_short}", candidate.short_venue),
                ),
                checklist_item(
                    "funding_not_passed",
                    funding_not_passed,
                    format!("remaining_ms={remaining_ms}"),
                ),
                checklist_item(
                    "near_term_settlement_leg",
                    near_term_settlement_leg,
                    if near_term_settlement_leg {
                        format!("remaining_ms={remaining_ms}")
                    } else {
                        format!("remaining_ms={remaining_ms} exceeds_current_hour")
                    },
                ),
                checklist_item(
                    "inside_entry_window",
                    inside_entry_window,
                    format!(
                        "remaining_ms={remaining_ms} window={}..{}min",
                        self.config.strategy.min_scan_minutes_before_funding,
                        self.config.strategy.max_scan_minutes_before_funding
                    ),
                ),
                checklist_item(
                    "stagger_gap_ok",
                    stagger_gap_ok,
                    format!(
                        "gap_ms={} max_gap_ms={stagger_gap_limit_ms}",
                        candidate.stagger_gap_ms
                    ),
                ),
                checklist_item(
                    "order_size_positive",
                    order_size_positive,
                    format!("quantity={:.8}", candidate.quantity),
                ),
                checklist_item(
                    "funding_edge_ok",
                    funding_edge_ok,
                    format!(
                        "{:.4}>={:.4}",
                        candidate.funding_edge_bps, self.config.strategy.min_funding_edge_bps
                    ),
                ),
                checklist_item(
                    "expected_edge_ok",
                    expected_edge_ok,
                    format!(
                        "{:.4}>={:.4}",
                        candidate.expected_edge_bps, self.config.strategy.min_expected_edge_bps
                    ),
                ),
                checklist_item(
                    "worst_case_edge_ok",
                    worst_case_edge_ok,
                    format!(
                        "{:.4}>={:.4}",
                        candidate.worst_case_edge_bps, self.config.strategy.min_worst_case_edge_bps
                    ),
                ),
                checklist_item(
                    "hyperliquid_latency_gate_ok",
                    hyperliquid_gate_reason.is_none(),
                    hyperliquid_gate_reason.unwrap_or_else(|| "clear".to_string()),
                ),
                checklist_item(
                    "long_venue_cooldown_clear",
                    long_cooldown_reason.is_none(),
                    long_cooldown_reason.unwrap_or_else(|| "clear".to_string()),
                ),
                checklist_item(
                    "short_venue_cooldown_clear",
                    short_cooldown_reason.is_none(),
                    short_cooldown_reason.unwrap_or_else(|| "clear".to_string()),
                ),
                checklist_item(
                    "cached_entry_balance_clear",
                    cached_balance_reason.is_none(),
                    cached_balance_reason.unwrap_or_else(|| "clear".to_string()),
                ),
                checklist_item(
                    "active_symbol_clear",
                    active_symbol_clear,
                    format!("active_symbol_conflict={}", !active_symbol_clear),
                ),
                checklist_item(
                    "slot_available",
                    slot_available,
                    format!("remaining_slots={remaining_slots}"),
                ),
            ],
        }
    }

    fn runtime_candidate_selection_score(
        &self,
        market: &MarketView,
        candidate: &CandidateOpportunity,
    ) -> f64 {
        let long_risk = self.build_entry_leg_risk(
            market,
            HedgeLeg::Long,
            candidate.long_venue,
            &candidate.symbol,
            Side::Buy,
            candidate.quantity,
        );
        let short_risk = self.build_entry_leg_risk(
            market,
            HedgeLeg::Short,
            candidate.short_venue,
            &candidate.symbol,
            Side::Sell,
            candidate.quantity,
        );
        let risk = long_risk.total_score.max(short_risk.total_score).max(0.0);
        candidate.ranking_edge_bps / (1.0 + risk)
    }

    async fn reconcile_running_slots_if_needed(
        &mut self,
        now_ms: i64,
        candidates: &[CandidateOpportunity],
    ) -> Result<()> {
        let capacity = self.config.strategy.max_concurrent_positions.max(1);
        let has_tradeable_candidate_waiting_for_slot = candidates.iter().any(|candidate| {
            candidate.is_tradeable() && !self.has_active_symbol(&candidate.symbol)
        });
        if self.state.mode != EngineMode::Running
            || self.active_positions().is_empty()
            || self.active_position_count() < capacity
            || !has_tradeable_candidate_waiting_for_slot
            || !should_probe_live_recovery(
                self.last_running_slot_reconcile_probe_ms,
                now_ms,
                RUNNING_SLOT_RECONCILE_INTERVAL_MS,
            )
        {
            return Ok(());
        }

        let before = self.active_position_count();
        self.reconcile_open_positions_internal(false).await?;
        self.last_running_slot_reconcile_probe_ms = Some(now_ms.max(0));
        let after = self.active_position_count();
        if after < before {
            self.log_event_at(
                now_ms,
                "runtime.slot_reconcile",
                &json!({
                    "reason": "stale_open_positions_cleared",
                    "open_position_count_before": before,
                    "open_position_count_after": after,
                    "released_slots": before.saturating_sub(after),
                }),
            );
        }
        Ok(())
    }

    async fn reconcile_open_positions_internal(&mut self, log_resumed_events: bool) -> Result<()> {
        if self.active_positions().is_empty() {
            self.state.mode = EngineMode::Running;
            return Ok(());
        }

        let mut surviving_positions = Vec::new();
        for position in self.active_positions().to_vec() {
            let long_position = self
                .adapter(position.long_venue)?
                .fetch_position(&position.symbol)
                .await?;
            let short_position = self
                .adapter(position.short_venue)?
                .fetch_position(&position.symbol)
                .await?;
            let target = position.quantity;

            if approx_zero(long_position.size) && approx_zero(short_position.size) {
                self.log_critical_event(
                    "recovery.flat",
                    &json!({
                        "symbol": position.symbol,
                        "position_id": position.position_id,
                    }),
                )?;
                continue;
            }

            if approx_eq(long_position.size, target) && approx_eq(short_position.size, -target) {
                if log_resumed_events {
                    self.log_critical_event(
                        "recovery.resumed",
                        &json!({
                            "symbol": position.symbol,
                            "position_id": position.position_id,
                        }),
                    )?;
                }
                surviving_positions.push(position);
                continue;
            }

            self.state.mode = EngineMode::FailClosed;
            self.state.last_error = Some(format!(
                "exposure mismatch on restart: long={}, short={}, expected={}",
                long_position.size, short_position.size, target
            ));
            self.log_critical_event(
                "recovery.blocked",
                &json!({
                    "symbol": position.symbol,
                    "position_id": position.position_id,
                    "long_size": long_position.size,
                    "short_size": short_position.size,
                    "expected_size": target,
                }),
            )?;
            return Ok(());
        }

        self.state.open_positions = surviving_positions;
        self.sync_open_position_mirror();
        self.state.mode = EngineMode::Running;
        self.state.last_error = None;
        Ok(())
    }

    async fn manage_open_positions(&mut self, market: &MarketView) -> Result<()> {
        let position_ids = self
            .active_positions()
            .iter()
            .map(|position| position.position_id.clone())
            .collect::<Vec<_>>();
        for position_id in position_ids {
            self.manage_open_position(&position_id, market).await?;
            if self.state.mode != EngineMode::Running {
                break;
            }
        }
        Ok(())
    }

    async fn best_effort_position_market(
        &self,
        position: &OpenPosition,
        market: &MarketView,
    ) -> Result<MarketView> {
        let mut snapshots = Vec::new();
        for venue in [position.long_venue, position.short_venue] {
            let fallback_snapshot = market.venue_symbol_snapshot(venue, &position.symbol);
            let needs_refresh = fallback_snapshot.is_none()
                || !market.is_fresh(venue, self.config.runtime.max_market_age_ms);
            if needs_refresh {
                match self
                    .adapter(venue)?
                    .refresh_market_snapshot(&position.symbol)
                    .await
                {
                    Ok(snapshot) => snapshots.push(snapshot),
                    Err(_) => {
                        if let Some(snapshot) = fallback_snapshot {
                            snapshots.push(snapshot);
                        }
                    }
                }
            } else if let Some(snapshot) = fallback_snapshot {
                snapshots.push(snapshot);
            }
        }

        if snapshots.is_empty() {
            return Ok(market.clone());
        }

        Ok(MarketView::from_snapshots(snapshots))
    }

    async fn half_close_quantity(&self, position: &OpenPosition) -> Result<f64> {
        let target_quantity = ((position.initial_quantity.max(position.quantity))
            * SETTLEMENT_HALF_CLOSE_RATIO
            - position.settlement_half_closed_quantity)
            .max(0.0)
            .min(position.quantity);
        if target_quantity <= 0.0 {
            return Ok(0.0);
        }
        let plan = self
            .plan_executable_quantity(
                position.long_venue,
                position.short_venue,
                &position.symbol,
                target_quantity,
            )
            .await?;
        Ok(plan.executable_quantity.min(position.quantity))
    }

    fn close_leg_exchange_min_notional_violation(
        &self,
        venue: Venue,
        symbol: &str,
        side: Side,
        quantity: f64,
        market: &MarketView,
    ) -> Option<(Venue, f64, f64)> {
        if quantity <= 0.0 {
            return None;
        }
        let price_hint = self.order_price_hint(market, venue, symbol, side)?;
        let min_notional = self
            .adapters
            .get(&venue)
            .and_then(|adapter| adapter.min_entry_notional_quote_hint(symbol, Some(price_hint)))?;
        let leg_notional = quantity * price_hint;
        if leg_notional + 1e-9 < min_notional {
            Some((venue, leg_notional, min_notional))
        } else {
            None
        }
    }

    async fn settlement_partial_close_plan(
        &self,
        position: &OpenPosition,
        market: &MarketView,
    ) -> Result<PartialClosePlan> {
        let half_quantity = self.half_close_quantity(position).await?;
        if half_quantity <= 0.0 {
            return Ok(PartialClosePlan {
                quantity: 0.0,
                promoted_to_full_close: false,
                min_notional_violation: None,
            });
        }

        let half_violation = self
            .close_leg_exchange_min_notional_violation(
                position.short_venue,
                &position.symbol,
                Side::Buy,
                half_quantity,
                market,
            )
            .or_else(|| {
                self.close_leg_exchange_min_notional_violation(
                    position.long_venue,
                    &position.symbol,
                    Side::Sell,
                    half_quantity,
                    market,
                )
            });
        if half_violation.is_none() {
            return Ok(PartialClosePlan {
                quantity: half_quantity,
                promoted_to_full_close: false,
                min_notional_violation: None,
            });
        }

        let full_quantity = position.quantity.max(0.0);
        let full_violation = self
            .close_leg_exchange_min_notional_violation(
                position.short_venue,
                &position.symbol,
                Side::Buy,
                full_quantity,
                market,
            )
            .or_else(|| {
                self.close_leg_exchange_min_notional_violation(
                    position.long_venue,
                    &position.symbol,
                    Side::Sell,
                    full_quantity,
                    market,
                )
            });
        if full_violation.is_none() {
            return Ok(PartialClosePlan {
                quantity: full_quantity,
                promoted_to_full_close: true,
                min_notional_violation: half_violation,
            });
        }

        Ok(PartialClosePlan {
            quantity: 0.0,
            promoted_to_full_close: false,
            min_notional_violation: full_violation.or(half_violation),
        })
    }

    async fn manage_open_position(&mut self, position_id: &str, market: &MarketView) -> Result<()> {
        let max_market_age_ms = self.config.runtime.max_market_age_ms;
        let post_funding_hold_ms = self
            .config
            .strategy
            .post_funding_hold_secs
            .saturating_mul(1_000);
        let stop_loss_quote = self.config.strategy.stop_loss_quote;
        let profit_take_quote = self.config.strategy.profit_take_quote;
        let trailing_drawdown_quote = self.config.strategy.trailing_drawdown_quote;
        let Some(position_snapshot) = self
            .active_positions()
            .iter()
            .find(|position| position.position_id == position_id)
            .cloned()
        else {
            return Ok(());
        };
        let position_market = self
            .best_effort_position_market(&position_snapshot, market)
            .await?;
        let evaluation_now_ms = position_market.now_ms().max(market.now_ms());
        let mut guard_failure = None;
        let mut guard_bypass = None;
        let mut exit_payload = None;
        let mut partial_exit_payload = None;
        let mut close_reason = None;
        let partial_close_due;
        let force_close_due;

        {
            let Some(position) = self
                .active_positions_mut()
                .iter_mut()
                .find(|position| position.position_id == position_id)
            else {
                return Ok(());
            };

            let market_has_quotes = position_market
                .symbol(position.long_venue, &position.symbol)
                .is_some()
                && position_market
                    .symbol(position.short_venue, &position.symbol)
                    .is_some();
            let market_fresh = position_market.is_fresh(position.long_venue, max_market_age_ms)
                && position_market.is_fresh(position.short_venue, max_market_age_ms);
            partial_close_due = position.settlement_half_closed_at_ms <= 0
                && evaluation_now_ms >= position.funding_timestamp_ms;
            force_close_due = evaluation_now_ms
                >= position
                    .funding_timestamp_ms
                    .saturating_add(SETTLEMENT_FORCE_CLOSE_DELAY_MS);

            Self::update_position_funding_capture_state(
                position,
                evaluation_now_ms,
                post_funding_hold_ms,
            );

            if !market_has_quotes && !partial_close_due && !force_close_due {
                if let Some(reason) =
                    Self::stale_market_safe_close_reason(position, evaluation_now_ms)
                {
                    exit_payload = Some(json!({
                        "position_id": &position.position_id,
                        "symbol": &position.symbol,
                        "long_venue": position.long_venue,
                        "short_venue": position.short_venue,
                        "quantity": position.quantity,
                        "reason": reason,
                        "current_net_quote": position.current_net_quote,
                        "peak_net_quote": position.peak_net_quote,
                        "captured_funding_quote": position.captured_funding_quote,
                        "second_stage_funding_quote": position.second_stage_funding_quote,
                        "funding_captured": position.funding_captured,
                        "second_stage_funding_captured": position.second_stage_funding_captured,
                        "opportunity_type": position.opportunity_type,
                        "market_guard_reason": "missing_market_data",
                    }));
                    close_reason = Some(reason.to_string());
                    guard_bypass = Some(json!({
                        "position_id": &position.position_id,
                        "symbol": &position.symbol,
                        "long_venue": position.long_venue,
                        "short_venue": position.short_venue,
                        "guard_reason": "missing_market_data",
                        "close_reason": reason,
                    }));
                } else {
                    guard_failure = Some(json!({
                        "position_id": &position.position_id,
                        "symbol": &position.symbol,
                        "long_venue": position.long_venue,
                        "short_venue": position.short_venue,
                        "reason": "missing_market_data",
                    }));
                }
            } else if !market_fresh && !partial_close_due && !force_close_due {
                if let Some(reason) =
                    Self::stale_market_safe_close_reason(position, evaluation_now_ms)
                {
                    exit_payload = Some(json!({
                        "position_id": &position.position_id,
                        "symbol": &position.symbol,
                        "long_venue": position.long_venue,
                        "short_venue": position.short_venue,
                        "quantity": position.quantity,
                        "reason": reason,
                        "current_net_quote": position.current_net_quote,
                        "peak_net_quote": position.peak_net_quote,
                        "captured_funding_quote": position.captured_funding_quote,
                        "second_stage_funding_quote": position.second_stage_funding_quote,
                        "funding_captured": position.funding_captured,
                        "second_stage_funding_captured": position.second_stage_funding_captured,
                        "opportunity_type": position.opportunity_type,
                        "market_guard_reason": "stale_market_data",
                    }));
                    close_reason = Some(reason.to_string());
                    guard_bypass = Some(json!({
                        "position_id": &position.position_id,
                        "symbol": &position.symbol,
                        "long_venue": position.long_venue,
                        "short_venue": position.short_venue,
                        "guard_reason": "stale_market_data",
                        "close_reason": reason,
                    }));
                } else {
                    guard_failure = Some(json!({
                        "position_id": &position.position_id,
                        "symbol": &position.symbol,
                        "long_venue": position.long_venue,
                        "short_venue": position.short_venue,
                        "reason": "stale_market_data",
                    }));
                }
            } else {
                let unrealized_price_pnl = match (
                    position_market.symbol(position.long_venue, &position.symbol),
                    position_market.symbol(position.short_venue, &position.symbol),
                ) {
                    (Some(long_quote), Some(short_quote)) => {
                        (long_quote.mid_price() - position.long_entry_price) * position.quantity
                            + (position.short_entry_price - short_quote.mid_price())
                                * position.quantity
                    }
                    _ => 0.0,
                };
                position.current_net_quote = position.realized_price_pnl_quote
                    + unrealized_price_pnl
                    + position.captured_funding_quote
                    + position.second_stage_funding_quote
                    - position.total_entry_fee_quote
                    - position.realized_exit_fee_quote;
                if position.current_net_quote > position.peak_net_quote {
                    position.peak_net_quote = position.current_net_quote;
                }

                let standard_close_reason = Self::standard_close_reason(
                    position,
                    stop_loss_quote,
                    profit_take_quote,
                    trailing_drawdown_quote,
                    evaluation_now_ms,
                );
                let reason = if force_close_due && position.quantity > 0.0 {
                    Some("settlement_force_close")
                } else if partial_close_due && position.quantity > 0.0 {
                    Some("settlement_half_close")
                } else {
                    standard_close_reason
                };

                if let Some(reason) = reason {
                    let payload = json!({
                        "position_id": &position.position_id,
                        "symbol": &position.symbol,
                        "long_venue": position.long_venue,
                        "short_venue": position.short_venue,
                        "quantity": position.quantity,
                        "reason": reason,
                        "current_net_quote": position.current_net_quote,
                        "peak_net_quote": position.peak_net_quote,
                        "captured_funding_quote": position.captured_funding_quote,
                        "second_stage_funding_quote": position.second_stage_funding_quote,
                        "funding_captured": position.funding_captured,
                        "second_stage_funding_captured": position.second_stage_funding_captured,
                        "opportunity_type": position.opportunity_type,
                    });
                    if reason == "settlement_half_close" {
                        partial_exit_payload = Some(payload);
                    } else {
                        exit_payload = Some(payload);
                    }
                    close_reason = Some(reason.to_string());
                }
            }
        }

        if let Some(payload) = guard_failure {
            self.log_event("execution.guard_failed", &payload);
            return Ok(());
        }
        if let Some(payload) = guard_bypass {
            self.log_event("execution.guard_bypassed", &payload);
        }

        if let Some(payload) = partial_exit_payload {
            self.log_event("execution.partial_exit_triggered", &payload);
        }
        if let Some(payload) = exit_payload {
            self.log_event("execution.exit_triggered", &payload);
        }

        let Some(reason) = close_reason else {
            return Ok(());
        };

        if reason == "settlement_half_close" {
            let close_plan = self
                .settlement_partial_close_plan(&position_snapshot, &position_market)
                .await?;
            if let Some((venue, leg_notional, min_notional)) = close_plan.min_notional_violation {
                self.log_event(
                    "execution.partial_exit_quantity_adjusted",
                    &json!({
                        "position_id": position_id,
                        "symbol": &position_snapshot.symbol,
                        "reason": "exchange_min_notional",
                        "requested_half_close_quantity": position_snapshot.initial_quantity.max(position_snapshot.quantity) * SETTLEMENT_HALF_CLOSE_RATIO,
                        "adjusted_quantity": close_plan.quantity,
                        "promoted_to_full_close": close_plan.promoted_to_full_close,
                        "violating_venue": venue,
                        "violating_leg_notional_quote": leg_notional,
                        "venue_min_notional_quote": min_notional,
                    }),
                );
            }
            if close_plan.quantity <= 0.0 {
                self.log_event(
                    "execution.order_blocked",
                    &json!({
                        "position_id": position_id,
                        "stage": "partial_exit",
                        "reason": close_plan
                            .min_notional_violation
                            .map(|(venue, leg_notional, min_notional)| format!(
                                "close_leg_notional_below_exchange_minimum:{leg_notional:.6}<{min_notional:.6} on {venue}"
                            ))
                            .unwrap_or_else(|| "quantity_rounded_to_zero".to_string()),
                    }),
                );
                return Ok(());
            }
            if close_plan.promoted_to_full_close {
                let fallback_reason = self
                    .active_positions()
                    .iter()
                    .find(|position| position.position_id == position_id)
                    .and_then(|position| {
                        Self::standard_close_reason(
                            position,
                            stop_loss_quote,
                            profit_take_quote,
                            trailing_drawdown_quote,
                            evaluation_now_ms,
                        )
                    });
                if let Some(fallback_reason) = fallback_reason {
                    self.try_close_position(position_id, fallback_reason, &position_market)
                        .await?;
                } else {
                    self.log_event(
                        "execution.partial_exit_skipped",
                        &json!({
                            "position_id": position_id,
                            "symbol": &position_snapshot.symbol,
                            "reason": "exchange_min_notional_requires_full_position_but_standard_close_not_ready",
                        }),
                    );
                }
            } else {
                self.try_partial_close_position(
                    position_id,
                    &reason,
                    close_plan.quantity,
                    evaluation_now_ms,
                    &position_market,
                )
                .await?;
            }
        } else {
            self.try_close_position(position_id, &reason, &position_market)
                .await?;
        }
        Ok(())
    }

    fn standard_close_reason(
        position: &OpenPosition,
        stop_loss_quote: f64,
        profit_take_quote: f64,
        trailing_drawdown_quote: f64,
        evaluation_now_ms: i64,
    ) -> Option<&'static str> {
        if position.current_net_quote <= -stop_loss_quote {
            Some("hard_stop")
        } else if Self::remaining_close_delay_active(position, evaluation_now_ms) {
            None
        } else if position.peak_net_quote >= profit_take_quote
            && position.peak_net_quote - position.current_net_quote >= trailing_drawdown_quote
        {
            Some("trailing_exit")
        } else if position.opportunity_type == FundingOpportunityType::Staggered
            && position.funding_captured
            && position.exit_after_first_stage
        {
            Some("first_stage_capture")
        } else if position.opportunity_type == FundingOpportunityType::Staggered
            && position.second_stage_enabled_at_entry
            && position.second_stage_funding_captured
        {
            Some("second_stage_capture")
        } else if position.funding_captured
            && position.current_net_quote >= 0.0
            && !(position.opportunity_type == FundingOpportunityType::Staggered
                && position.second_stage_enabled_at_entry
                && !position.second_stage_funding_captured)
        {
            Some("funding_capture")
        } else {
            None
        }
    }

    fn stale_market_safe_close_reason(
        position: &OpenPosition,
        evaluation_now_ms: i64,
    ) -> Option<&'static str> {
        if Self::remaining_close_delay_active(position, evaluation_now_ms) {
            return None;
        }
        if position.opportunity_type == FundingOpportunityType::Staggered
            && position.funding_captured
            && position.exit_after_first_stage
        {
            Some("first_stage_capture")
        } else if position.opportunity_type == FundingOpportunityType::Staggered
            && position.second_stage_enabled_at_entry
            && position.second_stage_funding_captured
        {
            Some("second_stage_capture")
        } else if position.funding_captured
            && position.current_net_quote >= 0.0
            && !(position.opportunity_type == FundingOpportunityType::Staggered
                && position.second_stage_enabled_at_entry
                && !position.second_stage_funding_captured)
        {
            Some("funding_capture")
        } else {
            None
        }
    }

    fn remaining_close_delay_active(position: &OpenPosition, evaluation_now_ms: i64) -> bool {
        position.settlement_half_closed_at_ms > 0
            && evaluation_now_ms
                < position
                    .funding_timestamp_ms
                    .saturating_add(SETTLEMENT_REMAINDER_CLOSE_DELAY_MS)
    }

    fn update_position_funding_capture_state(
        position: &mut OpenPosition,
        evaluation_now_ms: i64,
        post_funding_hold_ms: i64,
    ) {
        let previous_total_funding_quote =
            position.captured_funding_quote + position.second_stage_funding_quote;

        if !position.funding_captured
            && evaluation_now_ms >= position.funding_timestamp_ms + post_funding_hold_ms
        {
            position.funding_captured = true;
            position.captured_funding_quote =
                position.entry_notional_quote * position.funding_edge_bps_entry / 10_000.0;
        }
        if position.funding_captured
            && position.second_stage_enabled_at_entry
            && !position.second_stage_funding_captured
            && position.second_funding_timestamp_ms > position.funding_timestamp_ms
            && evaluation_now_ms >= position.second_funding_timestamp_ms + post_funding_hold_ms
        {
            position.second_stage_funding_captured = true;
            position.second_stage_funding_quote = position.entry_notional_quote
                * (position.total_funding_edge_bps_entry - position.funding_edge_bps_entry)
                / 10_000.0;
        }

        let funding_delta_quote = position.captured_funding_quote
            + position.second_stage_funding_quote
            - previous_total_funding_quote;
        if funding_delta_quote.abs() > f64::EPSILON {
            position.current_net_quote += funding_delta_quote;
            if position.current_net_quote > position.peak_net_quote {
                position.peak_net_quote = position.current_net_quote;
            }
        }
    }

    fn build_segment_outcome_diagnostics(
        &self,
        position: &OpenPosition,
        market: &MarketView,
        reason: &str,
        segment_kind: &str,
        closed_quantity: f64,
        realized_price_pnl_quote: f64,
        total_exit_fee_quote: f64,
    ) -> Option<serde_json::Value> {
        if !closed_quantity.is_finite() || closed_quantity <= 0.0 {
            return None;
        }
        let total_quantity = position
            .initial_quantity
            .max(position.quantity + position.settlement_half_closed_quantity)
            .max(closed_quantity);
        if !total_quantity.is_finite() || total_quantity <= 0.0 {
            return None;
        }
        let closed_fraction = (closed_quantity / total_quantity).clamp(0.0, 1.0);
        let segment_entry_notional_quote = position.entry_notional_quote * closed_fraction;
        let actual_funding_quote_total =
            position.captured_funding_quote + position.second_stage_funding_quote;
        let actual_funding_quote = actual_funding_quote_total * closed_fraction;
        let expected_total_funding_quote =
            position.entry_notional_quote * position.total_funding_edge_bps_entry / 10_000.0;
        let expected_funding_quote = expected_total_funding_quote * closed_fraction;
        let funding_shortfall_quote = (expected_funding_quote - actual_funding_quote).max(0.0);
        let allocated_entry_fee_quote = position.total_entry_fee_quote * closed_fraction;
        let fee_drag_quote = allocated_entry_fee_quote + total_exit_fee_quote;
        let estimated_entry_impact_quote =
            segment_entry_notional_quote * position.entry_slippage_bps_entry.max(0.0) / 10_000.0;
        let adverse_price_move_quote = (-realized_price_pnl_quote).max(0.0);
        let favorable_price_move_quote = realized_price_pnl_quote.max(0.0);
        let realized_net_quote = realized_price_pnl_quote + actual_funding_quote
            - allocated_entry_fee_quote
            - total_exit_fee_quote;
        let current_total_funding_edge_bps = self.current_total_funding_edge_bps(position, market);
        let funding_edge_narrowed_quote = current_total_funding_edge_bps
            .map(|current_bps| {
                ((position.total_funding_edge_bps_entry - current_bps).max(0.0)
                    * segment_entry_notional_quote)
                    / 10_000.0
            })
            .unwrap_or_default();
        let funding_edge_widened_quote = current_total_funding_edge_bps
            .map(|current_bps| {
                ((current_bps - position.total_funding_edge_bps_entry).max(0.0)
                    * segment_entry_notional_quote)
                    / 10_000.0
            })
            .unwrap_or_default();
        let timing_unfavorable = !position.funding_captured && adverse_price_move_quote > 0.0;
        let timing_favorable = !position.funding_captured && favorable_price_move_quote > 0.0;
        let outcome = if realized_net_quote > 0.0 {
            "profit"
        } else if realized_net_quote < 0.0 {
            "loss"
        } else {
            "flat"
        };

        let mut contributing_reasons = Vec::new();
        if adverse_price_move_quote > 0.0 {
            contributing_reasons.push("adverse_price_move");
        }
        if favorable_price_move_quote > 0.0 {
            contributing_reasons.push("favorable_price_move");
        }
        if timing_unfavorable {
            contributing_reasons.push("entry_timing_unfavorable");
        }
        if timing_favorable {
            contributing_reasons.push("entry_timing_favorable");
        }
        if funding_shortfall_quote > 0.0 {
            contributing_reasons.push("funding_shortfall");
        }
        if actual_funding_quote > 0.0 {
            contributing_reasons.push("funding_realized");
        }
        if funding_edge_narrowed_quote > 0.0 {
            contributing_reasons.push("funding_edge_narrowed");
        }
        if funding_edge_widened_quote > 0.0 {
            contributing_reasons.push("funding_edge_widened");
        }
        if position.entry_depth_capped_at_entry {
            contributing_reasons.push("entry_depth_constrained");
        }
        if estimated_entry_impact_quote > 0.0 {
            contributing_reasons.push("entry_impact_high");
        }
        if fee_drag_quote > 0.0 {
            contributing_reasons.push("fee_drag");
        }

        let funding_pressure_quote = funding_shortfall_quote.max(funding_edge_narrowed_quote);
        let entry_impact_pressure_quote = if position.entry_depth_capped_at_entry {
            estimated_entry_impact_quote.max(0.000_001)
        } else {
            estimated_entry_impact_quote
        };
        let primary_reason = match outcome {
            "loss" => {
                if timing_unfavorable {
                    "entry_timing_unfavorable"
                } else {
                    [
                        ("adverse_price_move", adverse_price_move_quote),
                        ("funding_edge_narrowed", funding_pressure_quote),
                        ("entry_depth_or_slippage", entry_impact_pressure_quote),
                        ("fee_drag", fee_drag_quote),
                    ]
                    .into_iter()
                    .max_by(|left, right| left.1.total_cmp(&right.1))
                    .map(|(reason, _)| reason)
                    .unwrap_or("net_loss")
                }
            }
            "profit" => [
                ("favorable_price_move", favorable_price_move_quote),
                ("funding_realized", actual_funding_quote),
                ("funding_edge_widened", funding_edge_widened_quote),
            ]
            .into_iter()
            .max_by(|left, right| left.1.total_cmp(&right.1))
            .map(|(reason, _)| reason)
            .unwrap_or("net_profit"),
            _ => "flat_outcome",
        };

        Some(json!({
            "segment_kind": segment_kind,
            "closed_quantity": closed_quantity,
            "closed_fraction": closed_fraction,
            "net_quote": realized_net_quote,
            "profit_quote": realized_net_quote.max(0.0),
            "loss_quote": (-realized_net_quote).max(0.0),
            "outcome": outcome,
            "primary_reason": primary_reason,
            "contributing_reasons": contributing_reasons,
            "actual_funding_quote": actual_funding_quote,
            "funding_shortfall_quote": funding_shortfall_quote,
            "realized_price_pnl_quote": realized_price_pnl_quote,
            "adverse_price_move_quote": adverse_price_move_quote,
            "favorable_price_move_quote": favorable_price_move_quote,
            "fee_drag_quote": fee_drag_quote,
            "funding_edge_narrowed_quote": funding_edge_narrowed_quote,
            "funding_edge_widened_quote": funding_edge_widened_quote,
            "entry_depth_capped_at_entry": position.entry_depth_capped_at_entry,
            "entry_total_funding_edge_bps": position.total_funding_edge_bps_entry,
            "current_total_funding_edge_bps": current_total_funding_edge_bps,
            "exit_reason": reason,
        }))
    }

    async fn try_open_position(
        &mut self,
        candidate: CandidateOpportunity,
        market: &MarketView,
    ) -> Result<()> {
        let mut entry_market = market.clone();
        let entry_started_at = Instant::now();
        let position_id = format!("pos-{}-{}", self.state.cycle, candidate.pair_id);
        let fee_snapshots = self.cached_live_fee_snapshots();
        let long_fee = resolved_fee_log_entry(&self.config, &fee_snapshots, candidate.long_venue);
        let short_fee = resolved_fee_log_entry(&self.config, &fee_snapshots, candidate.short_venue);
        self.log_event(
            "execution.entry_selected",
            &json!({
                "position_id": &position_id,
                "pair_id": &candidate.pair_id,
                "candidate": &candidate,
                "fee_resolution": {
                    "long": &long_fee,
                    "short": &short_fee,
                },
            }),
        );
        let quote_refresh_started_at = Instant::now();
        let quote_refresh_result = self
            .refresh_entry_candidate_quotes_if_needed(&candidate, &position_id, &mut entry_market)
            .await?;
        let proactive_entry_quote_refresh_ms = duration_ms_u64(quote_refresh_started_at.elapsed());
        if quote_refresh_result.refreshed_leg_count > 0
            || quote_refresh_result.block_reason.is_some()
        {
            self.log_event(
                "execution.entry_quote_verification",
                &json!({
                    "position_id": &position_id,
                    "pair_id": &candidate.pair_id,
                    "symbol": &candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "max_order_quote_age_ms": self.config.runtime.max_order_quote_age_ms,
                    "proactive_refresh_quote_age_ms": self.config.runtime.max_order_quote_age_ms.saturating_mul(PROACTIVE_ENTRY_QUOTE_REFRESH_MULTIPLIER),
                    "hard_block_quote_age_ms": self.config.runtime.max_order_quote_age_ms.saturating_mul(3),
                    "refresh_count": quote_refresh_result.refreshed_leg_count,
                    "max_pre_refresh_quote_age_ms": quote_refresh_result.max_pre_refresh_age_ms,
                    "block_reason": &quote_refresh_result.block_reason,
                    "legs": &quote_refresh_result.legs,
                }),
            );
        }
        if let Some(reason) = quote_refresh_result.block_reason {
            self.state.last_error = Some(format!(
                "entry quote stale after refresh for {} on {} and {}",
                candidate.symbol, candidate.long_venue, candidate.short_venue
            ));
            self.log_event(
                "execution.entry_blocked",
                &json!({
                    "position_id": &position_id,
                    "pair_id": &candidate.pair_id,
                    "symbol": &candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "reason": reason,
                }),
            );
            return Ok(());
        }
        let quantity_plan_started_at = Instant::now();
        let quantity_plan = self
            .plan_executable_quantity(
                candidate.long_venue,
                candidate.short_venue,
                &candidate.symbol,
                candidate.quantity,
            )
            .await?;
        let quantity_plan_ms = duration_ms_u64(quantity_plan_started_at.elapsed());
        let executable_quantity = quantity_plan.executable_quantity;
        let target_notional_quote = self
            .config
            .venue(candidate.long_venue)
            .map(|item| item.max_notional)
            .unwrap_or_default()
            .min(
                self.config
                    .venue(candidate.short_venue)
                    .map(|item| item.max_notional)
                    .unwrap_or_default(),
            )
            .min(crate::strategy::effective_max_entry_notional(&self.config));
        let normalized_entry_notional =
            candidate.entry_notional_quote * (executable_quantity / candidate.quantity.max(1e-9));
        self.log_event(
            "execution.quantity_normalized",
            &json!({
                "position_id": &position_id,
                "pair_id": &candidate.pair_id,
                "symbol": &candidate.symbol,
                "long_venue": candidate.long_venue,
                "short_venue": candidate.short_venue,
                "requested_entry_notional_quote": candidate.entry_notional_quote,
                "executable_entry_notional_quote": normalized_entry_notional,
                "plan": &quantity_plan,
            }),
        );
        if executable_quantity <= 0.0 {
            self.state.last_error = Some(format!(
                "entry quantity rounded to zero for {} on {} and {}",
                candidate.symbol, candidate.long_venue, candidate.short_venue
            ));
            self.log_event(
                "execution.entry_blocked",
                &json!({
                    "position_id": &position_id,
                    "pair_id": &candidate.pair_id,
                    "symbol": &candidate.symbol,
                    "long_venue": candidate.long_venue,
                    "short_venue": candidate.short_venue,
                    "reason": "quantity_rounded_to_zero",
                }),
            );
            return Ok(());
        }
        let short_request = OrderRequest {
            symbol: candidate.symbol.clone(),
            side: Side::Sell,
            quantity: executable_quantity,
            reduce_only: false,
            client_order_id: compact_client_order_id(position_id.as_str(), "entry_short"),
            price_hint: self.order_price_hint(
                &entry_market,
                candidate.short_venue,
                &candidate.symbol,
                Side::Sell,
            ),
            mark_price_hint: self.order_mark_price_hint(
                &entry_market,
                candidate.short_venue,
                &candidate.symbol,
            ),
            observed_at_ms: entry_market.observed_at_ms(candidate.short_venue),
        };
        let long_request = OrderRequest {
            symbol: candidate.symbol.clone(),
            side: Side::Buy,
            quantity: executable_quantity,
            reduce_only: false,
            client_order_id: compact_client_order_id(position_id.as_str(), "entry_long"),
            price_hint: self.order_price_hint(
                &entry_market,
                candidate.long_venue,
                &candidate.symbol,
                Side::Buy,
            ),
            mark_price_hint: self.order_mark_price_hint(
                &entry_market,
                candidate.long_venue,
                &candidate.symbol,
            ),
            observed_at_ms: entry_market.observed_at_ms(candidate.long_venue),
        };
        let short_leg = EntryLegPlan {
            leg: HedgeLeg::Short,
            venue: candidate.short_venue,
            request: short_request,
        };
        let long_leg = EntryLegPlan {
            leg: HedgeLeg::Long,
            venue: candidate.long_venue,
            request: long_request,
        };
        let long_risk = self.build_entry_leg_risk(
            &entry_market,
            HedgeLeg::Long,
            candidate.long_venue,
            &candidate.symbol,
            long_leg.request.side,
            executable_quantity,
        );
        let long_leg_notional_quote = order_request_notional_quote(&long_leg.request);
        let short_risk = self.build_entry_leg_risk(
            &entry_market,
            HedgeLeg::Short,
            candidate.short_venue,
            &candidate.symbol,
            short_leg.request.side,
            executable_quantity,
        );
        let short_leg_notional_quote = order_request_notional_quote(&short_leg.request);
        let long_leg_trace = self.entry_leg_notional_trace(candidate.long_venue, &long_leg.request);
        let short_leg_trace =
            self.entry_leg_notional_trace(candidate.short_venue, &short_leg.request);
        let (first_leg, first_risk, second_leg, second_risk) =
            if long_risk.total_score > short_risk.total_score {
                (long_leg, long_risk, short_leg, short_risk)
            } else {
                (short_leg, short_risk, long_leg, long_risk)
            };
        self.log_event(
            "execution.entry_notional_trace",
            &json!({
                "position_id": &position_id,
                "pair_id": &candidate.pair_id,
                "symbol": &candidate.symbol,
                "target_notional_quote": target_notional_quote,
                "candidate_entry_notional_quote": candidate.entry_notional_quote,
                "normalized_quantity": executable_quantity,
                "long": long_leg_trace,
                "short": short_leg_trace,
            }),
        );
        self.log_event(
            "execution.entry_order_plan",
            &json!({
                "position_id": &position_id,
                "pair_id": &candidate.pair_id,
                "symbol": &candidate.symbol,
                "first": &first_risk,
                "second": &second_risk,
            }),
        );
        if let Some((stage, venue, leg_notional, min_notional)) =
            self.entry_leg_notional_floor_violation(&[&first_leg, &second_leg])
        {
            self.state.last_error = Some(format!(
                "entry leg notional below minimum: {leg_notional:.6}<{min_notional:.6} on {venue}"
            ));
            self.log_event(
                "execution.entry_blocked",
                &json!({
                    "position_id": &position_id,
                    "pair_id": &candidate.pair_id,
                    "symbol": &candidate.symbol,
                    "reason": "entry_leg_notional_below_minimum",
                    "stage": stage,
                    "venue": venue,
                    "leg_notional_quote": leg_notional,
                    "min_entry_leg_notional_quote": min_notional,
                    "long_leg_notional_quote": long_leg_notional_quote,
                    "short_leg_notional_quote": short_leg_notional_quote,
                }),
            );
            return Ok(());
        }
        let selected_to_first_submit_ms = duration_ms_u64(entry_started_at.elapsed());
        let request_build_ms = selected_to_first_submit_ms
            .saturating_sub(proactive_entry_quote_refresh_ms)
            .saturating_sub(quantity_plan_ms);
        self.log_event(
            "execution.entry_prepare_timing",
            &json!({
                "position_id": &position_id,
                "pair_id": &candidate.pair_id,
                "symbol": &candidate.symbol,
                "first_stage": first_leg.leg.entry_stage(),
                "first_venue": first_leg.venue,
                "second_stage": second_leg.leg.entry_stage(),
                "second_venue": second_leg.venue,
                "selected_to_first_submit_ms": selected_to_first_submit_ms,
                "proactive_entry_quote_refresh_ms": proactive_entry_quote_refresh_ms,
                "proactive_entry_quote_refresh_count": quote_refresh_result.refreshed_leg_count,
                "max_pre_refresh_quote_age_ms": quote_refresh_result.max_pre_refresh_age_ms,
                "quantity_plan_ms": quantity_plan_ms,
                "request_build_ms": request_build_ms,
            }),
        );

        let (first_fill, first_latency_ms) = match self
            .execute_order_leg(
                first_leg.venue,
                first_leg.request.clone(),
                OrderLegContext {
                    stage: first_leg.leg.entry_stage(),
                    position_id: &position_id,
                    pair_id: Some(&candidate.pair_id),
                },
            )
            .await
        {
            Ok(fill) => fill,
            Err(error) => {
                let cleanup_attempted = order_error_may_have_created_exposure(&error);
                let cleanup_result = if cleanup_attempted {
                    self.cleanup_failed_leg_exposure(
                        first_leg.venue,
                        &candidate.symbol,
                        &position_id,
                        Some(&candidate.pair_id),
                        first_leg.leg.cleanup_stage(),
                        &entry_market,
                    )
                    .await
                } else {
                    Ok(false)
                };
                self.state.last_error = Some(format!(
                    "entry {} leg failed: {error:#}",
                    first_leg.leg.label()
                ));
                let event_kind = format!("entry.{}_failed", first_leg.leg.label());
                self.log_event(
                    event_kind.as_str(),
                    &json!({
                        "position_id": &position_id,
                        "pair_id": &candidate.pair_id,
                        "venue": first_leg.venue,
                        "leg": first_leg.leg.label(),
                        "error": error.to_string(),
                        "cleanup_attempted": cleanup_attempted,
                        "cleanup_succeeded": cleanup_result.as_ref().ok().copied(),
                        "cleanup_error": cleanup_result.as_ref().err().map(|item| item.to_string()),
                    }),
                );
                if let Err(cleanup_error) = cleanup_result {
                    self.state.mode = EngineMode::FailClosed;
                    self.log_critical_event(
                        "execution.compensation_failed",
                        &json!({
                            "position_id": &position_id,
                            "pair_id": &candidate.pair_id,
                            "symbol": &candidate.symbol,
                            "venue": first_leg.venue,
                            "leg": first_leg.leg.label(),
                            "error": error.to_string(),
                            "flatten_error": cleanup_error.to_string(),
                        }),
                    )?;
                }
                return Ok(());
            }
        };

        let (second_fill, second_latency_ms) = match self
            .execute_order_leg(
                second_leg.venue,
                second_leg.request.clone(),
                OrderLegContext {
                    stage: second_leg.leg.entry_stage(),
                    position_id: &position_id,
                    pair_id: Some(&candidate.pair_id),
                },
            )
            .await
        {
            Ok(fill) => fill,
            Err(error) => {
                let flatten_result = self
                    .flatten_single_leg(
                        first_leg.venue,
                        &candidate.symbol,
                        first_fill.quantity,
                        first_leg.request.side.opposite(),
                        &position_id,
                        Some(&candidate.pair_id),
                        first_leg.leg.compensate_stage(),
                        &entry_market,
                    )
                    .await;
                let failed_leg_cleanup = if order_error_may_have_created_exposure(&error) {
                    self.cleanup_failed_leg_exposure(
                        second_leg.venue,
                        &candidate.symbol,
                        &position_id,
                        Some(&candidate.pair_id),
                        second_leg.leg.cleanup_stage(),
                        &entry_market,
                    )
                    .await
                } else {
                    Ok(false)
                };
                self.state.last_error = Some(format!(
                    "entry {} leg failed: {error:#}",
                    second_leg.leg.label()
                ));
                self.log_event(
                    "entry.compensated",
                    &json!({
                        "position_id": &position_id,
                        "pair_id": &candidate.pair_id,
                        "error": error.to_string(),
                        "first_leg": first_leg.leg.label(),
                        "first_venue": first_leg.venue,
                        "second_leg": second_leg.leg.label(),
                        "second_venue": second_leg.venue,
                        "flattened": flatten_result.is_ok(),
                        "flatten_error": flatten_result.as_ref().err().map(|item| item.to_string()),
                        "failed_leg_cleanup": failed_leg_cleanup.as_ref().ok().copied(),
                        "failed_leg_cleanup_error": failed_leg_cleanup.as_ref().err().map(|item| item.to_string()),
                        "filled_first_quantity": first_fill.quantity,
                    }),
                );
                if let Err(flatten_error) = flatten_result {
                    self.state.mode = EngineMode::FailClosed;
                    self.log_critical_event(
                        "execution.compensation_failed",
                        &json!({
                            "position_id": &position_id,
                            "pair_id": &candidate.pair_id,
                            "symbol": &candidate.symbol,
                            "venue": first_leg.venue,
                            "leg": first_leg.leg.label(),
                            "error": error.to_string(),
                            "flatten_error": flatten_error.to_string(),
                            "filled_first_quantity": first_fill.quantity,
                        }),
                    )?;
                } else if let Err(cleanup_error) = failed_leg_cleanup {
                    self.state.mode = EngineMode::FailClosed;
                    self.log_critical_event(
                        "execution.compensation_failed",
                        &json!({
                            "position_id": &position_id,
                            "pair_id": &candidate.pair_id,
                            "symbol": &candidate.symbol,
                            "venue": second_leg.venue,
                            "leg": second_leg.leg.label(),
                            "error": error.to_string(),
                            "flatten_error": cleanup_error.to_string(),
                            "filled_first_quantity": first_fill.quantity,
                        }),
                    )?;
                }
                return Ok(());
            }
        };

        let (long_fill, long_latency_ms, short_fill, short_latency_ms) = match first_leg.leg {
            HedgeLeg::Long => (first_fill, first_latency_ms, second_fill, second_latency_ms),
            HedgeLeg::Short => (second_fill, second_latency_ms, first_fill, first_latency_ms),
        };

        self.log_event(
            "execution.entry_latency_summary",
            &json!({
                "position_id": &position_id,
                "pair_id": &candidate.pair_id,
                "symbol": &candidate.symbol,
                "long_venue": candidate.long_venue,
                "short_venue": candidate.short_venue,
                "total_roundtrip_ms": short_latency_ms + long_latency_ms,
                "max_single_order_ms": short_latency_ms.max(long_latency_ms),
            }),
        );

        let position = OpenPosition {
            position_id: position_id.clone(),
            symbol: candidate.symbol.clone(),
            long_venue: candidate.long_venue,
            short_venue: candidate.short_venue,
            quantity: executable_quantity,
            initial_quantity: executable_quantity,
            long_entry_price: long_fill.average_price,
            short_entry_price: short_fill.average_price,
            entry_notional_quote: normalized_entry_notional,
            funding_timestamp_ms: candidate.funding_timestamp_ms,
            long_funding_timestamp_ms: candidate.long_funding_timestamp_ms,
            short_funding_timestamp_ms: candidate.short_funding_timestamp_ms,
            opportunity_type: candidate.opportunity_type,
            first_funding_leg: candidate.first_funding_leg,
            second_funding_timestamp_ms: candidate.second_funding_timestamp_ms,
            funding_edge_bps_entry: candidate.funding_edge_bps,
            total_funding_edge_bps_entry: candidate.total_funding_edge_bps,
            expected_edge_bps_entry: candidate.expected_edge_bps,
            worst_case_edge_bps_entry: candidate.worst_case_edge_bps,
            entry_cross_bps_entry: candidate.entry_cross_bps,
            fee_bps_entry: candidate.fee_bps,
            entry_slippage_bps_entry: candidate.entry_slippage_bps,
            entry_depth_capped_at_entry: candidate
                .advisories
                .iter()
                .any(|item| item == "quantity_capped_by_top_book_depth"),
            total_entry_fee_quote: long_fill.fee_quote + short_fill.fee_quote,
            realized_price_pnl_quote: 0.0,
            realized_exit_fee_quote: 0.0,
            entered_at_ms: short_fill.filled_at_ms.max(long_fill.filled_at_ms),
            current_net_quote: -(long_fill.fee_quote + short_fill.fee_quote),
            peak_net_quote: -(long_fill.fee_quote + short_fill.fee_quote),
            funding_captured: false,
            second_stage_funding_captured: false,
            captured_funding_quote: 0.0,
            second_stage_funding_quote: 0.0,
            settlement_half_closed_quantity: 0.0,
            settlement_half_closed_at_ms: 0,
            exit_after_first_stage: !matches!(
                self.config.strategy.staggered_exit_mode,
                StaggeredExitMode::EvaluateSecondStage
            ) || candidate.opportunity_type
                == FundingOpportunityType::Aligned,
            second_stage_enabled_at_entry: matches!(
                self.config.strategy.staggered_exit_mode,
                StaggeredExitMode::EvaluateSecondStage
            ) && candidate.opportunity_type
                == FundingOpportunityType::Staggered
                && candidate.second_stage_incremental_funding_edge_bps > 0.0,
            exit_reason: None,
        };
        self.add_open_position(position.clone());
        self.log_critical_event("entry.opened", &position)?;
        self.persist_state()?;
        Ok(())
    }

    async fn execute_close_orders(
        &mut self,
        position: &OpenPosition,
        market: &MarketView,
        quantity: f64,
        short_stage: &str,
        long_stage: &str,
    ) -> Result<CloseExecution> {
        let short_close = OrderRequest {
            symbol: position.symbol.clone(),
            side: Side::Buy,
            quantity,
            reduce_only: true,
            client_order_id: compact_client_order_id(position.position_id.as_str(), short_stage),
            price_hint: self.order_price_hint(
                market,
                position.short_venue,
                &position.symbol,
                Side::Buy,
            ),
            mark_price_hint: self.order_mark_price_hint(
                market,
                position.short_venue,
                &position.symbol,
            ),
            observed_at_ms: market.observed_at_ms(position.short_venue),
        };
        let short_client_order_id = short_close.client_order_id.clone();
        let long_close = OrderRequest {
            symbol: position.symbol.clone(),
            side: Side::Sell,
            quantity,
            reduce_only: true,
            client_order_id: compact_client_order_id(position.position_id.as_str(), long_stage),
            price_hint: self.order_price_hint(
                market,
                position.long_venue,
                &position.symbol,
                Side::Sell,
            ),
            mark_price_hint: self.order_mark_price_hint(
                market,
                position.long_venue,
                &position.symbol,
            ),
            observed_at_ms: market.observed_at_ms(position.long_venue),
        };
        let long_client_order_id = long_close.client_order_id.clone();

        let short_submit_started_at_ms = wall_clock_now_ms();
        let (short_fill, short_latency_ms) = self
            .execute_order_leg(
                position.short_venue,
                short_close,
                OrderLegContext {
                    stage: short_stage,
                    position_id: &position.position_id,
                    pair_id: None,
                },
            )
            .await?;
        let long_submit_started_at_ms = wall_clock_now_ms();
        let (long_fill, long_latency_ms) = self
            .execute_order_leg(
                position.long_venue,
                long_close,
                OrderLegContext {
                    stage: long_stage,
                    position_id: &position.position_id,
                    pair_id: None,
                },
            )
            .await?;

        let total_exit_fee_quote = short_fill.fee_quote + long_fill.fee_quote;
        let realized_price_pnl_quote = (long_fill.average_price - position.long_entry_price)
            * long_fill.quantity
            + (position.short_entry_price - short_fill.average_price) * short_fill.quantity;

        Ok(CloseExecution {
            short_fill,
            short_client_order_id,
            short_submit_started_at_ms,
            short_latency_ms,
            long_fill,
            long_client_order_id,
            long_submit_started_at_ms,
            long_latency_ms,
            realized_price_pnl_quote,
            total_exit_fee_quote,
        })
    }

    async fn try_partial_close_position(
        &mut self,
        position_id: &str,
        reason: &str,
        quantity: f64,
        triggered_at_ms: i64,
        market: &MarketView,
    ) -> Result<()> {
        let Some(position) = self
            .active_positions()
            .iter()
            .find(|position| position.position_id == position_id)
            .cloned()
        else {
            return Ok(());
        };

        let execution = self
            .execute_close_orders(
                &position,
                market,
                quantity,
                "partial_exit_short",
                "partial_exit_long",
            )
            .await?;
        self.log_event(
            "execution.partial_exit_prepare_timing",
            &json!({
                "position_id": &position.position_id,
                "symbol": &position.symbol,
                "reason": reason,
                "funding_timestamp_ms": position.funding_timestamp_ms,
                "partial_close_triggered_at_ms": triggered_at_ms,
                "first_close_order_submitted_at_ms": execution.short_submit_started_at_ms,
                "second_close_order_submitted_at_ms": execution.long_submit_started_at_ms,
                "settlement_to_trigger_ms": triggered_at_ms.saturating_sub(position.funding_timestamp_ms),
                "trigger_to_submit_ms": execution.short_submit_started_at_ms.saturating_sub(triggered_at_ms),
                "settlement_to_first_submit_ms": execution.short_submit_started_at_ms.saturating_sub(position.funding_timestamp_ms),
            }),
        );
        let closed_quantity = execution
            .short_fill
            .quantity
            .min(execution.long_fill.quantity);
        let closed_at_ms = execution
            .short_fill
            .filled_at_ms
            .max(execution.long_fill.filled_at_ms);

        self.log_event(
            "execution.partial_exit_latency_summary",
            &json!({
                "position_id": &position.position_id,
                "symbol": &position.symbol,
                "reason": reason,
                "long_venue": position.long_venue,
                "short_venue": position.short_venue,
                "closed_quantity": closed_quantity,
                "total_roundtrip_ms": execution.short_latency_ms + execution.long_latency_ms,
                "max_single_order_ms": execution.short_latency_ms.max(execution.long_latency_ms),
                "partial_exit_fee_quote": execution.total_exit_fee_quote,
            }),
        );

        let updated_position = {
            let Some(position) = self
                .active_positions_mut()
                .iter_mut()
                .find(|position| position.position_id == position_id)
            else {
                return Ok(());
            };
            position.quantity = (position.quantity - closed_quantity).max(0.0);
            position.realized_price_pnl_quote += execution.realized_price_pnl_quote;
            position.realized_exit_fee_quote += execution.total_exit_fee_quote;
            position.settlement_half_closed_quantity += closed_quantity;
            if position.settlement_half_closed_at_ms <= 0 {
                position.settlement_half_closed_at_ms = closed_at_ms;
            }
            let unrealized_price_pnl = match (
                market.symbol(position.long_venue, &position.symbol),
                market.symbol(position.short_venue, &position.symbol),
            ) {
                (Some(long_quote), Some(short_quote)) => {
                    (long_quote.mid_price() - position.long_entry_price) * position.quantity
                        + (position.short_entry_price - short_quote.mid_price()) * position.quantity
                }
                _ => 0.0,
            };
            position.current_net_quote = position.realized_price_pnl_quote
                + unrealized_price_pnl
                + position.captured_funding_quote
                + position.second_stage_funding_quote
                - position.total_entry_fee_quote
                - position.realized_exit_fee_quote;
            if position.current_net_quote > position.peak_net_quote {
                position.peak_net_quote = position.current_net_quote;
            }
            position.clone()
        };
        self.sync_open_position_mirror();

        let partial_outcome_diagnostics = self.build_segment_outcome_diagnostics(
            &updated_position,
            market,
            reason,
            "settlement_half_close",
            closed_quantity,
            execution.realized_price_pnl_quote,
            execution.total_exit_fee_quote,
        );
        let partial_loss_diagnostics =
            self.compact_loss_diagnostics(partial_outcome_diagnostics.as_ref());

        let mut partial_close_payload = serde_json::to_value(&updated_position)?;
        let payload_object = partial_close_payload
            .as_object_mut()
            .ok_or_else(|| anyhow!("partial close payload is not an object"))?;
        payload_object.insert("reason".to_string(), json!(reason));
        payload_object.insert("closed_quantity".to_string(), json!(closed_quantity));
        payload_object.insert(
            "remaining_quantity".to_string(),
            json!(updated_position.quantity),
        );
        payload_object.insert(
            "partial_realized_price_pnl_quote".to_string(),
            json!(execution.realized_price_pnl_quote),
        );
        payload_object.insert(
            "partial_exit_fee_quote".to_string(),
            json!(execution.total_exit_fee_quote),
        );
        payload_object.insert("closed_at_ms".to_string(), json!(closed_at_ms));
        payload_object.insert(
            "long_exit_order_id".to_string(),
            json!(&execution.long_fill.order_id),
        );
        payload_object.insert(
            "short_exit_order_id".to_string(),
            json!(&execution.short_fill.order_id),
        );
        payload_object.insert(
            "long_exit_client_order_id".to_string(),
            json!(&execution.long_client_order_id),
        );
        payload_object.insert(
            "short_exit_client_order_id".to_string(),
            json!(&execution.short_client_order_id),
        );
        payload_object.insert(
            "long_exit_average_price".to_string(),
            json!(execution.long_fill.average_price),
        );
        payload_object.insert(
            "short_exit_average_price".to_string(),
            json!(execution.short_fill.average_price),
        );
        payload_object.insert(
            "long_exit_quantity".to_string(),
            json!(execution.long_fill.quantity),
        );
        payload_object.insert(
            "short_exit_quantity".to_string(),
            json!(execution.short_fill.quantity),
        );
        payload_object.insert(
            "outcome_diagnostics".to_string(),
            json!(partial_outcome_diagnostics),
        );
        payload_object.insert(
            "loss_diagnostics".to_string(),
            json!(partial_loss_diagnostics),
        );

        self.log_critical_event("exit.partial_closed", &partial_close_payload)?;
        self.schedule_pending_close_reconciliation(
            CloseReconciliationKind::Partial,
            updated_position.clone(),
            partial_close_payload.clone(),
            &execution,
            reason,
            closed_at_ms,
        );
        self.persist_state()?;
        Ok(())
    }

    async fn try_close_position(
        &mut self,
        position_id: &str,
        reason: &str,
        market: &MarketView,
    ) -> Result<()> {
        let Some(position) = self
            .active_positions()
            .iter()
            .find(|position| position.position_id == position_id)
            .cloned()
        else {
            return Ok(());
        };

        let execution = self
            .execute_close_orders(
                &position,
                market,
                position.quantity,
                "exit_short",
                "exit_long",
            )
            .await?;
        let cumulative_realized_price_pnl_quote =
            position.realized_price_pnl_quote + execution.realized_price_pnl_quote;
        let cumulative_exit_fee_quote =
            position.realized_exit_fee_quote + execution.total_exit_fee_quote;
        let realized_net_quote = cumulative_realized_price_pnl_quote
            + position.captured_funding_quote
            + position.second_stage_funding_quote
            - position.total_entry_fee_quote
            - cumulative_exit_fee_quote;
        let settlement_half_outcome_diagnostics = if position.settlement_half_closed_quantity > 0.0
        {
            self.build_segment_outcome_diagnostics(
                &position,
                market,
                "settlement_half_close",
                "settlement_half_close",
                position.settlement_half_closed_quantity,
                position.realized_price_pnl_quote,
                position.realized_exit_fee_quote,
            )
        } else {
            None
        };
        let remaining_outcome_diagnostics = self.build_segment_outcome_diagnostics(
            &position,
            market,
            reason,
            if position.settlement_half_closed_quantity > 0.0 {
                "post_settlement_remaining_close"
            } else {
                "full_close"
            },
            execution
                .short_fill
                .quantity
                .min(execution.long_fill.quantity),
            execution.realized_price_pnl_quote,
            execution.total_exit_fee_quote,
        );
        let outcome_diagnostics = self.build_exit_outcome_diagnostics(
            &position,
            market,
            reason,
            cumulative_realized_price_pnl_quote,
            cumulative_exit_fee_quote,
            realized_net_quote,
        );
        let loss_diagnostics = self.compact_loss_diagnostics(outcome_diagnostics.as_ref());
        let remaining_loss_diagnostics =
            self.compact_loss_diagnostics(remaining_outcome_diagnostics.as_ref());

        self.log_event(
            "execution.exit_latency_summary",
            &json!({
                "position_id": &position.position_id,
                "symbol": &position.symbol,
                "reason": reason,
                "long_venue": position.long_venue,
                "short_venue": position.short_venue,
                "total_roundtrip_ms": execution.short_latency_ms + execution.long_latency_ms,
                "max_single_order_ms": execution.short_latency_ms.max(execution.long_latency_ms),
                "total_exit_fee_quote": cumulative_exit_fee_quote,
            }),
        );

        let close_payload = json!({
            "position_id": &position.position_id,
            "symbol": &position.symbol,
            "reason": reason,
            "net_quote": realized_net_quote,
            "mark_to_market_net_quote": position.current_net_quote,
            "realized_price_pnl_quote": cumulative_realized_price_pnl_quote,
            "captured_funding_quote": position.captured_funding_quote,
            "second_stage_funding_quote": position.second_stage_funding_quote,
            "total_entry_fee_quote": position.total_entry_fee_quote,
            "total_exit_fee_quote": cumulative_exit_fee_quote,
            "long_exit_average_price": execution.long_fill.average_price,
            "short_exit_average_price": execution.short_fill.average_price,
            "long_exit_quantity": execution.long_fill.quantity,
            "short_exit_quantity": execution.short_fill.quantity,
            "long_exit_order_id": &execution.long_fill.order_id,
            "short_exit_order_id": &execution.short_fill.order_id,
            "long_exit_client_order_id": &execution.long_client_order_id,
            "short_exit_client_order_id": &execution.short_client_order_id,
            "settlement_half_closed_quantity": position.settlement_half_closed_quantity,
            "settlement_half_closed_at_ms": position.settlement_half_closed_at_ms,
            "settlement_half_outcome_diagnostics": settlement_half_outcome_diagnostics,
            "remaining_outcome_diagnostics": remaining_outcome_diagnostics,
            "remaining_loss_diagnostics": remaining_loss_diagnostics,
            "outcome_diagnostics": outcome_diagnostics,
            "loss_diagnostics": loss_diagnostics,
            "closed_at_ms": execution.short_fill.filled_at_ms.max(execution.long_fill.filled_at_ms),
        });
        self.log_critical_event("exit.closed", &close_payload)?;
        self.schedule_pending_close_reconciliation(
            CloseReconciliationKind::Final,
            position.clone(),
            close_payload,
            &execution,
            reason,
            execution
                .short_fill
                .filled_at_ms
                .max(execution.long_fill.filled_at_ms),
        );
        self.remove_open_position(&position.position_id);
        self.state.last_error = None;
        self.state.mode = EngineMode::Running;
        self.persist_state()?;
        Ok(())
    }

    fn schedule_pending_close_reconciliation(
        &mut self,
        kind: CloseReconciliationKind,
        position_snapshot: OpenPosition,
        original_payload: serde_json::Value,
        execution: &CloseExecution,
        reason: &str,
        closed_at_ms: i64,
    ) {
        if !matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live) {
            return;
        }
        self.state
            .pending_close_reconciliations
            .push(PendingCloseReconciliation {
                position_id: position_snapshot.position_id.clone(),
                symbol: position_snapshot.symbol.clone(),
                kind,
                reason: reason.to_string(),
                position_snapshot,
                original_payload,
                long_leg: close_leg_record(
                    execution.long_fill.venue,
                    execution.long_fill.side,
                    &execution.long_fill,
                    &execution.long_client_order_id,
                ),
                short_leg: close_leg_record(
                    execution.short_fill.venue,
                    execution.short_fill.side,
                    &execution.short_fill,
                    &execution.short_client_order_id,
                ),
                closed_at_ms,
                created_cycle: self.state.cycle,
                next_attempt_ms: closed_at_ms,
                attempt_count: 0,
            });
    }

    fn build_partial_close_reconciled_payload(
        &self,
        task: &PendingCloseReconciliation,
        market: &MarketView,
        long_leg: &OrderFillReconciliation,
        short_leg: &OrderFillReconciliation,
    ) -> Result<serde_json::Value> {
        let mut corrected_position = task.position_snapshot.clone();
        let original_total_quantity = (task.position_snapshot.quantity
            + task.position_snapshot.settlement_half_closed_quantity)
            .max(task.position_snapshot.initial_quantity)
            .max(0.0);
        let closed_quantity = long_leg
            .quantity
            .min(short_leg.quantity)
            .clamp(0.0, original_total_quantity);
        corrected_position.settlement_half_closed_quantity = closed_quantity;
        corrected_position.quantity = (original_total_quantity - closed_quantity).max(0.0);
        let reconciled_segment_realized_price_pnl_quote =
            realized_price_pnl_quote_from_reconciled_legs(
                &task.position_snapshot,
                long_leg,
                short_leg,
            );
        let reconciled_segment_exit_fee_quote = reconciled_exit_fee_quote(&task.long_leg, long_leg)
            + reconciled_exit_fee_quote(&task.short_leg, short_leg);
        let original_segment_realized_price_pnl_quote = task
            .original_payload
            .get("partial_realized_price_pnl_quote")
            .and_then(|value| value.as_f64())
            .unwrap_or_default();
        let original_segment_exit_fee_quote = task
            .original_payload
            .get("partial_exit_fee_quote")
            .and_then(|value| value.as_f64())
            .unwrap_or_default();

        corrected_position.realized_price_pnl_quote = corrected_position.realized_price_pnl_quote
            - original_segment_realized_price_pnl_quote
            + reconciled_segment_realized_price_pnl_quote;
        corrected_position.realized_exit_fee_quote = corrected_position.realized_exit_fee_quote
            - original_segment_exit_fee_quote
            + reconciled_segment_exit_fee_quote;
        if let (Some(long_quote), Some(short_quote)) = (
            market.symbol(corrected_position.long_venue, &corrected_position.symbol),
            market.symbol(corrected_position.short_venue, &corrected_position.symbol),
        ) {
            let unrealized_price_pnl = (long_quote.mid_price()
                - corrected_position.long_entry_price)
                * corrected_position.quantity
                + (corrected_position.short_entry_price - short_quote.mid_price())
                    * corrected_position.quantity;
            corrected_position.current_net_quote = corrected_position.realized_price_pnl_quote
                + unrealized_price_pnl
                + corrected_position.captured_funding_quote
                + corrected_position.second_stage_funding_quote
                - corrected_position.total_entry_fee_quote
                - corrected_position.realized_exit_fee_quote;
            if corrected_position.current_net_quote > corrected_position.peak_net_quote {
                corrected_position.peak_net_quote = corrected_position.current_net_quote;
            }
        }

        let outcome_diagnostics = self.build_segment_outcome_diagnostics(
            &corrected_position,
            market,
            &task.reason,
            "settlement_half_close",
            closed_quantity,
            reconciled_segment_realized_price_pnl_quote,
            reconciled_segment_exit_fee_quote,
        );
        let loss_diagnostics = self.compact_loss_diagnostics(outcome_diagnostics.as_ref());
        let original_segment_net_quote = task
            .original_payload
            .get("outcome_diagnostics")
            .and_then(|value| value.get("net_quote"))
            .and_then(|value| value.as_f64())
            .unwrap_or_default();
        let reconciled_segment_net_quote = outcome_diagnostics
            .as_ref()
            .and_then(|value| value.get("net_quote"))
            .and_then(|value| value.as_f64())
            .unwrap_or_default();
        let reconciled_closed_at_ms = long_leg.filled_at_ms.max(short_leg.filled_at_ms);
        corrected_position.settlement_half_closed_at_ms = reconciled_closed_at_ms;

        let mut payload = task.original_payload.clone();
        let object = payload
            .as_object_mut()
            .ok_or_else(|| anyhow!("partial close reconciliation payload is not an object"))?;
        object.insert("quantity".to_string(), json!(corrected_position.quantity));
        object.insert(
            "settlement_half_closed_quantity".to_string(),
            json!(corrected_position.settlement_half_closed_quantity),
        );
        object.insert(
            "settlement_half_closed_at_ms".to_string(),
            json!(corrected_position.settlement_half_closed_at_ms),
        );
        object.insert(
            "realized_price_pnl_quote".to_string(),
            json!(corrected_position.realized_price_pnl_quote),
        );
        object.insert(
            "realized_exit_fee_quote".to_string(),
            json!(corrected_position.realized_exit_fee_quote),
        );
        object.insert(
            "current_net_quote".to_string(),
            json!(corrected_position.current_net_quote),
        );
        object.insert(
            "peak_net_quote".to_string(),
            json!(corrected_position.peak_net_quote),
        );
        object.insert(
            "partial_realized_price_pnl_quote".to_string(),
            json!(reconciled_segment_realized_price_pnl_quote),
        );
        object.insert(
            "partial_exit_fee_quote".to_string(),
            json!(reconciled_segment_exit_fee_quote),
        );
        object.insert("closed_quantity".to_string(), json!(closed_quantity));
        object.insert("closed_at_ms".to_string(), json!(reconciled_closed_at_ms));
        object.insert(
            "remaining_quantity".to_string(),
            json!(corrected_position.quantity),
        );
        object.insert(
            "long_exit_average_price".to_string(),
            json!(long_leg.average_price),
        );
        object.insert(
            "short_exit_average_price".to_string(),
            json!(short_leg.average_price),
        );
        object.insert("long_exit_quantity".to_string(), json!(long_leg.quantity));
        object.insert("short_exit_quantity".to_string(), json!(short_leg.quantity));
        object.insert("long_leg".to_string(), json!(long_leg));
        object.insert("short_leg".to_string(), json!(short_leg));
        object.insert(
            "outcome_diagnostics".to_string(),
            json!(outcome_diagnostics),
        );
        object.insert("loss_diagnostics".to_string(), json!(loss_diagnostics));
        object.insert(
            "reconciled_realized_price_pnl_quote".to_string(),
            json!(corrected_position.realized_price_pnl_quote),
        );
        object.insert(
            "reconciled_realized_exit_fee_quote".to_string(),
            json!(corrected_position.realized_exit_fee_quote),
        );
        object.insert(
            "reconciliation_delta_quote".to_string(),
            json!(reconciled_segment_net_quote - original_segment_net_quote),
        );
        object.insert("reconciled_at_ms".to_string(), json!(wall_clock_now_ms()));
        Ok(payload)
    }

    fn build_final_close_reconciled_payload(
        &self,
        task: &PendingCloseReconciliation,
        market: &MarketView,
        long_leg: &OrderFillReconciliation,
        short_leg: &OrderFillReconciliation,
    ) -> Result<serde_json::Value> {
        let closed_quantity = long_leg.quantity.min(short_leg.quantity);
        let reconciled_segment_realized_price_pnl_quote =
            realized_price_pnl_quote_from_reconciled_legs(
                &task.position_snapshot,
                long_leg,
                short_leg,
            );
        let reconciled_segment_exit_fee_quote = reconciled_exit_fee_quote(&task.long_leg, long_leg)
            + reconciled_exit_fee_quote(&task.short_leg, short_leg);
        let reconciled_realized_price_pnl_quote = task.position_snapshot.realized_price_pnl_quote
            + reconciled_segment_realized_price_pnl_quote;
        let reconciled_total_exit_fee_quote =
            task.position_snapshot.realized_exit_fee_quote + reconciled_segment_exit_fee_quote;
        let reconciled_net_quote = reconciled_realized_price_pnl_quote
            + task.position_snapshot.captured_funding_quote
            + task.position_snapshot.second_stage_funding_quote
            - task.position_snapshot.total_entry_fee_quote
            - reconciled_total_exit_fee_quote;
        let segment_kind = if task.position_snapshot.settlement_half_closed_quantity > 0.0 {
            "post_settlement_remaining_close"
        } else {
            "full_close"
        };
        let remaining_outcome_diagnostics = self.build_segment_outcome_diagnostics(
            &task.position_snapshot,
            market,
            &task.reason,
            segment_kind,
            closed_quantity,
            reconciled_segment_realized_price_pnl_quote,
            reconciled_segment_exit_fee_quote,
        );
        let remaining_loss_diagnostics =
            self.compact_loss_diagnostics(remaining_outcome_diagnostics.as_ref());
        let outcome_diagnostics = self.build_exit_outcome_diagnostics(
            &task.position_snapshot,
            market,
            &task.reason,
            reconciled_realized_price_pnl_quote,
            reconciled_total_exit_fee_quote,
            reconciled_net_quote,
        );
        let loss_diagnostics = self.compact_loss_diagnostics(outcome_diagnostics.as_ref());
        let original_net_quote = task
            .original_payload
            .get("net_quote")
            .and_then(|value| value.as_f64())
            .unwrap_or_default();
        let original_remaining_net_quote = task
            .original_payload
            .get("remaining_outcome_diagnostics")
            .and_then(|value| value.get("net_quote"))
            .and_then(|value| value.as_f64())
            .unwrap_or_default();
        let reconciled_remaining_net_quote = remaining_outcome_diagnostics
            .as_ref()
            .and_then(|value| value.get("net_quote"))
            .and_then(|value| value.as_f64())
            .unwrap_or_default();
        let reconciled_closed_at_ms = long_leg.filled_at_ms.max(short_leg.filled_at_ms);

        let mut payload = task.original_payload.clone();
        let object = payload
            .as_object_mut()
            .ok_or_else(|| anyhow!("final close reconciliation payload is not an object"))?;
        object.insert("net_quote".to_string(), json!(reconciled_net_quote));
        object.insert(
            "realized_price_pnl_quote".to_string(),
            json!(reconciled_realized_price_pnl_quote),
        );
        object.insert(
            "total_exit_fee_quote".to_string(),
            json!(reconciled_total_exit_fee_quote),
        );
        object.insert(
            "long_exit_average_price".to_string(),
            json!(long_leg.average_price),
        );
        object.insert(
            "short_exit_average_price".to_string(),
            json!(short_leg.average_price),
        );
        object.insert("long_exit_quantity".to_string(), json!(long_leg.quantity));
        object.insert("short_exit_quantity".to_string(), json!(short_leg.quantity));
        object.insert("closed_at_ms".to_string(), json!(reconciled_closed_at_ms));
        object.insert(
            "remaining_outcome_diagnostics".to_string(),
            json!(remaining_outcome_diagnostics),
        );
        object.insert(
            "remaining_loss_diagnostics".to_string(),
            json!(remaining_loss_diagnostics),
        );
        object.insert(
            "outcome_diagnostics".to_string(),
            json!(outcome_diagnostics),
        );
        object.insert("loss_diagnostics".to_string(), json!(loss_diagnostics));
        object.insert("long_leg".to_string(), json!(long_leg));
        object.insert("short_leg".to_string(), json!(short_leg));
        object.insert(
            "reconciled_segment_realized_price_pnl_quote".to_string(),
            json!(reconciled_segment_realized_price_pnl_quote),
        );
        object.insert(
            "reconciled_segment_exit_fee_quote".to_string(),
            json!(reconciled_segment_exit_fee_quote),
        );
        object.insert(
            "reconciliation_delta_quote".to_string(),
            json!(reconciled_net_quote - original_net_quote),
        );
        object.insert(
            "remaining_reconciliation_delta_quote".to_string(),
            json!(reconciled_remaining_net_quote - original_remaining_net_quote),
        );
        object.insert("reconciled_at_ms".to_string(), json!(wall_clock_now_ms()));
        Ok(payload)
    }

    fn build_exit_outcome_diagnostics(
        &self,
        position: &OpenPosition,
        market: &MarketView,
        reason: &str,
        realized_price_pnl_quote: f64,
        total_exit_fee_quote: f64,
        realized_net_quote: f64,
    ) -> Option<serde_json::Value> {
        if !realized_net_quote.is_finite() {
            return None;
        }

        let actual_funding_quote =
            position.captured_funding_quote + position.second_stage_funding_quote;
        let expected_total_funding_quote =
            position.entry_notional_quote * position.total_funding_edge_bps_entry / 10_000.0;
        let funding_shortfall_quote =
            (expected_total_funding_quote - actual_funding_quote).max(0.0);
        let fee_drag_quote = position.total_entry_fee_quote + total_exit_fee_quote;
        let estimated_entry_impact_quote =
            position.entry_notional_quote * position.entry_slippage_bps_entry.max(0.0) / 10_000.0;
        let adverse_price_move_quote = (-realized_price_pnl_quote).max(0.0);
        let favorable_price_move_quote = realized_price_pnl_quote.max(0.0);
        let current_total_funding_edge_bps = self.current_total_funding_edge_bps(position, market);
        let funding_edge_narrowed_quote = current_total_funding_edge_bps
            .map(|current_bps| {
                ((position.total_funding_edge_bps_entry - current_bps).max(0.0)
                    * position.entry_notional_quote)
                    / 10_000.0
            })
            .unwrap_or_default();
        let funding_edge_widened_quote = current_total_funding_edge_bps
            .map(|current_bps| {
                ((current_bps - position.total_funding_edge_bps_entry).max(0.0)
                    * position.entry_notional_quote)
                    / 10_000.0
            })
            .unwrap_or_default();
        let timing_unfavorable = !position.funding_captured && adverse_price_move_quote > 0.0;
        let timing_favorable = !position.funding_captured && favorable_price_move_quote > 0.0;
        let outcome = if realized_net_quote > 0.0 {
            "profit"
        } else if realized_net_quote < 0.0 {
            "loss"
        } else {
            "flat"
        };

        let mut contributing_reasons = Vec::new();
        if adverse_price_move_quote > 0.0 {
            contributing_reasons.push("adverse_price_move");
        }
        if favorable_price_move_quote > 0.0 {
            contributing_reasons.push("favorable_price_move");
        }
        if timing_unfavorable {
            contributing_reasons.push("entry_timing_unfavorable");
        }
        if timing_favorable {
            contributing_reasons.push("entry_timing_favorable");
        }
        if funding_shortfall_quote > 0.0 {
            contributing_reasons.push("funding_shortfall");
        }
        if actual_funding_quote > 0.0 {
            contributing_reasons.push("funding_realized");
        }
        if funding_edge_narrowed_quote > 0.0 {
            contributing_reasons.push("funding_edge_narrowed");
        }
        if funding_edge_widened_quote > 0.0 {
            contributing_reasons.push("funding_edge_widened");
        }
        if position.entry_depth_capped_at_entry {
            contributing_reasons.push("entry_depth_constrained");
        }
        if estimated_entry_impact_quote > 0.0 {
            contributing_reasons.push("entry_impact_high");
        }
        if fee_drag_quote > 0.0 {
            contributing_reasons.push("fee_drag");
        }

        let funding_pressure_quote = funding_shortfall_quote.max(funding_edge_narrowed_quote);
        let entry_impact_pressure_quote = if position.entry_depth_capped_at_entry {
            estimated_entry_impact_quote.max(0.000_001)
        } else {
            estimated_entry_impact_quote
        };
        let primary_reason = match outcome {
            "loss" => {
                if timing_unfavorable {
                    "entry_timing_unfavorable"
                } else {
                    [
                        ("adverse_price_move", adverse_price_move_quote),
                        ("funding_edge_narrowed", funding_pressure_quote),
                        ("entry_depth_or_slippage", entry_impact_pressure_quote),
                        ("fee_drag", fee_drag_quote),
                    ]
                    .into_iter()
                    .max_by(|left, right| left.1.total_cmp(&right.1))
                    .map(|(reason, _)| reason)
                    .unwrap_or("net_loss")
                }
            }
            "profit" => {
                if timing_favorable {
                    [
                        ("favorable_price_move", favorable_price_move_quote),
                        ("funding_realized", actual_funding_quote),
                        ("funding_edge_widened", funding_edge_widened_quote),
                    ]
                    .into_iter()
                    .max_by(|left, right| left.1.total_cmp(&right.1))
                    .map(|(reason, _)| reason)
                    .unwrap_or("net_profit")
                } else {
                    [
                        ("favorable_price_move", favorable_price_move_quote),
                        ("funding_realized", actual_funding_quote),
                        ("funding_edge_widened", funding_edge_widened_quote),
                    ]
                    .into_iter()
                    .max_by(|left, right| left.1.total_cmp(&right.1))
                    .map(|(reason, _)| reason)
                    .unwrap_or("net_profit")
                }
            }
            _ => "flat_outcome",
        };

        Some(json!({
            "outcome": outcome,
            "primary_reason": primary_reason,
            "contributing_reasons": contributing_reasons,
            "net_quote": realized_net_quote,
            "profit_quote": realized_net_quote.max(0.0),
            "loss_quote": (-realized_net_quote).max(0.0),
            "entry_total_funding_edge_bps": position.total_funding_edge_bps_entry,
            "current_total_funding_edge_bps": current_total_funding_edge_bps,
            "actual_funding_quote": actual_funding_quote,
            "funding_shortfall_quote": funding_shortfall_quote,
            "realized_price_pnl_quote": realized_price_pnl_quote,
            "adverse_price_move_quote": adverse_price_move_quote,
            "favorable_price_move_quote": favorable_price_move_quote,
            "fee_drag_quote": fee_drag_quote,
            "funding_edge_narrowed_quote": funding_edge_narrowed_quote,
            "funding_edge_widened_quote": funding_edge_widened_quote,
            "entry_depth_capped_at_entry": position.entry_depth_capped_at_entry,
            "exit_reason": reason,
        }))
    }

    fn current_total_funding_edge_bps(
        &self,
        position: &OpenPosition,
        market: &MarketView,
    ) -> Option<f64> {
        let long_quote = market.symbol(position.long_venue, &position.symbol)?;
        let short_quote = market.symbol(position.short_venue, &position.symbol)?;
        Some((short_quote.funding_rate - long_quote.funding_rate) * 10_000.0)
    }

    fn compact_loss_diagnostics(
        &self,
        diagnostics: Option<&serde_json::Value>,
    ) -> Option<serde_json::Value> {
        let diagnostics = diagnostics?;
        if diagnostics.get("outcome").and_then(|value| value.as_str()) != Some("loss") {
            return None;
        }
        compact_outcome_diagnostics(Some(diagnostics))
    }

    async fn flatten_single_leg(
        &mut self,
        venue: Venue,
        symbol: &str,
        quantity: f64,
        side: Side,
        position_id: &str,
        pair_id: Option<&str>,
        stage: &str,
        market: &MarketView,
    ) -> Result<()> {
        let request = OrderRequest {
            symbol: symbol.to_string(),
            side,
            quantity,
            reduce_only: true,
            client_order_id: compact_client_order_id(position_id, "compensate"),
            price_hint: self.order_price_hint(market, venue, symbol, side),
            mark_price_hint: self.order_mark_price_hint(market, venue, symbol),
            observed_at_ms: market.observed_at_ms(venue),
        };
        self.execute_order_leg(
            venue,
            request,
            OrderLegContext {
                stage,
                position_id,
                pair_id,
            },
        )
        .await?;
        Ok(())
    }

    fn adapter(&self, venue: Venue) -> Result<Arc<dyn VenueAdapter>> {
        self.adapters
            .get(&venue)
            .cloned()
            .ok_or_else(|| anyhow!("missing adapter for {venue}"))
    }

    fn order_price_hint(
        &self,
        market: &MarketView,
        venue: Venue,
        symbol: &str,
        side: Side,
    ) -> Option<f64> {
        market.symbol(venue, symbol).map(|quote| match side {
            Side::Buy => quote.best_ask,
            Side::Sell => quote.best_bid,
        })
    }

    fn order_mark_price_hint(
        &self,
        market: &MarketView,
        venue: Venue,
        symbol: &str,
    ) -> Option<f64> {
        market
            .symbol(venue, symbol)
            .and_then(|quote| quote.mark_price)
    }

    fn order_notional_floor_reason(
        &self,
        venue: Venue,
        request: &OrderRequest,
    ) -> Option<(String, f64)> {
        let exchange_min_notional_quote = self.adapters.get(&venue).and_then(|adapter| {
            adapter.min_entry_notional_quote_hint(&request.symbol, request.price_hint)
        });
        let min_notional_quote = effective_entry_leg_notional_floor(
            self.config.strategy.min_entry_leg_notional_quote,
            exchange_min_notional_quote,
        );
        let reason = order_notional_floor_reason(
            min_notional_quote,
            request.price_hint,
            request.quantity,
            request.reduce_only,
        )?;
        Some((reason, min_notional_quote))
    }

    fn entry_leg_notional_trace(&self, venue: Venue, request: &OrderRequest) -> serde_json::Value {
        let exchange_min_notional_quote = self.adapters.get(&venue).and_then(|adapter| {
            adapter.min_entry_notional_quote_hint(&request.symbol, request.price_hint)
        });
        let venue_min_notional_quote = effective_entry_leg_notional_floor(
            self.config.strategy.min_entry_leg_notional_quote,
            exchange_min_notional_quote,
        );
        json!({
            "venue": venue,
            "normalized_quantity": request.quantity,
            "price_hint": request.price_hint,
            "final_leg_notional_quote": order_request_notional_quote(request),
            "exchange_min_notional_quote": exchange_min_notional_quote,
            "venue_min_notional_quote": venue_min_notional_quote,
        })
    }

    fn entry_leg_notional_floor_violation(
        &self,
        legs: &[&EntryLegPlan],
    ) -> Option<(&'static str, Venue, f64, f64)> {
        legs.iter().find_map(|leg| {
            let leg_notional = order_request_notional_quote(&leg.request)?;
            let exchange_min_notional_quote = self.adapters.get(&leg.venue).and_then(|adapter| {
                adapter.min_entry_notional_quote_hint(&leg.request.symbol, leg.request.price_hint)
            });
            let min_notional = effective_entry_leg_notional_floor(
                self.config.strategy.min_entry_leg_notional_quote,
                exchange_min_notional_quote,
            );
            if leg.request.reduce_only || min_notional <= 0.0 || leg_notional + 1e-9 >= min_notional
            {
                return None;
            }
            Some((leg.leg.entry_stage(), leg.venue, leg_notional, min_notional))
        })
    }

    async fn apply_live_entry_leverage_guards(
        &mut self,
        candidates: &mut [CandidateOpportunity],
    ) -> Result<()> {
        if !matches!(self.config.runtime.mode, RuntimeMode::Live) {
            return Ok(());
        }
        let target_leverage = self.config.strategy.live_target_leverage;
        if target_leverage == 0 {
            return Ok(());
        }
        let prep_limit = self
            .config
            .strategy
            .max_concurrent_positions
            .max(1)
            .saturating_mul(2)
            .max(4);
        let mut prepared = 0usize;
        for candidate in candidates.iter_mut() {
            if !candidate.is_tradeable() {
                continue;
            }
            if prepared >= prep_limit {
                break;
            }
            let started_at = Instant::now();
            match self
                .ensure_live_entry_leverage_for_symbol(
                    target_leverage,
                    candidate.long_venue,
                    candidate.short_venue,
                    &candidate.symbol,
                )
                .await
            {
                Ok(()) => {
                    prepared += 1;
                    self.log_event(
                        "execution.entry_candidate_leverage_ready",
                        &json!({
                            "pair_id": &candidate.pair_id,
                            "symbol": &candidate.symbol,
                            "long_venue": candidate.long_venue,
                            "short_venue": candidate.short_venue,
                            "target_leverage": target_leverage,
                            "setup_ms": duration_ms_u64(started_at.elapsed()),
                        }),
                    );
                }
                Err(error) => {
                    candidate
                        .blocked_reasons
                        .push("entry_leverage_unavailable".to_string());
                    self.log_event(
                        "execution.entry_leverage_unavailable",
                        &json!({
                            "pair_id": &candidate.pair_id,
                            "symbol": &candidate.symbol,
                            "long_venue": candidate.long_venue,
                            "short_venue": candidate.short_venue,
                            "target_leverage": target_leverage,
                            "setup_ms": duration_ms_u64(started_at.elapsed()),
                            "error": error.to_string(),
                        }),
                    );
                }
            }
        }
        Ok(())
    }

    async fn ensure_live_entry_leverage_for_symbol(
        &self,
        target_leverage: u32,
        long_venue: Venue,
        short_venue: Venue,
        symbol: &str,
    ) -> Result<()> {
        if !matches!(self.config.runtime.mode, RuntimeMode::Live) || target_leverage == 0 {
            return Ok(());
        }
        if long_venue == short_venue {
            return self
                .adapter(long_venue)?
                .ensure_entry_leverage(symbol, target_leverage)
                .await;
        }

        let first_adapter = self.adapter(long_venue)?;
        let second_adapter = self.adapter(short_venue)?;
        tokio::try_join!(
            first_adapter.ensure_entry_leverage(symbol, target_leverage),
            second_adapter.ensure_entry_leverage(symbol, target_leverage),
        )?;
        Ok(())
    }

    fn persist_state(&mut self) -> Result<()> {
        self.state.venue_health = build_persisted_venue_health_state(
            &self.recent_submit_ack_ms,
            &self.recent_order_health,
            &self.venue_health_updated_at_ms,
        );
        let snapshot = persistent_state_view(&self.state);
        if self.last_persisted_state.as_ref() == Some(&snapshot) {
            return Ok(());
        }
        self.store.save(&snapshot)?;
        self.last_persisted_state = Some(snapshot);
        Ok(())
    }

    async fn plan_executable_quantity(
        &self,
        long_venue: Venue,
        short_venue: Venue,
        symbol: &str,
        desired_quantity: f64,
    ) -> Result<QuantityPlan> {
        let long_adapter = self.adapter(long_venue)?;
        let short_adapter = self.adapter(short_venue)?;
        let long_requested_quantity = long_adapter
            .normalize_quantity(symbol, desired_quantity)
            .await?;
        let short_requested_quantity = short_adapter
            .normalize_quantity(symbol, desired_quantity)
            .await?;
        let common_requested_quantity = long_requested_quantity.min(short_requested_quantity);
        if common_requested_quantity <= 0.0 {
            return Ok(QuantityPlan {
                requested_quantity: desired_quantity,
                long_requested_quantity,
                short_requested_quantity,
                common_requested_quantity,
                long_executable_quantity: 0.0,
                short_executable_quantity: 0.0,
                executable_quantity: 0.0,
            });
        }

        let long_executable_quantity = long_adapter
            .normalize_quantity(symbol, common_requested_quantity)
            .await?;
        let short_executable_quantity = short_adapter
            .normalize_quantity(symbol, common_requested_quantity)
            .await?;
        Ok(QuantityPlan {
            requested_quantity: desired_quantity,
            long_requested_quantity,
            short_requested_quantity,
            common_requested_quantity,
            long_executable_quantity,
            short_executable_quantity,
            executable_quantity: long_executable_quantity.min(short_executable_quantity),
        })
    }

    async fn execute_order_leg(
        &mut self,
        venue: Venue,
        request: OrderRequest,
        context: OrderLegContext<'_>,
    ) -> Result<(crate::models::OrderFill, u64)> {
        let mut request = request;
        if matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live) {
            if let Some(mut reason) = order_quote_expired_reason(
                self.config.runtime.max_order_quote_age_ms,
                request.observed_at_ms,
                request.reduce_only,
            ) {
                if self
                    .refresh_order_request_quote(venue, &mut request, &context, &reason)
                    .await?
                {
                    if let Some(refreshed_reason) = order_quote_expired_reason(
                        self.config.runtime.max_order_quote_age_ms,
                        request.observed_at_ms,
                        request.reduce_only,
                    ) {
                        reason = refreshed_reason;
                    } else {
                        reason.clear();
                    }
                }
                if !reason.is_empty() {
                    self.log_event(
                        "execution.order_blocked",
                        &json!({
                            "position_id": context.position_id,
                            "pair_id": context.pair_id,
                            "stage": context.stage,
                            "venue": venue,
                            "symbol": &request.symbol,
                            "side": request.side,
                            "requested_quantity": request.quantity,
                            "reduce_only": request.reduce_only,
                            "client_order_id": &request.client_order_id,
                            "reason": reason,
                        }),
                    );
                    return Err(anyhow!("order blocked: {reason}"));
                }
            }
        }
        if let Some((reason, min_notional_quote)) =
            self.order_notional_floor_reason(venue, &request)
        {
            self.log_event(
                "execution.order_blocked",
                &json!({
                    "position_id": context.position_id,
                    "pair_id": context.pair_id,
                    "stage": context.stage,
                    "venue": venue,
                    "symbol": &request.symbol,
                    "side": request.side,
                    "requested_quantity": request.quantity,
                    "reduce_only": request.reduce_only,
                    "client_order_id": &request.client_order_id,
                    "reason": reason,
                    "venue_min_notional_quote": min_notional_quote,
                }),
            );
            return Err(anyhow!("order blocked: entry_leg_notional_below_minimum"));
        }
        self.log_event(
            "execution.order_submitted",
            &json!({
                "position_id": context.position_id,
                "pair_id": context.pair_id,
                "stage": context.stage,
                "venue": venue,
                "symbol": &request.symbol,
                "side": request.side,
                "requested_quantity": request.quantity,
                "reduce_only": request.reduce_only,
                "client_order_id": &request.client_order_id,
            }),
        );

        let started_at = Instant::now();
        match self.adapter(venue)?.place_order(request.clone()).await {
            Ok(fill) => {
                self.record_order_health(fill.venue, false, false);
                if let Some(submit_ack_ms) =
                    fill.timing.as_ref().and_then(|timing| timing.submit_ack_ms)
                {
                    self.record_submit_ack_latency(fill.venue, submit_ack_ms);
                }
                let latency_ms = duration_ms_u64(started_at.elapsed());
                self.log_event_at(
                    fill.filled_at_ms,
                    "execution.order_filled",
                    &json!({
                        "position_id": context.position_id,
                        "pair_id": context.pair_id,
                        "stage": context.stage,
                        "venue": fill.venue,
                        "symbol": &fill.symbol,
                        "side": fill.side,
                        "requested_quantity": request.quantity,
                        "executed_quantity": fill.quantity,
                        "average_price": fill.average_price,
                        "fee_quote": fill.fee_quote,
                        "order_id": &fill.order_id,
                        "filled_at_ms": fill.filled_at_ms,
                        "local_roundtrip_ms": latency_ms,
                        "adapter_timing": &fill.timing,
                        "reduce_only": request.reduce_only,
                        "client_order_id": &request.client_order_id,
                    }),
                );
                Ok((fill, latency_ms))
            }
            Err(error) => {
                let latency_ms = duration_ms_u64(started_at.elapsed());
                let uncertain = order_error_may_have_created_exposure(&error);
                self.record_order_health(venue, true, uncertain);
                if uncertain {
                    self.arm_venue_entry_cooldown(venue, "uncertain_order_status");
                }
                self.log_event(
                    "execution.order_failed",
                    &json!({
                        "position_id": context.position_id,
                        "pair_id": context.pair_id,
                        "stage": context.stage,
                        "venue": venue,
                        "symbol": &request.symbol,
                        "side": request.side,
                        "requested_quantity": request.quantity,
                        "reduce_only": request.reduce_only,
                        "client_order_id": &request.client_order_id,
                        "local_roundtrip_ms": latency_ms,
                        "error": error.to_string(),
                    }),
                );
                Err(error)
            }
        }
    }

    async fn refresh_order_request_quote(
        &mut self,
        venue: Venue,
        request: &mut OrderRequest,
        context: &OrderLegContext<'_>,
        expired_reason: &str,
    ) -> Result<bool> {
        if request.reduce_only {
            return Ok(false);
        }
        let refreshed = self
            .adapter(venue)?
            .refresh_market_snapshot(&request.symbol)
            .await;
        let snapshot = match refreshed {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.log_event(
                    "execution.order_quote_refresh_failed",
                    &json!({
                        "position_id": context.position_id,
                        "pair_id": context.pair_id,
                        "stage": context.stage,
                        "venue": venue,
                        "symbol": &request.symbol,
                        "side": request.side,
                        "client_order_id": &request.client_order_id,
                        "expired_reason": expired_reason,
                        "error": error.to_string(),
                    }),
                );
                return Ok(false);
            }
        };
        let previous_observed_at_ms = request.observed_at_ms;
        let (price_hint, observed_at_ms) = quote_fill(&snapshot, &request.symbol, request.side)?;
        request.price_hint = Some(price_hint);
        request.observed_at_ms = Some(observed_at_ms);
        self.log_event(
            "execution.order_quote_refreshed",
            &json!({
                "position_id": context.position_id,
                "pair_id": context.pair_id,
                "stage": context.stage,
                "venue": venue,
                "symbol": &request.symbol,
                "side": request.side,
                "client_order_id": &request.client_order_id,
                "expired_reason": expired_reason,
                "previous_observed_at_ms": previous_observed_at_ms,
                "refreshed_observed_at_ms": observed_at_ms,
                "price_hint": price_hint,
            }),
        );
        Ok(true)
    }

    async fn cleanup_failed_leg_exposure(
        &mut self,
        venue: Venue,
        symbol: &str,
        position_id: &str,
        pair_id: Option<&str>,
        stage: &str,
        market: &MarketView,
    ) -> Result<bool> {
        let position = self.adapter(venue)?.fetch_position(symbol).await?;
        if approx_zero(position.size) {
            return Ok(false);
        }

        let cleanup_side = if position.size > 0.0 {
            Side::Sell
        } else {
            Side::Buy
        };
        self.log_event(
            "execution.uncertain_leg_detected",
            &json!({
                "position_id": position_id,
                "pair_id": pair_id,
                "stage": stage,
                "venue": venue,
                "symbol": symbol,
                "size": position.size,
                "updated_at_ms": position.updated_at_ms,
            }),
        );
        self.flatten_single_leg(
            venue,
            symbol,
            position.size.abs(),
            cleanup_side,
            position_id,
            pair_id,
            stage,
            market,
        )
        .await?;
        Ok(true)
    }

    fn apply_runtime_entry_guards(
        &mut self,
        market: &MarketView,
        candidates: &mut [CandidateOpportunity],
    ) {
        for candidate in candidates.iter_mut() {
            let was_tradeable = candidate.is_tradeable();
            if let Some(reason) = self.runtime_entry_gate_reason(market, candidate) {
                if !candidate.blocked_reasons.iter().any(|item| item == &reason) {
                    candidate.blocked_reasons.push(reason.clone());
                    candidate.blocked_reasons.sort();
                    candidate.blocked_reasons.dedup();
                    if was_tradeable {
                        self.log_event(
                            "scan.runtime_gate_blocked",
                            &json!({
                                "pair_id": &candidate.pair_id,
                                "symbol": &candidate.symbol,
                                "long_venue": candidate.long_venue,
                                "short_venue": candidate.short_venue,
                                "reason": reason,
                            }),
                        );
                    }
                }
            }
        }
    }

    fn runtime_entry_gate_reason(
        &self,
        _market: &MarketView,
        candidate: &CandidateOpportunity,
    ) -> Option<String> {
        if candidate.long_venue == Venue::Hyperliquid || candidate.short_venue == Venue::Hyperliquid
        {
            if let Some(samples) = self.recent_submit_ack_ms.get(&Venue::Hyperliquid) {
                if let Some(reason) = hyperliquid_entry_gate_reason(&self.config.strategy, samples)
                {
                    return Some(reason);
                }
            }
            if let Some(reason) = self.venue_entry_cooldown_reason(Venue::Hyperliquid) {
                return Some(reason);
            }
            let other_venue = if candidate.long_venue == Venue::Hyperliquid {
                candidate.short_venue
            } else {
                candidate.long_venue
            };
            if let Some(reason) = self.venue_entry_cooldown_reason(other_venue) {
                return Some(reason);
            }
            return self.cached_entry_balance_reason(candidate);
        }

        self.venue_entry_cooldown_reason(candidate.long_venue)
            .or_else(|| self.venue_entry_cooldown_reason(candidate.short_venue))
            .or_else(|| self.cached_entry_balance_reason(candidate))
    }

    async fn refresh_entry_candidate_quotes_if_needed(
        &mut self,
        candidate: &CandidateOpportunity,
        position_id: &str,
        market: &mut MarketView,
    ) -> Result<EntryQuoteRefreshResult> {
        let mut stale_legs = self.stale_entry_candidate_legs(market, candidate);
        if stale_legs.is_empty() {
            return Ok(EntryQuoteRefreshResult::default());
        }
        let max_pre_refresh_age_ms = stale_legs.iter().filter_map(|leg| leg.age_ms).max();
        let mut refreshed_leg_count = 0;
        let mut verification_legs = Vec::with_capacity(stale_legs.len());

        for leg in stale_legs.drain(..) {
            let venue = leg.venue;
            let side = leg.side;
            let stage = leg.stage;
            let expired_reason = leg.expired_reason;
            let previous_observed_at_ms = market.observed_at_ms(venue);
            let mut verification_leg = EntryQuoteVerificationLeg {
                venue,
                side,
                stage,
                expired_reason: expired_reason.clone(),
                previous_observed_at_ms,
                pre_refresh_age_ms: leg.age_ms,
                refresh_succeeded: false,
                refresh_error: None,
                refreshed_observed_at_ms: None,
                post_refresh_age_ms: None,
                refreshed_price_hint: None,
            };
            let snapshot = match self
                .adapter(venue)?
                .refresh_market_snapshot(&candidate.symbol)
                .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    verification_leg.refresh_error = Some(error.to_string());
                    verification_legs.push(verification_leg);
                    self.log_event(
                        "execution.entry_quote_refresh_failed",
                        &json!({
                            "position_id": position_id,
                            "pair_id": &candidate.pair_id,
                            "stage": stage,
                            "venue": venue,
                            "symbol": &candidate.symbol,
                            "side": side,
                            "expired_reason": expired_reason,
                            "previous_observed_at_ms": previous_observed_at_ms,
                            "pre_refresh_age_ms": leg.age_ms,
                            "error": error.to_string(),
                        }),
                    );
                    return Ok(EntryQuoteRefreshResult {
                        block_reason: Some(format!("entry_quote_refresh_failed:{venue}:{error}")),
                        refreshed_leg_count,
                        max_pre_refresh_age_ms,
                        legs: verification_legs,
                    });
                }
            };
            let (price_hint, refreshed_observed_at_ms) =
                match quote_fill(&snapshot, &candidate.symbol, side) {
                    Ok(fill) => fill,
                    Err(error) => {
                        verification_leg.refresh_error = Some(error.to_string());
                        verification_legs.push(verification_leg);
                        self.log_event(
                            "execution.entry_quote_refresh_failed",
                            &json!({
                                "position_id": position_id,
                                "pair_id": &candidate.pair_id,
                                "stage": stage,
                                "venue": venue,
                                "symbol": &candidate.symbol,
                                "side": side,
                                "expired_reason": expired_reason,
                                "previous_observed_at_ms": previous_observed_at_ms,
                                "pre_refresh_age_ms": leg.age_ms,
                                "error": error.to_string(),
                            }),
                        );
                        return Ok(EntryQuoteRefreshResult {
                            block_reason: Some(format!(
                                "entry_quote_refresh_failed:{venue}:{error}"
                            )),
                            refreshed_leg_count,
                            max_pre_refresh_age_ms,
                            legs: verification_legs,
                        });
                    }
                };
            market.merge_snapshot(snapshot);
            let post_refresh_age_ms = market
                .observed_at_ms(venue)
                .map(|observed_at_ms| wall_clock_now_ms().saturating_sub(observed_at_ms));
            verification_leg.refresh_succeeded = true;
            verification_leg.refreshed_observed_at_ms = Some(refreshed_observed_at_ms);
            verification_leg.post_refresh_age_ms = post_refresh_age_ms;
            verification_leg.refreshed_price_hint = Some(price_hint);
            refreshed_leg_count += 1;
            self.log_event(
                "execution.entry_quote_refreshed",
                &json!({
                    "position_id": position_id,
                    "pair_id": &candidate.pair_id,
                    "stage": stage,
                    "venue": venue,
                    "symbol": &candidate.symbol,
                    "side": side,
                    "expired_reason": expired_reason,
                    "previous_observed_at_ms": previous_observed_at_ms,
                    "pre_refresh_age_ms": leg.age_ms,
                    "refreshed_observed_at_ms": refreshed_observed_at_ms,
                    "post_refresh_age_ms": post_refresh_age_ms,
                    "price_hint": price_hint,
                }),
            );
            verification_legs.push(verification_leg);
        }

        Ok(EntryQuoteRefreshResult {
            block_reason: self.runtime_entry_quote_freshness_reason(market, candidate),
            refreshed_leg_count,
            max_pre_refresh_age_ms,
            legs: verification_legs,
        })
    }

    fn stale_entry_candidate_legs(
        &self,
        market: &MarketView,
        candidate: &CandidateOpportunity,
    ) -> Vec<EntryQuoteRefreshLeg> {
        let max_order_quote_age_ms = self.config.runtime.max_order_quote_age_ms;
        if !matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live)
            || max_order_quote_age_ms <= 0
        {
            return Vec::new();
        }
        let proactive_refresh_quote_age_ms =
            max_order_quote_age_ms.saturating_mul(PROACTIVE_ENTRY_QUOTE_REFRESH_MULTIPLIER);
        if proactive_refresh_quote_age_ms <= 0 {
            return Vec::new();
        }

        let mut stale = Vec::new();
        for (venue, side, stage) in [
            (candidate.long_venue, Side::Buy, "entry_long"),
            (candidate.short_venue, Side::Sell, "entry_short"),
        ] {
            let Some(observed_at_ms) = market.observed_at_ms(venue) else {
                stale.push(EntryQuoteRefreshLeg {
                    venue,
                    side,
                    stage,
                    expired_reason: format!(
                        "entry_quote_stale:{venue}:missing>{proactive_refresh_quote_age_ms}"
                    ),
                    age_ms: None,
                });
                continue;
            };
            let age_ms = wall_clock_now_ms().saturating_sub(observed_at_ms);
            if age_ms > proactive_refresh_quote_age_ms {
                stale.push(EntryQuoteRefreshLeg {
                    venue,
                    side,
                    stage,
                    expired_reason: format!(
                        "entry_quote_stale:{venue}:{age_ms}>{proactive_refresh_quote_age_ms}"
                    ),
                    age_ms: Some(age_ms),
                });
            }
        }
        stale
    }

    fn runtime_entry_quote_freshness_reason(
        &self,
        market: &MarketView,
        candidate: &CandidateOpportunity,
    ) -> Option<String> {
        if !matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live) {
            return None;
        }
        let max_order_quote_age_ms = self.config.runtime.max_order_quote_age_ms;
        if max_order_quote_age_ms <= 0 {
            return None;
        }
        let hard_block_quote_age_ms = max_order_quote_age_ms.saturating_mul(3);
        if hard_block_quote_age_ms <= 0 {
            return None;
        }

        for venue in [candidate.long_venue, candidate.short_venue] {
            let observed_at_ms = market.observed_at_ms(venue)?;
            let age_ms = wall_clock_now_ms().saturating_sub(observed_at_ms);
            if age_ms > hard_block_quote_age_ms {
                return Some(format!(
                    "entry_quote_stale:{venue}:{age_ms}>{hard_block_quote_age_ms}"
                ));
            }
        }

        None
    }

    fn record_submit_ack_latency(&mut self, venue: Venue, submit_ack_ms: u64) {
        let window_size = self
            .config
            .strategy
            .hyperliquid_submit_ack_window_size
            .max(1);
        let samples = self.recent_submit_ack_ms.entry(venue).or_default();
        samples.push_back(submit_ack_ms);
        while samples.len() > window_size {
            samples.pop_front();
        }
        self.venue_health_updated_at_ms
            .insert(venue, wall_clock_now_ms());
    }

    fn record_order_health(&mut self, venue: Venue, failed: bool, uncertain: bool) {
        let samples = self.recent_order_health.entry(venue).or_default();
        samples.push_back(VenueOrderHealthSample { failed, uncertain });
        while samples.len() > ORDER_HEALTH_WINDOW_SIZE {
            samples.pop_front();
        }
        self.venue_health_updated_at_ms
            .insert(venue, wall_clock_now_ms());
    }

    fn venue_entry_health_score(&self, venue: Venue) -> f64 {
        let Some(samples) = self.recent_order_health.get(&venue) else {
            return 0.0;
        };
        venue_order_health_risk_score(samples)
    }

    fn venue_submit_ack_score(&self, venue: Venue) -> f64 {
        let Some(samples) = self.recent_submit_ack_ms.get(&venue) else {
            return 0.0;
        };
        if samples.is_empty() {
            return 0.0;
        }
        let average_ms = samples.iter().copied().sum::<u64>() as f64 / samples.len() as f64;
        average_ms / 1_000.0
    }

    fn leg_depth_score(
        &self,
        market: &MarketView,
        venue: Venue,
        symbol: &str,
        side: Side,
        quantity: f64,
    ) -> f64 {
        let Some(quote) = market.symbol(venue, symbol) else {
            return 10.0;
        };
        let top_size = match side {
            Side::Buy => quote.ask_size,
            Side::Sell => quote.bid_size,
        };
        if top_size <= 0.0 {
            return 10.0;
        }
        quantity / top_size
    }

    fn build_entry_leg_risk(
        &self,
        market: &MarketView,
        leg: HedgeLeg,
        venue: Venue,
        symbol: &str,
        side: Side,
        quantity: f64,
    ) -> EntryLegRisk {
        let health_score = self.venue_entry_health_score(venue);
        let submit_ack_score = self.venue_submit_ack_score(venue);
        let depth_score = self.leg_depth_score(market, venue, symbol, side, quantity);
        EntryLegRisk {
            venue,
            leg: leg.label(),
            total_score: health_score + submit_ack_score + depth_score,
            health_score,
            submit_ack_score,
            depth_score,
        }
    }

    fn cached_position(
        &self,
        venue: Venue,
        symbol: &str,
    ) -> Option<crate::models::PositionSnapshot> {
        self.adapters
            .get(&venue)
            .and_then(|adapter| adapter.cached_position(symbol))
    }

    fn cached_entry_balance_reason(&self, candidate: &CandidateOpportunity) -> Option<String> {
        if !matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live) {
            return None;
        }

        let long_position = self.cached_position(candidate.long_venue, &candidate.symbol)?;
        let short_position = self.cached_position(candidate.short_venue, &candidate.symbol)?;
        cached_flat_guard_reason(&long_position, &short_position, candidate.quantity)
    }

    fn expire_entry_cooldowns(&mut self) {
        let now_ms = wall_clock_now_ms();
        self.venue_entry_cooldowns
            .retain(|_, cooldown| cooldown.until_wall_clock_ms > now_ms);
    }

    fn venue_entry_cooldown_reason(&self, venue: Venue) -> Option<String> {
        let cooldown = self.venue_entry_cooldowns.get(&venue)?;
        if cooldown.until_wall_clock_ms <= wall_clock_now_ms() {
            return None;
        }

        Some(format!("venue_entry_cooldown:{venue}:{}", cooldown.reason))
    }

    fn arm_venue_entry_cooldown(&mut self, venue: Venue, reason: &str) {
        let cooldown_ms = self.config.runtime.uncertain_order_cooldown_ms;
        if cooldown_ms <= 0 {
            return;
        }

        let now_ms = wall_clock_now_ms();
        let until_wall_clock_ms = now_ms.saturating_add(cooldown_ms);
        self.venue_entry_cooldowns.insert(
            venue,
            VenueEntryCooldown {
                until_wall_clock_ms,
                reason: reason.to_string(),
            },
        );
        self.log_event(
            "runtime.venue_cooldown_started",
            &json!({
                "venue": venue,
                "reason": reason,
                "cooldown_ms": cooldown_ms,
                "until_wall_clock_ms": until_wall_clock_ms,
            }),
        );
    }

    async fn discover_live_open_position(&mut self, market: &MarketView) -> Result<()> {
        let mut recovery_market = market.clone();
        let mut recovered = Vec::new();
        let mut mismatches = Vec::new();
        let mut venue_positions = HashMap::new();

        for venue in self.adapters.keys().copied() {
            if let Some(positions) = self.adapter(venue)?.fetch_all_positions().await? {
                let positions_by_symbol = positions
                    .into_iter()
                    .map(|position| (position.symbol.clone(), position))
                    .collect::<HashMap<_, _>>();
                venue_positions.insert(venue, positions_by_symbol);
            }
        }

        for symbol in &self.config.symbols {
            let mut active_positions = Vec::new();
            for venue in self.adapters.keys().copied() {
                if let Some(positions_by_symbol) = venue_positions.get(&venue) {
                    if let Some(position) = positions_by_symbol.get(symbol) {
                        if !approx_zero(position.size) {
                            active_positions.push(position.clone());
                        }
                    }
                    continue;
                }
                if recovery_market.symbol(venue, symbol).is_none()
                    && self.cached_position(venue, symbol).is_none()
                {
                    continue;
                }
                let position = self.adapter(venue)?.fetch_position(symbol).await?;
                if !approx_zero(position.size) {
                    active_positions.push(position);
                }
            }

            if active_positions.is_empty() {
                continue;
            }
            if active_positions.len() != 2 {
                mismatches.push(format!(
                    "{symbol}:unexpected_leg_count:{}",
                    active_positions.len()
                ));
                continue;
            }

            let mut long_leg = None;
            let mut short_leg = None;
            for position in active_positions {
                if position.size > 0.0 {
                    long_leg = Some(position);
                } else if position.size < 0.0 {
                    short_leg = Some(position);
                }
            }

            let (Some(long_leg), Some(short_leg)) = (long_leg, short_leg) else {
                mismatches.push(format!("{symbol}:same_direction_exposure"));
                continue;
            };
            if !approx_eq(long_leg.size.abs(), short_leg.size.abs()) {
                mismatches.push(format!(
                    "{symbol}:size_mismatch:{}:{}",
                    long_leg.size, short_leg.size
                ));
                continue;
            }

            if recovery_market.symbol(long_leg.venue, symbol).is_none() {
                if let Ok(snapshot) = self
                    .adapter(long_leg.venue)?
                    .refresh_market_snapshot(symbol)
                    .await
                {
                    recovery_market.merge_snapshot(snapshot);
                }
            }
            if recovery_market.symbol(short_leg.venue, symbol).is_none() {
                if let Ok(snapshot) = self
                    .adapter(short_leg.venue)?
                    .refresh_market_snapshot(symbol)
                    .await
                {
                    recovery_market.merge_snapshot(snapshot);
                }
            }

            let Some(long_quote) = recovery_market.symbol(long_leg.venue, symbol) else {
                mismatches.push(format!("{symbol}:missing_market:{}", long_leg.venue));
                continue;
            };
            let Some(short_quote) = recovery_market.symbol(short_leg.venue, symbol) else {
                mismatches.push(format!("{symbol}:missing_market:{}", short_leg.venue));
                continue;
            };

            let quantity = long_leg.size.abs();
            let reference_mid =
                ((long_quote.mid_price() + short_quote.mid_price()) / 2.0).max(1e-9);
            let long_funding_timestamp_ms = long_quote.funding_timestamp_ms;
            let short_funding_timestamp_ms = short_quote.funding_timestamp_ms;
            let opportunity_type =
                if (long_funding_timestamp_ms - short_funding_timestamp_ms).abs() <= 60_000 {
                    FundingOpportunityType::Aligned
                } else {
                    FundingOpportunityType::Staggered
                };
            let first_funding_leg = if long_funding_timestamp_ms <= short_funding_timestamp_ms {
                FundingLeg::Long
            } else {
                FundingLeg::Short
            };
            let first_funding_timestamp_ms =
                long_funding_timestamp_ms.min(short_funding_timestamp_ms);
            let second_funding_timestamp_ms =
                long_funding_timestamp_ms.max(short_funding_timestamp_ms);
            let total_funding_edge_bps =
                (short_quote.funding_rate - long_quote.funding_rate) * 10_000.0;
            let funding_edge_bps_entry = match opportunity_type {
                FundingOpportunityType::Aligned => total_funding_edge_bps,
                FundingOpportunityType::Staggered => match first_funding_leg {
                    FundingLeg::Long => -long_quote.funding_rate * 10_000.0,
                    FundingLeg::Short => short_quote.funding_rate * 10_000.0,
                },
            };
            recovered.push(OpenPosition {
                position_id: format!(
                    "live-recovered-{}-{}-{}",
                    recovery_market.now_ms(),
                    symbol.to_ascii_lowercase(),
                    long_leg.venue
                ),
                symbol: symbol.clone(),
                long_venue: long_leg.venue,
                short_venue: short_leg.venue,
                quantity,
                initial_quantity: quantity,
                long_entry_price: long_quote.mid_price(),
                short_entry_price: short_quote.mid_price(),
                entry_notional_quote: quantity * reference_mid,
                funding_timestamp_ms: first_funding_timestamp_ms,
                long_funding_timestamp_ms,
                short_funding_timestamp_ms,
                opportunity_type,
                first_funding_leg,
                second_funding_timestamp_ms,
                funding_edge_bps_entry,
                total_funding_edge_bps_entry: total_funding_edge_bps,
                expected_edge_bps_entry: 0.0,
                worst_case_edge_bps_entry: 0.0,
                entry_cross_bps_entry: 0.0,
                fee_bps_entry: 0.0,
                entry_slippage_bps_entry: 0.0,
                entry_depth_capped_at_entry: false,
                total_entry_fee_quote: 0.0,
                realized_price_pnl_quote: 0.0,
                realized_exit_fee_quote: 0.0,
                entered_at_ms: recovery_market.now_ms(),
                current_net_quote: 0.0,
                peak_net_quote: 0.0,
                funding_captured: false,
                second_stage_funding_captured: false,
                captured_funding_quote: 0.0,
                second_stage_funding_quote: 0.0,
                settlement_half_closed_quantity: 0.0,
                settlement_half_closed_at_ms: 0,
                exit_after_first_stage: !matches!(
                    self.config.strategy.staggered_exit_mode,
                    StaggeredExitMode::EvaluateSecondStage
                ) || opportunity_type == FundingOpportunityType::Aligned,
                second_stage_enabled_at_entry: matches!(
                    self.config.strategy.staggered_exit_mode,
                    StaggeredExitMode::EvaluateSecondStage
                ) && opportunity_type
                    == FundingOpportunityType::Staggered
                    && (total_funding_edge_bps - funding_edge_bps_entry) > 0.0,
                exit_reason: Some("recovered_from_live".to_string()),
            });
        }

        if mismatches.is_empty() && recovered.is_empty() {
            return Ok(());
        }

        let max_positions = self.config.strategy.max_concurrent_positions.max(1);
        if mismatches.is_empty() && !recovered.is_empty() && recovered.len() <= max_positions {
            for position in &recovered {
                self.log_critical_event("recovery.live_detected", position)?;
            }
            self.state.open_positions = recovered;
            self.sync_open_position_mirror();
            self.state.mode = EngineMode::Recovering;
            self.state.last_error = None;
            self.persist_state()?;
            return Ok(());
        }

        self.state.mode = EngineMode::FailClosed;
        self.state.last_error = Some(format!(
            "unable to auto-recover live exposure: recovered={}, mismatches={}",
            recovered.len(),
            mismatches.join(",")
        ));
        self.log_critical_event(
            "recovery.live_blocked",
            &json!({
                "recovered": recovered,
                "mismatches": mismatches,
            }),
        )?;
        self.persist_state()?;
        Ok(())
    }
}

fn should_activate_windowed_market_data_with_hysteresis(
    config: &AppConfig,
    remaining_ms: i64,
    currently_active: bool,
) -> bool {
    if is_within_funding_scan_window_ms(config, remaining_ms) {
        return true;
    }
    if remaining_ms <= 0 {
        return false;
    }

    let max_before_ms = config
        .strategy
        .max_scan_minutes_before_funding
        .max(0)
        .saturating_mul(60_000);
    let min_before_ms = config
        .strategy
        .min_scan_minutes_before_funding
        .max(0)
        .saturating_mul(60_000);

    if max_before_ms > 0 {
        if remaining_ms > max_before_ms
            && remaining_ms <= max_before_ms.saturating_add(MARKET_DATA_WINDOW_PREWARM_MS)
        {
            return true;
        }
        if currently_active
            && remaining_ms < min_before_ms
            && remaining_ms >= min_before_ms.saturating_sub(MARKET_DATA_WINDOW_LINGER_MS)
        {
            return true;
        }
        return false;
    }

    let entry_window_ms = config.strategy.entry_window_secs.saturating_mul(1_000);
    remaining_ms > entry_window_ms
        && remaining_ms <= entry_window_ms.saturating_add(MARKET_DATA_WINDOW_PREWARM_MS)
}

fn is_soft_market_fetch_failure(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("market snapshot unavailable for requested symbols")
            || message.contains("symbol not supported")
            || message.contains("instrument metadata missing")
            || message.contains("ticker missing")
    })
}

fn approx_zero(value: f64) -> bool {
    value.abs() <= 1e-6
}

fn approx_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-6
}

fn duration_ms_u64(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn compact_client_order_id(position_id: &str, stage: &str) -> String {
    let raw_stage_code = match stage {
        "entry_long" => "el",
        "entry_short" => "es",
        "exit_long" => "xl",
        "exit_short" => "xs",
        "compensate" => "cp",
        other => {
            if other.len() >= 2 {
                &other[..2]
            } else {
                other
            }
        }
    };
    let stage_code = raw_stage_code
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(2)
        .collect::<String>();
    let mut hasher = DefaultHasher::new();
    position_id.hash(&mut hasher);
    stage.hash(&mut hasher);
    format!("lf{stage_code}{:016x}", hasher.finish())
}

fn wall_clock_now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn order_quote_expired_reason(
    max_order_quote_age_ms: i64,
    observed_at_ms: Option<i64>,
    reduce_only: bool,
) -> Option<String> {
    if reduce_only || max_order_quote_age_ms <= 0 {
        return None;
    }
    let observed_at_ms = observed_at_ms?;
    let age_ms = wall_clock_now_ms().saturating_sub(observed_at_ms);
    if age_ms > max_order_quote_age_ms {
        return Some(format!("quote_expired:{age_ms}>{max_order_quote_age_ms}"));
    }

    None
}

fn order_request_notional_quote(request: &OrderRequest) -> Option<f64> {
    request
        .price_hint
        .filter(|price| price.is_finite() && *price > 0.0)
        .map(|price| request.quantity.max(0.0) * price)
}

fn effective_entry_leg_notional_floor(
    global_min_entry_leg_notional_quote: f64,
    exchange_min_notional_quote: Option<f64>,
) -> f64 {
    global_min_entry_leg_notional_quote
        .max(0.0)
        .max(exchange_min_notional_quote.unwrap_or_default())
}

fn order_notional_floor_reason(
    min_entry_leg_notional_quote: f64,
    price_hint: Option<f64>,
    quantity: f64,
    reduce_only: bool,
) -> Option<String> {
    if reduce_only || min_entry_leg_notional_quote <= 0.0 {
        return None;
    }
    let price_hint = price_hint.filter(|price| price.is_finite() && *price > 0.0)?;
    let notional = quantity.max(0.0) * price_hint;
    if notional + 1e-9 < min_entry_leg_notional_quote {
        return Some(format!(
            "entry_leg_notional_below_minimum:{notional:.6}<{}",
            min_entry_leg_notional_quote
        ));
    }
    None
}

fn cached_flat_guard_reason(
    long_position: &crate::models::PositionSnapshot,
    short_position: &crate::models::PositionSnapshot,
    candidate_quantity: f64,
) -> Option<String> {
    if approx_zero(long_position.size) && approx_zero(short_position.size) {
        return None;
    }
    if !approx_zero(long_position.size)
        && !approx_zero(short_position.size)
        && long_position.size.is_sign_positive() == short_position.size.is_sign_positive()
    {
        return Some("cached_position_same_direction".to_string());
    }

    let imbalance = (long_position.size.abs() - short_position.size.abs()).abs();
    if imbalance >= candidate_quantity.max(1e-9) {
        return Some(format!(
            "cached_position_imbalance:{imbalance:.8}>={:.8}",
            candidate_quantity.max(1e-9)
        ));
    }

    None
}

fn venue_order_health_risk_score(samples: &VecDeque<VenueOrderHealthSample>) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }

    let len = samples.len() as f64;
    let failed = samples.iter().filter(|sample| sample.failed).count() as f64 / len;
    let uncertain = samples.iter().filter(|sample| sample.uncertain).count() as f64 / len;
    failed + uncertain
}

fn order_error_may_have_created_exposure(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("uncertain") || message.contains("pending")
}

fn hyperliquid_entry_gate_reason(
    strategy: &crate::config::StrategyConfig,
    samples: &VecDeque<u64>,
) -> Option<String> {
    if strategy.hyperliquid_max_submit_ack_p95_ms == 0 {
        return None;
    }
    if samples.len() < strategy.hyperliquid_submit_ack_min_samples.max(1) {
        return None;
    }

    let mut sorted = samples.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let p95 = percentile_nearest_rank_u64(&sorted, 0.95)?;
    if p95 > strategy.hyperliquid_max_submit_ack_p95_ms {
        return Some(format!(
            "hyperliquid_recent_submit_ack_p95_ms_above_limit:{p95}>{}",
            strategy.hyperliquid_max_submit_ack_p95_ms
        ));
    }

    None
}

fn percentile_nearest_rank_u64(values: &[u64], percentile: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }

    let percentile = percentile.clamp(0.0, 1.0);
    let rank = ((values.len() as f64) * percentile).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    Some(values[index])
}

fn should_probe_live_recovery(last_probe_ms: Option<i64>, now_ms: i64, interval_ms: i64) -> bool {
    if now_ms <= 0 {
        return last_probe_ms.is_none();
    }

    last_probe_ms
        .map(|last| now_ms.saturating_sub(last) >= interval_ms)
        .unwrap_or(true)
}

fn load_persisted_scan_symbol_cache(config: &AppConfig) -> Option<PersistedScanSymbolCache> {
    let cache = load_json_cache::<PersistedScanSymbolCache>(&scan_symbol_cache_filename(config))?;
    if !cache_is_fresh(
        cache.updated_at_ms,
        wall_clock_now_ms(),
        SYMBOL_CACHE_TTL_MS,
    ) {
        return None;
    }
    if !scan_symbol_cache_matches_config(&cache, config) {
        return None;
    }
    Some(cache)
}

fn build_scan_symbol_cache(
    config: &AppConfig,
    adapters: &BTreeMap<Venue, Arc<dyn VenueAdapter>>,
) -> Option<PersistedScanSymbolCache> {
    let requested_symbols = normalized_requested_symbols(&config.symbols);
    if requested_symbols.is_empty() {
        return None;
    }
    let pairs = canonical_scan_pairs(config);
    if pairs.is_empty() {
        return None;
    }

    let unique_venues = pairs
        .iter()
        .flat_map(|pair| [pair.long, pair.short])
        .collect::<BTreeSet<_>>();
    let mut supported_by_venue = BTreeMap::<Venue, HashSet<String>>::new();
    for venue in unique_venues {
        let supported = adapters
            .get(&venue)?
            .supported_symbols(&requested_symbols)?
            .into_iter()
            .collect::<HashSet<_>>();
        supported_by_venue.insert(venue, supported);
    }

    let mut scan_symbols = BTreeSet::new();
    for pair in &pairs {
        let long_supported = supported_by_venue.get(&pair.long)?;
        let short_supported = supported_by_venue.get(&pair.short)?;
        let allowed_symbols = if pair.symbols.is_empty() {
            requested_symbols.clone()
        } else {
            pair.symbols.clone()
        };
        for symbol in allowed_symbols {
            if long_supported.contains(symbol.as_str()) && short_supported.contains(symbol.as_str())
            {
                scan_symbols.insert(symbol);
            }
        }
    }

    Some(PersistedScanSymbolCache {
        updated_at_ms: wall_clock_now_ms(),
        requested_symbols,
        pairs,
        scan_symbols: scan_symbols.into_iter().collect(),
    })
}

fn normalized_requested_symbols(symbols: &[String]) -> Vec<String> {
    let mut normalized = symbols.to_vec();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn scan_symbol_cache_filename(config: &AppConfig) -> String {
    let mut hasher = DefaultHasher::new();
    normalized_requested_symbols(&config.symbols).hash(&mut hasher);
    config.persistence.event_log_path.hash(&mut hasher);
    config.persistence.snapshot_path.hash(&mut hasher);
    for pair in canonical_scan_pairs(config) {
        pair.long.hash(&mut hasher);
        pair.short.hash(&mut hasher);
        pair.symbols.hash(&mut hasher);
    }
    format!("scan-symbols-{:016x}.json", hasher.finish())
}

fn canonical_scan_pairs(config: &AppConfig) -> Vec<PersistedScanPair> {
    let mut pairs = config
        .directed_pairs_or_all()
        .into_iter()
        .map(|pair| {
            let mut symbols = pair.symbols;
            symbols.sort();
            symbols.dedup();
            PersistedScanPair {
                long: pair.long,
                short: pair.short,
                symbols,
            }
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| {
        left.long
            .cmp(&right.long)
            .then_with(|| left.short.cmp(&right.short))
            .then_with(|| left.symbols.cmp(&right.symbols))
    });
    pairs
}

fn scan_symbol_cache_matches_config(cache: &PersistedScanSymbolCache, config: &AppConfig) -> bool {
    cache.requested_symbols == normalized_requested_symbols(&config.symbols)
        && cache.pairs == canonical_scan_pairs(config)
}

fn restore_venue_health_from_state(
    state: &EngineState,
) -> (
    BTreeMap<Venue, VecDeque<u64>>,
    BTreeMap<Venue, VecDeque<VenueOrderHealthSample>>,
    BTreeMap<Venue, i64>,
) {
    let mut recent_submit_ack_ms = BTreeMap::new();
    let mut recent_order_health = BTreeMap::new();
    let mut updated_at_ms = BTreeMap::new();

    for (venue, persisted) in &state.venue_health {
        if persisted.updated_at_ms > 0
            && !cache_is_fresh(
                persisted.updated_at_ms,
                wall_clock_now_ms(),
                SYMBOL_CACHE_TTL_MS,
            )
        {
            continue;
        }
        if !persisted.recent_submit_ack_ms.is_empty() {
            recent_submit_ack_ms.insert(
                *venue,
                persisted
                    .recent_submit_ack_ms
                    .iter()
                    .copied()
                    .collect::<VecDeque<_>>(),
            );
        }
        if !persisted.recent_order_health.is_empty() {
            recent_order_health.insert(
                *venue,
                persisted
                    .recent_order_health
                    .iter()
                    .copied()
                    .collect::<VecDeque<_>>(),
            );
        }
        if persisted.updated_at_ms > 0 {
            updated_at_ms.insert(*venue, persisted.updated_at_ms);
        }
    }

    (recent_submit_ack_ms, recent_order_health, updated_at_ms)
}

fn build_persisted_venue_health_state(
    recent_submit_ack_ms: &BTreeMap<Venue, VecDeque<u64>>,
    recent_order_health: &BTreeMap<Venue, VecDeque<VenueOrderHealthSample>>,
    updated_at_ms: &BTreeMap<Venue, i64>,
) -> BTreeMap<Venue, PersistedVenueHealthState> {
    let venues = recent_submit_ack_ms
        .keys()
        .chain(recent_order_health.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    venues
        .into_iter()
        .map(|venue| {
            (
                venue,
                PersistedVenueHealthState {
                    updated_at_ms: updated_at_ms.get(&venue).copied().unwrap_or_default(),
                    recent_submit_ack_ms: recent_submit_ack_ms
                        .get(&venue)
                        .map(|samples| samples.iter().copied().collect())
                        .unwrap_or_default(),
                    recent_order_health: recent_order_health
                        .get(&venue)
                        .map(|samples| samples.iter().copied().collect())
                        .unwrap_or_default(),
                },
            )
        })
        .collect()
}

fn persistent_state_view(state: &EngineState) -> EngineState {
    let mut snapshot = state.clone();
    snapshot.cycle = 0;
    snapshot.last_market_ts_ms = None;
    snapshot.last_scan = None;
    normalize_engine_state_positions(&mut snapshot);
    snapshot
}

fn normalize_engine_state_positions(state: &mut EngineState) {
    if state.open_positions.is_empty() {
        if let Some(position) = state.open_position.clone() {
            state.open_positions.push(position);
        }
    }
    for position in &mut state.open_positions {
        if position.long_funding_timestamp_ms == 0 {
            position.long_funding_timestamp_ms = position.funding_timestamp_ms;
        }
        if position.short_funding_timestamp_ms == 0 {
            position.short_funding_timestamp_ms = position.funding_timestamp_ms;
        }
        if position.second_funding_timestamp_ms == 0 {
            position.second_funding_timestamp_ms = position.funding_timestamp_ms;
        }
        if position.total_funding_edge_bps_entry == 0.0 {
            position.total_funding_edge_bps_entry = position.funding_edge_bps_entry;
        }
        if position.initial_quantity <= 0.0 {
            position.initial_quantity = position.quantity;
        }
        if position.opportunity_type == FundingOpportunityType::Aligned {
            position.second_stage_enabled_at_entry = false;
            position.second_stage_funding_captured = false;
        }
    }
    state
        .open_positions
        .sort_by(|left, right| left.position_id.cmp(&right.position_id));
    state
        .open_positions
        .dedup_by(|left, right| left.position_id == right.position_id);
    state.open_position = state.open_positions.first().cloned();
}

fn recover_open_positions_from_journal(journal: &JsonlJournal) -> Result<Vec<OpenPosition>> {
    let mut open_positions = BTreeMap::new();
    journal.scan_records_matching_kinds(
        &[
            "entry.opened",
            "recovery.live_detected",
            "exit.partial_closed",
            "exit.partial_reconciled",
            "exit.closed",
            "recovery.flat",
        ],
        |record| {
            match record.kind.as_str() {
                "entry.opened"
                | "recovery.live_detected"
                | "exit.partial_closed"
                | "exit.partial_reconciled" => {
                    if let Ok(position) =
                        serde_json::from_value::<OpenPosition>(record.payload.clone())
                    {
                        open_positions.insert(position.position_id.clone(), position);
                    }
                }
                "exit.closed" | "recovery.flat" => {
                    let closed_id = record
                        .payload
                        .get("position_id")
                        .and_then(|value| value.as_str());
                    if let Some(closed_id) = closed_id {
                        open_positions.remove(closed_id);
                    }
                }
                _ => {}
            }
            Ok(())
        },
    )?;
    Ok(open_positions.into_values().collect())
}

fn close_leg_record(
    venue: Venue,
    side: Side,
    fill: &crate::models::OrderFill,
    client_order_id: &str,
) -> CloseLegRecord {
    CloseLegRecord {
        venue,
        side,
        order_id: fill.order_id.clone(),
        client_order_id: client_order_id.to_string(),
        quantity: fill.quantity,
        average_price: fill.average_price,
        fee_quote: fill.fee_quote,
        filled_at_ms: fill.filled_at_ms,
    }
}

fn reconciled_exit_fee_quote(
    original: &CloseLegRecord,
    reconciled: &OrderFillReconciliation,
) -> f64 {
    reconciled.fee_quote.unwrap_or(original.fee_quote)
}

fn realized_price_pnl_quote_from_reconciled_legs(
    position: &OpenPosition,
    long_leg: &OrderFillReconciliation,
    short_leg: &OrderFillReconciliation,
) -> f64 {
    (long_leg.average_price - position.long_entry_price) * long_leg.quantity
        + (position.short_entry_price - short_leg.average_price) * short_leg.quantity
}

fn close_reconciliation_kind_rank(kind: CloseReconciliationKind) -> u8 {
    match kind {
        CloseReconciliationKind::Partial => 0,
        CloseReconciliationKind::Final => 1,
    }
}

fn close_reconciliation_retry_delay_ms(attempt_count: u32) -> i64 {
    let exponent = attempt_count.saturating_sub(1).min(4);
    let multiplier = 1_i64 << exponent;
    (CLOSE_RECONCILIATION_RETRY_BASE_MS * multiplier).min(CLOSE_RECONCILIATION_RETRY_MAX_MS)
}

fn no_entry_diagnostics_fingerprint(
    reason: &str,
    candidate_count: usize,
    tradeable_count: usize,
    selected_candidate_count: usize,
    active_position_count: usize,
    remaining_slots: usize,
    blocked_reason_counts: &BTreeMap<String, usize>,
    advisory_counts: &BTreeMap<String, usize>,
) -> String {
    json!({
        "reason": reason,
        "candidate_count": candidate_count,
        "tradeable_count": tradeable_count,
        "selected_candidate_count": selected_candidate_count,
        "active_position_count": active_position_count,
        "remaining_slots": remaining_slots,
        "blocked_reason_counts": blocked_reason_counts,
        "advisory_counts": advisory_counts,
    })
    .to_string()
}

fn append_object_field(
    payload: &serde_json::Value,
    key: &str,
    value: serde_json::Value,
) -> serde_json::Value {
    match payload {
        serde_json::Value::Object(object) => {
            let mut object = object.clone();
            object.insert(key.to_string(), value);
            serde_json::Value::Object(object)
        }
        _ => payload.clone(),
    }
}

fn compact_outcome_diagnostics(
    diagnostics: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
    let diagnostics = diagnostics?;
    let mut payload = serde_json::Map::new();
    for key in [
        "segment_kind",
        "outcome",
        "primary_reason",
        "contributing_reasons",
        "net_quote",
        "profit_quote",
        "loss_quote",
        "current_total_funding_edge_bps",
        "entry_total_funding_edge_bps",
        "exit_reason",
    ] {
        if let Some(value) = diagnostics.get(key) {
            payload.insert(key.to_string(), value.clone());
        }
    }
    Some(serde_json::Value::Object(payload))
}

fn should_compact_reconciliation_payload(payload: &serde_json::Value) -> bool {
    let reconciliation_delta = payload
        .get("reconciliation_delta_quote")
        .and_then(|value| value.as_f64())
        .unwrap_or(f64::INFINITY)
        .abs();
    let remaining_delta = payload
        .get("remaining_reconciliation_delta_quote")
        .and_then(|value| value.as_f64())
        .unwrap_or_default()
        .abs();
    reconciliation_delta <= RECONCILIATION_SUMMARY_ONLY_DELTA_QUOTE
        && remaining_delta <= RECONCILIATION_SUMMARY_ONLY_DELTA_QUOTE
}

fn count_strings(items: impl IntoIterator<Item = String>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for item in items {
        *counts.entry(item).or_insert(0) += 1;
    }
    counts
}

fn checklist_item(key: &'static str, ok: bool, detail: String) -> OpportunityChecklistItem {
    OpportunityChecklistItem { key, ok, detail }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use crate::config::{AppConfig, PersistenceConfig, RuntimeConfig, StrategyConfig, VenueConfig};
    use crate::models::{
        AccountFeeSnapshot, CandidateOpportunity, FundingLeg, FundingOpportunityType,
    };
    use anyhow::anyhow;

    use super::{
        apply_cached_fee_snapshots_to_candidates, cached_flat_guard_reason,
        compact_client_order_id, effective_entry_leg_notional_floor, hyperliquid_entry_gate_reason,
        order_error_may_have_created_exposure, order_quote_expired_reason, persistent_state_view,
        remap_no_entry_diagnostic_reason, resolved_fee_log_entry,
        should_activate_windowed_market_data_with_hysteresis, should_probe_live_recovery,
        venue_order_health_risk_score, EngineMode, EngineState, VenueOrderHealthSample,
    };
    use crate::models::{PositionSnapshot, Venue};

    #[test]
    fn cached_fee_snapshots_can_unblock_candidates() {
        let mut config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                min_expected_edge_bps: 5.0,
                min_worst_case_edge_bps: 4.0,
                exit_slippage_reserve_bps: 0.5,
                execution_buffer_bps: 0.5,
                capital_buffer_bps: 0.25,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig {
                event_log_path: "runtime/test-events.jsonl".to_string(),
                snapshot_path: "runtime/test-state.json".to_string(),
            },
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 4.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 4.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: Vec::new(),
        };
        let mut candidates = vec![CandidateOpportunity {
            pair_id: "btcusdt:binance->okx".to_string(),
            symbol: "BTCUSDT".to_string(),
            long_venue: Venue::Binance,
            short_venue: Venue::Okx,
            quantity: 1.0,
            entry_notional_quote: 100.0,
            funding_timestamp_ms: 1_000,
            long_funding_timestamp_ms: 1_000,
            short_funding_timestamp_ms: 1_000,
            opportunity_type: FundingOpportunityType::Aligned,
            first_funding_leg: FundingLeg::Short,
            first_funding_timestamp_ms: 1_000,
            second_funding_timestamp_ms: 1_000,
            funding_edge_bps: 12.0,
            total_funding_edge_bps: 12.0,
            first_stage_funding_edge_bps: 12.0,
            first_stage_expected_edge_bps: 2.25,
            second_stage_incremental_funding_edge_bps: 0.0,
            stagger_gap_ms: 0,
            entry_cross_bps: 0.0,
            fee_bps: 8.0,
            entry_slippage_bps: 1.0,
            expected_edge_bps: 2.25,
            worst_case_edge_bps: 1.75,
            ranking_edge_bps: 2.0,
            transfer_bias_bps: 0.25,
            transfer_state: None,
            advisories: Vec::new(),
            blocked_reasons: vec![
                "expected_edge_below_floor".to_string(),
                "worst_case_edge_below_floor".to_string(),
            ],
        }];
        let fee_snapshots = BTreeMap::from([
            (
                Venue::Binance,
                AccountFeeSnapshot {
                    venue: Venue::Binance,
                    taker_fee_bps: 1.0,
                    maker_fee_bps: 0.5,
                    observed_at_ms: 1,
                    source: "test".to_string(),
                },
            ),
            (
                Venue::Okx,
                AccountFeeSnapshot {
                    venue: Venue::Okx,
                    taker_fee_bps: 1.0,
                    maker_fee_bps: 0.2,
                    observed_at_ms: 1,
                    source: "test".to_string(),
                },
            ),
        ]);

        let fallback_venues =
            apply_cached_fee_snapshots_to_candidates(&config, &mut candidates, &fee_snapshots);
        let candidate = &candidates[0];

        assert!(fallback_venues.is_empty());
        assert_eq!(candidate.fee_bps, 2.0);
        assert_eq!(candidate.expected_edge_bps, 8.25);
        assert_eq!(candidate.worst_case_edge_bps, 7.75);
        assert_eq!(candidate.first_stage_expected_edge_bps, 8.25);
        assert_eq!(candidate.ranking_edge_bps, 8.0);
        assert!(candidate.blocked_reasons.is_empty());

        config.strategy.min_expected_edge_bps = 0.0;
    }

    #[test]
    fn market_data_activity_uses_window_prewarm_and_linger() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                max_scan_minutes_before_funding: 25,
                min_scan_minutes_before_funding: 5,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig {
                event_log_path: "runtime/test-events.jsonl".to_string(),
                snapshot_path: "runtime/test-state.json".to_string(),
            },
            venues: vec![],
            symbols: vec![],
            directed_pairs: vec![],
        };

        assert!(should_activate_windowed_market_data_with_hysteresis(
            &config,
            26 * 60 * 1_000,
            false,
        ));
        assert!(!should_activate_windowed_market_data_with_hysteresis(
            &config,
            28 * 60 * 1_000,
            false,
        ));
        assert!(should_activate_windowed_market_data_with_hysteresis(
            &config,
            4 * 60 * 1_000,
            true,
        ));
        assert!(!should_activate_windowed_market_data_with_hysteresis(
            &config,
            60 * 1_000,
            true,
        ));
    }

    #[test]
    fn cached_fee_snapshots_fall_back_to_config_when_missing() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                min_expected_edge_bps: -1_000.0,
                min_worst_case_edge_bps: -1_000.0,
                exit_slippage_reserve_bps: 0.5,
                execution_buffer_bps: 0.5,
                capital_buffer_bps: 0.25,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig {
                event_log_path: "runtime/test-events.jsonl".to_string(),
                snapshot_path: "runtime/test-state.json".to_string(),
            },
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 3.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 2.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: Vec::new(),
        };
        let mut candidates = vec![CandidateOpportunity {
            pair_id: "btcusdt:binance->okx".to_string(),
            symbol: "BTCUSDT".to_string(),
            long_venue: Venue::Binance,
            short_venue: Venue::Okx,
            quantity: 1.0,
            entry_notional_quote: 100.0,
            funding_timestamp_ms: 1_000,
            long_funding_timestamp_ms: 1_000,
            short_funding_timestamp_ms: 1_000,
            opportunity_type: FundingOpportunityType::Aligned,
            first_funding_leg: FundingLeg::Short,
            first_funding_timestamp_ms: 1_000,
            second_funding_timestamp_ms: 1_000,
            funding_edge_bps: 10.0,
            total_funding_edge_bps: 10.0,
            first_stage_funding_edge_bps: 10.0,
            first_stage_expected_edge_bps: 0.0,
            second_stage_incremental_funding_edge_bps: 0.0,
            stagger_gap_ms: 0,
            entry_cross_bps: 0.0,
            fee_bps: 5.0,
            entry_slippage_bps: 1.0,
            expected_edge_bps: 3.25,
            worst_case_edge_bps: 2.75,
            ranking_edge_bps: 2.75,
            transfer_bias_bps: 0.0,
            transfer_state: None,
            advisories: Vec::new(),
            blocked_reasons: Vec::new(),
        }];
        let fee_snapshots = BTreeMap::from([(
            Venue::Binance,
            AccountFeeSnapshot {
                venue: Venue::Binance,
                taker_fee_bps: 0.8,
                maker_fee_bps: 0.4,
                observed_at_ms: 1,
                source: "test".to_string(),
            },
        )]);

        let fallback_venues =
            apply_cached_fee_snapshots_to_candidates(&config, &mut candidates, &fee_snapshots);
        let candidate = &candidates[0];

        assert_eq!(fallback_venues, BTreeMap::from([(Venue::Okx, 2.0)]));
        assert!((candidate.fee_bps - 2.8).abs() < 1e-9);
        assert!((candidate.expected_edge_bps - 5.45).abs() < 1e-9);
        assert!((candidate.worst_case_edge_bps - 4.95).abs() < 1e-9);
        assert!((candidate.first_stage_expected_edge_bps - 5.45).abs() < 1e-9);
    }

    #[test]
    fn fee_resolution_log_entry_marks_cached_snapshot_source() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig::default(),
            persistence: PersistenceConfig {
                event_log_path: "runtime/test-events.jsonl".to_string(),
                snapshot_path: "runtime/test-state.json".to_string(),
            },
            venues: vec![VenueConfig {
                venue: Venue::Binance,
                enabled: true,
                taker_fee_bps: 4.0,
                max_notional: 1_000.0,
                market_data_file: None,
                live: Default::default(),
            }],
            symbols: vec![],
            directed_pairs: vec![],
        };
        let fee_snapshots = BTreeMap::from([(
            Venue::Binance,
            AccountFeeSnapshot {
                venue: Venue::Binance,
                taker_fee_bps: 0.8,
                maker_fee_bps: 0.4,
                observed_at_ms: 123,
                source: "account".to_string(),
            },
        )]);

        let fee_entry = resolved_fee_log_entry(&config, &fee_snapshots, Venue::Binance);

        assert_eq!(fee_entry.venue, Venue::Binance);
        assert!((fee_entry.taker_fee_bps - 0.8).abs() < 1e-9);
        assert_eq!(fee_entry.source, "account_fee_snapshot");
        assert!(!fee_entry.used_fallback);
    }

    #[test]
    fn fee_resolution_log_entry_marks_config_fallback_source() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig::default(),
            persistence: PersistenceConfig {
                event_log_path: "runtime/test-events.jsonl".to_string(),
                snapshot_path: "runtime/test-state.json".to_string(),
            },
            venues: vec![VenueConfig {
                venue: Venue::Okx,
                enabled: true,
                taker_fee_bps: 2.5,
                max_notional: 1_000.0,
                market_data_file: None,
                live: Default::default(),
            }],
            symbols: vec![],
            directed_pairs: vec![],
        };
        let fee_snapshots = BTreeMap::new();

        let fee_entry = resolved_fee_log_entry(&config, &fee_snapshots, Venue::Okx);

        assert_eq!(fee_entry.venue, Venue::Okx);
        assert!((fee_entry.taker_fee_bps - 2.5).abs() < 1e-9);
        assert_eq!(fee_entry.source, "configured_taker_fee_bps");
        assert!(fee_entry.used_fallback);
    }

    #[test]
    fn flat_recovery_probe_is_throttled_between_intervals() {
        assert!(should_probe_live_recovery(None, 1_000, 30_000));
        assert!(!should_probe_live_recovery(Some(1_000), 5_000, 30_000));
        assert!(should_probe_live_recovery(Some(1_000), 31_000, 30_000));
    }

    #[test]
    fn persistence_view_strips_volatile_scan_fields() {
        let state = EngineState {
            cycle: 42,
            mode: EngineMode::Running,
            last_market_ts_ms: Some(123_456),
            last_scan: Some(super::ScanSnapshot {
                candidate_count: 3,
                tradeable_count: 1,
                best_candidate: None,
            }),
            open_positions: Vec::new(),
            open_position: None,
            last_error: None,
            venue_health: BTreeMap::new(),
            pending_close_reconciliations: Vec::new(),
        };

        let persisted = persistent_state_view(&state);

        assert_eq!(persisted.cycle, 0);
        assert_eq!(persisted.last_market_ts_ms, None);
        assert_eq!(persisted.last_scan, None);
        assert_eq!(persisted.mode, EngineMode::Running);
    }

    #[test]
    fn compact_client_order_id_stays_short_and_stage_specific() {
        let entry_long = compact_client_order_id(
            "pos-1234567890-binance->hyperliquid-stableusdt",
            "entry_long",
        );
        let entry_short = compact_client_order_id(
            "pos-1234567890-binance->hyperliquid-stableusdt",
            "entry_short",
        );
        assert!(entry_long.len() < 36);
        assert!(entry_short.len() < 36);
        assert_ne!(entry_long, entry_short);
        assert!(entry_long.chars().all(|ch| ch.is_ascii_alphanumeric()));
        assert!(entry_short.chars().all(|ch| ch.is_ascii_alphanumeric()));
        assert!(entry_long.starts_with("lfel"));
        assert!(entry_short.starts_with("lfes"));
    }

    #[test]
    fn hyperliquid_entry_gate_stays_open_until_min_samples_reached() {
        let strategy = StrategyConfig {
            hyperliquid_max_submit_ack_p95_ms: 1_200,
            hyperliquid_submit_ack_window_size: 5,
            hyperliquid_submit_ack_min_samples: 3,
            ..StrategyConfig::default()
        };

        let samples = VecDeque::from(vec![1_350_u64, 1_420_u64]);

        assert!(hyperliquid_entry_gate_reason(&strategy, &samples).is_none());
    }

    #[test]
    fn hyperliquid_entry_gate_blocks_when_recent_p95_exceeds_limit() {
        let strategy = StrategyConfig {
            hyperliquid_max_submit_ack_p95_ms: 1_200,
            hyperliquid_submit_ack_window_size: 5,
            hyperliquid_submit_ack_min_samples: 3,
            ..StrategyConfig::default()
        };

        let samples = VecDeque::from(vec![920_u64, 1_010_u64, 1_080_u64, 1_190_u64, 1_280_u64]);

        assert_eq!(
            hyperliquid_entry_gate_reason(&strategy, &samples),
            Some("hyperliquid_recent_submit_ack_p95_ms_above_limit:1280>1200".to_string())
        );
    }

    #[test]
    fn order_quote_expiry_blocks_only_non_reduce_orders() {
        let observed_at_ms = chrono::Utc::now().timestamp_millis() - 4_000;
        assert!(order_quote_expired_reason(3_000, Some(observed_at_ms), false).is_some());
        assert!(order_quote_expired_reason(6_000, Some(observed_at_ms), false).is_none());
        assert!(order_quote_expired_reason(6_000, Some(observed_at_ms), true).is_none());
        assert!(order_quote_expired_reason(6_000, None, false).is_none());
    }

    #[test]
    fn effective_entry_leg_notional_floor_prefers_exchange_floor_when_higher() {
        assert_eq!(effective_entry_leg_notional_floor(8.0, None), 8.0);
        assert_eq!(effective_entry_leg_notional_floor(8.0, Some(5.0)), 8.0);
        assert_eq!(effective_entry_leg_notional_floor(8.0, Some(10.0)), 10.0);
    }

    #[test]
    fn no_entry_diagnostics_demotes_entry_quote_stale_to_refresh_advisory() {
        let (blocked_reason, advisory_reason) =
            remap_no_entry_diagnostic_reason("entry_quote_stale:binance:48683>30000");
        assert!(blocked_reason.is_none());
        assert_eq!(
            advisory_reason,
            Some("entry_quote_refresh_needed:binance:48683>30000".to_string())
        );

        let (blocked_reason, advisory_reason) =
            remap_no_entry_diagnostic_reason("outside_entry_window");
        assert_eq!(blocked_reason, Some("outside_entry_window".to_string()));
        assert!(advisory_reason.is_none());
    }

    #[test]
    fn uncertain_error_classifier_only_flags_pending_like_failures() {
        assert!(order_error_may_have_created_exposure(&anyhow!(
            "order status uncertain"
        )));
        assert!(order_error_may_have_created_exposure(&anyhow!(
            "hyperliquid-pending"
        )));
        assert!(!order_error_may_have_created_exposure(&anyhow!(
            "quote_expired:4001>6000"
        )));
    }

    #[test]
    fn cached_flat_guard_blocks_same_direction_or_large_imbalance() {
        let long = PositionSnapshot {
            venue: Venue::Binance,
            symbol: "BTCUSDT".to_string(),
            size: 1.2,
            updated_at_ms: 1,
        };
        let short_same_direction = PositionSnapshot {
            venue: Venue::Okx,
            symbol: "BTCUSDT".to_string(),
            size: 0.8,
            updated_at_ms: 1,
        };
        let short_imbalanced = PositionSnapshot {
            venue: Venue::Okx,
            symbol: "BTCUSDT".to_string(),
            size: -0.1,
            updated_at_ms: 1,
        };

        assert_eq!(
            cached_flat_guard_reason(&long, &short_same_direction, 0.5),
            Some("cached_position_same_direction".to_string())
        );
        assert!(cached_flat_guard_reason(&long, &short_imbalanced, 0.5).is_some());
    }

    #[test]
    fn uncertain_failures_raise_entry_risk_score() {
        let stable = VecDeque::from(vec![
            VenueOrderHealthSample {
                failed: false,
                uncertain: false,
            },
            VenueOrderHealthSample {
                failed: true,
                uncertain: false,
            },
        ]);
        let unstable = VecDeque::from(vec![
            VenueOrderHealthSample {
                failed: true,
                uncertain: true,
            },
            VenueOrderHealthSample {
                failed: true,
                uncertain: false,
            },
        ]);

        assert!(venue_order_health_risk_score(&unstable) > venue_order_health_risk_score(&stable));
    }

    #[test]
    fn clean_history_has_lower_health_risk_than_pending_history() {
        let clean = VecDeque::from(vec![
            VenueOrderHealthSample {
                failed: false,
                uncertain: false,
            },
            VenueOrderHealthSample {
                failed: false,
                uncertain: false,
            },
        ]);
        let pending = VecDeque::from(vec![
            VenueOrderHealthSample {
                failed: true,
                uncertain: true,
            },
            VenueOrderHealthSample {
                failed: false,
                uncertain: false,
            },
        ]);

        assert!(venue_order_health_risk_score(&pending) > venue_order_health_risk_score(&clean));
    }
}
