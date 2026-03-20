use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use anyhow::{anyhow, Result};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    config::{AppConfig, StaggeredExitMode},
    journal::JsonlJournal,
    market::MarketView,
    models::{CandidateOpportunity, FundingLeg, FundingOpportunityType, OrderRequest, Side, Venue},
    opportunity_source::{
        normalize_symbol_key, OpportunityHint, OpportunityHintSource, TransferStatusSource,
    },
    store::FileStateStore,
    strategy::{discover_candidates, is_within_funding_scan_window_ms, sort_candidates},
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
    pub total_entry_fee_quote: f64,
    pub entered_at_ms: i64,
    pub current_net_quote: f64,
    pub peak_net_quote: f64,
    pub funding_captured: bool,
    #[serde(default)]
    pub second_stage_funding_captured: bool,
    pub captured_funding_quote: f64,
    #[serde(default)]
    pub second_stage_funding_quote: f64,
    #[serde(default = "default_exit_after_first_stage")]
    pub exit_after_first_stage: bool,
    #[serde(default)]
    pub second_stage_enabled_at_entry: bool,
    pub exit_reason: Option<String>,
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
    last_persisted_state: Option<EngineState>,
    recent_submit_ack_ms: BTreeMap<Venue, VecDeque<u64>>,
    venue_entry_cooldowns: BTreeMap<Venue, VenueEntryCooldown>,
    recent_order_health: BTreeMap<Venue, VecDeque<VenueOrderHealthSample>>,
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

#[derive(Clone, Copy, Debug, Default)]
struct VenueOrderHealthSample {
    failed: bool,
    uncertain: bool,
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
    blocked_reason_counts: BTreeMap<String, usize>,
    advisory_counts: BTreeMap<String, usize>,
    candidates: Vec<OpportunityCandidateChecklist>,
}

const FLAT_RECOVERY_PROBE_INTERVAL_MS: i64 = 30_000;
const ORDER_HEALTH_WINDOW_SIZE: usize = 20;

fn default_exit_after_first_stage() -> bool {
    true
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
        if state.open_positions.len() > config.strategy.max_concurrent_positions.max(1) {
            state.mode = EngineMode::FailClosed;
            state.last_error = Some(format!(
                "open positions exceed configured max: {}>{}",
                state.open_positions.len(),
                config.strategy.max_concurrent_positions.max(1)
            ));
        } else if !state.open_positions.is_empty() {
            state.mode = EngineMode::Recovering;
        }

        Ok(Self {
            journal,
            store,
            config,
            adapters,
            opportunity_source,
            transfer_status_source,
            state,
            last_live_recovery_probe_ms: None,
            last_persisted_state: loaded_state.as_ref().map(persistent_state_view),
            recent_submit_ack_ms: BTreeMap::new(),
            venue_entry_cooldowns: BTreeMap::new(),
            recent_order_health: BTreeMap::new(),
        })
    }

    pub fn state(&self) -> &EngineState {
        &self.state
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

    pub async fn tick(&mut self) -> Result<()> {
        self.state.cycle += 1;
        let market = match self.fetch_market_view().await {
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
            self.reconcile_open_positions().await?;
            self.persist_state()?;
            if self.state.mode != EngineMode::Running {
                return Ok(());
            }
        }

        if !self.active_positions().is_empty() {
            self.manage_open_positions(&market).await?;
        }

        let should_scan = self.should_scan_entries(&market);
        let selected_candidates = if should_scan {
            let hints = self.fetch_hints().await;
            let transfer_statuses = self.fetch_transfer_status_view().await;
            let mut candidates = discover_candidates(
                &self.config,
                &market,
                hints.as_deref(),
                transfer_statuses.as_ref(),
            );
            self.apply_runtime_entry_guards(&mut candidates);
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

    async fn fetch_market_view(&mut self) -> Result<MarketView> {
        let symbols = self.config.symbols.clone();
        let futures = self.adapters.iter().map(|(venue, adapter)| {
            let adapter = Arc::clone(adapter);
            let symbols = symbols.clone();
            async move { (*venue, adapter.fetch_market_snapshot(&symbols).await) }
        });

        let mut snapshots = Vec::new();
        for (venue, result) in join_all(futures).await {
            match result {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => {
                    self.state.last_error =
                        Some(format!("market fetch failed on {venue}: {error:#}"));
                    self.log_event(
                        "market.fetch_failed",
                        &json!({
                            "venue": venue,
                            "error": error.to_string(),
                        }),
                    );
                }
            }
        }
        if snapshots.is_empty() {
            return Err(anyhow!("market fetch failed on all venues"));
        }
        Ok(MarketView::from_snapshots(snapshots))
    }

    async fn fetch_hints(&mut self) -> Option<Vec<OpportunityHint>> {
        let Some(source) = self.opportunity_source.as_ref().cloned() else {
            return None;
        };

        match source.fetch_hints(&self.config.symbols).await {
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
                self.log_event(
                    "hint_source.failed",
                    &json!({
                        "error": error.to_string(),
                    }),
                );
                None
            }
        }
    }

    async fn fetch_transfer_status_view(&mut self) -> Option<TransferStatusView> {
        let assets = self
            .config
            .symbols
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
                    self.log_event(
                        "transfer_source.used",
                        &json!({
                            "source": "external",
                            "count": statuses.len(),
                        }),
                    );
                    return Some(TransferStatusView::from_statuses(statuses));
                }
                Ok(_) => {}
                Err(error) => {
                    self.state.last_error = Some(format!("transfer source failed: {error:#}"));
                    self.log_event(
                        "transfer_source.failed",
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
                    self.log_event(
                        "transfer.fetch_failed",
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
            Some(TransferStatusView::from_statuses(statuses))
        }
    }

    fn should_scan_entries(&self, market: &MarketView) -> bool {
        if self.config.strategy.max_scan_minutes_before_funding <= 0 {
            return true;
        }

        self.config.symbols.iter().any(|symbol| {
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
            if selected.len() >= remaining_slots {
                break;
            }
        }
        selected
    }

    fn log_no_entry_diagnostics(
        &self,
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
        let blocked_reason_counts = count_strings(
            candidates
                .iter()
                .flat_map(|candidate| candidate.blocked_reasons.iter().cloned()),
        );
        let advisory_counts = count_strings(
            candidates
                .iter()
                .flat_map(|candidate| candidate.advisories.iter().cloned()),
        );
        let diagnostics = NoEntryDiagnostics {
            reason: self.no_entry_reason(candidates, selected_candidates, remaining_slots),
            candidate_count: candidates.len(),
            tradeable_count,
            selected_candidate_count: selected_candidates.len(),
            active_position_count: self.active_position_count(),
            remaining_slots,
            blocked_reason_counts,
            advisory_counts,
            candidates: candidates
                .iter()
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
        let inside_entry_window =
            funding_not_passed && is_within_funding_scan_window_ms(&self.config, remaining_ms);
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
            blocked_reasons: candidate.blocked_reasons.clone(),
            advisories: candidate.advisories.clone(),
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

    async fn reconcile_open_positions(&mut self) -> Result<()> {
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
                self.log_critical_event(
                    "recovery.resumed",
                    &json!({
                        "symbol": position.symbol,
                        "position_id": position.position_id,
                    }),
                )?;
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
        let mut guard_failure = None;
        let mut exit_payload = None;
        let mut stale_market_failure = false;
        let exit_reason = {
            let Some(position) = self
                .active_positions_mut()
                .iter_mut()
                .find(|position| position.position_id == position_id)
            else {
                return Ok(());
            };
            if !market.is_fresh(position.long_venue, max_market_age_ms)
                || !market.is_fresh(position.short_venue, max_market_age_ms)
            {
                stale_market_failure = true;
                guard_failure = Some(json!({
                    "position_id": &position.position_id,
                    "symbol": &position.symbol,
                    "long_venue": position.long_venue,
                    "short_venue": position.short_venue,
                    "reason": "stale_market_data",
                }));
                None
            } else {
                let Some(long_quote) = market.symbol(position.long_venue, &position.symbol) else {
                    return Ok(());
                };
                let Some(short_quote) = market.symbol(position.short_venue, &position.symbol)
                else {
                    return Ok(());
                };

                let price_pnl = (long_quote.mid_price() - position.long_entry_price)
                    * position.quantity
                    + (position.short_entry_price - short_quote.mid_price()) * position.quantity;
                if !position.funding_captured
                    && market.now_ms() >= position.funding_timestamp_ms + post_funding_hold_ms
                {
                    position.funding_captured = true;
                    position.captured_funding_quote =
                        position.entry_notional_quote * position.funding_edge_bps_entry / 10_000.0;
                }
                if position.funding_captured
                    && position.second_stage_enabled_at_entry
                    && !position.second_stage_funding_captured
                    && position.second_funding_timestamp_ms > position.funding_timestamp_ms
                    && market.now_ms()
                        >= position.second_funding_timestamp_ms + post_funding_hold_ms
                {
                    position.second_stage_funding_captured = true;
                    position.second_stage_funding_quote = position.entry_notional_quote
                        * (position.total_funding_edge_bps_entry - position.funding_edge_bps_entry)
                        / 10_000.0;
                }
                position.current_net_quote = price_pnl
                    + position.captured_funding_quote
                    + position.second_stage_funding_quote
                    - position.total_entry_fee_quote;
                if position.current_net_quote > position.peak_net_quote {
                    position.peak_net_quote = position.current_net_quote;
                }

                let reason = if position.current_net_quote <= -stop_loss_quote {
                    Some("hard_stop")
                } else if position.peak_net_quote >= profit_take_quote
                    && position.peak_net_quote - position.current_net_quote
                        >= trailing_drawdown_quote
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
                };

                if let Some(reason) = reason {
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
                    }));
                }
                reason.map(str::to_string)
            }
        };

        if stale_market_failure {
            self.state.mode = EngineMode::FailClosed;
            self.state.last_error = Some("stale market data on open position".to_string());
        }

        if let Some(payload) = guard_failure {
            self.log_critical_event("execution.guard_failed", &payload)?;
            return Ok(());
        }

        if let Some(payload) = exit_payload {
            self.log_event("execution.exit_triggered", &payload);
        }

        if let Some(reason) = exit_reason {
            self.try_close_position(position_id, &reason, market)
                .await?;
        }
        Ok(())
    }

    async fn try_open_position(
        &mut self,
        candidate: CandidateOpportunity,
        market: &MarketView,
    ) -> Result<()> {
        let position_id = format!("pos-{}-{}", self.state.cycle, candidate.pair_id);
        self.log_event(
            "execution.entry_selected",
            &json!({
                "position_id": &position_id,
                "pair_id": &candidate.pair_id,
                "candidate": &candidate,
            }),
        );
        let quantity_plan = self
            .plan_executable_quantity(
                candidate.long_venue,
                candidate.short_venue,
                &candidate.symbol,
                candidate.quantity,
            )
            .await?;
        let executable_quantity = quantity_plan.executable_quantity;
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
            client_order_id: format!("{position_id}-entry-short"),
            price_hint: self.order_price_hint(
                market,
                candidate.short_venue,
                &candidate.symbol,
                Side::Sell,
            ),
            observed_at_ms: market.observed_at_ms(candidate.short_venue),
        };
        let long_request = OrderRequest {
            symbol: candidate.symbol.clone(),
            side: Side::Buy,
            quantity: executable_quantity,
            reduce_only: false,
            client_order_id: format!("{position_id}-entry-long"),
            price_hint: self.order_price_hint(
                market,
                candidate.long_venue,
                &candidate.symbol,
                Side::Buy,
            ),
            observed_at_ms: market.observed_at_ms(candidate.long_venue),
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
            market,
            HedgeLeg::Long,
            candidate.long_venue,
            &candidate.symbol,
            long_leg.request.side,
            executable_quantity,
        );
        let short_risk = self.build_entry_leg_risk(
            market,
            HedgeLeg::Short,
            candidate.short_venue,
            &candidate.symbol,
            short_leg.request.side,
            executable_quantity,
        );
        let (first_leg, first_risk, second_leg, second_risk) =
            if long_risk.total_score > short_risk.total_score {
                (long_leg, long_risk, short_leg, short_risk)
            } else {
                (short_leg, short_risk, long_leg, long_risk)
            };
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
                        market,
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
                        market,
                    )
                    .await;
                let failed_leg_cleanup = if order_error_may_have_created_exposure(&error) {
                    self.cleanup_failed_leg_exposure(
                        second_leg.venue,
                        &candidate.symbol,
                        &position_id,
                        Some(&candidate.pair_id),
                        second_leg.leg.cleanup_stage(),
                        market,
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
            total_entry_fee_quote: long_fill.fee_quote + short_fill.fee_quote,
            entered_at_ms: short_fill.filled_at_ms.max(long_fill.filled_at_ms),
            current_net_quote: -(long_fill.fee_quote + short_fill.fee_quote),
            peak_net_quote: -(long_fill.fee_quote + short_fill.fee_quote),
            funding_captured: false,
            second_stage_funding_captured: false,
            captured_funding_quote: 0.0,
            second_stage_funding_quote: 0.0,
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

        let short_close = OrderRequest {
            symbol: position.symbol.clone(),
            side: Side::Buy,
            quantity: position.quantity,
            reduce_only: true,
            client_order_id: format!("{}-exit-short", position.position_id),
            price_hint: self.order_price_hint(
                market,
                position.short_venue,
                &position.symbol,
                Side::Buy,
            ),
            observed_at_ms: market.observed_at_ms(position.short_venue),
        };
        let long_close = OrderRequest {
            symbol: position.symbol.clone(),
            side: Side::Sell,
            quantity: position.quantity,
            reduce_only: true,
            client_order_id: format!("{}-exit-long", position.position_id),
            price_hint: self.order_price_hint(
                market,
                position.long_venue,
                &position.symbol,
                Side::Sell,
            ),
            observed_at_ms: market.observed_at_ms(position.long_venue),
        };

        let (short_fill, short_latency_ms) = self
            .execute_order_leg(
                position.short_venue,
                short_close,
                OrderLegContext {
                    stage: "exit_short",
                    position_id: &position.position_id,
                    pair_id: None,
                },
            )
            .await?;
        let (long_fill, long_latency_ms) = self
            .execute_order_leg(
                position.long_venue,
                long_close,
                OrderLegContext {
                    stage: "exit_long",
                    position_id: &position.position_id,
                    pair_id: None,
                },
            )
            .await?;

        self.log_event(
            "execution.exit_latency_summary",
            &json!({
                "position_id": &position.position_id,
                "symbol": &position.symbol,
                "reason": reason,
                "long_venue": position.long_venue,
                "short_venue": position.short_venue,
                "total_roundtrip_ms": short_latency_ms + long_latency_ms,
                "max_single_order_ms": short_latency_ms.max(long_latency_ms),
                "total_exit_fee_quote": short_fill.fee_quote + long_fill.fee_quote,
            }),
        );

        self.log_critical_event(
            "exit.closed",
            &json!({
                "position_id": &position.position_id,
                "symbol": &position.symbol,
                "reason": reason,
                "net_quote": position.current_net_quote,
                "total_exit_fee_quote": short_fill.fee_quote + long_fill.fee_quote,
                "closed_at_ms": short_fill.filled_at_ms.max(long_fill.filled_at_ms),
            }),
        )?;
        self.remove_open_position(&position.position_id);
        self.state.last_error = None;
        self.state.mode = EngineMode::Running;
        self.persist_state()?;
        Ok(())
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
            client_order_id: format!("{position_id}-compensate"),
            price_hint: self.order_price_hint(market, venue, symbol, side),
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

    fn persist_state(&mut self) -> Result<()> {
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
        if matches!(self.config.runtime.mode, crate::config::RuntimeMode::Live) {
            if let Some(reason) = order_quote_expired_reason(
                self.config.runtime.max_order_quote_age_ms,
                request.observed_at_ms,
                request.reduce_only,
            ) {
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

    fn apply_runtime_entry_guards(&mut self, candidates: &mut [CandidateOpportunity]) {
        for candidate in candidates.iter_mut() {
            let was_tradeable = candidate.is_tradeable();
            if let Some(reason) = self.runtime_entry_gate_reason(candidate) {
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

    fn runtime_entry_gate_reason(&self, candidate: &CandidateOpportunity) -> Option<String> {
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
    }

    fn record_order_health(&mut self, venue: Venue, failed: bool, uncertain: bool) {
        let samples = self.recent_order_health.entry(venue).or_default();
        samples.push_back(VenueOrderHealthSample { failed, uncertain });
        while samples.len() > ORDER_HEALTH_WINDOW_SIZE {
            samples.pop_front();
        }
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
        let mut recovered = Vec::new();
        let mut mismatches = Vec::new();

        for symbol in &self.config.symbols {
            let mut active_positions = Vec::new();
            for venue in self.adapters.keys().copied() {
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

            let Some(long_quote) = market.symbol(long_leg.venue, symbol) else {
                mismatches.push(format!("{symbol}:missing_market:{}", long_leg.venue));
                continue;
            };
            let Some(short_quote) = market.symbol(short_leg.venue, symbol) else {
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
                    market.now_ms(),
                    symbol.to_ascii_lowercase(),
                    long_leg.venue
                ),
                symbol: symbol.clone(),
                long_venue: long_leg.venue,
                short_venue: short_leg.venue,
                quantity,
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
                total_entry_fee_quote: 0.0,
                entered_at_ms: market.now_ms(),
                current_net_quote: 0.0,
                peak_net_quote: 0.0,
                funding_captured: false,
                second_stage_funding_captured: false,
                captured_funding_quote: 0.0,
                second_stage_funding_quote: 0.0,
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

fn approx_zero(value: f64) -> bool {
    value.abs() <= 1e-6
}

fn approx_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-6
}

fn duration_ms_u64(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
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
    for record in journal.read_records()? {
        match record.kind.as_str() {
            "entry.opened" | "recovery.live_detected" => {
                if let Ok(position) = serde_json::from_value::<OpenPosition>(record.payload.clone())
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
    }
    Ok(open_positions.into_values().collect())
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
    use std::collections::VecDeque;

    use crate::config::StrategyConfig;
    use anyhow::anyhow;

    use super::{
        cached_flat_guard_reason, hyperliquid_entry_gate_reason,
        order_error_may_have_created_exposure, order_quote_expired_reason, persistent_state_view,
        should_probe_live_recovery, venue_order_health_risk_score, EngineMode, EngineState,
        VenueOrderHealthSample,
    };
    use crate::models::{PositionSnapshot, Venue};

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
        };

        let persisted = persistent_state_view(&state);

        assert_eq!(persisted.cycle, 0);
        assert_eq!(persisted.last_market_ts_ms, None);
        assert_eq!(persisted.last_scan, None);
        assert_eq!(persisted.mode, EngineMode::Running);
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
        assert!(order_quote_expired_reason(3_000, Some(observed_at_ms), true).is_none());
        assert!(order_quote_expired_reason(3_000, None, false).is_none());
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
            "quote_expired:4001>3000"
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
