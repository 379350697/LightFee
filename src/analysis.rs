use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use anyhow::Result;
use chrono::{Local, TimeZone};
use serde::Serialize;

use crate::{
    journal::{scan_path_records, JournalRecord},
    models::{AccountBalanceSnapshot, Venue},
};

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct VenueJournalStats {
    pub submitted_orders: u64,
    pub filled_orders: u64,
    pub failed_orders: u64,
    pub failure_rate_pct: f64,
    pub latency_ms_p50: Option<u64>,
    pub latency_ms_p95: Option<u64>,
    pub latency_ms_p99: Option<u64>,
    pub latency_ms_max: Option<u64>,
    pub total_fee_quote: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct JournalAnalysisReport {
    pub total_records: usize,
    pub run_count: usize,
    pub venue_stats: BTreeMap<Venue, VenueJournalStats>,
    pub recovery_counts: BTreeMap<String, u64>,
    pub fail_closed_reason_counts: BTreeMap<String, u64>,
    pub optimization_stats: JournalOptimizationStats,
    pub recommendations: Vec<OptimizationRecommendation>,
    pub current_balance_snapshot: Option<BalanceSnapshotReport>,
    pub daily_profit_summaries: Vec<DailyProfitSummary>,
    pub trade_replays: Vec<TradeReplay>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct DailyJournalAnalysisReport {
    pub current_balance_snapshot: Option<BalanceSnapshotReport>,
    pub daily_profit_summaries: Vec<DailyProfitSummary>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JournalAnalysisTimeRange {
    pub since_ts_ms: Option<i64>,
    pub until_ts_ms: Option<i64>,
}

impl JournalAnalysisTimeRange {
    fn contains(&self, ts_ms: i64) -> bool {
        let after_start = self.since_ts_ms.map(|value| ts_ms >= value).unwrap_or(true);
        let before_end = self.until_ts_ms.map(|value| ts_ms <= value).unwrap_or(true);
        after_start && before_end
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct BalanceSnapshotFailure {
    pub venue: Venue,
    pub error: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct BalanceSnapshotReport {
    pub observed_at_ms: i64,
    pub total_equity_quote: Option<f64>,
    pub venues: Vec<AccountBalanceSnapshot>,
    pub failed_venues: Vec<BalanceSnapshotFailure>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct DailySymbolRevenue {
    pub symbol: String,
    pub position_count: u64,
    pub partial_close_count: u64,
    pub closed_position_count: u64,
    pub realized_net_quote: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct DailyProfitSummary {
    pub date: String,
    pub realized_revenue_quote: f64,
    pub partial_realized_revenue_quote: f64,
    pub remaining_close_realized_revenue_quote: f64,
    pub opened_position_count: u64,
    pub partial_close_count: u64,
    pub closed_position_count: u64,
    pub latest_total_equity_quote: Option<f64>,
    pub latest_balance_observed_at_ms: Option<i64>,
    pub venue_equity_quote: BTreeMap<Venue, f64>,
    pub opened_symbol_revenues: Vec<DailySymbolRevenue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct JournalOptimizationStats {
    pub runtime_gate_block_counts: BTreeMap<String, u64>,
    pub order_block_reason_counts: BTreeMap<String, u64>,
    pub venue_cooldown_counts: BTreeMap<Venue, u64>,
    pub first_leg_counts: BTreeMap<String, u64>,
    pub first_leg_dominant_factor_counts: BTreeMap<String, u64>,
    pub order_error_counts: BTreeMap<String, u64>,
    pub no_entry_reason_counts: BTreeMap<String, u64>,
    pub no_entry_blocked_reason_counts: BTreeMap<String, u64>,
    pub no_entry_advisory_counts: BTreeMap<String, u64>,
    pub no_entry_checklist_fail_counts: BTreeMap<String, u64>,
    pub no_entry_selection_blocker_counts: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct OptimizationRecommendation {
    pub priority: u8,
    pub category: String,
    pub title: String,
    pub summary: String,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct TradeLegReplay {
    pub stage: String,
    pub venue: Option<Venue>,
    pub side: Option<String>,
    pub reduce_only: Option<bool>,
    pub requested_quantity: Option<f64>,
    pub executed_quantity: Option<f64>,
    pub average_price: Option<f64>,
    pub fee_quote: Option<f64>,
    pub local_roundtrip_ms: Option<u64>,
    pub adapter_timing: Option<serde_json::Value>,
    pub outcome: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct TradeReplay {
    pub position_id: String,
    pub first_ts_ms: i64,
    pub last_ts_ms: i64,
    pub pair_id: Option<String>,
    pub symbol: Option<String>,
    pub state: String,
    pub selected_candidate: Option<serde_json::Value>,
    pub quantity_plan: Option<serde_json::Value>,
    pub entry_order_plan: Option<serde_json::Value>,
    pub entry_latency_summary: Option<serde_json::Value>,
    pub exit_latency_summary: Option<serde_json::Value>,
    pub opened_position: Option<serde_json::Value>,
    pub partial_close_summaries: Vec<serde_json::Value>,
    pub close_summary: Option<serde_json::Value>,
    pub order_legs: Vec<TradeLegReplay>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub total_fee_quote: f64,
}

#[derive(Default)]
struct VenueAccumulator {
    submitted_orders: u64,
    filled_orders: u64,
    failed_orders: u64,
    latency_histogram: ApproxLatencyHistogram,
    total_fee_quote: f64,
}

const LATENCY_HISTOGRAM_MAX_MS: usize = 60_000;
const LATENCY_HISTOGRAM_BUCKETS: usize = LATENCY_HISTOGRAM_MAX_MS + 1;

#[derive(Default)]
struct ApproxLatencyHistogram {
    counts: Option<Box<[u32]>>,
    total_count: u64,
    max_latency_ms: Option<u64>,
}

impl ApproxLatencyHistogram {
    fn observe(&mut self, latency_ms: u64) {
        let bucket = latency_ms.min(LATENCY_HISTOGRAM_MAX_MS as u64) as usize;
        let counts = self
            .counts
            .get_or_insert_with(|| vec![0; LATENCY_HISTOGRAM_BUCKETS].into_boxed_slice());
        counts[bucket] = counts[bucket].saturating_add(1);
        self.total_count = self.total_count.saturating_add(1);
        self.max_latency_ms = Some(self.max_latency_ms.unwrap_or_default().max(latency_ms));
    }

    fn percentile(&self, percentile: f64) -> Option<u64> {
        let counts = self.counts.as_ref()?;
        if self.total_count == 0 {
            return None;
        }
        let target_rank = (percentile.clamp(0.0, 1.0) * self.total_count as f64)
            .ceil()
            .max(1.0) as u64;
        let mut cumulative = 0_u64;
        for (bucket, count) in counts.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count as u64);
            if cumulative >= target_rank {
                return Some(bucket as u64);
            }
        }
        self.max_latency_ms
    }

    fn max(&self) -> Option<u64> {
        self.max_latency_ms
    }
}

#[derive(Default)]
struct TradeReplayAccumulator {
    position_id: String,
    first_ts_ms: i64,
    last_ts_ms: i64,
    pair_id: Option<String>,
    symbol: Option<String>,
    selected_candidate: Option<serde_json::Value>,
    quantity_plan: Option<serde_json::Value>,
    entry_order_plan: Option<serde_json::Value>,
    entry_latency_summary: Option<serde_json::Value>,
    exit_latency_summary: Option<serde_json::Value>,
    opened_position: Option<serde_json::Value>,
    partial_close_summaries: Vec<serde_json::Value>,
    close_summary: Option<serde_json::Value>,
    order_legs: BTreeMap<String, TradeLegReplay>,
    warnings: Vec<String>,
    errors: Vec<String>,
    total_fee_quote: f64,
}

#[derive(Default)]
struct DailySymbolRevenueAccumulator {
    symbol: String,
    position_ids: BTreeSet<String>,
    partial_close_count: u64,
    closed_position_count: u64,
    realized_net_quote: f64,
}

#[derive(Default)]
struct DailyProfitAccumulator {
    date: String,
    realized_revenue_quote: f64,
    partial_realized_revenue_quote: f64,
    remaining_close_realized_revenue_quote: f64,
    opened_position_count: u64,
    partial_close_count: u64,
    closed_position_count: u64,
    latest_total_equity_quote: Option<f64>,
    latest_balance_observed_at_ms: Option<i64>,
    venue_equity_quote: BTreeMap<Venue, f64>,
    opened_symbol_revenues: BTreeMap<String, DailySymbolRevenueAccumulator>,
}

#[derive(Default)]
struct PositionDayInfo {
    symbol: Option<String>,
    opened_date: Option<String>,
}

#[derive(Default)]
struct DailyAnalysisState {
    current_balance_snapshot: Option<BalanceSnapshotReport>,
    daily_profit_accumulators: BTreeMap<String, DailyProfitAccumulator>,
    position_day_info: BTreeMap<String, PositionDayInfo>,
    partial_close_contributions: BTreeMap<(String, i64), PartialCloseContribution>,
    final_close_contributions: BTreeMap<String, FinalCloseContribution>,
}

#[derive(Clone)]
struct PartialCloseContribution {
    position_id: String,
    date: String,
    realized_net_quote: f64,
    opened_date: Option<String>,
    symbol: Option<String>,
}

#[derive(Clone)]
struct FinalCloseContribution {
    position_id: String,
    date: String,
    realized_net_quote: f64,
    opened_date: Option<String>,
    symbol: Option<String>,
}

#[derive(Default)]
struct JournalAnalysisBuilder {
    total_records: usize,
    runs: BTreeSet<String>,
    venue_accumulators: BTreeMap<Venue, VenueAccumulator>,
    recovery_counts: BTreeMap<String, u64>,
    fail_closed_reason_counts: BTreeMap<String, u64>,
    optimization_stats: JournalOptimizationStats,
    trade_replays: BTreeMap<String, TradeReplayAccumulator>,
    daily: DailyAnalysisState,
}

pub fn analyze_journal_records(records: &[JournalRecord]) -> JournalAnalysisReport {
    let mut builder = JournalAnalysisBuilder::default();
    for record in records {
        builder.ingest(record);
    }
    builder.finish()
}

pub fn analyze_journal_file(path: &Path) -> Result<JournalAnalysisReport> {
    analyze_journal_file_in_range(path, &JournalAnalysisTimeRange::default())
}

pub fn analyze_journal_file_in_range(
    path: &Path,
    range: &JournalAnalysisTimeRange,
) -> Result<JournalAnalysisReport> {
    let mut builder = JournalAnalysisBuilder::default();
    scan_path_records(path, |record| {
        if range.contains(record.ts_ms) {
            builder.ingest_owned(record);
        }
        Ok(())
    })?;
    Ok(builder.finish())
}

pub fn analyze_daily_journal_records(records: &[JournalRecord]) -> DailyJournalAnalysisReport {
    let mut state = DailyAnalysisState::default();
    for record in records {
        ingest_daily_record(&mut state, record);
    }
    finalize_daily_analysis(state)
}

pub fn analyze_daily_journal_file(path: &Path) -> Result<DailyJournalAnalysisReport> {
    let mut state = DailyAnalysisState::default();
    scan_path_records(path, |record| {
        ingest_daily_record(&mut state, &record);
        Ok(())
    })?;
    Ok(finalize_daily_analysis(state))
}

impl JournalAnalysisBuilder {
    fn ingest_owned(&mut self, record: JournalRecord) {
        self.ingest(&record);
    }

    fn ingest(&mut self, record: &JournalRecord) {
        self.total_records += 1;
        if !record.run_id.trim().is_empty() {
            self.runs.insert(record.run_id.clone());
        }
        if record.kind.starts_with("recovery.") {
            *self.recovery_counts.entry(record.kind.clone()).or_default() += 1;
        }

        match record.kind.as_str() {
            "execution.order_submitted" => {
                if let Some(venue) = payload_venue(record) {
                    self.venue_accumulators
                        .entry(venue)
                        .or_default()
                        .submitted_orders += 1;
                }
            }
            "execution.order_filled" => {
                if let Some(venue) = payload_venue(record) {
                    let stats = self.venue_accumulators.entry(venue).or_default();
                    stats.filled_orders += 1;
                    if let Some(latency_ms) = payload_u64(record, "local_roundtrip_ms") {
                        stats.latency_histogram.observe(latency_ms);
                    }
                    stats.total_fee_quote += payload_f64(record, "fee_quote").unwrap_or_default();
                }
            }
            "execution.order_failed" => {
                if let Some(venue) = payload_venue(record) {
                    self.venue_accumulators
                        .entry(venue)
                        .or_default()
                        .failed_orders += 1;
                }
                if let Some(error) = payload_string(record, "error") {
                    *self
                        .optimization_stats
                        .order_error_counts
                        .entry(classify_error_reason(error))
                        .or_default() += 1;
                }
            }
            "execution.guard_failed" => {
                let reason = payload_string(record, "reason").unwrap_or("guard_failed");
                *self
                    .fail_closed_reason_counts
                    .entry(reason.to_string())
                    .or_default() += 1;
            }
            "execution.compensation_failed" => {
                *self
                    .fail_closed_reason_counts
                    .entry("compensation_failed".to_string())
                    .or_default() += 1;
            }
            "recovery.blocked" => {
                *self
                    .fail_closed_reason_counts
                    .entry("recovery_blocked".to_string())
                    .or_default() += 1;
            }
            "recovery.live_blocked" => {
                *self
                    .fail_closed_reason_counts
                    .entry("live_recovery_blocked".to_string())
                    .or_default() += 1;
            }
            "scan.runtime_gate_blocked" => {
                if let Some(reason) = payload_string(record, "reason") {
                    *self
                        .optimization_stats
                        .runtime_gate_block_counts
                        .entry(reason.to_string())
                        .or_default() += 1;
                }
            }
            "execution.order_blocked" => {
                if let Some(reason) = payload_string(record, "reason") {
                    *self
                        .optimization_stats
                        .order_block_reason_counts
                        .entry(reason.to_string())
                        .or_default() += 1;
                }
            }
            "runtime.venue_cooldown_started" => {
                if let Some(venue) = payload_venue(record) {
                    *self
                        .optimization_stats
                        .venue_cooldown_counts
                        .entry(venue)
                        .or_default() += 1;
                }
            }
            "scan.no_entry_diagnostics" => {
                if let Some(reason) = payload_string(record, "reason") {
                    *self
                        .optimization_stats
                        .no_entry_reason_counts
                        .entry(reason.to_string())
                        .or_default() += 1;
                }
                extend_counter_map(
                    &mut self.optimization_stats.no_entry_blocked_reason_counts,
                    record.payload.get("blocked_reason_counts"),
                );
                extend_counter_map(
                    &mut self.optimization_stats.no_entry_advisory_counts,
                    record.payload.get("advisory_counts"),
                );
                if let Some(candidates) =
                    record.payload.get("candidates").and_then(|v| v.as_array())
                {
                    for candidate in candidates {
                        if let Some(selection_blocker) =
                            nested_string(candidate, "selection_blocker")
                        {
                            *self
                                .optimization_stats
                                .no_entry_selection_blocker_counts
                                .entry(selection_blocker.to_string())
                                .or_default() += 1;
                        }
                        if let Some(checklist) =
                            candidate.get("checklist").and_then(|v| v.as_array())
                        {
                            for item in checklist {
                                if item.get("ok").and_then(|v| v.as_bool()) == Some(false) {
                                    if let Some(key) = nested_string(item, "key") {
                                        *self
                                            .optimization_stats
                                            .no_entry_checklist_fail_counts
                                            .entry(key.to_string())
                                            .or_default() += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "execution.entry_order_plan" => {
                if let Some(first) = record.payload.get("first") {
                    if let (Some(venue), Some(leg)) = (
                        nested_string(first, "venue").and_then(|value| value.parse::<Venue>().ok()),
                        nested_string(first, "leg"),
                    ) {
                        *self
                            .optimization_stats
                            .first_leg_counts
                            .entry(format!("{venue}:{leg}"))
                            .or_default() += 1;
                    }
                    if let Some(factor) = dominant_first_leg_factor(first) {
                        *self
                            .optimization_stats
                            .first_leg_dominant_factor_counts
                            .entry(factor)
                            .or_default() += 1;
                    }
                }
            }
            _ => {}
        }

        ingest_daily_record(&mut self.daily, record);

        if let Some(position_id) = payload_string(record, "position_id") {
            let replay = self
                .trade_replays
                .entry(position_id.to_string())
                .or_insert_with(|| TradeReplayAccumulator {
                    position_id: position_id.to_string(),
                    first_ts_ms: record.ts_ms,
                    last_ts_ms: record.ts_ms,
                    ..TradeReplayAccumulator::default()
                });
            update_trade_replay(replay, record);
        }
    }

    fn finish(self) -> JournalAnalysisReport {
        let venue_stats = self
            .venue_accumulators
            .into_iter()
            .map(|(venue, accumulator)| {
                let submitted_orders = accumulator.submitted_orders;
                let failed_orders = accumulator.failed_orders;
                let failure_rate_pct = if submitted_orders == 0 {
                    0.0
                } else {
                    (failed_orders as f64 / submitted_orders as f64) * 100.0
                };

                (
                    venue,
                    VenueJournalStats {
                        submitted_orders,
                        filled_orders: accumulator.filled_orders,
                        failed_orders,
                        failure_rate_pct,
                        latency_ms_p50: accumulator.latency_histogram.percentile(0.50),
                        latency_ms_p95: accumulator.latency_histogram.percentile(0.95),
                        latency_ms_p99: accumulator.latency_histogram.percentile(0.99),
                        latency_ms_max: accumulator.latency_histogram.max(),
                        total_fee_quote: accumulator.total_fee_quote,
                    },
                )
            })
            .collect();

        let mut trade_replays = self
            .trade_replays
            .into_values()
            .map(finalize_trade_replay)
            .collect::<Vec<_>>();
        trade_replays.sort_by_key(|replay| (replay.first_ts_ms, replay.position_id.clone()));
        let recommendations =
            build_recommendations(&venue_stats, &self.optimization_stats, &trade_replays);
        let daily = finalize_daily_analysis(self.daily);

        JournalAnalysisReport {
            total_records: self.total_records,
            run_count: self.runs.len(),
            venue_stats,
            recovery_counts: self.recovery_counts,
            fail_closed_reason_counts: self.fail_closed_reason_counts,
            optimization_stats: self.optimization_stats,
            recommendations,
            current_balance_snapshot: daily.current_balance_snapshot,
            daily_profit_summaries: daily.daily_profit_summaries,
            trade_replays,
        }
    }
}

fn parse_balance_snapshot_report(record: &JournalRecord) -> Option<BalanceSnapshotReport> {
    let observed_at_ms = record.ts_ms;
    let total_equity_quote = payload_f64(record, "total_equity_quote");
    let venues = record
        .payload
        .get("venues")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    serde_json::from_value::<AccountBalanceSnapshot>(item.clone()).ok()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let failed_venues = record
        .payload
        .get("failed_venues")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    Some(BalanceSnapshotFailure {
                        venue: item.get("venue")?.as_str()?.parse().ok()?,
                        error: item.get("error")?.as_str()?.to_string(),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(BalanceSnapshotReport {
        observed_at_ms,
        total_equity_quote,
        venues,
        failed_venues,
    })
}

fn local_date_key(ts_ms: i64) -> String {
    Local
        .timestamp_millis_opt(ts_ms)
        .single()
        .map(|value| value.date_naive().to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

fn payload_venue(record: &JournalRecord) -> Option<Venue> {
    payload_string(record, "venue").and_then(|value| value.parse().ok())
}

fn payload_string<'a>(record: &'a JournalRecord, key: &str) -> Option<&'a str> {
    record.payload.get(key).and_then(|value| value.as_str())
}

fn payload_u64(record: &JournalRecord, key: &str) -> Option<u64> {
    record.payload.get(key).and_then(|value| value.as_u64())
}

fn payload_i64(record: &JournalRecord, key: &str) -> Option<i64> {
    record.payload.get(key).and_then(|value| value.as_i64())
}

fn payload_f64(record: &JournalRecord, key: &str) -> Option<f64> {
    record.payload.get(key).and_then(|value| value.as_f64())
}

fn nested_string<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(|item| item.as_str())
}

fn nested_f64(value: &serde_json::Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|item| item.as_f64())
}

fn extend_counter_map(target: &mut BTreeMap<String, u64>, value: Option<&serde_json::Value>) {
    let Some(object) = value.and_then(|item| item.as_object()) else {
        return;
    };
    for (key, count) in object {
        let Some(count) = count.as_u64() else {
            continue;
        };
        *target.entry(key.clone()).or_default() += count;
    }
}

fn payload_date_key(record: &JournalRecord) -> String {
    local_date_key(payload_i64(record, "closed_at_ms").unwrap_or(record.ts_ms))
}

fn partial_reconciliation_key(record: &JournalRecord) -> Option<(String, i64)> {
    Some((
        payload_string(record, "position_id")?.to_string(),
        payload_i64(record, "closed_at_ms").unwrap_or(record.ts_ms),
    ))
}

fn ingest_daily_record(state: &mut DailyAnalysisState, record: &JournalRecord) {
    if record.kind == "balance.snapshot" {
        if let Some(snapshot) = parse_balance_snapshot_report(record) {
            if state
                .current_balance_snapshot
                .as_ref()
                .map(|current| current.observed_at_ms <= snapshot.observed_at_ms)
                .unwrap_or(true)
            {
                state.current_balance_snapshot = Some(snapshot.clone());
            }
            let date = local_date_key(record.ts_ms);
            let daily = state
                .daily_profit_accumulators
                .entry(date.clone())
                .or_insert_with(|| DailyProfitAccumulator {
                    date,
                    ..DailyProfitAccumulator::default()
                });
            if daily
                .latest_balance_observed_at_ms
                .map(|observed| observed <= snapshot.observed_at_ms)
                .unwrap_or(true)
            {
                daily.latest_total_equity_quote = snapshot.total_equity_quote;
                daily.latest_balance_observed_at_ms = Some(snapshot.observed_at_ms);
                daily.venue_equity_quote = snapshot
                    .venues
                    .iter()
                    .map(|item| (item.venue, item.equity_quote))
                    .collect();
            }
        }
    }

    let date = local_date_key(record.ts_ms);
    match record.kind.as_str() {
        "entry.opened" => {
            let Some(position_id) = payload_string(record, "position_id") else {
                return;
            };
            let symbol = payload_string(record, "symbol").map(ToOwned::to_owned);
            let info = state
                .position_day_info
                .entry(position_id.to_string())
                .or_default();
            if info.symbol.is_none() {
                info.symbol = symbol.clone();
            }
            if info.opened_date.is_none() {
                info.opened_date = Some(date.clone());
            }
            let daily = state
                .daily_profit_accumulators
                .entry(date.clone())
                .or_insert_with(|| DailyProfitAccumulator {
                    date: date.clone(),
                    ..DailyProfitAccumulator::default()
                });
            daily.opened_position_count += 1;
            if let Some(symbol) = symbol {
                let symbol_daily = daily
                    .opened_symbol_revenues
                    .entry(symbol.clone())
                    .or_insert_with(|| DailySymbolRevenueAccumulator {
                        symbol,
                        ..DailySymbolRevenueAccumulator::default()
                    });
                symbol_daily.position_ids.insert(position_id.to_string());
            }
        }
        "exit.partial_closed" | "exit.partial_reconciled" => {
            let Some(position_id) = payload_string(record, "position_id") else {
                return;
            };
            let Some(key) = partial_reconciliation_key(record) else {
                return;
            };
            if let Some(previous) = state.partial_close_contributions.remove(&key) {
                apply_partial_close_contribution(
                    &mut state.daily_profit_accumulators,
                    &previous,
                    false,
                );
            }
            let contribution = build_partial_close_contribution(
                &state.position_day_info,
                if record.kind == "exit.partial_reconciled" {
                    payload_date_key(record)
                } else {
                    date
                },
                position_id,
                record,
            );
            apply_partial_close_contribution(
                &mut state.daily_profit_accumulators,
                &contribution,
                true,
            );
            state.partial_close_contributions.insert(key, contribution);
        }
        "exit.closed" | "exit.reconciled" => {
            let Some(position_id) = payload_string(record, "position_id") else {
                return;
            };
            if let Some(previous) = state.final_close_contributions.remove(position_id) {
                apply_final_close_contribution(
                    &mut state.daily_profit_accumulators,
                    &previous,
                    false,
                );
            }
            let contribution = build_final_close_contribution(
                &state.position_day_info,
                if record.kind == "exit.reconciled" {
                    payload_date_key(record)
                } else {
                    date
                },
                position_id,
                record,
            );
            apply_final_close_contribution(
                &mut state.daily_profit_accumulators,
                &contribution,
                true,
            );
            state
                .final_close_contributions
                .insert(position_id.to_string(), contribution);
        }
        _ => {}
    }
}

fn build_partial_close_contribution(
    position_day_info: &BTreeMap<String, PositionDayInfo>,
    date: String,
    position_id: &str,
    record: &JournalRecord,
) -> PartialCloseContribution {
    let realized_net_quote = record
        .payload
        .get("outcome_diagnostics")
        .and_then(|value| value.get("net_quote"))
        .and_then(|value| value.as_f64())
        .unwrap_or_default();
    let info = position_day_info.get(position_id);
    PartialCloseContribution {
        position_id: position_id.to_string(),
        date,
        realized_net_quote,
        opened_date: info.and_then(|item| item.opened_date.clone()),
        symbol: info.and_then(|item| item.symbol.clone()),
    }
}

fn build_final_close_contribution(
    position_day_info: &BTreeMap<String, PositionDayInfo>,
    date: String,
    position_id: &str,
    record: &JournalRecord,
) -> FinalCloseContribution {
    let realized_net_quote = record
        .payload
        .get("remaining_outcome_diagnostics")
        .and_then(|value| value.get("net_quote"))
        .and_then(|value| value.as_f64())
        .or_else(|| payload_f64(record, "net_quote"))
        .unwrap_or_default();
    let info = position_day_info.get(position_id);
    FinalCloseContribution {
        position_id: position_id.to_string(),
        date,
        realized_net_quote,
        opened_date: info.and_then(|item| item.opened_date.clone()),
        symbol: info.and_then(|item| item.symbol.clone()),
    }
}

fn apply_partial_close_contribution(
    daily_profit_accumulators: &mut BTreeMap<String, DailyProfitAccumulator>,
    contribution: &PartialCloseContribution,
    add: bool,
) {
    let sign = if add { 1.0 } else { -1.0 };
    let count_delta = if add { 1_i64 } else { -1_i64 };
    let daily = daily_profit_accumulators
        .entry(contribution.date.clone())
        .or_insert_with(|| DailyProfitAccumulator {
            date: contribution.date.clone(),
            ..DailyProfitAccumulator::default()
        });
    daily.realized_revenue_quote += contribution.realized_net_quote * sign;
    daily.partial_realized_revenue_quote += contribution.realized_net_quote * sign;
    daily.partial_close_count = saturating_apply_count(daily.partial_close_count, count_delta);

    if let (Some(opened_date), Some(symbol)) = (
        contribution.opened_date.as_ref(),
        contribution.symbol.as_ref(),
    ) {
        let opened_daily = daily_profit_accumulators
            .entry(opened_date.clone())
            .or_insert_with(|| DailyProfitAccumulator {
                date: opened_date.clone(),
                ..DailyProfitAccumulator::default()
            });
        let symbol_daily = opened_daily
            .opened_symbol_revenues
            .entry(symbol.clone())
            .or_insert_with(|| DailySymbolRevenueAccumulator {
                symbol: symbol.clone(),
                ..DailySymbolRevenueAccumulator::default()
            });
        symbol_daily
            .position_ids
            .insert(contribution.position_id.clone());
        symbol_daily.partial_close_count =
            saturating_apply_count(symbol_daily.partial_close_count, count_delta);
        symbol_daily.realized_net_quote += contribution.realized_net_quote * sign;
    }
}

fn apply_final_close_contribution(
    daily_profit_accumulators: &mut BTreeMap<String, DailyProfitAccumulator>,
    contribution: &FinalCloseContribution,
    add: bool,
) {
    let sign = if add { 1.0 } else { -1.0 };
    let count_delta = if add { 1_i64 } else { -1_i64 };
    let daily = daily_profit_accumulators
        .entry(contribution.date.clone())
        .or_insert_with(|| DailyProfitAccumulator {
            date: contribution.date.clone(),
            ..DailyProfitAccumulator::default()
        });
    daily.realized_revenue_quote += contribution.realized_net_quote * sign;
    daily.remaining_close_realized_revenue_quote += contribution.realized_net_quote * sign;
    daily.closed_position_count = saturating_apply_count(daily.closed_position_count, count_delta);

    if let (Some(opened_date), Some(symbol)) = (
        contribution.opened_date.as_ref(),
        contribution.symbol.as_ref(),
    ) {
        let opened_daily = daily_profit_accumulators
            .entry(opened_date.clone())
            .or_insert_with(|| DailyProfitAccumulator {
                date: opened_date.clone(),
                ..DailyProfitAccumulator::default()
            });
        let symbol_daily = opened_daily
            .opened_symbol_revenues
            .entry(symbol.clone())
            .or_insert_with(|| DailySymbolRevenueAccumulator {
                symbol: symbol.clone(),
                ..DailySymbolRevenueAccumulator::default()
            });
        symbol_daily
            .position_ids
            .insert(contribution.position_id.clone());
        symbol_daily.closed_position_count =
            saturating_apply_count(symbol_daily.closed_position_count, count_delta);
        symbol_daily.realized_net_quote += contribution.realized_net_quote * sign;
    }
}

fn saturating_apply_count(current: u64, delta: i64) -> u64 {
    if delta >= 0 {
        current.saturating_add(delta as u64)
    } else {
        current.saturating_sub((-delta) as u64)
    }
}

fn finalize_daily_analysis(state: DailyAnalysisState) -> DailyJournalAnalysisReport {
    let mut daily_profit_summaries = state
        .daily_profit_accumulators
        .into_values()
        .map(finalize_daily_profit_summary)
        .collect::<Vec<_>>();
    daily_profit_summaries.sort_by(|left, right| left.date.cmp(&right.date));

    DailyJournalAnalysisReport {
        current_balance_snapshot: state.current_balance_snapshot,
        daily_profit_summaries,
    }
}

fn update_trade_replay(replay: &mut TradeReplayAccumulator, record: &JournalRecord) {
    replay.first_ts_ms = replay.first_ts_ms.min(record.ts_ms);
    replay.last_ts_ms = replay.last_ts_ms.max(record.ts_ms);
    if replay.pair_id.is_none() {
        replay.pair_id = payload_string(record, "pair_id").map(ToOwned::to_owned);
    }
    if replay.symbol.is_none() {
        replay.symbol = payload_string(record, "symbol").map(ToOwned::to_owned);
    }

    match record.kind.as_str() {
        "execution.entry_selected" => {
            replay.selected_candidate = record.payload.get("candidate").cloned();
        }
        "execution.quantity_normalized" => {
            replay.quantity_plan = Some(record.payload.clone());
        }
        "execution.entry_order_plan" => {
            replay.entry_order_plan = Some(record.payload.clone());
        }
        "execution.entry_latency_summary" => {
            replay.entry_latency_summary = Some(record.payload.clone());
        }
        "execution.exit_latency_summary" => {
            replay.exit_latency_summary = Some(record.payload.clone());
        }
        "entry.opened" => {
            replay.opened_position = Some(record.payload.clone());
        }
        "exit.partial_closed" | "exit.partial_reconciled" => {
            let closed_at_ms = payload_i64(record, "closed_at_ms");
            if let Some(closed_at_ms) = closed_at_ms {
                if let Some(existing) = replay.partial_close_summaries.iter_mut().find(|item| {
                    item.get("closed_at_ms").and_then(|value| value.as_i64()) == Some(closed_at_ms)
                }) {
                    *existing = record.payload.clone();
                } else {
                    replay.partial_close_summaries.push(record.payload.clone());
                }
            } else {
                replay.partial_close_summaries.push(record.payload.clone());
            }
        }
        "exit.closed" | "exit.reconciled" => {
            replay.close_summary = Some(record.payload.clone());
        }
        "entry.compensated" | "execution.compensation_failed" => {
            if let Some(error) = payload_string(record, "error") {
                replay.errors.push(error.to_string());
            } else {
                replay.errors.push(record.kind.clone());
            }
        }
        "entry.long_failed" | "entry.short_failed" => {
            if let Some(error) = payload_string(record, "error") {
                replay.errors.push(error.to_string());
            }
        }
        "execution.uncertain_leg_detected" => {
            if let Some(stage) = payload_string(record, "stage") {
                replay
                    .warnings
                    .push(format!("uncertain_leg_detected:{stage}"));
            } else {
                replay.warnings.push("uncertain_leg_detected".to_string());
            }
        }
        "execution.exit_triggered" => {
            if let Some(reason) = payload_string(record, "reason") {
                replay.warnings.push(format!("exit_triggered:{reason}"));
            }
        }
        "execution.partial_exit_triggered" => {
            if let Some(reason) = payload_string(record, "reason") {
                replay
                    .warnings
                    .push(format!("partial_exit_triggered:{reason}"));
            }
        }
        "execution.order_submitted"
        | "execution.order_filled"
        | "execution.order_failed"
        | "execution.order_blocked" => {
            let stage = payload_string(record, "stage")
                .unwrap_or("unknown")
                .to_string();
            let leg = replay
                .order_legs
                .entry(stage.clone())
                .or_insert_with(|| TradeLegReplay {
                    stage,
                    ..TradeLegReplay::default()
                });
            if leg.venue.is_none() {
                leg.venue = payload_venue(record);
            }
            if leg.side.is_none() {
                leg.side = payload_string(record, "side").map(ToOwned::to_owned);
            }
            if leg.reduce_only.is_none() {
                leg.reduce_only = record
                    .payload
                    .get("reduce_only")
                    .and_then(|item| item.as_bool());
            }
            if leg.requested_quantity.is_none() {
                leg.requested_quantity = payload_f64(record, "requested_quantity");
            }

            match record.kind.as_str() {
                "execution.order_submitted" => {
                    if leg.outcome.is_empty() {
                        leg.outcome = "submitted".to_string();
                    }
                }
                "execution.order_filled" => {
                    leg.outcome = "filled".to_string();
                    leg.executed_quantity = payload_f64(record, "executed_quantity");
                    leg.average_price = payload_f64(record, "average_price");
                    leg.fee_quote = payload_f64(record, "fee_quote");
                    leg.local_roundtrip_ms = payload_u64(record, "local_roundtrip_ms");
                    leg.adapter_timing = record.payload.get("adapter_timing").cloned();
                    replay.total_fee_quote += leg.fee_quote.unwrap_or_default();
                }
                "execution.order_failed" => {
                    leg.outcome = "failed".to_string();
                    leg.local_roundtrip_ms = payload_u64(record, "local_roundtrip_ms");
                    leg.error = payload_string(record, "error").map(ToOwned::to_owned);
                }
                "execution.order_blocked" => {
                    leg.outcome = "blocked".to_string();
                    leg.error = payload_string(record, "reason").map(ToOwned::to_owned);
                }
                _ => {}
            }
        }
        _ => {}
    }
}

fn finalize_trade_replay(mut accumulator: TradeReplayAccumulator) -> TradeReplay {
    accumulator.warnings.sort();
    accumulator.warnings.dedup();
    accumulator.errors.sort();
    accumulator.errors.dedup();

    let state = if accumulator.close_summary.is_some() {
        "closed"
    } else if accumulator.opened_position.is_some() {
        "open"
    } else if !accumulator.errors.is_empty() {
        "failed"
    } else {
        "attempted"
    };

    TradeReplay {
        position_id: accumulator.position_id,
        first_ts_ms: accumulator.first_ts_ms,
        last_ts_ms: accumulator.last_ts_ms,
        pair_id: accumulator.pair_id,
        symbol: accumulator.symbol,
        state: state.to_string(),
        selected_candidate: accumulator.selected_candidate,
        quantity_plan: accumulator.quantity_plan,
        entry_order_plan: accumulator.entry_order_plan,
        entry_latency_summary: accumulator.entry_latency_summary,
        exit_latency_summary: accumulator.exit_latency_summary,
        opened_position: accumulator.opened_position,
        partial_close_summaries: accumulator.partial_close_summaries,
        close_summary: accumulator.close_summary,
        order_legs: accumulator.order_legs.into_values().collect(),
        warnings: accumulator.warnings,
        errors: accumulator.errors,
        total_fee_quote: accumulator.total_fee_quote,
    }
}

fn finalize_daily_profit_summary(accumulator: DailyProfitAccumulator) -> DailyProfitSummary {
    let mut opened_symbol_revenues = accumulator
        .opened_symbol_revenues
        .into_values()
        .map(|item| DailySymbolRevenue {
            symbol: item.symbol,
            position_count: item.position_ids.len() as u64,
            partial_close_count: item.partial_close_count,
            closed_position_count: item.closed_position_count,
            realized_net_quote: item.realized_net_quote,
        })
        .collect::<Vec<_>>();
    opened_symbol_revenues.sort_by(|left, right| left.symbol.cmp(&right.symbol));

    DailyProfitSummary {
        date: accumulator.date,
        realized_revenue_quote: accumulator.realized_revenue_quote,
        partial_realized_revenue_quote: accumulator.partial_realized_revenue_quote,
        remaining_close_realized_revenue_quote: accumulator.remaining_close_realized_revenue_quote,
        opened_position_count: accumulator.opened_position_count,
        partial_close_count: accumulator.partial_close_count,
        closed_position_count: accumulator.closed_position_count,
        latest_total_equity_quote: accumulator.latest_total_equity_quote,
        latest_balance_observed_at_ms: accumulator.latest_balance_observed_at_ms,
        venue_equity_quote: accumulator.venue_equity_quote,
        opened_symbol_revenues,
    }
}

fn classify_error_reason(message: &str) -> String {
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("quote_expired") {
        "quote_expired".to_string()
    } else if normalized.contains("uncertain") || normalized.contains("pending") {
        "uncertain_or_pending".to_string()
    } else if normalized.contains("timeout") {
        "timeout".to_string()
    } else if normalized.contains("reduce_only") {
        "reduce_only".to_string()
    } else if normalized.contains("stale") {
        "stale_data".to_string()
    } else {
        normalized
    }
}

fn dominant_first_leg_factor(value: &serde_json::Value) -> Option<String> {
    let health = nested_f64(value, "health_score").unwrap_or_default();
    let submit_ack = nested_f64(value, "submit_ack_score").unwrap_or_default();
    let depth = nested_f64(value, "depth_score").unwrap_or_default();

    let (factor, score) = [
        ("health", health),
        ("submit_ack", submit_ack),
        ("depth", depth),
    ]
    .into_iter()
    .max_by(|left, right| left.1.total_cmp(&right.1))?;
    if score <= 0.0 {
        return None;
    }
    Some(factor.to_string())
}

fn build_recommendations(
    venue_stats: &BTreeMap<Venue, VenueJournalStats>,
    optimization_stats: &JournalOptimizationStats,
    trade_replays: &[TradeReplay],
) -> Vec<OptimizationRecommendation> {
    let mut recommendations = Vec::new();

    for (venue, stats) in venue_stats {
        if stats.failed_orders > 0 && stats.failure_rate_pct >= 20.0 {
            recommendations.push(OptimizationRecommendation {
                priority: 1,
                category: "venue_stability".to_string(),
                title: format!("{venue} needs execution-path review"),
                summary: format!(
                    "{venue} shows elevated failure rate in the sampled journal. Treat it as a higher-risk first leg until the adapter, auth flow, or venue path is stabilized."
                ),
                evidence: vec![
                    format!("failure_rate_pct={:.2}", stats.failure_rate_pct),
                    format!("failed_orders={}", stats.failed_orders),
                    format!("submitted_orders={}", stats.submitted_orders),
                ],
            });
        }
        if let Some(p95) = stats.latency_ms_p95.filter(|value| *value >= 1_000) {
            recommendations.push(OptimizationRecommendation {
                priority: 1,
                category: "venue_latency".to_string(),
                title: format!("{venue} has high tail latency"),
                summary: format!(
                    "{venue} is showing >=1s p95 local round-trip latency. Tighten venue-specific entry thresholds or avoid using it as the first leg unless the edge is wider."
                ),
                evidence: vec![
                    format!("latency_ms_p95={p95}"),
                    format!("latency_ms_max={:?}", stats.latency_ms_max),
                ],
            });
        }
    }

    for (venue, count) in &optimization_stats.venue_cooldown_counts {
        if *count > 0 {
            recommendations.push(OptimizationRecommendation {
                priority: 1,
                category: "uncertain_orders".to_string(),
                title: format!("{venue} triggered uncertain-order cooldowns"),
                summary: format!(
                    "{venue} produced uncertain or pending order outcomes often enough to start cooldowns. Review private-stream correlation and keep it off the most aggressive first-leg paths until that is cleaner."
                ),
                evidence: vec![format!("cooldown_count={count}")],
            });
        }
    }

    if let Some(count) = optimization_stats
        .first_leg_dominant_factor_counts
        .get("depth")
        .copied()
        .filter(|count| *count > 0)
    {
        recommendations.push(OptimizationRecommendation {
            priority: 2,
            category: "depth_capacity".to_string(),
            title: "Top-of-book depth is frequently driving first-leg choice".to_string(),
            summary: "Depth pressure is repeatedly dominating the entry plan. Lower notional, lower `max_top_book_usage_ratio`, or restrict thinner symbols so the first leg is not chosen mainly because one side is harder to fill cleanly.".to_string(),
            evidence: vec![format!("depth_dominant_first_leg_count={count}")],
        });
    }

    if let Some(count) = optimization_stats
        .order_error_counts
        .get("timeout")
        .copied()
    {
        if count > 0 {
            recommendations.push(OptimizationRecommendation {
                priority: 2,
                category: "timeouts".to_string(),
                title: "Timeout errors need venue-level investigation".to_string(),
                summary: "Timeout-like failures are showing up in the order path. Check venue connectivity, server-time sync, and whether the venue should temporarily be demoted from first-leg priority.".to_string(),
                evidence: vec![format!("timeout_error_count={count}")],
            });
        }
    }

    if let Some(count) = optimization_stats
        .runtime_gate_block_counts
        .iter()
        .find(|(reason, _)| reason.starts_with("venue_entry_cooldown:"))
        .map(|(_, count)| *count)
    {
        recommendations.push(OptimizationRecommendation {
            priority: 2,
            category: "entry_gating".to_string(),
            title: "Runtime cooldown gates are actively suppressing entries".to_string(),
            summary: "Cooldown-based entry blocking is happening in live scans. That is good for safety, but it also means execution quality is poor enough to reduce opportunity capture; fix the underlying venue path before widening filters.".to_string(),
            evidence: vec![format!("cooldown_gate_blocks={count}")],
        });
    }

    if let Some((reason, count)) = optimization_stats
        .no_entry_reason_counts
        .iter()
        .max_by(|left, right| left.1.cmp(right.1).then_with(|| left.0.cmp(right.0)))
        .filter(|(_, count)| **count > 0)
    {
        let mut evidence = vec![format!("no_entry_reason_count[{reason}]={count}")];
        if let Some((blocked_reason, blocked_count)) = optimization_stats
            .no_entry_blocked_reason_counts
            .iter()
            .max_by(|left, right| left.1.cmp(right.1).then_with(|| left.0.cmp(right.0)))
        {
            evidence.push(format!(
                "top_blocked_reason[{blocked_reason}]={blocked_count}"
            ));
        }
        if let Some((check_key, check_count)) = optimization_stats
            .no_entry_checklist_fail_counts
            .iter()
            .max_by(|left, right| left.1.cmp(right.1).then_with(|| left.0.cmp(right.0)))
        {
            evidence.push(format!("top_checklist_fail[{check_key}]={check_count}"));
        }
        recommendations.push(OptimizationRecommendation {
            priority: 2,
            category: "opportunity_capture".to_string(),
            title: "No-entry diagnostics are showing the dominant opportunity blocker".to_string(),
            summary: format!(
                "The journal now shows a repeated no-entry pattern led by `{reason}`. Use the blocked-reason and checklist-fail breakdown to decide whether to tune thresholds, widen symbol coverage, or accept that the market window simply did not offer a safe entry."
            ),
            evidence,
        });
    }

    if let Some((check_key, count)) = optimization_stats
        .no_entry_checklist_fail_counts
        .iter()
        .max_by(|left, right| left.1.cmp(right.1).then_with(|| left.0.cmp(right.0)))
        .filter(|(_, count)| **count > 0)
    {
        recommendations.push(OptimizationRecommendation {
            priority: 3,
            category: "opportunity_checklist".to_string(),
            title: format!("`{check_key}` is the most common failed opportunity check"),
            summary: "Review the no-entry checklist section in the offline report before changing filters broadly. This pinpoints whether the real bottleneck is freshness, edge thresholds, stagger-gap policy, slot pressure, or symbol conflicts.".to_string(),
            evidence: vec![format!("failed_check_count={count}")],
        });
    }

    let failed_replays = trade_replays
        .iter()
        .filter(|replay| replay.state == "failed")
        .count();
    if failed_replays > 0 {
        recommendations.push(OptimizationRecommendation {
            priority: 2,
            category: "trade_replay".to_string(),
            title: "There are failed trade attempts worth replaying first".to_string(),
            summary: "Use the per-position replay section to inspect the failed attempts before changing thresholds broadly. The replay contains selected candidate, entry plan, leg outcomes, warnings, and errors.".to_string(),
            evidence: vec![format!("failed_trade_replays={failed_replays}")],
        });
    }

    recommendations.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.category.cmp(&right.category))
            .then_with(|| left.title.cmp(&right.title))
    });
    recommendations
}
