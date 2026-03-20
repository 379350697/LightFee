use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::{journal::JournalRecord, models::Venue};

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
    pub trade_replays: Vec<TradeReplay>,
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
    latencies_ms: Vec<u64>,
    total_fee_quote: f64,
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
    close_summary: Option<serde_json::Value>,
    order_legs: BTreeMap<String, TradeLegReplay>,
    warnings: Vec<String>,
    errors: Vec<String>,
    total_fee_quote: f64,
}

pub fn analyze_journal_records(records: &[JournalRecord]) -> JournalAnalysisReport {
    let mut runs = BTreeSet::new();
    let mut venue_accumulators = BTreeMap::<Venue, VenueAccumulator>::new();
    let mut recovery_counts = BTreeMap::<String, u64>::new();
    let mut fail_closed_reason_counts = BTreeMap::<String, u64>::new();
    let mut optimization_stats = JournalOptimizationStats::default();
    let mut trade_replays = BTreeMap::<String, TradeReplayAccumulator>::new();

    for record in records {
        if !record.run_id.trim().is_empty() {
            runs.insert(record.run_id.clone());
        }
        if record.kind.starts_with("recovery.") {
            *recovery_counts.entry(record.kind.clone()).or_default() += 1;
        }

        match record.kind.as_str() {
            "execution.order_submitted" => {
                if let Some(venue) = payload_venue(record) {
                    venue_accumulators
                        .entry(venue)
                        .or_default()
                        .submitted_orders += 1;
                }
            }
            "execution.order_filled" => {
                if let Some(venue) = payload_venue(record) {
                    let stats = venue_accumulators.entry(venue).or_default();
                    stats.filled_orders += 1;
                    if let Some(latency_ms) = payload_u64(record, "local_roundtrip_ms") {
                        stats.latencies_ms.push(latency_ms);
                    }
                    stats.total_fee_quote += payload_f64(record, "fee_quote").unwrap_or_default();
                }
            }
            "execution.order_failed" => {
                if let Some(venue) = payload_venue(record) {
                    venue_accumulators.entry(venue).or_default().failed_orders += 1;
                }
                if let Some(error) = payload_string(record, "error") {
                    *optimization_stats
                        .order_error_counts
                        .entry(classify_error_reason(error))
                        .or_default() += 1;
                }
            }
            "execution.guard_failed" => {
                let reason = payload_string(record, "reason").unwrap_or("guard_failed");
                *fail_closed_reason_counts
                    .entry(reason.to_string())
                    .or_default() += 1;
            }
            "execution.compensation_failed" => {
                *fail_closed_reason_counts
                    .entry("compensation_failed".to_string())
                    .or_default() += 1;
            }
            "recovery.blocked" => {
                *fail_closed_reason_counts
                    .entry("recovery_blocked".to_string())
                    .or_default() += 1;
            }
            "recovery.live_blocked" => {
                *fail_closed_reason_counts
                    .entry("live_recovery_blocked".to_string())
                    .or_default() += 1;
            }
            "scan.runtime_gate_blocked" => {
                if let Some(reason) = payload_string(record, "reason") {
                    *optimization_stats
                        .runtime_gate_block_counts
                        .entry(reason.to_string())
                        .or_default() += 1;
                }
            }
            "execution.order_blocked" => {
                if let Some(reason) = payload_string(record, "reason") {
                    *optimization_stats
                        .order_block_reason_counts
                        .entry(reason.to_string())
                        .or_default() += 1;
                }
            }
            "runtime.venue_cooldown_started" => {
                if let Some(venue) = payload_venue(record) {
                    *optimization_stats
                        .venue_cooldown_counts
                        .entry(venue)
                        .or_default() += 1;
                }
            }
            "scan.no_entry_diagnostics" => {
                if let Some(reason) = payload_string(record, "reason") {
                    *optimization_stats
                        .no_entry_reason_counts
                        .entry(reason.to_string())
                        .or_default() += 1;
                }
                extend_counter_map(
                    &mut optimization_stats.no_entry_blocked_reason_counts,
                    record.payload.get("blocked_reason_counts"),
                );
                extend_counter_map(
                    &mut optimization_stats.no_entry_advisory_counts,
                    record.payload.get("advisory_counts"),
                );
                if let Some(candidates) =
                    record.payload.get("candidates").and_then(|v| v.as_array())
                {
                    for candidate in candidates {
                        if let Some(selection_blocker) =
                            nested_string(candidate, "selection_blocker")
                        {
                            *optimization_stats
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
                                        *optimization_stats
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
                        *optimization_stats
                            .first_leg_counts
                            .entry(format!("{venue}:{leg}"))
                            .or_default() += 1;
                    }
                    if let Some(factor) = dominant_first_leg_factor(first) {
                        *optimization_stats
                            .first_leg_dominant_factor_counts
                            .entry(factor)
                            .or_default() += 1;
                    }
                }
            }
            _ => {}
        }

        if let Some(position_id) = payload_string(record, "position_id") {
            let replay = trade_replays
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

    let venue_stats = venue_accumulators
        .into_iter()
        .map(|(venue, accumulator)| {
            let mut latencies_ms = accumulator.latencies_ms;
            latencies_ms.sort_unstable();
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
                    latency_ms_p50: percentile_nearest_rank(&latencies_ms, 0.50),
                    latency_ms_p95: percentile_nearest_rank(&latencies_ms, 0.95),
                    latency_ms_p99: percentile_nearest_rank(&latencies_ms, 0.99),
                    latency_ms_max: latencies_ms.last().copied(),
                    total_fee_quote: accumulator.total_fee_quote,
                },
            )
        })
        .collect();

    let mut trade_replays = trade_replays
        .into_values()
        .map(finalize_trade_replay)
        .collect::<Vec<_>>();
    trade_replays.sort_by_key(|replay| (replay.first_ts_ms, replay.position_id.clone()));
    let recommendations = build_recommendations(&venue_stats, &optimization_stats, &trade_replays);

    JournalAnalysisReport {
        total_records: records.len(),
        run_count: runs.len(),
        venue_stats,
        recovery_counts,
        fail_closed_reason_counts,
        optimization_stats,
        recommendations,
        trade_replays,
    }
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
        "exit.closed" => {
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
        close_summary: accumulator.close_summary,
        order_legs: accumulator.order_legs.into_values().collect(),
        warnings: accumulator.warnings,
        errors: accumulator.errors,
        total_fee_quote: accumulator.total_fee_quote,
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

fn percentile_nearest_rank(values: &[u64], percentile: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let rank = (percentile.clamp(0.0, 1.0) * values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    values.get(index).copied()
}
