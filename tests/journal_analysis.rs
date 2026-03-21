use lightfee::{
    analyze_journal_records, JournalRecord, JournalRuntimeMetricsSnapshot, JsonlJournal, Venue,
};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn journal_analysis_reports_latency_failure_and_recovery_breakdown() {
    let records = vec![
        record(
            0,
            "run-a",
            999,
            "execution.entry_selected",
            json!({
                "position_id": "pos-1",
                "pair_id": "btcusdt:binance->okx",
                "symbol": "BTCUSDT",
                "candidate": {
                    "symbol": "BTCUSDT",
                    "long_venue": "binance",
                    "short_venue": "okx",
                    "expected_edge_bps": 3.2
                }
            }),
        ),
        record(
            1,
            "run-a",
            1_000,
            "execution.order_submitted",
            json!({
                "position_id": "pos-1",
                "pair_id": "btcusdt:binance->okx",
                "stage": "entry_long",
                "venue": "binance",
                "side": "buy",
                "requested_quantity": 0.1,
            }),
        ),
        record(
            2,
            "run-a",
            1_001,
            "execution.order_filled",
            json!({
                "position_id": "pos-1",
                "pair_id": "btcusdt:binance->okx",
                "stage": "entry_long",
                "venue": "binance",
                "side": "buy",
                "requested_quantity": 0.1,
                "executed_quantity": 0.1,
                "average_price": 10000.0,
                "local_roundtrip_ms": 100_u64,
                "fee_quote": 0.001,
            }),
        ),
        record(
            3,
            "run-a",
            1_002,
            "execution.entry_order_plan",
            json!({
                "position_id": "pos-1",
                "pair_id": "btcusdt:binance->okx",
                "symbol": "BTCUSDT",
                "first": {
                    "venue": "binance",
                    "leg": "long",
                    "total_score": 1.4,
                    "health_score": 0.2,
                    "submit_ack_score": 0.1,
                    "depth_score": 1.1
                },
                "second": {
                    "venue": "okx",
                    "leg": "short",
                    "total_score": 0.4,
                    "health_score": 0.1,
                    "submit_ack_score": 0.1,
                    "depth_score": 0.2
                }
            }),
        ),
        record(
            4,
            "run-a",
            1_003,
            "entry.opened",
            json!({
                "position_id": "pos-1",
                "symbol": "BTCUSDT",
                "long_venue": "binance",
                "short_venue": "okx",
                "quantity": 0.1
            }),
        ),
        record(
            5,
            "run-a",
            1_004,
            "execution.order_submitted",
            json!({
                "position_id": "pos-1",
                "stage": "entry_short",
                "venue": "binance",
            }),
        ),
        record(
            6,
            "run-a",
            1_005,
            "execution.order_filled",
            json!({
                "position_id": "pos-1",
                "stage": "entry_short",
                "venue": "binance",
                "local_roundtrip_ms": 250_u64,
                "fee_quote": 0.002,
            }),
        ),
        record(
            7,
            "run-a",
            1_005,
            "execution.partial_exit_triggered",
            json!({
                "position_id": "pos-1",
                "reason": "settlement_half_close",
            }),
        ),
        record(
            8,
            "run-a",
            1_006,
            "exit.partial_closed",
            json!({
                "position_id": "pos-1",
                "symbol": "BTCUSDT",
                "reason": "settlement_half_close",
                "closed_quantity": 0.05,
                "remaining_quantity": 0.05
            }),
        ),
        record(
            9,
            "run-a",
            1_007,
            "execution.order_submitted",
            json!({
                "venue": "okx",
            }),
        ),
        record(
            10,
            "run-a",
            1_008,
            "execution.order_failed",
            json!({
                "venue": "okx",
                "local_roundtrip_ms": 410_u64,
                "error": "timeout",
            }),
        ),
        record(
            11,
            "run-a",
            1_009,
            "recovery.resumed",
            json!({
                "position_id": "pos-1",
            }),
        ),
        record(
            12,
            "run-a",
            1_010,
            "recovery.live_blocked",
            json!({
                "mismatches": ["BTC:unexpected_leg_count:1"],
            }),
        ),
        record(
            13,
            "run-a",
            1_011,
            "execution.guard_failed",
            json!({
                "reason": "stale_market_data",
            }),
        ),
        record(
            14,
            "run-a",
            1_012,
            "scan.runtime_gate_blocked",
            json!({
                "reason": "venue_entry_cooldown:hyperliquid:uncertain_order_status",
            }),
        ),
        record(
            15,
            "run-a",
            1_013,
            "runtime.venue_cooldown_started",
            json!({
                "venue": "hyperliquid",
                "reason": "uncertain_order_status",
            }),
        ),
        record(
            16,
            "run-a",
            1_014,
            "scan.no_entry_diagnostics",
            json!({
                "reason": "no_tradeable_candidates",
                "blocked_reason_counts": {
                    "stagger_gap_too_wide": 2,
                    "expected_edge_below_floor": 1
                },
                "advisory_counts": {
                    "transfer_status_unknown:hyperliquid": 1
                },
                "candidates": [
                    {
                        "pair_id": "ethusdt:hyperliquid->okx",
                        "selection_blocker": "candidate_blocked",
                        "checklist": [
                            { "key": "market_fresh_long", "ok": true, "detail": "ok" },
                            { "key": "stagger_gap_ok", "ok": false, "detail": "too_wide" },
                            { "key": "expected_edge_ok", "ok": false, "detail": "below_floor" }
                        ]
                    },
                    {
                        "pair_id": "btcusdt:hyperliquid->okx",
                        "selection_blocker": "candidate_blocked",
                        "checklist": [
                            { "key": "stagger_gap_ok", "ok": false, "detail": "too_wide" },
                            { "key": "active_symbol_clear", "ok": true, "detail": "clear" }
                        ]
                    }
                ]
            }),
        ),
    ];

    let report = analyze_journal_records(&records);

    assert_eq!(report.total_records, 17);
    assert_eq!(report.run_count, 1);
    assert_eq!(
        report
            .venue_stats
            .get(&Venue::Binance)
            .expect("binance")
            .filled_orders,
        2
    );
    assert_eq!(
        report
            .venue_stats
            .get(&Venue::Binance)
            .expect("binance")
            .latency_ms_p50,
        Some(100)
    );
    assert_eq!(
        report
            .venue_stats
            .get(&Venue::Binance)
            .expect("binance")
            .latency_ms_p95,
        Some(250)
    );
    assert_eq!(
        report
            .venue_stats
            .get(&Venue::Okx)
            .expect("okx")
            .failed_orders,
        1
    );
    assert_eq!(
        report
            .venue_stats
            .get(&Venue::Okx)
            .expect("okx")
            .failure_rate_pct,
        100.0
    );
    assert_eq!(
        report
            .recovery_counts
            .get("recovery.resumed")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .recovery_counts
            .get("recovery.live_blocked")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .fail_closed_reason_counts
            .get("stale_market_data")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .fail_closed_reason_counts
            .get("live_recovery_blocked")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .optimization_stats
            .runtime_gate_block_counts
            .get("venue_entry_cooldown:hyperliquid:uncertain_order_status")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .optimization_stats
            .venue_cooldown_counts
            .get(&Venue::Hyperliquid)
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .optimization_stats
            .first_leg_counts
            .get("binance:long")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .optimization_stats
            .first_leg_dominant_factor_counts
            .get("depth")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .optimization_stats
            .no_entry_reason_counts
            .get("no_tradeable_candidates")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .optimization_stats
            .no_entry_blocked_reason_counts
            .get("stagger_gap_too_wide")
            .copied()
            .unwrap_or_default(),
        2
    );
    assert_eq!(
        report
            .optimization_stats
            .no_entry_advisory_counts
            .get("transfer_status_unknown:hyperliquid")
            .copied()
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        report
            .optimization_stats
            .no_entry_checklist_fail_counts
            .get("stagger_gap_ok")
            .copied()
            .unwrap_or_default(),
        2
    );
    assert_eq!(
        report
            .optimization_stats
            .no_entry_selection_blocker_counts
            .get("candidate_blocked")
            .copied()
            .unwrap_or_default(),
        2
    );
    let replay = report
        .trade_replays
        .iter()
        .find(|item| item.position_id == "pos-1")
        .unwrap();
    assert_eq!(replay.state, "open");
    assert_eq!(replay.pair_id.as_deref(), Some("btcusdt:binance->okx"));
    assert!(replay.entry_order_plan.is_some());
    assert_eq!(replay.partial_close_summaries.len(), 1);
    assert_eq!(
        replay.partial_close_summaries[0]["reason"].as_str(),
        Some("settlement_half_close")
    );
    assert_eq!(replay.order_legs.len(), 2);
    assert!(replay.total_fee_quote > 0.0);
    assert!(replay
        .warnings
        .iter()
        .any(|warning| warning == "partial_exit_triggered:settlement_half_close"));
    assert!(report
        .recommendations
        .iter()
        .any(|item| item.category == "depth_capacity"));
    assert!(report
        .recommendations
        .iter()
        .any(|item| item.category == "uncertain_orders"));
    assert!(report
        .recommendations
        .iter()
        .any(|item| item.category == "timeouts"));
    assert!(report
        .recommendations
        .iter()
        .any(|item| item.category == "opportunity_capture"));
    assert!(report
        .recommendations
        .iter()
        .any(|item| item.category == "opportunity_checklist"));
}

#[test]
fn journal_runtime_metrics_snapshot_tracks_lightweight_write_path_counters() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("events.jsonl");
    let journal = JsonlJournal::new(&path);

    journal
        .append(1_000, "scan.completed", &json!({"count": 1}))
        .expect("append");
    journal
        .append_critical(1_001, "entry.opened", &json!({"position_id": "pos-1"}))
        .expect("append critical");
    journal.flush().expect("flush");

    let metrics = journal.metrics_snapshot();
    assert_eq!(
        metrics,
        JournalRuntimeMetricsSnapshot {
            async_appends: 1,
            critical_appends: 1,
            sync_fallback_appends: 0,
            dropped_async_appends: 0,
            flush_requests: 1,
            writer_flushes: 2,
            writer_failures: 0,
            queue_disconnects: 0,
        }
    );
}

fn record(
    seq: u64,
    run_id: &str,
    ts_ms: i64,
    kind: &str,
    payload: serde_json::Value,
) -> JournalRecord {
    JournalRecord {
        seq,
        run_id: run_id.to_string(),
        ts_ms,
        kind: kind.to_string(),
        payload,
    }
}
