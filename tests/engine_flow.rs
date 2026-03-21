use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use lightfee::{
    config::{OpportunitySourceMode, RuntimeMode, StaggeredExitMode},
    strategy::discover_candidates,
    AppConfig, AssetTransferStatus, DirectedPairConfig, Engine, EngineMode, FundingOpportunityType,
    MarketView, OpportunityHint, OpportunityHintSource, OrderExecutionTiming,
    OrderFillReconciliation, PersistenceConfig, RuntimeConfig, ScriptedVenueAdapter,
    StrategyConfig, SymbolMarketSnapshot, TransferStatusView, Venue, VenueAdapter, VenueConfig,
    VenueMarketSnapshot,
};
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn ranks_the_best_four_venue_candidate() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, false);
    let adapters = adapters_for_ranking();
    let mut engine = Engine::new(config, to_dyn(adapters.clone()))
        .await
        .expect("engine");

    engine.tick().await.expect("tick");

    let best = engine
        .state()
        .last_scan
        .as_ref()
        .and_then(|scan| scan.best_candidate.as_ref())
        .expect("best candidate");
    assert_eq!(best.long_venue, Venue::Binance);
    assert_eq!(best.short_venue, Venue::Okx);
    assert!(engine.state().open_position.is_none());
}

#[tokio::test]
async fn opens_then_exits_after_funding_capture() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, true);
    let adapters = adapters_for_capture();
    let mut engine = Engine::new(config.clone(), to_dyn(adapters))
        .await
        .expect("engine");

    engine.tick().await.expect("entry tick");
    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("settlement half close tick");
    assert!(engine.state().open_position.is_some());
    engine.tick().await.expect("final exit tick");
    assert!(engine.state().open_position.is_none());
    assert_eq!(engine.state().mode, EngineMode::Running);

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "entry.opened"));
    assert!(has_event(&records, "exit.closed"));
    assert!(has_event(&records, "execution.quantity_normalized"));
    assert!(has_event(&records, "execution.order_submitted"));
    assert!(has_event(&records, "execution.order_filled"));
    assert!(has_event(&records, "execution.entry_latency_summary"));
    assert!(has_event(&records, "execution.exit_latency_summary"));
    assert_eq!(event_count(&records, "execution.order_filled"), 6);
    assert!(has_event(&records, "exit.partial_closed"));
    assert!(has_event(&records, "execution.partial_exit_prepare_timing"));

    let first = records.first().expect("first record");
    assert!(first.get("seq").and_then(Value::as_u64).unwrap_or_default() >= 1);
    assert!(first
        .get("run_id")
        .and_then(Value::as_str)
        .expect("run_id")
        .starts_with("lightfee-"));

    let quantity_event = records
        .iter()
        .find(|record| record_kind(record) == Some("execution.quantity_normalized"))
        .expect("quantity event");
    assert!(
        quantity_event["payload"]["plan"]["executable_quantity"]
            .as_f64()
            .expect("executable quantity")
            > 0.0
    );

    let latency_event = records
        .iter()
        .find(|record| record_kind(record) == Some("execution.entry_latency_summary"))
        .expect("latency event");
    assert!(latency_event["payload"]["max_single_order_ms"]
        .as_u64()
        .is_some());
}

#[tokio::test]
async fn exit_closed_net_quote_uses_realized_exit_fills() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, true);
    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![
            venue_snapshot(
                Venue::Binance,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 101.0, 200.0, 200.0, -0.0005, 60_000,
                )],
            ),
            venue_snapshot(
                Venue::Binance,
                61_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 105.0, 106.0, 200.0, 200.0, -0.0005, 60_000,
                )],
            ),
            venue_snapshot(
                Venue::Binance,
                361_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 105.0, 106.0, 200.0, 200.0, -0.0005, 60_000,
                )],
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![
            venue_snapshot(
                Venue::Okx,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 101.0, 200.0, 200.0, 0.0015, 60_000,
                )],
            ),
            venue_snapshot(
                Venue::Okx,
                61_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 95.0, 96.0, 200.0, 200.0, 0.0015, 60_000,
                )],
            ),
            venue_snapshot(
                Venue::Okx,
                361_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 95.0, 96.0, 200.0, 200.0, 0.0015, 60_000,
                )],
            ),
        ],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));
    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    engine.tick().await.expect("settlement half close tick");
    engine.tick().await.expect("final exit tick");

    let records = read_event_records(&config.persistence.event_log_path);
    let entry_long = records
        .iter()
        .find(|record| {
            record_kind(record) == Some("execution.order_filled")
                && record["payload"]["stage"].as_str() == Some("entry_long")
        })
        .expect("entry long fill");
    let entry_short = records
        .iter()
        .find(|record| {
            record_kind(record) == Some("execution.order_filled")
                && record["payload"]["stage"].as_str() == Some("entry_short")
        })
        .expect("entry short fill");
    let long_exit_fills: Vec<_> = records
        .iter()
        .filter(|record| {
            record_kind(record) == Some("execution.order_filled")
                && matches!(
                    record["payload"]["stage"].as_str(),
                    Some("partial_exit_long" | "exit_long")
                )
        })
        .collect();
    let short_exit_fills: Vec<_> = records
        .iter()
        .filter(|record| {
            record_kind(record) == Some("execution.order_filled")
                && matches!(
                    record["payload"]["stage"].as_str(),
                    Some("partial_exit_short" | "exit_short")
                )
        })
        .collect();
    let exit_triggered = records
        .iter()
        .find(|record| record_kind(record) == Some("execution.exit_triggered"))
        .expect("exit trigger");
    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");

    let entry_long_average_price = entry_long["payload"]["average_price"]
        .as_f64()
        .expect("entry long avg");
    let entry_short_average_price = entry_short["payload"]["average_price"]
        .as_f64()
        .expect("entry short avg");
    let realized_price_pnl = long_exit_fills
        .iter()
        .map(|fill| {
            (fill["payload"]["average_price"]
                .as_f64()
                .expect("exit long avg")
                - entry_long_average_price)
                * fill["payload"]["executed_quantity"]
                    .as_f64()
                    .expect("exit long qty")
        })
        .sum::<f64>()
        + short_exit_fills
            .iter()
            .map(|fill| {
                (entry_short_average_price
                    - fill["payload"]["average_price"]
                        .as_f64()
                        .expect("exit short avg"))
                    * fill["payload"]["executed_quantity"]
                        .as_f64()
                        .expect("exit short qty")
            })
            .sum::<f64>();
    let captured_funding_quote = exit_triggered["payload"]["captured_funding_quote"]
        .as_f64()
        .expect("captured funding");
    let second_stage_funding_quote = exit_triggered["payload"]["second_stage_funding_quote"]
        .as_f64()
        .expect("second stage funding");
    let total_entry_fee_quote = entry_long["payload"]["fee_quote"]
        .as_f64()
        .expect("entry long fee")
        + entry_short["payload"]["fee_quote"]
            .as_f64()
            .expect("entry short fee");
    let total_exit_fee_quote = exit_closed["payload"]["total_exit_fee_quote"]
        .as_f64()
        .expect("total exit fee");
    let expected_realized_net_quote =
        realized_price_pnl + captured_funding_quote + second_stage_funding_quote
            - total_entry_fee_quote
            - total_exit_fee_quote;
    let actual_net_quote = exit_closed["payload"]["net_quote"]
        .as_f64()
        .expect("exit net quote");

    assert!(
        (actual_net_quote - expected_realized_net_quote).abs() < 1e-9,
        "expected realized net {expected_realized_net_quote}, got {actual_net_quote}"
    );
    assert_ne!(
        exit_closed["payload"]["mark_to_market_net_quote"].as_f64(),
        Some(actual_net_quote)
    );
    assert_eq!(
        exit_closed["payload"]["outcome_diagnostics"]["outcome"].as_str(),
        Some("profit")
    );
    assert_eq!(
        exit_closed["payload"]["outcome_diagnostics"]["primary_reason"].as_str(),
        Some("favorable_price_move")
    );
    assert_eq!(
        exit_closed["payload"]["settlement_half_outcome_diagnostics"]["segment_kind"].as_str(),
        Some("settlement_half_close")
    );
    assert_eq!(
        exit_closed["payload"]["remaining_outcome_diagnostics"]["segment_kind"].as_str(),
        Some("post_settlement_remaining_close")
    );
    let contributing = exit_closed["payload"]["outcome_diagnostics"]["contributing_reasons"]
        .as_array()
        .expect("profit contributing reasons");
    assert!(contributing
        .iter()
        .any(|item| item.as_str() == Some("favorable_price_move")));
    assert!(contributing
        .iter()
        .any(|item| item.as_str() == Some("funding_realized")));
}

#[tokio::test]
async fn exit_closed_records_loss_diagnostics_for_losing_trade() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.stop_loss_quote = 1.0;
    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![
            venue_snapshot(
                Venue::Binance,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 101.0, 0.1, 0.1, -0.0002, 60_000,
                )],
            ),
            venue_snapshot(
                Venue::Binance,
                1_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 89.0, 90.0, 0.1, 0.1, 0.0002, 60_000,
                )],
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![
            venue_snapshot(
                Venue::Okx,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 101.0, 0.1, 0.1, 0.0008, 60_000,
                )],
            ),
            venue_snapshot(
                Venue::Okx,
                1_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 111.0, 112.0, 0.1, 0.1, 0.0002, 60_000,
                )],
            ),
        ],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    engine.tick().await.expect("settlement half close tick");
    engine.tick().await.expect("remaining close tick");

    let records = read_event_records(&config.persistence.event_log_path);
    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");

    assert!(
        exit_closed["payload"]["net_quote"]
            .as_f64()
            .expect("net quote")
            < 0.0
    );
    assert_eq!(
        exit_closed["payload"]["outcome_diagnostics"]["outcome"].as_str(),
        Some("loss")
    );
    assert_eq!(
        exit_closed["payload"]["outcome_diagnostics"]["primary_reason"].as_str(),
        Some("entry_timing_unfavorable")
    );
    let contributing = exit_closed["payload"]["outcome_diagnostics"]["contributing_reasons"]
        .as_array()
        .expect("contributing reasons");
    assert!(contributing
        .iter()
        .any(|item| item.as_str() == Some("adverse_price_move")));
    assert!(contributing
        .iter()
        .any(|item| item.as_str() == Some("funding_edge_narrowed")));
    assert!(contributing
        .iter()
        .any(|item| item.as_str() == Some("entry_depth_constrained")));
    assert!(
        exit_closed["payload"]["outcome_diagnostics"]["current_total_funding_edge_bps"]
            .as_f64()
            .expect("current funding edge")
            < exit_closed["payload"]["outcome_diagnostics"]["entry_total_funding_edge_bps"]
                .as_f64()
                .expect("entry funding edge")
    );
    assert_eq!(
        exit_closed["payload"]["loss_diagnostics"]["primary_reason"].as_str(),
        Some("entry_timing_unfavorable")
    );
}

#[tokio::test]
async fn aligned_position_half_closes_at_settlement_then_force_closes_remainder() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.stop_loss_quote = 1_000.0;
    config.strategy.profit_take_quote = 1_000.0;
    config.strategy.trailing_drawdown_quote = 1_000.0;
    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![
            snapshot(
                Venue::Binance,
                0,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                60_000,
                90.0,
                90.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                1_260_000,
                89.0,
                89.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![
            snapshot(Venue::Okx, 0, 100.0, 100.0, 200.0, 200.0, 0.0015, 360_000),
            snapshot(
                Venue::Okx,
                60_000,
                110.0,
                110.0,
                200.0,
                200.0,
                0.0015,
                60_000,
            ),
            snapshot(
                Venue::Okx,
                1_260_000,
                111.0,
                111.0,
                200.0,
                200.0,
                0.0015,
                60_000,
            ),
        ],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    let opened = engine
        .state()
        .open_position
        .as_ref()
        .expect("opened position")
        .clone();

    engine.tick().await.expect("settlement tick");
    let after_half = engine
        .state()
        .open_position
        .as_ref()
        .expect("position remains after half close")
        .clone();
    assert!(after_half.quantity < opened.quantity);
    assert!(after_half.quantity > 0.0);
    assert!((after_half.quantity - (opened.quantity * 0.5)).abs() < 1e-9);

    engine.tick().await.expect("forced close tick");
    assert!(engine.state().open_position.is_none());

    let records = read_event_records(&config.persistence.event_log_path);
    let partial = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.partial_closed"))
        .expect("partial close event");
    assert_eq!(
        partial["payload"]["reason"].as_str(),
        Some("settlement_half_close")
    );
    assert_eq!(
        partial["payload"]["remaining_quantity"].as_f64(),
        Some(after_half.quantity)
    );
    assert_eq!(
        partial["payload"]["outcome_diagnostics"]["segment_kind"].as_str(),
        Some("settlement_half_close")
    );
    assert_eq!(
        partial["payload"]["outcome_diagnostics"]["closed_quantity"].as_f64(),
        Some(opened.quantity * 0.5)
    );
    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");
    assert_eq!(
        exit_closed["payload"]["reason"].as_str(),
        Some("settlement_force_close")
    );
    assert_eq!(
        exit_closed["payload"]["settlement_half_outcome_diagnostics"]["segment_kind"].as_str(),
        Some("settlement_half_close")
    );
    assert_eq!(
        exit_closed["payload"]["remaining_outcome_diagnostics"]["segment_kind"].as_str(),
        Some("post_settlement_remaining_close")
    );
}

#[tokio::test]
async fn settlement_half_close_does_not_fail_closed_on_stale_market_data() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.max_market_age_ms = 10_000;
    config.strategy.stop_loss_quote = 1_000.0;
    config.strategy.profit_take_quote = 1_000.0;
    config.strategy.trailing_drawdown_quote = 1_000.0;
    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![
            snapshot(
                Venue::Binance,
                0,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                60_000,
                90.0,
                90.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![
            snapshot(Venue::Okx, 0, 100.0, 100.0, 200.0, 200.0, 0.0015, 60_000),
            snapshot(Venue::Okx, 0, 110.0, 110.0, 200.0, 200.0, 0.0015, 60_000),
        ],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    engine.tick().await.expect("settlement tick");

    assert_eq!(engine.state().mode, EngineMode::Running);
    let remaining = engine
        .state()
        .open_position
        .as_ref()
        .expect("position remains")
        .quantity;
    assert!(remaining > 0.0);

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "exit.partial_closed"));
}

#[tokio::test]
async fn stale_market_does_not_block_remaining_funding_capture_close() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.max_market_age_ms = 10_000;
    config.strategy.stop_loss_quote = 1_000.0;
    config.strategy.profit_take_quote = 1_000.0;
    config.strategy.trailing_drawdown_quote = 1_000.0;
    config.venues = vec![venue(Venue::Binance, 0.0), venue(Venue::Okx, 0.0)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.0,
        vec![
            snapshot(
                Venue::Binance,
                0,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                60_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.0,
        vec![snapshot(
            Venue::Okx,
            0,
            100.0,
            100.0,
            200.0,
            200.0,
            0.0015,
            60_000,
        )],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("settlement half close tick");
    assert!(engine.state().open_position.is_some());

    engine
        .tick()
        .await
        .expect("stale funding capture close tick");
    assert!(engine.state().open_position.is_none());
    assert_eq!(engine.state().mode, EngineMode::Running);

    let records = read_event_records(&config.persistence.event_log_path);
    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");
    assert_eq!(
        exit_closed["payload"]["reason"].as_str(),
        Some("funding_capture")
    );
}

#[tokio::test]
async fn remaining_close_waits_five_minutes_after_settlement() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.stop_loss_quote = 1_000.0;
    config.strategy.profit_take_quote = 1_000.0;
    config.strategy.trailing_drawdown_quote = 1_000.0;
    config.venues = vec![venue(Venue::Binance, 0.0), venue(Venue::Okx, 0.0)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.0,
        vec![
            snapshot(
                Venue::Binance,
                0,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                60_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                300_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                60_000,
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.0,
        vec![
            snapshot(Venue::Okx, 0, 100.0, 100.0, 200.0, 200.0, 0.0015, 60_000),
            snapshot(
                Venue::Okx,
                60_000,
                100.0,
                100.0,
                200.0,
                200.0,
                0.0015,
                60_000,
            ),
            snapshot(
                Venue::Okx,
                300_000,
                100.0,
                100.0,
                200.0,
                200.0,
                0.0015,
                60_000,
            ),
            snapshot(
                Venue::Okx,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                0.0015,
                60_000,
            ),
        ],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    engine.tick().await.expect("settlement half close tick");
    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("delay window tick");
    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("post delay close tick");
    assert!(engine.state().open_position.is_none());

    let records = read_event_records(&config.persistence.event_log_path);
    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");
    assert_eq!(
        exit_closed["payload"]["reason"].as_str(),
        Some("funding_capture")
    );
}

#[tokio::test]
async fn close_reconciliation_runs_after_close_outside_entry_window() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.runtime.max_order_quote_age_ms = 0;
    config.strategy.max_scan_minutes_before_funding = 25;
    config.strategy.min_scan_minutes_before_funding = 5;
    config.strategy.stop_loss_quote = 1_000.0;
    config.strategy.profit_take_quote = 1_000.0;
    config.strategy.trailing_drawdown_quote = 1_000.0;
    config.venues = vec![venue(Venue::Binance, 0.0), venue(Venue::Okx, 0.0)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance = Arc::new(TimedFillAdapter::new(
        Venue::Binance,
        vec![
            snapshot(
                Venue::Binance,
                0,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                1_561_000,
                105.0,
                106.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                1_561_000,
                105.0,
                106.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                1_562_000,
                105.0,
                106.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
        ],
        None,
    ));
    let okx = Arc::new(TimedFillAdapter::new(
        Venue::Okx,
        vec![
            snapshot(Venue::Okx, 0, 100.0, 100.0, 200.0, 200.0, 0.0015, 360_000),
            snapshot(
                Venue::Okx,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                1_561_000,
                94.0,
                95.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                1_561_000,
                94.0,
                95.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                1_562_000,
                94.0,
                95.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
        ],
        None,
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    engine.tick().await.expect("settlement half close tick");
    engine.tick().await.expect("post-settlement wait tick");
    engine.tick().await.expect("final close tick");
    engine.shutdown().await.expect("shutdown");
    assert!(engine.state().open_position.is_none());

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(!has_event(&records, "exit.reconciled"));

    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");
    let long_order_id = exit_closed["payload"]["long_exit_order_id"]
        .as_str()
        .expect("long exit order id");
    let short_order_id = exit_closed["payload"]["short_exit_order_id"]
        .as_str()
        .expect("short exit order id");

    binance.set_reconciled_fill(
        long_order_id,
        OrderFillReconciliation {
            order_id: long_order_id.to_string(),
            client_order_id: Some("close-long-recon".to_string()),
            quantity: 0.5,
            average_price: 105.5,
            fee_quote: Some(0.15),
            filled_at_ms: 361_500,
        },
    );
    okx.set_reconciled_fill(
        short_order_id,
        OrderFillReconciliation {
            order_id: short_order_id.to_string(),
            client_order_id: Some("close-short-recon".to_string()),
            quantity: 0.5,
            average_price: 94.5,
            fee_quote: Some(0.12),
            filled_at_ms: 361_550,
        },
    );

    engine.tick().await.expect("reconciliation tick");

    let records = read_event_records(&config.persistence.event_log_path);
    let reconciled = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.reconciled"))
        .unwrap_or_else(|| {
            panic!(
                "exit reconciled missing; kinds={:?} binance_calls={} okx_calls={}",
                records.iter().filter_map(record_kind).collect::<Vec<_>>(),
                binance.reconciliation_call_count(),
                okx.reconciliation_call_count()
            )
        });
    assert_eq!(
        reconciled["payload"]["long_leg"]["average_price"].as_f64(),
        Some(105.5)
    );
    assert_eq!(
        reconciled["payload"]["short_leg"]["average_price"].as_f64(),
        Some(94.5)
    );
    assert!(binance.reconciliation_call_count() > 0);
    assert!(okx.reconciliation_call_count() > 0);
}

#[tokio::test]
async fn tiny_close_reconciliation_delta_emits_summary_payload() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.runtime.max_order_quote_age_ms = 0;
    config.strategy.max_scan_minutes_before_funding = 25;
    config.strategy.min_scan_minutes_before_funding = 5;
    config.strategy.stop_loss_quote = 1_000.0;
    config.strategy.profit_take_quote = 1_000.0;
    config.strategy.trailing_drawdown_quote = 1_000.0;
    config.venues = vec![venue(Venue::Binance, 0.0), venue(Venue::Okx, 0.0)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance = Arc::new(TimedFillAdapter::new(
        Venue::Binance,
        vec![
            snapshot(
                Venue::Binance,
                0,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                1_561_000,
                105.0,
                106.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                1_561_000,
                105.0,
                106.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
            snapshot(
                Venue::Binance,
                1_562_000,
                105.0,
                106.0,
                200.0,
                200.0,
                -0.0005,
                360_000,
            ),
        ],
        None,
    ));
    let okx = Arc::new(TimedFillAdapter::new(
        Venue::Okx,
        vec![
            snapshot(Venue::Okx, 0, 100.0, 100.0, 200.0, 200.0, 0.0015, 360_000),
            snapshot(
                Venue::Okx,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                361_000,
                100.0,
                100.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                1_561_000,
                94.0,
                95.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                1_561_000,
                94.0,
                95.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
            snapshot(
                Venue::Okx,
                1_562_000,
                94.0,
                95.0,
                200.0,
                200.0,
                0.0015,
                360_000,
            ),
        ],
        None,
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    engine.tick().await.expect("settlement half close tick");
    engine.tick().await.expect("post-settlement wait tick");
    engine.tick().await.expect("final close tick");
    engine.shutdown().await.expect("shutdown");

    let records = read_event_records(&config.persistence.event_log_path);
    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");
    let long_order_id = exit_closed["payload"]["long_exit_order_id"]
        .as_str()
        .expect("long exit order id");
    let short_order_id = exit_closed["payload"]["short_exit_order_id"]
        .as_str()
        .expect("short exit order id");
    let long_exit_average_price = exit_closed["payload"]["long_exit_average_price"]
        .as_f64()
        .expect("long exit average price");
    let short_exit_average_price = exit_closed["payload"]["short_exit_average_price"]
        .as_f64()
        .expect("short exit average price");
    let long_exit_quantity = exit_closed["payload"]["long_exit_quantity"]
        .as_f64()
        .expect("long exit quantity");
    let short_exit_quantity = exit_closed["payload"]["short_exit_quantity"]
        .as_f64()
        .expect("short exit quantity");

    binance.set_reconciled_fill(
        long_order_id,
        OrderFillReconciliation {
            order_id: long_order_id.to_string(),
            client_order_id: Some("close-long-recon".to_string()),
            quantity: long_exit_quantity,
            average_price: long_exit_average_price,
            fee_quote: Some(0.0),
            filled_at_ms: 361_500,
        },
    );
    okx.set_reconciled_fill(
        short_order_id,
        OrderFillReconciliation {
            order_id: short_order_id.to_string(),
            client_order_id: Some("close-short-recon".to_string()),
            quantity: short_exit_quantity,
            average_price: short_exit_average_price,
            fee_quote: Some(0.0),
            filled_at_ms: 361_550,
        },
    );

    engine.tick().await.expect("reconciliation tick");

    let records = read_event_records(&config.persistence.event_log_path);
    let reconciled = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.reconciled"))
        .expect("exit reconciled");
    assert_eq!(reconciled["payload"]["summary_only"].as_bool(), Some(true));
    assert!(reconciled["payload"].get("long_leg").is_none());
    assert!(reconciled["payload"].get("short_leg").is_none());
}

#[tokio::test]
async fn settlement_half_close_below_exchange_minimum_skips_partial_and_keeps_full_position() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.max_entry_notional = 12.0;
    config.strategy.live_max_entry_notional = 12.0;
    config.strategy.min_funding_edge_bps = -1_000.0;
    config.strategy.min_expected_edge_bps = -1_000.0;
    config.strategy.min_worst_case_edge_bps = -1_000.0;
    config.strategy.stop_loss_quote = 1_000.0;
    config.strategy.profit_take_quote = 1_000.0;
    config.strategy.trailing_drawdown_quote = 1_000.0;
    config.venues = vec![venue(Venue::Binance, 50.0), venue(Venue::Hyperliquid, 50.0)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Hyperliquid,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        50.0,
        vec![
            snapshot(Venue::Binance, 0, 1.0, 1.0, 200.0, 200.0, 0.0, 60_000),
            snapshot(Venue::Binance, 60_000, 1.0, 1.0, 200.0, 200.0, 0.0, 60_000),
            snapshot(
                Venue::Binance,
                1_260_000,
                1.0,
                1.0,
                200.0,
                200.0,
                0.0,
                60_000,
            ),
        ],
    ));
    let hyper = Arc::new(
        ScriptedVenueAdapter::new(
            Venue::Hyperliquid,
            50.0,
            vec![
                snapshot(Venue::Hyperliquid, 0, 1.0, 1.0, 200.0, 200.0, 0.0, 60_000),
                snapshot(
                    Venue::Hyperliquid,
                    60_000,
                    1.0,
                    1.0,
                    200.0,
                    200.0,
                    0.0,
                    60_000,
                ),
                snapshot(
                    Venue::Hyperliquid,
                    1_260_000,
                    1.0,
                    1.0,
                    200.0,
                    200.0,
                    0.0,
                    60_000,
                ),
            ],
        )
        .with_min_notional_quote_hint(10.0),
    );
    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("settlement tick");
    assert!(engine.state().open_position.is_some());

    sleep(Duration::from_millis(250)).await;
    let records = read_event_records(&config.persistence.event_log_path);
    assert!(!has_event(&records, "exit.partial_closed"));
    assert!(has_event(
        &records,
        "execution.partial_exit_quantity_adjusted"
    ));
    assert!(has_event(&records, "execution.partial_exit_skipped"));

    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("force close tick");
    assert!(engine.state().open_position.is_none());

    let records = read_event_records(&config.persistence.event_log_path);
    let exit_closed = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit closed");
    assert_eq!(
        exit_closed["payload"]["reason"].as_str(),
        Some("settlement_force_close")
    );
    assert!(!has_event(&records, "exit.partial_closed"));
}

#[tokio::test]
async fn staggered_position_exits_after_first_stage_capture_by_default() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];
    let adapters = adapters_for_staggered_capture();
    let mut engine = Engine::new(config.clone(), to_dyn(adapters))
        .await
        .expect("engine");

    engine.tick().await.expect("entry tick");
    let position = engine
        .state()
        .open_position
        .as_ref()
        .expect("open position")
        .clone();
    assert_eq!(position.opportunity_type, FundingOpportunityType::Staggered);

    engine.tick().await.expect("first stage half close tick");
    assert!(engine.state().open_position.is_some());
    engine
        .tick()
        .await
        .expect("first stage remaining close tick");
    assert!(engine.state().open_position.is_none());
    assert_eq!(engine.state().mode, EngineMode::Running);

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "exit.partial_closed"));
    let exit_record = records
        .iter()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit record");
    assert_eq!(
        exit_record["payload"]["reason"].as_str(),
        Some("first_stage_capture")
    );
}

#[tokio::test]
async fn staggered_position_can_continue_until_second_stage_when_configured() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.staggered_exit_mode = StaggeredExitMode::EvaluateSecondStage;
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];
    let adapters = adapters_for_staggered_second_stage_capture();
    let mut engine = Engine::new(config.clone(), to_dyn(adapters))
        .await
        .expect("engine");

    engine.tick().await.expect("entry tick");
    let position = engine
        .state()
        .open_position
        .as_ref()
        .expect("open position")
        .clone();
    assert_eq!(position.opportunity_type, FundingOpportunityType::Staggered);
    assert!(!position.exit_after_first_stage);
    assert!(position.second_stage_enabled_at_entry);

    engine.tick().await.expect("first stage half close tick");
    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("force close tick");
    assert!(engine.state().open_position.is_none());

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "exit.partial_closed"));
    let exit_record = records
        .iter()
        .rev()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit record");
    assert_eq!(
        exit_record["payload"]["reason"].as_str(),
        Some("settlement_force_close")
    );
}

#[tokio::test]
async fn external_hint_source_pins_long_short_direction() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, false);
    let adapters = adapters_for_ranking();
    let hint_source = Arc::new(MockHintSource::success(vec![OpportunityHint {
        symbol: "BTC".to_string(),
        long_venue: Venue::Binance,
        short_venue: Venue::Bybit,
        price_diff_pct: 0.1,
        funding_diff_pct_per_hour: 0.05,
        direction_consistent: true,
        interval_aligned: true,
        source: "mock".to_string(),
    }]));
    let mut engine = Engine::with_opportunity_source(config, to_dyn(adapters), Some(hint_source))
        .await
        .expect("engine");

    engine.tick().await.expect("tick");

    let best = engine
        .state()
        .last_scan
        .as_ref()
        .and_then(|scan| scan.best_candidate.as_ref())
        .expect("best candidate");
    assert_eq!(best.long_venue, Venue::Binance);
    assert_eq!(best.short_venue, Venue::Bybit);
}

#[tokio::test]
async fn hint_source_failure_falls_back_to_exchange_scan() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, false);
    let adapters = adapters_for_ranking();
    let hint_source = Arc::new(MockHintSource::failure());
    let mut engine = Engine::with_opportunity_source(config, to_dyn(adapters), Some(hint_source))
        .await
        .expect("engine");

    engine.tick().await.expect("tick");

    let best = engine
        .state()
        .last_scan
        .as_ref()
        .and_then(|scan| scan.best_candidate.as_ref())
        .expect("best candidate");
    assert_eq!(best.long_venue, Venue::Binance);
    assert_eq!(best.short_venue, Venue::Okx);
}

#[tokio::test]
async fn no_entry_window_emits_candidate_checklist_diagnostics() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.min_funding_edge_bps = 50.0;
    config.strategy.min_expected_edge_bps = 50.0;
    config.strategy.min_worst_case_edge_bps = 50.0;
    let mut engine = Engine::new(config.clone(), to_dyn(adapters_for_ranking()))
        .await
        .expect("engine");

    engine.tick().await.expect("tick");
    sleep(Duration::from_millis(300)).await;

    let records = read_event_records(&config.persistence.event_log_path);
    let diagnostic = records
        .iter()
        .find(|record| record_kind(record) == Some("scan.no_entry_diagnostics"))
        .expect("diagnostic event");
    assert_eq!(
        diagnostic["payload"]["reason"].as_str(),
        Some("no_tradeable_candidates")
    );
    assert_eq!(diagnostic["payload"]["tradeable_count"].as_u64(), Some(0));
    let first_candidate = diagnostic["payload"]["candidates"]
        .as_array()
        .and_then(|items| items.first())
        .expect("candidate checklist");
    let checklist = first_candidate["checklist"]
        .as_array()
        .expect("candidate factors");
    assert!(checklist.iter().any(|item| {
        item["key"].as_str() == Some("funding_edge_ok") && item["ok"].as_bool() == Some(false)
    }));
    assert!(checklist.iter().any(|item| {
        item["key"].as_str() == Some("market_fresh_long") && item["ok"].as_bool() == Some(true)
    }));
}

#[tokio::test]
async fn repeated_no_entry_diagnostics_are_sampled_across_cycles() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.min_funding_edge_bps = 50.0;
    config.strategy.min_expected_edge_bps = 50.0;
    config.strategy.min_worst_case_edge_bps = 50.0;
    let mut engine = Engine::new(config.clone(), to_dyn(adapters_for_ranking()))
        .await
        .expect("engine");

    for _ in 0..11 {
        engine.tick().await.expect("tick");
    }
    sleep(Duration::from_millis(300)).await;

    let records = read_event_records(&config.persistence.event_log_path);
    let diagnostics = records
        .iter()
        .filter(|record| record_kind(record) == Some("scan.no_entry_diagnostics"))
        .collect::<Vec<_>>();
    assert_eq!(diagnostics.len(), 2);
    assert_eq!(
        diagnostics[0]["payload"]["suppressed_repeat_count"].as_u64(),
        Some(0)
    );
    assert_eq!(
        diagnostics[1]["payload"]["suppressed_repeat_count"].as_u64(),
        Some(9)
    );
}

#[tokio::test]
async fn transfer_status_fetch_is_cached_between_ticks() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, false);
    config.runtime.transfer_status_cache_ms = 60_000;
    config.venues = vec![venue(Venue::Binance, 0.5), venue(Venue::Okx, 0.5)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance_calls = Arc::new(Mutex::new(0_usize));
    let okx_calls = Arc::new(Mutex::new(0_usize));
    let adapters: Vec<Arc<dyn VenueAdapter>> = vec![
        Arc::new(CountingTransferAdapter::new(
            Venue::Binance,
            snapshot(
                Venue::Binance,
                1_000,
                100.0,
                100.2,
                10.0,
                10.0,
                0.0005,
                60_000,
            ),
            transfer_status(Venue::Binance, "BTC", true, true),
            binance_calls.clone(),
        )),
        Arc::new(CountingTransferAdapter::new(
            Venue::Okx,
            snapshot(Venue::Okx, 1_000, 100.4, 100.6, 10.0, 10.0, -0.0004, 60_000),
            transfer_status(Venue::Okx, "BTC", true, true),
            okx_calls.clone(),
        )),
    ];

    let mut engine = Engine::new(config, adapters).await.expect("engine");
    engine.tick().await.expect("first tick");
    engine.tick().await.expect("second tick");

    assert_eq!(*binance_calls.lock().expect("lock"), 1);
    assert_eq!(*okx_calls.lock().expect("lock"), 1);
}

#[tokio::test]
async fn restart_restores_venue_health_snapshot_for_hyperliquid_gate() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.runtime.max_order_quote_age_ms = 0;
    config.strategy.hyperliquid_submit_ack_min_samples = 1;
    config.strategy.hyperliquid_max_submit_ack_p95_ms = 500;
    config.strategy.max_concurrent_positions = 1;
    config.venues = vec![venue(Venue::Binance, 0.5), venue(Venue::Hyperliquid, 0.3)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Hyperliquid,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance = Arc::new(TimedFillAdapter::new(
        Venue::Binance,
        vec![
            snapshot(
                Venue::Binance,
                0,
                99.95,
                100.0,
                200.0,
                200.0,
                -0.0004,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                61_000,
                99.95,
                100.0,
                200.0,
                200.0,
                -0.0004,
                60_000,
            ),
            snapshot(
                Venue::Binance,
                361_000,
                99.95,
                100.0,
                200.0,
                200.0,
                -0.0004,
                60_000,
            ),
        ],
        None,
    ));
    let hyper = Arc::new(TimedFillAdapter::new(
        Venue::Hyperliquid,
        vec![
            snapshot(
                Venue::Hyperliquid,
                0,
                100.2,
                100.25,
                200.0,
                200.0,
                0.0010,
                60_000,
            ),
            snapshot(
                Venue::Hyperliquid,
                61_000,
                100.2,
                100.25,
                200.0,
                200.0,
                0.0010,
                60_000,
            ),
            snapshot(
                Venue::Hyperliquid,
                361_000,
                100.2,
                100.25,
                200.0,
                200.0,
                0.0010,
                60_000,
            ),
        ],
        Some(OrderExecutionTiming {
            quote_resolve_ms: Some(1),
            order_prepare_ms: Some(1),
            request_sign_ms: None,
            submit_http_ms: None,
            response_decode_ms: None,
            private_fill_wait_ms: None,
            submit_ack_ms: Some(1_500),
        }),
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            hyper.clone() as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("entry tick");
    engine.tick().await.expect("settlement half close tick");
    engine.tick().await.expect("final exit tick");
    assert!(engine.state().open_position.is_none());
    drop(engine);

    let mut restarted = Engine::new(
        config,
        vec![
            Arc::new(TimedFillAdapter::new(
                Venue::Binance,
                vec![snapshot(
                    Venue::Binance,
                    0,
                    99.95,
                    100.0,
                    200.0,
                    200.0,
                    -0.0004,
                    60_000,
                )],
                None,
            )) as Arc<dyn VenueAdapter>,
            Arc::new(TimedFillAdapter::new(
                Venue::Hyperliquid,
                vec![snapshot(
                    Venue::Hyperliquid,
                    0,
                    100.2,
                    100.25,
                    200.0,
                    200.0,
                    0.0010,
                    60_000,
                )],
                None,
            )) as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("restart");
    restarted.tick().await.expect("restart tick");

    let best = restarted
        .state()
        .last_scan
        .as_ref()
        .and_then(|scan| scan.best_candidate.as_ref())
        .expect("best candidate");
    assert!(
        best.blocked_reasons
            .iter()
            .any(|reason| reason.contains("hyperliquid_recent_submit_ack_p95_ms_above_limit")),
        "blocked_reasons={:?}",
        best.blocked_reasons
    );
}

#[tokio::test]
async fn restart_reuses_scan_symbol_cache_to_filter_requested_symbols() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, false);
    config.venues = vec![venue(Venue::Binance, 0.5), venue(Venue::Okx, 0.5)];
    config.symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];

    let mut engine = Engine::new(
        config.clone(),
        vec![
            Arc::new(ScriptedVenueAdapter::new(
                Venue::Binance,
                0.5,
                vec![venue_snapshot(
                    Venue::Binance,
                    0,
                    vec![
                        symbol_snapshot("BTCUSDT", 100.0, 100.0, 200.0, 200.0, -0.0004, 60_000),
                        symbol_snapshot("ETHUSDT", 110.0, 110.0, 200.0, 200.0, -0.0003, 60_000),
                    ],
                )],
            )) as Arc<dyn VenueAdapter>,
            Arc::new(ScriptedVenueAdapter::new(
                Venue::Okx,
                0.5,
                vec![venue_snapshot(
                    Venue::Okx,
                    0,
                    vec![symbol_snapshot(
                        "BTCUSDT", 100.4, 100.4, 200.0, 200.0, 0.0010, 60_000,
                    )],
                )],
            )) as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");
    engine.tick().await.expect("seed cache tick");
    drop(engine);

    let mut restarted = Engine::new(
        config.clone(),
        vec![
            Arc::new(StrictRequestedSymbolsAdapter::new(
                Venue::Binance,
                vec!["BTCUSDT".to_string()],
                venue_snapshot(
                    Venue::Binance,
                    0,
                    vec![symbol_snapshot(
                        "BTCUSDT", 100.0, 100.0, 200.0, 200.0, -0.0004, 60_000,
                    )],
                ),
            )) as Arc<dyn VenueAdapter>,
            Arc::new(StrictRequestedSymbolsAdapter::new(
                Venue::Okx,
                vec!["BTCUSDT".to_string()],
                venue_snapshot(
                    Venue::Okx,
                    0,
                    vec![symbol_snapshot(
                        "BTCUSDT", 100.4, 100.4, 200.0, 200.0, 0.0010, 60_000,
                    )],
                ),
            )) as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("restart");

    restarted.tick().await.expect("cached scan tick");
    assert_eq!(
        restarted
            .state()
            .last_scan
            .as_ref()
            .expect("last scan")
            .candidate_count,
        1
    );
}

#[test]
fn degraded_transfer_status_only_changes_ranking_not_tradeability() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, false);
    config.strategy.transfer_healthy_bias_bps = 0.0;
    config.strategy.transfer_degraded_bias_bps = -100.0;

    let market = MarketView::from_snapshots(vec![
        snapshot(
            Venue::Binance,
            0,
            99.95,
            100.0,
            200.0,
            200.0,
            -0.0004,
            60_000,
        ),
        snapshot(Venue::Okx, 0, 100.2, 100.25, 200.0, 200.0, 0.0010, 60_000),
        snapshot(
            Venue::Bybit,
            0,
            100.05,
            100.1,
            200.0,
            200.0,
            0.00035,
            60_000,
        ),
    ]);
    let transfer_view = TransferStatusView::from_statuses(vec![
        transfer_status(Venue::Binance, "BTC", true, true),
        transfer_status(Venue::Okx, "BTC", false, false),
        transfer_status(Venue::Bybit, "BTC", true, true),
    ]);

    let candidates = discover_candidates(&config, &market, None, Some(&transfer_view));

    let best = candidates.first().expect("best candidate");
    let degraded = candidates
        .iter()
        .find(|candidate| candidate.short_venue == Venue::Okx)
        .expect("degraded candidate");
    assert_eq!(best.long_venue, Venue::Binance);
    assert_eq!(best.short_venue, Venue::Bybit);
    assert!(degraded.is_tradeable());
    assert_eq!(degraded.transfer_state.as_deref(), Some("degraded"));
}

#[tokio::test]
async fn restart_with_position_mismatch_goes_fail_closed() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, true);
    let event_log_path = config.persistence.event_log_path.clone();
    let adapters = adapters_for_capture();
    let mut engine = Engine::new(config.clone(), to_dyn(adapters.clone()))
        .await
        .expect("engine");

    engine.tick().await.expect("entry tick");
    let qty = engine
        .state()
        .open_position
        .as_ref()
        .expect("open position")
        .quantity;

    adapters.0.set_position_size("BTCUSDT", qty * 0.5);

    drop(engine);

    let mut restarted = Engine::new(config, to_dyn(adapters))
        .await
        .expect("restart");
    restarted.tick().await.expect("reconcile tick");

    assert_eq!(restarted.state().mode, EngineMode::FailClosed);
    assert!(restarted.state().open_position.is_some());
    assert!(restarted
        .state()
        .last_error
        .as_ref()
        .expect("error")
        .contains("exposure mismatch"));
    let records = read_event_records(&event_log_path);
    assert!(has_event(&records, "recovery.blocked"));
}

#[tokio::test]
async fn one_leg_failure_is_compensated_without_leaving_exposure() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, true);
    let adapters = adapters_for_capture();
    adapters.0.fail_next_orders(1);
    let mut engine = Engine::new(config, to_dyn(adapters.clone()))
        .await
        .expect("engine");

    engine.tick().await.expect("tick");

    assert!(engine.state().open_position.is_none());
    assert_eq!(engine.state().mode, EngineMode::Running);
    assert_eq!(adapters.0.position_size("BTCUSDT"), 0.0);
    assert_eq!(adapters.1.position_size("BTCUSDT"), 0.0);
}

#[tokio::test]
async fn restart_recovers_open_position_from_journal_when_snapshot_missing() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, true);
    let adapters = adapters_for_capture();
    let mut engine = Engine::new(config.clone(), to_dyn(adapters.clone()))
        .await
        .expect("engine");

    engine.tick().await.expect("entry tick");
    assert!(engine.state().open_position.is_some());
    fs::remove_file(&config.persistence.snapshot_path).expect("remove snapshot");

    drop(engine);

    let mut restart_config = config.clone();
    restart_config.strategy.post_funding_hold_secs = 60;
    let mut restarted = Engine::new(restart_config, to_dyn(adapters))
        .await
        .expect("restart");
    restarted.tick().await.expect("reconcile tick");

    assert_eq!(restarted.state().mode, EngineMode::Running);
    assert!(restarted.state().open_position.is_some());
    let events = fs::read_to_string(config.persistence.event_log_path).expect("events");
    assert!(events.contains("\"recovery.resumed\""));
    assert!(!events.contains("\"recovery.live_detected\""));
}

#[tokio::test]
async fn startup_prunes_flat_snapshot_positions_before_first_tick() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, true);
    let adapters = adapters_for_capture();
    let mut engine = Engine::new(config.clone(), to_dyn(adapters.clone()))
        .await
        .expect("engine");

    engine.tick().await.expect("entry tick");
    assert!(engine.state().open_position.is_some());
    adapters.0.set_position_size("BTCUSDT", 0.0);
    adapters.1.set_position_size("BTCUSDT", 0.0);
    drop(engine);

    let restarted = Engine::new(config.clone(), to_dyn(adapters))
        .await
        .expect("restart");

    assert_eq!(restarted.state().mode, EngineMode::Running);
    assert!(restarted.state().open_position.is_none());
    assert!(restarted.state().open_positions.is_empty());
    let events = fs::read_to_string(config.persistence.event_log_path).expect("events");
    assert!(events.contains("\"recovery.flat\""));
}

#[tokio::test]
async fn startup_prunes_flat_journal_positions_before_first_tick() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, true);
    let adapters = adapters_for_capture();
    let mut engine = Engine::new(config.clone(), to_dyn(adapters.clone()))
        .await
        .expect("engine");

    engine.tick().await.expect("entry tick");
    assert!(engine.state().open_position.is_some());
    adapters.0.set_position_size("BTCUSDT", 0.0);
    adapters.1.set_position_size("BTCUSDT", 0.0);
    fs::remove_file(&config.persistence.snapshot_path).expect("remove snapshot");
    drop(engine);

    let restarted = Engine::new(config.clone(), to_dyn(adapters))
        .await
        .expect("restart");

    assert_eq!(restarted.state().mode, EngineMode::Running);
    assert!(restarted.state().open_position.is_none());
    assert!(restarted.state().open_positions.is_empty());
    let events = fs::read_to_string(config.persistence.event_log_path).expect("events");
    assert!(events.contains("\"recovery.flat\""));
}

#[tokio::test]
async fn restart_with_more_positions_than_limit_goes_fail_closed() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    config.strategy.max_concurrent_positions = 2;

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![venue_snapshot(
            Venue::Binance,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, -0.0005, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, -0.0006, 600_000),
            ],
        )],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![venue_snapshot(
            Venue::Okx,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, 0.0018, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, 0.0016, 600_000),
            ],
        )],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");
    engine.tick().await.expect("entry tick");
    let recovered_positions = engine.state().open_positions.clone();
    assert_eq!(
        event_count(
            &read_event_records(&config.persistence.event_log_path),
            "entry.opened"
        ),
        2
    );
    drop(engine);

    let mut restart_config = config;
    restart_config.strategy.max_concurrent_positions = 1;
    let restart_binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![venue_snapshot(
            Venue::Binance,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, -0.0005, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, -0.0006, 600_000),
            ],
        )],
    ));
    let restart_okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![venue_snapshot(
            Venue::Okx,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, 0.0018, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, 0.0016, 600_000),
            ],
        )],
    ));
    for position in &recovered_positions {
        restart_binance.set_position_size(&position.symbol, position.quantity);
        restart_okx.set_position_size(&position.symbol, -position.quantity);
    }
    let restarted = Engine::new(
        restart_config.clone(),
        vec![
            restart_binance as Arc<dyn VenueAdapter>,
            restart_okx as Arc<dyn VenueAdapter>,
            Arc::new(ScriptedVenueAdapter::new(
                Venue::Bybit,
                0.6,
                vec![venue_snapshot(Venue::Bybit, 0, vec![])],
            )) as Arc<dyn VenueAdapter>,
            Arc::new(ScriptedVenueAdapter::new(
                Venue::Hyperliquid,
                0.3,
                vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
            )) as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("restart");

    assert_eq!(restarted.state().mode, EngineMode::FailClosed);
    assert!(restarted
        .state()
        .last_error
        .as_ref()
        .expect("error")
        .contains("open positions exceed configured max"));
}

#[tokio::test]
async fn restart_can_recover_from_live_positions_without_local_state() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, false);
    let adapters = adapters_for_capture();
    adapters.0.set_position_size("BTCUSDT", 5.0);
    adapters.1.set_position_size("BTCUSDT", -5.0);

    let mut engine = Engine::new(config.clone(), to_dyn(adapters))
        .await
        .expect("engine");
    engine.tick().await.expect("tick");

    assert_eq!(engine.state().mode, EngineMode::Running);
    let position = engine
        .state()
        .open_position
        .as_ref()
        .expect("recovered open position");
    assert_eq!(position.long_venue, Venue::Binance);
    assert_eq!(position.short_venue, Venue::Okx);
    assert_eq!(position.quantity, 5.0);

    let events = fs::read_to_string(config.persistence.event_log_path).expect("events");
    assert!(events.contains("\"recovery.live_detected\""));
}

#[tokio::test]
async fn restart_with_unpaired_live_position_goes_fail_closed() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, false);
    let event_log_path = config.persistence.event_log_path.clone();
    let adapters = adapters_for_capture();
    adapters.0.set_position_size("BTCUSDT", 5.0);

    let mut engine = Engine::new(config, to_dyn(adapters)).await.expect("engine");
    engine.tick().await.expect("tick");

    assert_eq!(engine.state().mode, EngineMode::FailClosed);
    assert!(engine.state().open_position.is_none());
    assert!(engine
        .state()
        .last_error
        .as_ref()
        .expect("error")
        .contains("unable to auto-recover live exposure"));
    let records = read_event_records(&event_log_path);
    assert!(has_event(&records, "recovery.live_blocked"));
}

#[tokio::test]
async fn uncertain_first_leg_is_flattened_before_engine_continues() {
    let temp = TempDir::new().expect("tempdir");
    let event_log = temp.path().join("events.jsonl");
    let snapshot_path = temp.path().join("state.json");
    let config = AppConfig {
        runtime: RuntimeConfig {
            mode: RuntimeMode::Paper,
            opportunity_source: OpportunitySourceMode::ExchangeOnly,
            chillybot_api_base: "https://api.chillybot.xyz".to_string(),
            chillybot_timeout_ms: 2_000,
            exchange_http_timeout_ms: 10_000,
            poll_interval_ms: 10,
            max_market_age_ms: 10_000,
            private_position_max_age_ms: 15_000,
            max_order_quote_age_ms: 3_000,
            uncertain_order_cooldown_ms: 60_000,
            transfer_status_cache_ms: 300_000,
            tick_failure_backoff_initial_ms: 1_000,
            tick_failure_backoff_max_ms: 30_000,
            ws_reconnect_initial_ms: 1_000,
            ws_reconnect_max_ms: 30_000,
            ws_unhealthy_after_failures: 5,
            journal_async_queue_capacity: 4_096,
            auto_trade_enabled: true,
        },
        strategy: StrategyConfig {
            entry_window_secs: 120,
            post_funding_hold_secs: 0,
            max_entry_notional: 1_000.0,
            live_max_entry_notional: 30.0,
            min_entry_leg_notional_quote: 8.0,
            max_concurrent_positions: 1,
            max_scan_minutes_before_funding: 0,
            min_scan_minutes_before_funding: 0,
            max_stagger_gap_minutes: 480,
            max_top_book_usage_ratio: 1.0,
            staggered_exit_mode: StaggeredExitMode::AfterFirstStage,
            min_funding_edge_bps: 5.0,
            min_expected_edge_bps: 0.5,
            min_worst_case_edge_bps: 0.0,
            exit_slippage_reserve_bps: 0.5,
            execution_buffer_bps: 0.5,
            capital_buffer_bps: 0.25,
            transfer_healthy_bias_bps: 0.25,
            transfer_unknown_bias_bps: 0.0,
            transfer_degraded_bias_bps: -0.5,
            profit_take_quote: 100.0,
            stop_loss_quote: 100.0,
            trailing_drawdown_quote: 50.0,
            hyperliquid_max_submit_ack_p95_ms: 1_200,
            hyperliquid_submit_ack_window_size: 5,
            hyperliquid_submit_ack_min_samples: 3,
        },
        persistence: PersistenceConfig {
            event_log_path: path(event_log),
            snapshot_path: path(snapshot_path),
        },
        venues: vec![venue(Venue::Binance, 0.5), venue(Venue::Hyperliquid, 0.3)],
        symbols: vec!["BTCUSDT".to_string()],
        directed_pairs: vec![DirectedPairConfig {
            long: Venue::Binance,
            short: Venue::Hyperliquid,
            symbols: vec!["BTCUSDT".to_string()],
        }],
    };

    let hyper = Arc::new(UncertainFillAdapter::new(
        Venue::Hyperliquid,
        snapshot(
            Venue::Hyperliquid,
            0,
            100.2,
            100.25,
            200.0,
            200.0,
            0.0010,
            60_000,
        ),
    ));
    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![snapshot(
            Venue::Binance,
            0,
            99.95,
            100.0,
            200.0,
            200.0,
            -0.0004,
            60_000,
        )],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            hyper.clone() as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");

    assert!(engine.state().open_position.is_none());
    assert_eq!(hyper.position_size("BTCUSDT"), 0.0);
}

#[tokio::test]
async fn uncertain_venue_is_cooled_down_before_next_entry_attempt() {
    let temp = TempDir::new().expect("tempdir");
    let config = AppConfig {
        runtime: RuntimeConfig {
            mode: RuntimeMode::Paper,
            opportunity_source: OpportunitySourceMode::ExchangeOnly,
            chillybot_api_base: "https://api.chillybot.xyz".to_string(),
            chillybot_timeout_ms: 2_000,
            exchange_http_timeout_ms: 10_000,
            poll_interval_ms: 10,
            max_market_age_ms: 10_000,
            private_position_max_age_ms: 15_000,
            max_order_quote_age_ms: 3_000,
            uncertain_order_cooldown_ms: 60_000,
            transfer_status_cache_ms: 300_000,
            tick_failure_backoff_initial_ms: 1_000,
            tick_failure_backoff_max_ms: 30_000,
            ws_reconnect_initial_ms: 1_000,
            ws_reconnect_max_ms: 30_000,
            ws_unhealthy_after_failures: 5,
            journal_async_queue_capacity: 4_096,
            auto_trade_enabled: true,
        },
        strategy: StrategyConfig {
            entry_window_secs: 120,
            post_funding_hold_secs: 0,
            max_entry_notional: 1_000.0,
            live_max_entry_notional: 30.0,
            min_entry_leg_notional_quote: 8.0,
            max_concurrent_positions: 1,
            max_scan_minutes_before_funding: 0,
            min_scan_minutes_before_funding: 0,
            max_stagger_gap_minutes: 480,
            max_top_book_usage_ratio: 1.0,
            staggered_exit_mode: StaggeredExitMode::AfterFirstStage,
            min_funding_edge_bps: 5.0,
            min_expected_edge_bps: 0.5,
            min_worst_case_edge_bps: 0.0,
            exit_slippage_reserve_bps: 0.5,
            execution_buffer_bps: 0.5,
            capital_buffer_bps: 0.25,
            transfer_healthy_bias_bps: 0.25,
            transfer_unknown_bias_bps: 0.0,
            transfer_degraded_bias_bps: -0.5,
            profit_take_quote: 100.0,
            stop_loss_quote: 100.0,
            trailing_drawdown_quote: 50.0,
            hyperliquid_max_submit_ack_p95_ms: 1_200,
            hyperliquid_submit_ack_window_size: 5,
            hyperliquid_submit_ack_min_samples: 3,
        },
        persistence: PersistenceConfig {
            event_log_path: path(temp.path().join("events.jsonl")),
            snapshot_path: path(temp.path().join("state.json")),
        },
        venues: vec![venue(Venue::Binance, 0.5), venue(Venue::Hyperliquid, 0.3)],
        symbols: vec!["BTCUSDT".to_string()],
        directed_pairs: vec![DirectedPairConfig {
            long: Venue::Binance,
            short: Venue::Hyperliquid,
            symbols: vec!["BTCUSDT".to_string()],
        }],
    };

    let hyper = Arc::new(UncertainFillAdapter::new(
        Venue::Hyperliquid,
        snapshot(
            Venue::Hyperliquid,
            0,
            100.2,
            100.25,
            200.0,
            200.0,
            0.0010,
            60_000,
        ),
    ));
    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![snapshot(
            Venue::Binance,
            0,
            99.95,
            100.0,
            200.0,
            200.0,
            -0.0004,
            60_000,
        )],
    ));

    let mut engine = Engine::new(
        config,
        vec![
            binance as Arc<dyn VenueAdapter>,
            hyper.clone() as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("first tick");
    engine.tick().await.expect("second tick");

    assert!(engine.state().open_position.is_none());
    let best = engine
        .state()
        .last_scan
        .as_ref()
        .and_then(|scan| scan.best_candidate.as_ref())
        .expect("best candidate");
    assert!(best
        .blocked_reasons
        .iter()
        .any(|item| item.contains("venue_entry_cooldown:hyperliquid")));
    assert_eq!(hyper.position_size("BTCUSDT"), 0.0);
}

#[tokio::test]
async fn live_cached_positions_block_entry_when_pair_is_not_flat() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.runtime.max_order_quote_age_ms = 0;
    config.strategy.max_entry_notional = 10.0;
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: vec!["BTCUSDT".to_string()],
    }];

    let binance = Arc::new(CachedPositionAdapter::new(
        Venue::Binance,
        snapshot(
            Venue::Binance,
            0,
            100.0,
            100.0,
            200.0,
            200.0,
            -0.0005,
            60_000,
        ),
        0.2,
        0.0,
    ));
    let okx = Arc::new(CachedPositionAdapter::new(
        Venue::Okx,
        snapshot(Venue::Okx, 0, 100.0, 100.0, 200.0, 200.0, 0.0015, 60_000),
        0.0,
        0.0,
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config,
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");
    engine.tick().await.expect("tick");

    assert!(engine.state().open_position.is_none());
    let best = engine
        .state()
        .last_scan
        .as_ref()
        .and_then(|scan| scan.best_candidate.as_ref())
        .expect("best candidate");
    assert!(best
        .blocked_reasons
        .iter()
        .any(|item| item.starts_with("cached_position_imbalance:")));
}

#[tokio::test]
async fn outside_funding_scan_window_skips_entry_discovery() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.strategy.entry_window_secs = 3_600;
    config.strategy.max_scan_minutes_before_funding = 25;
    config.strategy.min_scan_minutes_before_funding = 5;

    let adapters = VenueSet(
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![venue_snapshot(
                Venue::Binance,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, -0.0005, 1_800_000,
                )],
            )],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![venue_snapshot(
                Venue::Okx,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, 0.0015, 1_800_000,
                )],
            )],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Bybit,
            0.6,
            vec![venue_snapshot(
                Venue::Bybit,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, 0.0002, 1_800_000,
                )],
            )],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Hyperliquid,
            0.3,
            vec![venue_snapshot(
                Venue::Hyperliquid,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, 0.0001, 1_800_000,
                )],
            )],
        )),
    );

    let mut engine = Engine::new(config.clone(), to_dyn(adapters))
        .await
        .expect("engine");
    engine.tick().await.expect("tick");

    assert!(engine.state().open_position.is_none());
    assert!(engine.state().last_scan.is_none());

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(!has_event(&records, "scan.completed"));
}

#[tokio::test]
async fn soft_market_failures_on_all_venues_still_record_empty_scan() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, false);
    config.runtime.auto_trade_enabled = true;
    config.strategy.max_scan_minutes_before_funding = 15;

    let adapters: Vec<Arc<dyn VenueAdapter>> = vec![
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Binance,
            "binance market snapshot unavailable for requested symbols",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Okx,
            "okx instrument metadata missing for FET-USDT-SWAP",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Bybit,
            "bybit ticker missing for FETUSDT",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Hyperliquid,
            "hyperliquid symbol not supported for FETUSDT",
        )),
    ];

    let mut engine = Engine::new(config.clone(), adapters).await.expect("engine");
    engine.tick().await.expect("tick");

    let scan = engine.state().last_scan.as_ref().expect("last scan");
    assert_eq!(scan.candidate_count, 0);
    assert_eq!(scan.tradeable_count, 0);

    sleep(Duration::from_millis(250)).await;
    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "scan.completed"));
    assert!(has_event(&records, "scan.no_entry_diagnostics"));
    assert!(has_event(&records, "market.fetch_failed"));
}

#[tokio::test]
async fn repeated_soft_market_failures_are_aggregated_across_ticks() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, false);

    let adapters: Vec<Arc<dyn VenueAdapter>> = vec![
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Binance,
            "binance ticker missing for FETUSDT",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Okx,
            "okx instrument metadata missing for FET-USDT-SWAP",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Bybit,
            "bybit ticker missing for FETUSDT",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Hyperliquid,
            "hyperliquid symbol not supported for FETUSDT",
        )),
    ];

    let mut engine = Engine::new(config.clone(), adapters).await.expect("engine");
    for _ in 0..3 {
        engine.tick().await.expect("tick");
    }

    sleep(Duration::from_millis(250)).await;
    let records = read_event_records(&config.persistence.event_log_path);
    assert_eq!(event_count(&records, "market.fetch_failed"), 4);
}

#[tokio::test]
async fn live_scan_skips_venues_with_balance_below_minimum() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, false);
    config.runtime.mode = RuntimeMode::Live;
    config.strategy.entry_window_secs = 3_600;
    config.symbols = vec!["BTCUSDT".to_string()];
    config.venues = vec![venue(Venue::Binance, 0.5), venue(Venue::Okx, 0.5)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    let now_ms = chrono::Utc::now().timestamp_millis();
    let funding_timestamp_ms = now_ms + 60_000;

    let binance = Arc::new(
        ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![venue_snapshot(
                Venue::Binance,
                now_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.0,
                    100.1,
                    500.0,
                    500.0,
                    -0.0005,
                    funding_timestamp_ms,
                )],
            )],
        )
        .with_balance_snapshot(120.0, Some(120.0), Some(120.0))
        .with_entry_balance_gate(),
    );
    let okx = Arc::new(
        ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![venue_snapshot(
                Venue::Okx,
                now_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.2,
                    100.3,
                    500.0,
                    500.0,
                    0.0015,
                    funding_timestamp_ms,
                )],
            )],
        )
        .with_balance_snapshot(40.0, Some(40.0), Some(40.0))
        .with_entry_balance_gate(),
    );

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");
    sleep(Duration::from_millis(100)).await;

    let scan = engine.state().last_scan.as_ref().expect("scan");
    assert_eq!(scan.candidate_count, 0);
    assert_eq!(scan.tradeable_count, 0);

    let records = read_event_records(&config.persistence.event_log_path);
    let filter_event = records
        .iter()
        .find(|record| record_kind(record) == Some("runtime.entry_venue_filter.refreshed"))
        .expect("filter event");
    let statuses = filter_event["payload"]["statuses"]
        .as_array()
        .expect("statuses");
    let okx_status = statuses
        .iter()
        .find(|item| item["venue"] == "okx")
        .expect("okx status");
    assert_eq!(okx_status["eligible"], false);
    assert_eq!(okx_status["reason"], "balance_below_minimum");
    assert_eq!(
        okx_status["effective_balance_quote"].as_f64(),
        Some(40.0)
    );
}

#[tokio::test]
async fn live_scan_skips_venues_with_balance_fetch_failures() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, false);
    config.runtime.mode = RuntimeMode::Live;
    config.strategy.entry_window_secs = 3_600;
    config.symbols = vec!["BTCUSDT".to_string()];
    config.venues = vec![venue(Venue::Binance, 0.5), venue(Venue::Okx, 0.5)];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    let now_ms = chrono::Utc::now().timestamp_millis();
    let funding_timestamp_ms = now_ms + 60_000;

    let binance = Arc::new(
        ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![venue_snapshot(
                Venue::Binance,
                now_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.0,
                    100.1,
                    500.0,
                    500.0,
                    -0.0005,
                    funding_timestamp_ms,
                )],
            )],
        )
        .with_balance_snapshot(120.0, Some(120.0), Some(120.0))
        .with_entry_balance_gate(),
    );
    let okx = Arc::new(
        ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![venue_snapshot(
                Venue::Okx,
                now_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.2,
                    100.3,
                    500.0,
                    500.0,
                    0.0015,
                    funding_timestamp_ms,
                )],
            )],
        )
        .with_balance_fetch_error("okx login failed: code=50113 msg=invalid signature")
        .with_entry_balance_gate(),
    );

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");
    sleep(Duration::from_millis(100)).await;

    let scan = engine.state().last_scan.as_ref().expect("scan");
    assert_eq!(scan.candidate_count, 0);
    assert_eq!(scan.tradeable_count, 0);

    let records = read_event_records(&config.persistence.event_log_path);
    let filter_event = records
        .iter()
        .find(|record| record_kind(record) == Some("runtime.entry_venue_filter.refreshed"))
        .expect("filter event");
    let statuses = filter_event["payload"]["statuses"]
        .as_array()
        .expect("statuses");
    let okx_status = statuses
        .iter()
        .find(|item| item["venue"] == "okx")
        .expect("okx status");
    assert_eq!(okx_status["eligible"], false);
    assert_eq!(okx_status["reason"], "account_balance_fetch_failed");
    assert!(
        okx_status["error"]
            .as_str()
            .expect("error")
            .contains("code=50113")
    );
}

#[tokio::test]
async fn hard_market_failures_on_all_venues_still_fail_tick() {
    let temp = TempDir::new().expect("tempdir");
    let config = test_config(&temp, false);

    let adapters: Vec<Arc<dyn VenueAdapter>> = vec![
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Binance,
            "failed to request binance book ticker",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Okx,
            "failed to request okx order book",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Bybit,
            "failed to request bybit tickers",
        )),
        Arc::new(SoftFailingMarketAdapter::new(
            Venue::Hyperliquid,
            "failed to request hyperliquid l2 book",
        )),
    ];

    let mut engine = Engine::new(config.clone(), adapters).await.expect("engine");
    let error = engine.tick().await.expect_err("hard failure");
    assert!(error
        .to_string()
        .contains("market fetch failed on all venues"));

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(!has_event(&records, "scan.completed"));
}

#[test]
fn live_candidate_sizing_is_capped_to_30_quote_per_leg() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, false);
    config.runtime.mode = RuntimeMode::Live;
    config.strategy.max_entry_notional = 1_000.0;
    config.strategy.live_max_entry_notional = 30.0;
    config.strategy.entry_window_secs = 3_600;

    let market = MarketView::from_snapshots(vec![
        venue_snapshot(
            Venue::Binance,
            0,
            vec![symbol_snapshot(
                "BTCUSDT", 100.0, 100.0, 500.0, 500.0, -0.0005, 600_000,
            )],
        ),
        venue_snapshot(
            Venue::Okx,
            0,
            vec![symbol_snapshot(
                "BTCUSDT", 100.0, 100.0, 500.0, 500.0, 0.0015, 600_000,
            )],
        ),
    ]);

    let best = discover_candidates(&config, &market, None, None)
        .into_iter()
        .find(|candidate| {
            candidate.long_venue == Venue::Binance && candidate.short_venue == Venue::Okx
        })
        .expect("candidate");

    assert!(best.entry_notional_quote <= 30.0 + 1e-9);
}

#[tokio::test]
async fn live_quote_refresh_unblocks_entry_after_stale_selection_snapshot() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.runtime.max_order_quote_age_ms = 3_000;
    config.symbols = vec!["BTCUSDT".to_string()];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let stale_observed_at_ms = now_ms - 4_100;
    let funding_timestamp_ms = now_ms + 60_000;

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![
            venue_snapshot(
                Venue::Binance,
                stale_observed_at_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.0,
                    100.1,
                    500.0,
                    500.0,
                    -0.0005,
                    funding_timestamp_ms,
                )],
            ),
            venue_snapshot(
                Venue::Binance,
                now_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.0,
                    100.1,
                    500.0,
                    500.0,
                    -0.0005,
                    funding_timestamp_ms,
                )],
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![
            venue_snapshot(
                Venue::Okx,
                stale_observed_at_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.2,
                    100.3,
                    500.0,
                    500.0,
                    0.0015,
                    funding_timestamp_ms,
                )],
            ),
            venue_snapshot(
                Venue::Okx,
                now_ms,
                vec![symbol_snapshot(
                    "BTCUSDT",
                    100.2,
                    100.3,
                    500.0,
                    500.0,
                    0.0015,
                    funding_timestamp_ms,
                )],
            ),
        ],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, now_ms, Vec::new())],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, now_ms, Vec::new())],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");

    assert!(engine.state().open_position.is_some());
    sleep(Duration::from_millis(250)).await;
    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "execution.order_quote_refreshed"));
    assert!(has_event(&records, "entry.opened"));
}

#[tokio::test]
async fn entry_market_refresh_unblocks_candidate_stale_gate_before_order_submission() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.runtime.max_order_quote_age_ms = 3_000;
    config.runtime.max_market_age_ms = 60_000;
    config.symbols = vec!["ENJUSDT".to_string()];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let stale_observed_at_ms = now_ms - 7_100;
    let funding_timestamp_ms = now_ms + 60_000;

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![
            venue_snapshot(
                Venue::Binance,
                stale_observed_at_ms,
                vec![symbol_snapshot(
                    "ENJUSDT",
                    100.0,
                    100.1,
                    500.0,
                    500.0,
                    -0.0005,
                    funding_timestamp_ms,
                )],
            ),
            venue_snapshot(
                Venue::Binance,
                now_ms,
                vec![symbol_snapshot(
                    "ENJUSDT",
                    100.0,
                    100.1,
                    500.0,
                    500.0,
                    -0.0005,
                    funding_timestamp_ms,
                )],
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![
            venue_snapshot(
                Venue::Okx,
                stale_observed_at_ms,
                vec![symbol_snapshot(
                    "ENJUSDT",
                    100.2,
                    100.3,
                    500.0,
                    500.0,
                    0.0015,
                    funding_timestamp_ms,
                )],
            ),
            venue_snapshot(
                Venue::Okx,
                now_ms,
                vec![symbol_snapshot(
                    "ENJUSDT",
                    100.2,
                    100.3,
                    500.0,
                    500.0,
                    0.0015,
                    funding_timestamp_ms,
                )],
            ),
        ],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, now_ms, Vec::new())],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, now_ms, Vec::new())],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");

    sleep(Duration::from_millis(250)).await;
    let records = read_event_records(&config.persistence.event_log_path);
    assert!(
        engine.state().open_position.is_some(),
        "open position missing; last_error={:?} kinds={:?}",
        engine.state().last_error,
        records.iter().filter_map(record_kind).collect::<Vec<_>>()
    );
    assert!(has_event(&records, "execution.entry_quote_refreshed"));
    let verification = records
        .iter()
        .find(|record| record_kind(record) == Some("execution.entry_quote_verification"))
        .expect("entry quote verification");
    assert_eq!(verification["payload"]["refresh_count"].as_u64(), Some(2));
    assert!(verification["payload"]["max_pre_refresh_quote_age_ms"]
        .as_i64()
        .map(|age_ms| age_ms >= 7_100)
        .unwrap_or(false));
    assert!(verification["payload"]["block_reason"].is_null());
    let legs = verification["payload"]["legs"]
        .as_array()
        .expect("verification legs");
    assert_eq!(legs.len(), 2);
    assert!(legs.iter().all(|leg| {
        leg["pre_refresh_age_ms"]
            .as_i64()
            .map(|age_ms| age_ms >= 7_100)
            .unwrap_or(false)
            && leg["refresh_succeeded"].as_bool() == Some(true)
            && leg["refreshed_observed_at_ms"].as_i64().is_some()
            && leg["post_refresh_age_ms"].as_i64().is_some()
    }));
    assert!(has_event(&records, "execution.entry_prepare_timing"));
    assert!(has_event(&records, "entry.opened"));
    assert!(!records.iter().any(|record| {
        record_kind(record) == Some("execution.entry_blocked")
            && record["payload"]["reason"]
                .as_str()
                .map(|reason| reason.contains("entry_quote_stale"))
                .unwrap_or(false)
    }));
}

#[tokio::test]
async fn backup_candidate_is_tried_after_first_candidate_rounds_to_zero() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.symbols = vec!["FETUSDT".to_string(), "BTCUSDT".to_string()];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    config.strategy.max_concurrent_positions = 1;
    config.strategy.max_entry_notional = 30.0;
    config.strategy.live_max_entry_notional = 30.0;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let funding_timestamp_ms = now_ms + 60_000;

    let market_symbols = vec![
        symbol_snapshot(
            "FETUSDT",
            1.0,
            1.0,
            10_000.0,
            10_000.0,
            -0.0007,
            funding_timestamp_ms,
        ),
        symbol_snapshot(
            "BTCUSDT",
            100.0,
            100.0,
            10_000.0,
            10_000.0,
            -0.0004,
            funding_timestamp_ms,
        ),
    ];
    let hedge_symbols = vec![
        symbol_snapshot(
            "FETUSDT",
            1.01,
            1.01,
            10_000.0,
            10_000.0,
            0.0017,
            funding_timestamp_ms,
        ),
        symbol_snapshot(
            "BTCUSDT",
            100.1,
            100.1,
            10_000.0,
            10_000.0,
            0.0015,
            funding_timestamp_ms,
        ),
    ];

    let binance = Arc::new(SelectiveNormalizeAdapter::new(
        Venue::Binance,
        vec![venue_snapshot(Venue::Binance, now_ms, market_symbols)],
    ));
    let okx = Arc::new(SelectiveNormalizeAdapter::new(
        Venue::Okx,
        vec![venue_snapshot(Venue::Okx, now_ms, hedge_symbols)],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, now_ms, Vec::new())],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, now_ms, Vec::new())],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");

    assert!(engine.state().open_position.is_some());
    let position = engine.state().open_position.as_ref().expect("position");
    assert_eq!(position.symbol, "BTCUSDT");
    sleep(Duration::from_millis(250)).await;
    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "execution.entry_blocked"));
    assert!(has_event(&records, "entry.opened"));
}

#[tokio::test]
async fn live_candidate_is_blocked_when_perp_liquidity_is_unavailable() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.strategy.max_scan_minutes_before_funding = 25;
    config.strategy.min_scan_minutes_before_funding = 5;
    config.symbols = vec!["BTCUSDT".to_string()];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];

    let binance = Arc::new(
        ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![venue_snapshot(
                Venue::Binance,
                2_100_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 100.0, 500.0, 500.0, -0.0001, 3_600_000,
                )],
            )],
        )
        .with_perp_liquidity_snapshot("BTCUSDT", 8_000_000.0, 2_000_000.0),
    );
    let okx = Arc::new(
        ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![venue_snapshot(
                Venue::Okx,
                2_100_000,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 100.0, 500.0, 500.0, 0.0012, 3_600_000,
                )],
            )],
        )
        .with_perp_liquidity_error("BTCUSDT", "okx open interest unavailable"),
    );
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 2_100_000, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 2_100_000, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance as Arc<dyn VenueAdapter>,
            okx as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");
    engine.shutdown().await.expect("shutdown");

    assert!(engine.state().open_positions.is_empty());

    let records = read_event_records(&config.persistence.event_log_path);
    assert!(has_event(&records, "execution.entry_liquidity_unavailable"));
    let diagnostics = records
        .iter()
        .find(|record| record_kind(record) == Some("scan.no_entry_diagnostics"))
        .expect("no entry diagnostics");
    assert!(diagnostics["payload"]["blocked_reason_counts"]
        .as_object()
        .expect("blocked counts")
        .contains_key("perp_liquidity_unavailable:okx"));
}

#[tokio::test]
async fn entry_is_blocked_when_effective_leg_notional_falls_below_global_minimum() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.runtime.mode = RuntimeMode::Live;
    config.symbols = vec!["BTCUSDT".to_string()];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    config.strategy.max_entry_notional = 30.0;
    config.strategy.live_max_entry_notional = 30.0;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let funding_timestamp_ms = now_ms + 60_000;
    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![venue_snapshot(
            Venue::Binance,
            now_ms,
            vec![symbol_snapshot(
                "BTCUSDT",
                1_000.0,
                1_000.0,
                0.007,
                0.007,
                -0.0004,
                funding_timestamp_ms,
            )],
        )],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![venue_snapshot(
            Venue::Okx,
            now_ms,
            vec![symbol_snapshot(
                "BTCUSDT",
                1_000.0,
                1_000.0,
                0.007,
                0.007,
                0.0017,
                funding_timestamp_ms,
            )],
        )],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, now_ms, Vec::new())],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, now_ms, Vec::new())],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");
    sleep(Duration::from_millis(250)).await;

    assert!(engine.state().open_position.is_none());
    assert_eq!(binance.position_size("BTCUSDT"), 0.0);
    assert_eq!(okx.position_size("BTCUSDT"), 0.0);

    let records = read_event_records(&config.persistence.event_log_path);
    let trace = records
        .iter()
        .find(|record| record_kind(record) == Some("execution.entry_notional_trace"))
        .expect("entry notional trace");
    assert_eq!(
        trace["payload"]["target_notional_quote"].as_f64(),
        Some(30.0)
    );
    assert_eq!(
        trace["payload"]["candidate_entry_notional_quote"].as_f64(),
        Some(7.0)
    );
    assert!(
        trace["payload"]["normalized_quantity"]
            .as_f64()
            .unwrap_or_default()
            > 0.0
    );
    assert_eq!(
        trace["payload"]["long"]["venue_min_notional_quote"].as_f64(),
        Some(8.0)
    );
    assert_eq!(
        trace["payload"]["short"]["venue_min_notional_quote"].as_f64(),
        Some(8.0)
    );
    assert!(
        trace["payload"]["long"]["final_leg_notional_quote"]
            .as_f64()
            .unwrap_or_default()
            < 8.0
    );
    let blocked = records
        .iter()
        .find(|record| record_kind(record) == Some("execution.entry_blocked"))
        .expect("entry blocked event");
    assert_eq!(
        blocked["payload"]["reason"].as_str(),
        Some("entry_leg_notional_below_minimum")
    );
}

#[tokio::test]
async fn engine_opens_two_distinct_symbols_when_multiple_good_candidates_exist() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
    ];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    config.strategy.max_concurrent_positions = 2;
    config.strategy.max_entry_notional = 30.0;
    config.strategy.live_max_entry_notional = 30.0;

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![venue_snapshot(
            Venue::Binance,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, -0.0005, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, -0.0006, 600_000),
                symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, -0.0003, 600_000),
            ],
        )],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![venue_snapshot(
            Venue::Okx,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, 0.0018, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, 0.0016, 600_000),
                symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, 0.0007, 600_000),
            ],
        )],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");

    let records = read_event_records(&config.persistence.event_log_path);
    assert_eq!(event_count(&records, "entry.opened"), 2);
    assert!(binance.position_size("BTCUSDT") > 0.0);
    assert!(okx.position_size("BTCUSDT") < 0.0);
    assert!(binance.position_size("ETHUSDT") > 0.0);
    assert!(okx.position_size("ETHUSDT") < 0.0);
    assert_eq!(binance.position_size("SOLUSDT"), 0.0);
    assert_eq!(okx.position_size("SOLUSDT"), 0.0);
}

#[tokio::test]
async fn engine_skips_thinner_symbol_when_selecting_best_two() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
    ];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    config.strategy.max_concurrent_positions = 2;
    config.strategy.max_entry_notional = 30.0;
    config.strategy.live_max_entry_notional = 30.0;

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![venue_snapshot(
            Venue::Binance,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 0.3, 0.3, -0.0004, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, -0.0005, 600_000),
                symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, -0.0003, 600_000),
            ],
        )],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![venue_snapshot(
            Venue::Okx,
            0,
            vec![
                symbol_snapshot("BTCUSDT", 100.0, 100.0, 0.3, 0.3, 0.0017, 600_000),
                symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, 0.0015, 600_000),
                symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, 0.0014, 600_000),
            ],
        )],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("tick");

    assert_eq!(
        event_count(
            &read_event_records(&config.persistence.event_log_path),
            "entry.opened"
        ),
        2
    );
    assert_eq!(binance.position_size("BTCUSDT"), 0.0);
    assert_eq!(okx.position_size("BTCUSDT"), 0.0);
    assert!(binance.position_size("ETHUSDT") > 0.0);
    assert!(okx.position_size("ETHUSDT") < 0.0);
    assert!(binance.position_size("SOLUSDT") > 0.0);
    assert!(okx.position_size("SOLUSDT") < 0.0);
}

#[tokio::test]
async fn running_engine_releases_stale_slots_before_selecting_new_entries() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = test_config(&temp, true);
    config.symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
    ];
    config.directed_pairs = vec![DirectedPairConfig {
        long: Venue::Binance,
        short: Venue::Okx,
        symbols: config.symbols.clone(),
    }];
    config.strategy.entry_window_secs = 3_600;
    config.strategy.max_concurrent_positions = 2;
    config.strategy.max_entry_notional = 30.0;
    config.strategy.live_max_entry_notional = 30.0;

    let binance = Arc::new(ScriptedVenueAdapter::new(
        Venue::Binance,
        0.5,
        vec![
            venue_snapshot(
                Venue::Binance,
                0,
                vec![
                    symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, -0.0005, 600_000),
                    symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, -0.0006, 600_000),
                    symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, -0.0001, 600_000),
                ],
            ),
            venue_snapshot(
                Venue::Binance,
                1_000,
                vec![
                    symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, -0.0001, 600_000),
                    symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, -0.0001, 600_000),
                    symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, -0.0004, 600_000),
                ],
            ),
        ],
    ));
    let okx = Arc::new(ScriptedVenueAdapter::new(
        Venue::Okx,
        0.5,
        vec![
            venue_snapshot(
                Venue::Okx,
                0,
                vec![
                    symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, 0.0018, 600_000),
                    symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, 0.0016, 600_000),
                    symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, 0.0001, 600_000),
                ],
            ),
            venue_snapshot(
                Venue::Okx,
                1_000,
                vec![
                    symbol_snapshot("BTCUSDT", 100.0, 100.0, 500.0, 500.0, 0.0002, 600_000),
                    symbol_snapshot("ETHUSDT", 110.0, 110.0, 500.0, 500.0, 0.0002, 600_000),
                    symbol_snapshot("SOLUSDT", 120.0, 120.0, 500.0, 500.0, 0.0017, 600_000),
                ],
            ),
        ],
    ));
    let bybit = Arc::new(ScriptedVenueAdapter::new(
        Venue::Bybit,
        0.6,
        vec![venue_snapshot(Venue::Bybit, 0, vec![])],
    ));
    let hyper = Arc::new(ScriptedVenueAdapter::new(
        Venue::Hyperliquid,
        0.3,
        vec![venue_snapshot(Venue::Hyperliquid, 0, vec![])],
    ));

    let mut engine = Engine::new(
        config.clone(),
        vec![
            binance.clone() as Arc<dyn VenueAdapter>,
            okx.clone() as Arc<dyn VenueAdapter>,
            bybit as Arc<dyn VenueAdapter>,
            hyper as Arc<dyn VenueAdapter>,
        ],
    )
    .await
    .expect("engine");

    engine.tick().await.expect("initial tick");
    assert_eq!(engine.state().open_positions.len(), 2);

    for symbol in ["BTCUSDT", "ETHUSDT"] {
        binance.set_position_size(symbol, 0.0);
        okx.set_position_size(symbol, 0.0);
    }

    engine.tick().await.expect("reconcile tick");
    sleep(Duration::from_millis(250)).await;

    assert_eq!(engine.state().open_positions.len(), 1);
    assert_eq!(engine.state().open_positions[0].symbol, "SOLUSDT");
    assert_eq!(binance.position_size("BTCUSDT"), 0.0);
    assert_eq!(okx.position_size("BTCUSDT"), 0.0);
    assert_eq!(binance.position_size("ETHUSDT"), 0.0);
    assert_eq!(okx.position_size("ETHUSDT"), 0.0);
    assert!(binance.position_size("SOLUSDT") > 0.0);
    assert!(okx.position_size("SOLUSDT") < 0.0);

    let records = read_event_records(&config.persistence.event_log_path);
    assert_eq!(event_count(&records, "entry.opened"), 3);
    assert_eq!(event_count(&records, "recovery.flat"), 2);
    assert!(has_event(&records, "runtime.slot_reconcile"));
}

fn test_config(temp: &TempDir, auto_trade_enabled: bool) -> AppConfig {
    let event_log = temp.path().join("events.jsonl");
    let snapshot = temp.path().join("state.json");
    AppConfig {
        runtime: RuntimeConfig {
            mode: RuntimeMode::Paper,
            opportunity_source: OpportunitySourceMode::ExchangeOnly,
            chillybot_api_base: "https://api.chillybot.xyz".to_string(),
            chillybot_timeout_ms: 2_000,
            exchange_http_timeout_ms: 10_000,
            poll_interval_ms: 10,
            max_market_age_ms: 10_000,
            private_position_max_age_ms: 15_000,
            max_order_quote_age_ms: 3_000,
            uncertain_order_cooldown_ms: 30_000,
            transfer_status_cache_ms: 300_000,
            tick_failure_backoff_initial_ms: 1_000,
            tick_failure_backoff_max_ms: 30_000,
            ws_reconnect_initial_ms: 1_000,
            ws_reconnect_max_ms: 30_000,
            ws_unhealthy_after_failures: 5,
            journal_async_queue_capacity: 4_096,
            auto_trade_enabled,
        },
        strategy: StrategyConfig {
            entry_window_secs: 120,
            post_funding_hold_secs: 0,
            max_entry_notional: 1_000.0,
            live_max_entry_notional: 30.0,
            min_entry_leg_notional_quote: 8.0,
            max_concurrent_positions: 1,
            max_scan_minutes_before_funding: 0,
            min_scan_minutes_before_funding: 0,
            max_stagger_gap_minutes: 480,
            max_top_book_usage_ratio: 1.0,
            staggered_exit_mode: StaggeredExitMode::AfterFirstStage,
            min_funding_edge_bps: 5.0,
            min_expected_edge_bps: 0.5,
            min_worst_case_edge_bps: 0.0,
            exit_slippage_reserve_bps: 0.5,
            execution_buffer_bps: 0.5,
            capital_buffer_bps: 0.25,
            transfer_healthy_bias_bps: 0.25,
            transfer_unknown_bias_bps: 0.0,
            transfer_degraded_bias_bps: -0.5,
            profit_take_quote: 100.0,
            stop_loss_quote: 100.0,
            trailing_drawdown_quote: 50.0,
            hyperliquid_max_submit_ack_p95_ms: 1_200,
            hyperliquid_submit_ack_window_size: 5,
            hyperliquid_submit_ack_min_samples: 3,
        },
        persistence: PersistenceConfig {
            event_log_path: path(event_log),
            snapshot_path: path(snapshot),
        },
        venues: vec![
            venue(Venue::Binance, 0.5),
            venue(Venue::Okx, 0.5),
            venue(Venue::Bybit, 0.6),
            venue(Venue::Hyperliquid, 0.3),
        ],
        symbols: vec!["BTCUSDT".to_string()],
        directed_pairs: vec![
            DirectedPairConfig {
                long: Venue::Binance,
                short: Venue::Okx,
                symbols: vec!["BTCUSDT".to_string()],
            },
            DirectedPairConfig {
                long: Venue::Binance,
                short: Venue::Bybit,
                symbols: vec!["BTCUSDT".to_string()],
            },
            DirectedPairConfig {
                long: Venue::Binance,
                short: Venue::Hyperliquid,
                symbols: vec!["BTCUSDT".to_string()],
            },
        ],
    }
}

fn venue(venue: Venue, taker_fee_bps: f64) -> VenueConfig {
    VenueConfig {
        venue,
        enabled: true,
        taker_fee_bps,
        max_notional: 1_000.0,
        market_data_file: None,
        live: Default::default(),
    }
}

fn path(path: PathBuf) -> String {
    path.display().to_string()
}

fn read_event_records(path: &str) -> Vec<Value> {
    fs::read_to_string(path)
        .expect("events")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).expect("event json"))
        .collect()
}

fn record_kind(record: &Value) -> Option<&str> {
    record.get("kind").and_then(Value::as_str)
}

fn has_event(records: &[Value], kind: &str) -> bool {
    records
        .iter()
        .any(|record| record_kind(record) == Some(kind))
}

fn event_count(records: &[Value], kind: &str) -> usize {
    records
        .iter()
        .filter(|record| record_kind(record) == Some(kind))
        .count()
}

fn transfer_status(
    venue: Venue,
    asset: &str,
    deposit_enabled: bool,
    withdraw_enabled: bool,
) -> AssetTransferStatus {
    AssetTransferStatus {
        venue,
        asset: asset.to_string(),
        deposit_enabled,
        withdraw_enabled,
        observed_at_ms: 0,
        source: "test".to_string(),
    }
}

fn ample_perp_liquidity_snapshot(
    venue: Venue,
    symbol: &str,
    observed_at_ms: i64,
) -> lightfee::PerpLiquiditySnapshot {
    lightfee::PerpLiquiditySnapshot {
        venue,
        symbol: symbol.to_string(),
        volume_24h_quote: 10_000_000_000.0,
        open_interest_quote: 10_000_000_000.0,
        observed_at_ms,
    }
}

fn adapters_for_ranking() -> VenueSet {
    VenueSet(
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![snapshot(
                Venue::Binance,
                0,
                99.95,
                100.0,
                200.0,
                200.0,
                -0.0004,
                60_000,
            )],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![snapshot(
                Venue::Okx,
                0,
                100.2,
                100.25,
                200.0,
                200.0,
                0.0010,
                60_000,
            )],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Bybit,
            0.6,
            vec![snapshot(
                Venue::Bybit,
                0,
                100.05,
                100.1,
                200.0,
                200.0,
                0.00035,
                60_000,
            )],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Hyperliquid,
            0.3,
            vec![snapshot(
                Venue::Hyperliquid,
                0,
                100.0,
                100.05,
                200.0,
                200.0,
                0.0001,
                60_000,
            )],
        )),
    )
}

fn adapters_for_capture() -> VenueSet {
    VenueSet(
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![
                snapshot(
                    Venue::Binance,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0005,
                    60_000,
                ),
                snapshot(
                    Venue::Binance,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0005,
                    60_000,
                ),
                snapshot(
                    Venue::Binance,
                    361_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0005,
                    60_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![
                snapshot(Venue::Okx, 0, 100.0, 100.0, 200.0, 200.0, 0.0015, 60_000),
                snapshot(
                    Venue::Okx,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0015,
                    60_000,
                ),
                snapshot(
                    Venue::Okx,
                    361_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0015,
                    60_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Bybit,
            0.6,
            vec![
                snapshot(Venue::Bybit, 0, 100.0, 100.0, 200.0, 200.0, 0.0002, 60_000),
                snapshot(
                    Venue::Bybit,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0002,
                    60_000,
                ),
                snapshot(
                    Venue::Bybit,
                    361_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0002,
                    60_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Hyperliquid,
            0.3,
            vec![
                snapshot(
                    Venue::Hyperliquid,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
                snapshot(
                    Venue::Hyperliquid,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
                snapshot(
                    Venue::Hyperliquid,
                    361_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
            ],
        )),
    )
}

fn adapters_for_staggered_capture() -> VenueSet {
    VenueSet(
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![
                snapshot(
                    Venue::Binance,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0012,
                    60_000,
                ),
                snapshot(
                    Venue::Binance,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0012,
                    60_000,
                ),
                snapshot(
                    Venue::Binance,
                    361_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0012,
                    60_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![
                snapshot(
                    Venue::Okx,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0020,
                    4 * 60 * 60 * 1_000,
                ),
                snapshot(
                    Venue::Okx,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0020,
                    4 * 60 * 60 * 1_000,
                ),
                snapshot(
                    Venue::Okx,
                    361_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0020,
                    4 * 60 * 60 * 1_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Bybit,
            0.6,
            vec![
                snapshot(Venue::Bybit, 0, 100.0, 100.0, 200.0, 200.0, 0.0002, 60_000),
                snapshot(
                    Venue::Bybit,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0002,
                    60_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Hyperliquid,
            0.3,
            vec![
                snapshot(
                    Venue::Hyperliquid,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
                snapshot(
                    Venue::Hyperliquid,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
            ],
        )),
    )
}

fn adapters_for_staggered_second_stage_capture() -> VenueSet {
    VenueSet(
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![
                snapshot(
                    Venue::Binance,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0012,
                    60_000,
                ),
                snapshot(
                    Venue::Binance,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0012,
                    60_000,
                ),
                snapshot(
                    Venue::Binance,
                    4 * 60 * 60 * 1_000 + 1_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    -0.0012,
                    60_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Okx,
            0.5,
            vec![
                snapshot(
                    Venue::Okx,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0020,
                    4 * 60 * 60 * 1_000,
                ),
                snapshot(
                    Venue::Okx,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0020,
                    4 * 60 * 60 * 1_000,
                ),
                snapshot(
                    Venue::Okx,
                    4 * 60 * 60 * 1_000 + 1_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0020,
                    4 * 60 * 60 * 1_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Bybit,
            0.6,
            vec![
                snapshot(Venue::Bybit, 0, 100.0, 100.0, 200.0, 200.0, 0.0002, 60_000),
                snapshot(
                    Venue::Bybit,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0002,
                    60_000,
                ),
                snapshot(
                    Venue::Bybit,
                    4 * 60 * 60 * 1_000 + 1_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0002,
                    60_000,
                ),
            ],
        )),
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Hyperliquid,
            0.3,
            vec![
                snapshot(
                    Venue::Hyperliquid,
                    0,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
                snapshot(
                    Venue::Hyperliquid,
                    61_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
                snapshot(
                    Venue::Hyperliquid,
                    4 * 60 * 60 * 1_000 + 1_000,
                    100.0,
                    100.0,
                    200.0,
                    200.0,
                    0.0001,
                    60_000,
                ),
            ],
        )),
    )
}

fn snapshot(
    venue: Venue,
    observed_at_ms: i64,
    best_bid: f64,
    best_ask: f64,
    bid_size: f64,
    ask_size: f64,
    funding_rate: f64,
    funding_timestamp_ms: i64,
) -> VenueMarketSnapshot {
    venue_snapshot(
        venue,
        observed_at_ms,
        vec![symbol_snapshot(
            "BTCUSDT",
            best_bid,
            best_ask,
            bid_size,
            ask_size,
            funding_rate,
            funding_timestamp_ms,
        )],
    )
}

fn venue_snapshot(
    venue: Venue,
    observed_at_ms: i64,
    symbols: Vec<SymbolMarketSnapshot>,
) -> VenueMarketSnapshot {
    VenueMarketSnapshot {
        venue,
        observed_at_ms,
        symbols,
    }
}

fn symbol_snapshot(
    symbol: &str,
    best_bid: f64,
    best_ask: f64,
    bid_size: f64,
    ask_size: f64,
    funding_rate: f64,
    funding_timestamp_ms: i64,
) -> SymbolMarketSnapshot {
    SymbolMarketSnapshot {
        symbol: symbol.to_string(),
        best_bid,
        best_ask,
        bid_size,
        ask_size,
        mark_price: None,
        funding_rate,
        funding_timestamp_ms,
    }
}

#[derive(Clone)]
struct VenueSet(
    Arc<ScriptedVenueAdapter>,
    Arc<ScriptedVenueAdapter>,
    Arc<ScriptedVenueAdapter>,
    Arc<ScriptedVenueAdapter>,
);

fn to_dyn(venues: VenueSet) -> Vec<Arc<dyn VenueAdapter>> {
    vec![venues.0, venues.1, venues.2, venues.3]
}

#[derive(Clone)]
struct MockHintSource {
    hints: Vec<OpportunityHint>,
    fail: bool,
}

#[derive(Debug)]
struct UncertainFillAdapter {
    venue: Venue,
    snapshot: VenueMarketSnapshot,
    position: std::sync::Mutex<f64>,
    uncertain_failures_remaining: std::sync::Mutex<usize>,
}

#[derive(Debug)]
struct CountingTransferAdapter {
    venue: Venue,
    snapshot: VenueMarketSnapshot,
    transfer_status: AssetTransferStatus,
    transfer_calls: Arc<Mutex<usize>>,
}

#[derive(Debug)]
struct SoftFailingMarketAdapter {
    venue: Venue,
    error: String,
}

#[derive(Debug)]
struct TimedFillAdapter {
    venue: Venue,
    snapshots: Mutex<Vec<VenueMarketSnapshot>>,
    positions: Mutex<BTreeMap<String, f64>>,
    next_order_id: Mutex<u64>,
    reconciliations: Mutex<BTreeMap<String, OrderFillReconciliation>>,
    reconciliation_calls: Arc<Mutex<usize>>,
    timing: Option<OrderExecutionTiming>,
}

#[derive(Debug)]
struct StrictRequestedSymbolsAdapter {
    venue: Venue,
    allowed_symbols: Vec<String>,
    snapshot: VenueMarketSnapshot,
}

#[derive(Debug)]
struct CachedPositionAdapter {
    venue: Venue,
    snapshot: VenueMarketSnapshot,
    cached_size: f64,
    fetched_size: f64,
}

#[derive(Debug)]
struct SelectiveNormalizeAdapter {
    venue: Venue,
    snapshots: Mutex<Vec<VenueMarketSnapshot>>,
    positions: Mutex<BTreeMap<String, f64>>,
}

impl CountingTransferAdapter {
    fn new(
        venue: Venue,
        snapshot: VenueMarketSnapshot,
        transfer_status: AssetTransferStatus,
        transfer_calls: Arc<Mutex<usize>>,
    ) -> Self {
        Self {
            venue,
            snapshot,
            transfer_status,
            transfer_calls,
        }
    }
}

impl SoftFailingMarketAdapter {
    fn new(venue: Venue, error: impl Into<String>) -> Self {
        Self {
            venue,
            error: error.into(),
        }
    }
}

impl SelectiveNormalizeAdapter {
    fn new(venue: Venue, snapshots: Vec<VenueMarketSnapshot>) -> Self {
        Self {
            venue,
            snapshots: Mutex::new(snapshots),
            positions: Mutex::new(BTreeMap::new()),
        }
    }

    fn position_size(&self, symbol: &str) -> f64 {
        self.positions
            .lock()
            .expect("lock")
            .get(symbol)
            .copied()
            .unwrap_or_default()
    }
}

impl TimedFillAdapter {
    fn new(
        venue: Venue,
        snapshots: Vec<VenueMarketSnapshot>,
        timing: Option<OrderExecutionTiming>,
    ) -> Self {
        Self {
            venue,
            snapshots: Mutex::new(snapshots),
            positions: Mutex::new(BTreeMap::new()),
            next_order_id: Mutex::new(1),
            reconciliations: Mutex::new(BTreeMap::new()),
            reconciliation_calls: Arc::new(Mutex::new(0)),
            timing,
        }
    }

    fn current_snapshot(&self) -> VenueMarketSnapshot {
        self.snapshots
            .lock()
            .expect("lock")
            .first()
            .cloned()
            .expect("snapshot")
    }

    fn set_reconciled_fill(&self, order_id: &str, reconciliation: OrderFillReconciliation) {
        self.reconciliations
            .lock()
            .expect("lock")
            .insert(order_id.to_string(), reconciliation);
    }

    fn reconciliation_call_count(&self) -> usize {
        *self.reconciliation_calls.lock().expect("lock")
    }
}

impl StrictRequestedSymbolsAdapter {
    fn new(venue: Venue, allowed_symbols: Vec<String>, snapshot: VenueMarketSnapshot) -> Self {
        Self {
            venue,
            allowed_symbols,
            snapshot,
        }
    }
}

impl CachedPositionAdapter {
    fn new(
        venue: Venue,
        snapshot: VenueMarketSnapshot,
        cached_size: f64,
        fetched_size: f64,
    ) -> Self {
        Self {
            venue,
            snapshot,
            cached_size,
            fetched_size,
        }
    }
}

impl UncertainFillAdapter {
    fn new(venue: Venue, snapshot: VenueMarketSnapshot) -> Self {
        Self {
            venue,
            snapshot,
            position: std::sync::Mutex::new(0.0),
            uncertain_failures_remaining: std::sync::Mutex::new(1),
        }
    }

    fn position_size(&self, _symbol: &str) -> f64 {
        *self.position.lock().expect("lock")
    }
}

#[async_trait]
impl VenueAdapter for UncertainFillAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, _symbols: &[String]) -> Result<VenueMarketSnapshot> {
        Ok(self.snapshot.clone())
    }

    async fn place_order(&self, request: lightfee::OrderRequest) -> Result<lightfee::OrderFill> {
        let symbol = self
            .snapshot
            .symbols
            .iter()
            .find(|item| item.symbol == request.symbol)
            .expect("symbol snapshot");
        let mut position = self.position.lock().expect("lock");
        let mut uncertain_failures = self.uncertain_failures_remaining.lock().expect("lock");
        if !request.reduce_only && *uncertain_failures > 0 {
            *uncertain_failures -= 1;
            *position += request.side.signed_qty(request.quantity);
            return Err(anyhow!(
                "hyperliquid order status uncertain after pending ack"
            ));
        }

        let quantity = if request.reduce_only {
            position.abs().min(request.quantity)
        } else {
            request.quantity
        };
        *position += request.side.signed_qty(quantity);
        Ok(lightfee::OrderFill {
            venue: self.venue,
            symbol: request.symbol,
            side: request.side,
            quantity,
            average_price: match request.side {
                lightfee::Side::Buy => symbol.best_ask,
                lightfee::Side::Sell => symbol.best_bid,
            },
            fee_quote: 0.0,
            order_id: format!("{}-test", self.venue),
            filled_at_ms: self.snapshot.observed_at_ms,
            timing: None,
        })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<lightfee::PositionSnapshot> {
        Ok(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: *self.position.lock().expect("lock"),
            updated_at_ms: self.snapshot.observed_at_ms,
        })
    }
}

#[async_trait]
impl VenueAdapter for CountingTransferAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, _symbols: &[String]) -> Result<VenueMarketSnapshot> {
        Ok(self.snapshot.clone())
    }

    async fn place_order(&self, request: lightfee::OrderRequest) -> Result<lightfee::OrderFill> {
        let symbol = self
            .snapshot
            .symbols
            .iter()
            .find(|item| item.symbol == request.symbol)
            .expect("symbol snapshot");
        Ok(lightfee::OrderFill {
            venue: self.venue,
            symbol: request.symbol,
            side: request.side,
            quantity: request.quantity,
            average_price: match request.side {
                lightfee::Side::Buy => symbol.best_ask,
                lightfee::Side::Sell => symbol.best_bid,
            },
            fee_quote: 0.0,
            order_id: format!("{}-counting", self.venue),
            filled_at_ms: self.snapshot.observed_at_ms,
            timing: None,
        })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<lightfee::PositionSnapshot> {
        Ok(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: 0.0,
            updated_at_ms: self.snapshot.observed_at_ms,
        })
    }

    async fn fetch_transfer_statuses(
        &self,
        _assets: &[String],
    ) -> Result<Vec<AssetTransferStatus>> {
        *self.transfer_calls.lock().expect("lock") += 1;
        Ok(vec![self.transfer_status.clone()])
    }
}

#[async_trait]
impl VenueAdapter for SoftFailingMarketAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, _symbols: &[String]) -> Result<VenueMarketSnapshot> {
        Err(anyhow!("{}", self.error))
    }

    async fn place_order(&self, request: lightfee::OrderRequest) -> Result<lightfee::OrderFill> {
        Err(anyhow!(
            "{} should not place order for {}",
            self.venue,
            request.symbol
        ))
    }

    async fn fetch_position(&self, symbol: &str) -> Result<lightfee::PositionSnapshot> {
        Ok(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: 0.0,
            updated_at_ms: 0,
        })
    }
}

#[async_trait]
impl VenueAdapter for TimedFillAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut snapshots = self.snapshots.lock().expect("lock");
        let snapshot = if snapshots.len() > 1 {
            snapshots.remove(0)
        } else {
            snapshots.first().cloned().expect("snapshot")
        };
        let mut filtered = snapshot.clone();
        if !symbols.is_empty() {
            filtered
                .symbols
                .retain(|item| symbols.iter().any(|symbol| symbol == &item.symbol));
        }
        Ok(filtered)
    }

    async fn place_order(&self, request: lightfee::OrderRequest) -> Result<lightfee::OrderFill> {
        let snapshot = self.current_snapshot();
        let symbol = snapshot
            .symbols
            .iter()
            .find(|item| item.symbol == request.symbol)
            .expect("symbol snapshot");
        let mut positions = self.positions.lock().expect("lock");
        let position = positions.entry(request.symbol.clone()).or_default();
        let mut quantity = request.quantity;
        if request.reduce_only {
            quantity = match request.side {
                lightfee::Side::Buy if *position < 0.0 => (-*position).min(quantity),
                lightfee::Side::Sell if *position > 0.0 => position.abs().min(quantity),
                _ => 0.0,
            };
        }
        *position += request.side.signed_qty(quantity);
        let mut next_order_id = self.next_order_id.lock().expect("lock");
        let order_id = format!("{}-timed-{}", self.venue, *next_order_id);
        *next_order_id += 1;
        Ok(lightfee::OrderFill {
            venue: self.venue,
            symbol: request.symbol,
            side: request.side,
            quantity,
            average_price: match request.side {
                lightfee::Side::Buy => symbol.best_ask,
                lightfee::Side::Sell => symbol.best_bid,
            },
            fee_quote: 0.0,
            order_id,
            filled_at_ms: snapshot.observed_at_ms,
            timing: self.timing.clone(),
        })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<lightfee::PositionSnapshot> {
        Ok(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: self
                .positions
                .lock()
                .expect("lock")
                .get(symbol)
                .copied()
                .unwrap_or_default(),
            updated_at_ms: self.current_snapshot().observed_at_ms,
        })
    }

    async fn fetch_order_fill_reconciliation(
        &self,
        _symbol: &str,
        order_id: &str,
        _client_order_id: Option<&str>,
    ) -> Result<Option<OrderFillReconciliation>> {
        *self.reconciliation_calls.lock().expect("lock") += 1;
        Ok(self
            .reconciliations
            .lock()
            .expect("lock")
            .get(order_id)
            .cloned())
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<lightfee::PerpLiquiditySnapshot>> {
        Ok(Some(ample_perp_liquidity_snapshot(
            self.venue,
            symbol,
            self.current_snapshot().observed_at_ms,
        )))
    }
}

#[async_trait]
impl VenueAdapter for StrictRequestedSymbolsAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        if symbols
            .iter()
            .any(|symbol| !self.allowed_symbols.contains(symbol))
        {
            return Err(anyhow!(
                "{} received uncached symbol request: {:?}",
                self.venue,
                symbols
            ));
        }

        let mut filtered = self.snapshot.clone();
        if !symbols.is_empty() {
            filtered
                .symbols
                .retain(|item| symbols.iter().any(|symbol| symbol == &item.symbol));
        }
        Ok(filtered)
    }

    async fn place_order(&self, request: lightfee::OrderRequest) -> Result<lightfee::OrderFill> {
        Err(anyhow!(
            "{} should not place order for {}",
            self.venue,
            request.symbol
        ))
    }

    async fn fetch_position(&self, symbol: &str) -> Result<lightfee::PositionSnapshot> {
        Ok(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: 0.0,
            updated_at_ms: self.snapshot.observed_at_ms,
        })
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<lightfee::PerpLiquiditySnapshot>> {
        Ok(Some(ample_perp_liquidity_snapshot(
            self.venue,
            symbol,
            self.snapshot.observed_at_ms,
        )))
    }
}

#[async_trait]
impl VenueAdapter for CachedPositionAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut filtered = self.snapshot.clone();
        if !symbols.is_empty() {
            filtered
                .symbols
                .retain(|item| symbols.iter().any(|symbol| symbol == &item.symbol));
        }
        Ok(filtered)
    }

    async fn place_order(&self, request: lightfee::OrderRequest) -> Result<lightfee::OrderFill> {
        Err(anyhow!(
            "{} should not place test order for {}",
            self.venue,
            request.symbol
        ))
    }

    fn cached_position(&self, symbol: &str) -> Option<lightfee::PositionSnapshot> {
        Some(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: self.cached_size,
            updated_at_ms: self.snapshot.observed_at_ms,
        })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<lightfee::PositionSnapshot> {
        Ok(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: self.fetched_size,
            updated_at_ms: self.snapshot.observed_at_ms,
        })
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<lightfee::PerpLiquiditySnapshot>> {
        Ok(Some(ample_perp_liquidity_snapshot(
            self.venue,
            symbol,
            self.snapshot.observed_at_ms,
        )))
    }
}

#[async_trait]
impl VenueAdapter for SelectiveNormalizeAdapter {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Result<VenueMarketSnapshot> {
        let mut snapshots = self.snapshots.lock().expect("lock");
        let snapshot = if snapshots.len() > 1 {
            snapshots.remove(0)
        } else {
            snapshots.first().cloned().expect("snapshot")
        };
        let mut filtered = snapshot.clone();
        if !symbols.is_empty() {
            filtered
                .symbols
                .retain(|item| symbols.iter().any(|symbol| symbol == &item.symbol));
        }
        Ok(filtered)
    }

    async fn place_order(&self, request: lightfee::OrderRequest) -> Result<lightfee::OrderFill> {
        let snapshot = self
            .snapshots
            .lock()
            .expect("lock")
            .first()
            .cloned()
            .expect("snapshot");
        let symbol = snapshot
            .symbols
            .iter()
            .find(|item| item.symbol == request.symbol)
            .expect("symbol snapshot");
        let mut positions = self.positions.lock().expect("lock");
        let position = positions.entry(request.symbol.clone()).or_default();
        *position += request.side.signed_qty(request.quantity);
        Ok(lightfee::OrderFill {
            venue: self.venue,
            symbol: request.symbol,
            side: request.side,
            quantity: request.quantity,
            average_price: match request.side {
                lightfee::Side::Buy => symbol.best_ask,
                lightfee::Side::Sell => symbol.best_bid,
            },
            fee_quote: 0.0,
            order_id: format!("{}-selective", self.venue),
            filled_at_ms: snapshot.observed_at_ms,
            timing: None,
        })
    }

    async fn fetch_position(&self, symbol: &str) -> Result<lightfee::PositionSnapshot> {
        Ok(lightfee::PositionSnapshot {
            venue: self.venue,
            symbol: symbol.to_string(),
            size: self.position_size(symbol),
            updated_at_ms: 0,
        })
    }

    async fn normalize_quantity(&self, symbol: &str, quantity: f64) -> Result<f64> {
        if symbol == "FETUSDT" {
            Ok(0.0)
        } else {
            Ok(quantity)
        }
    }

    async fn fetch_perp_liquidity_snapshot(
        &self,
        symbol: &str,
    ) -> Result<Option<lightfee::PerpLiquiditySnapshot>> {
        let observed_at_ms = self
            .snapshots
            .lock()
            .expect("lock")
            .first()
            .map(|snapshot| snapshot.observed_at_ms)
            .unwrap_or_default();
        Ok(Some(ample_perp_liquidity_snapshot(
            self.venue,
            symbol,
            observed_at_ms,
        )))
    }
}

impl MockHintSource {
    fn success(hints: Vec<OpportunityHint>) -> Self {
        Self { hints, fail: false }
    }

    fn failure() -> Self {
        Self {
            hints: Vec::new(),
            fail: true,
        }
    }
}

#[async_trait]
impl OpportunityHintSource for MockHintSource {
    async fn fetch_hints(&self, _symbols: &[String]) -> Result<Vec<OpportunityHint>> {
        if self.fail {
            Err(anyhow!("mock hint source down"))
        } else {
            Ok(self.hints.clone())
        }
    }
}
