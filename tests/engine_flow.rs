use std::{
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
    MarketView, OpportunityHint, OpportunityHintSource, PersistenceConfig, RuntimeConfig,
    ScriptedVenueAdapter, StrategyConfig, SymbolMarketSnapshot, TransferStatusView, Venue,
    VenueAdapter, VenueConfig, VenueMarketSnapshot,
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

    engine.tick().await.expect("exit tick");
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
    assert_eq!(event_count(&records, "execution.order_filled"), 4);

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

    engine.tick().await.expect("exit tick");
    assert!(engine.state().open_position.is_none());
    assert_eq!(engine.state().mode, EngineMode::Running);

    let records = read_event_records(&config.persistence.event_log_path);
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

    engine.tick().await.expect("first stage tick");
    assert!(engine.state().open_position.is_some());

    engine.tick().await.expect("second stage tick");
    assert!(engine.state().open_position.is_none());

    let records = read_event_records(&config.persistence.event_log_path);
    let exit_record = records
        .iter()
        .rev()
        .find(|record| record_kind(record) == Some("exit.closed"))
        .expect("exit record");
    assert_eq!(
        exit_record["payload"]["reason"].as_str(),
        Some("second_stage_capture")
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
    let mut restarted = Engine::new(
        restart_config.clone(),
        vec![
            Arc::new(ScriptedVenueAdapter::new(
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
            )) as Arc<dyn VenueAdapter>,
            Arc::new(ScriptedVenueAdapter::new(
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
            )) as Arc<dyn VenueAdapter>,
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

    restarted.tick().await.expect("reconcile tick");

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

    let adapters = adapters_for_capture();
    adapters.0.set_position_size("BTCUSDT", 0.2);

    let mut engine = Engine::new(config, to_dyn(adapters)).await.expect("engine");
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
    config.strategy.max_scan_minutes_before_funding = 15;
    config.strategy.min_scan_minutes_before_funding = 5;

    let adapters = VenueSet(
        Arc::new(ScriptedVenueAdapter::new(
            Venue::Binance,
            0.5,
            vec![venue_snapshot(
                Venue::Binance,
                0,
                vec![symbol_snapshot(
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, -0.0005, 1_200_000,
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
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, 0.0015, 1_200_000,
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
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, 0.0002, 1_200_000,
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
                    "BTCUSDT", 100.0, 100.0, 200.0, 200.0, 0.0001, 1_200_000,
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
