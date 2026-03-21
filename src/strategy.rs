use crate::{
    config::{AppConfig, RuntimeMode},
    market::MarketView,
    models::{
        CandidateOpportunity, FundingLeg, FundingOpportunityType, SymbolMarketSnapshot, Venue,
    },
    opportunity_source::{normalize_symbol_key, OpportunityHint},
    transfer::TransferStatusView,
};

const ALIGNED_FUNDING_TOLERANCE_MS: i64 = 60_000;
const NEAR_TERM_SETTLEMENT_MAX_REMAINING_MS: i64 = 60 * 60 * 1_000;

#[derive(Clone, Copy)]
struct FundingOpportunityProfile {
    opportunity_type: FundingOpportunityType,
    funding_timestamp_ms: i64,
    long_funding_timestamp_ms: i64,
    short_funding_timestamp_ms: i64,
    first_funding_leg: FundingLeg,
    first_funding_timestamp_ms: i64,
    second_funding_timestamp_ms: i64,
    funding_edge_bps: f64,
    total_funding_edge_bps: f64,
    first_stage_funding_edge_bps: f64,
    second_stage_incremental_funding_edge_bps: f64,
    stagger_gap_ms: i64,
}

pub fn discover_candidates(
    config: &AppConfig,
    market: &MarketView,
    hints: Option<&[OpportunityHint]>,
    transfer_statuses: Option<&TransferStatusView>,
) -> Vec<CandidateOpportunity> {
    let mut candidates = Vec::new();

    for symbol in &config.symbols {
        let pair_universe = hinted_pairs_for_symbol(symbol, config, hints);

        for (long_venue, short_venue) in pair_universe {
            if long_venue == short_venue {
                continue;
            }

            let Some(long_quote) = market.symbol(long_venue, symbol) else {
                continue;
            };
            let Some(short_quote) = market.symbol(short_venue, symbol) else {
                continue;
            };
            let funding_profile = funding_opportunity_profile(long_quote, short_quote);

            let mut blocked_reasons = Vec::new();
            if !market.is_fresh(long_venue, config.runtime.max_market_age_ms) {
                blocked_reasons.push(format!("stale_market_data:{}", long_venue));
            }
            if !market.is_fresh(short_venue, config.runtime.max_market_age_ms) {
                blocked_reasons.push(format!("stale_market_data:{}", short_venue));
            }

            let remaining_ms = funding_profile
                .first_funding_timestamp_ms
                .saturating_sub(market.now_ms());
            let near_term_settlement_leg =
                has_near_term_settlement_leg(funding_profile.first_funding_timestamp_ms, market);
            if remaining_ms <= 0 {
                blocked_reasons.push("funding_window_passed".to_string());
            } else if !near_term_settlement_leg {
                blocked_reasons.push("no_near_term_settlement_leg".to_string());
            } else if !is_within_funding_scan_window_ms(config, remaining_ms) {
                blocked_reasons.push("outside_entry_window".to_string());
            }
            if funding_profile.opportunity_type == FundingOpportunityType::Staggered
                && config.strategy.max_stagger_gap_minutes > 0
                && funding_profile.stagger_gap_ms
                    > config
                        .strategy
                        .max_stagger_gap_minutes
                        .saturating_mul(60_000)
            {
                blocked_reasons.push("stagger_gap_too_wide".to_string());
            }

            let reference_mid = ((long_quote.best_ask + short_quote.best_bid) / 2.0).max(1e-9);
            let max_notional = effective_max_entry_notional(config)
                .min(
                    config
                        .venue(long_venue)
                        .map(|item| item.max_notional)
                        .unwrap_or_default(),
                )
                .min(
                    config
                        .venue(short_venue)
                        .map(|item| item.max_notional)
                        .unwrap_or_default(),
                );
            let requested_quantity = (max_notional / reference_mid).max(0.0);
            let top_book_capped_quantity = capped_quantity_by_top_book_depth(
                long_quote,
                short_quote,
                config.strategy.max_top_book_usage_ratio,
            );
            let quantity = requested_quantity.min(top_book_capped_quantity).max(0.0);
            if quantity <= 0.0 {
                blocked_reasons.push("zero_order_size".to_string());
            }

            let fee_bps = config
                .venue(long_venue)
                .map(|item| item.taker_fee_bps)
                .unwrap_or_default()
                + config
                    .venue(short_venue)
                    .map(|item| item.taker_fee_bps)
                    .unwrap_or_default();
            let entry_cross_bps =
                ((short_quote.best_bid - long_quote.best_ask) / reference_mid) * 10_000.0;
            let entry_slippage_bps = estimate_slippage_bps(long_quote, quantity, true)
                + estimate_slippage_bps(short_quote, quantity, false);
            let expected_edge_bps = funding_profile.funding_edge_bps
                - fee_bps
                - entry_slippage_bps
                - config.strategy.exit_slippage_reserve_bps
                - config.strategy.capital_buffer_bps;
            let worst_case_edge_bps = expected_edge_bps - config.strategy.execution_buffer_bps;
            let first_stage_expected_edge_bps = funding_profile.first_stage_funding_edge_bps
                - fee_bps
                - entry_slippage_bps
                - config.strategy.exit_slippage_reserve_bps
                - config.strategy.capital_buffer_bps;
            let (transfer_bias_bps, transfer_state, mut advisories) = evaluate_transfer_preference(
                config,
                transfer_statuses,
                symbol,
                long_venue,
                short_venue,
            );
            if quantity + 1e-9 < requested_quantity {
                advisories.push("quantity_capped_by_top_book_depth".to_string());
            }
            advisories.sort();
            advisories.dedup();
            let ranking_edge_bps = worst_case_edge_bps + transfer_bias_bps;

            if funding_profile.funding_edge_bps < config.strategy.min_funding_edge_bps {
                blocked_reasons.push("funding_edge_below_floor".to_string());
            }
            if expected_edge_bps < config.strategy.min_expected_edge_bps {
                blocked_reasons.push("expected_edge_below_floor".to_string());
            }
            if worst_case_edge_bps < config.strategy.min_worst_case_edge_bps {
                blocked_reasons.push("worst_case_edge_below_floor".to_string());
            }

            blocked_reasons.sort();
            blocked_reasons.dedup();

            candidates.push(CandidateOpportunity {
                pair_id: pair_id(symbol, long_venue, short_venue),
                symbol: symbol.clone(),
                long_venue,
                short_venue,
                quantity,
                entry_notional_quote: quantity * reference_mid,
                funding_timestamp_ms: funding_profile.funding_timestamp_ms,
                long_funding_timestamp_ms: funding_profile.long_funding_timestamp_ms,
                short_funding_timestamp_ms: funding_profile.short_funding_timestamp_ms,
                opportunity_type: funding_profile.opportunity_type,
                first_funding_leg: funding_profile.first_funding_leg,
                first_funding_timestamp_ms: funding_profile.first_funding_timestamp_ms,
                second_funding_timestamp_ms: funding_profile.second_funding_timestamp_ms,
                funding_edge_bps: funding_profile.funding_edge_bps,
                total_funding_edge_bps: funding_profile.total_funding_edge_bps,
                first_stage_funding_edge_bps: funding_profile.first_stage_funding_edge_bps,
                first_stage_expected_edge_bps,
                second_stage_incremental_funding_edge_bps: funding_profile
                    .second_stage_incremental_funding_edge_bps,
                stagger_gap_ms: funding_profile.stagger_gap_ms,
                entry_cross_bps,
                fee_bps,
                entry_slippage_bps,
                expected_edge_bps,
                worst_case_edge_bps,
                ranking_edge_bps,
                transfer_bias_bps,
                transfer_state,
                advisories,
                blocked_reasons,
            });
        }
    }

    sort_candidates(&mut candidates);
    candidates
}

pub fn effective_max_entry_notional(config: &AppConfig) -> f64 {
    match config.runtime.mode {
        RuntimeMode::Live => {
            let live_cap = config.strategy.live_max_entry_notional;
            if live_cap <= 0.0 {
                config.strategy.max_entry_notional
            } else {
                config.strategy.max_entry_notional.min(live_cap)
            }
        }
        RuntimeMode::Paper => config.strategy.max_entry_notional,
    }
}

pub fn is_within_funding_scan_window_ms(config: &AppConfig, remaining_ms: i64) -> bool {
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
        return remaining_ms <= max_before_ms && remaining_ms >= min_before_ms;
    }

    remaining_ms <= config.strategy.entry_window_secs.saturating_mul(1_000)
}

pub fn has_near_term_settlement_leg(funding_timestamp_ms: i64, market: &MarketView) -> bool {
    let remaining_ms = funding_timestamp_ms.saturating_sub(market.now_ms());
    remaining_ms > 0 && remaining_ms <= NEAR_TERM_SETTLEMENT_MAX_REMAINING_MS
}

pub fn sort_candidates(candidates: &mut [CandidateOpportunity]) {
    candidates.sort_by(|left, right| {
        right
            .is_tradeable()
            .cmp(&left.is_tradeable())
            .then_with(|| right.ranking_edge_bps.total_cmp(&left.ranking_edge_bps))
            .then_with(|| {
                right
                    .worst_case_edge_bps
                    .total_cmp(&left.worst_case_edge_bps)
            })
            .then_with(|| right.expected_edge_bps.total_cmp(&left.expected_edge_bps))
            .then_with(|| left.pair_id.cmp(&right.pair_id))
    });
}

fn funding_opportunity_profile(
    long_quote: &SymbolMarketSnapshot,
    short_quote: &SymbolMarketSnapshot,
) -> FundingOpportunityProfile {
    let long_contribution_bps = -long_quote.funding_rate * 10_000.0;
    let short_contribution_bps = short_quote.funding_rate * 10_000.0;
    let total_funding_edge_bps = short_contribution_bps + long_contribution_bps;
    let long_funding_timestamp_ms = long_quote.funding_timestamp_ms;
    let short_funding_timestamp_ms = short_quote.funding_timestamp_ms;
    let stagger_gap_ms = (long_funding_timestamp_ms - short_funding_timestamp_ms).abs();
    let opportunity_type = if stagger_gap_ms <= ALIGNED_FUNDING_TOLERANCE_MS {
        FundingOpportunityType::Aligned
    } else {
        FundingOpportunityType::Staggered
    };
    let (first_funding_leg, first_funding_timestamp_ms, second_funding_timestamp_ms) =
        if long_funding_timestamp_ms <= short_funding_timestamp_ms {
            (
                FundingLeg::Long,
                long_funding_timestamp_ms,
                short_funding_timestamp_ms,
            )
        } else {
            (
                FundingLeg::Short,
                short_funding_timestamp_ms,
                long_funding_timestamp_ms,
            )
        };

    let (funding_edge_bps, first_stage_funding_edge_bps, second_stage_incremental_funding_edge_bps) =
        match opportunity_type {
            FundingOpportunityType::Aligned => {
                (total_funding_edge_bps, total_funding_edge_bps, 0.0)
            }
            FundingOpportunityType::Staggered => match first_funding_leg {
                FundingLeg::Long => (
                    long_contribution_bps,
                    long_contribution_bps,
                    short_contribution_bps,
                ),
                FundingLeg::Short => (
                    short_contribution_bps,
                    short_contribution_bps,
                    long_contribution_bps,
                ),
            },
        };

    FundingOpportunityProfile {
        opportunity_type,
        funding_timestamp_ms: first_funding_timestamp_ms,
        long_funding_timestamp_ms,
        short_funding_timestamp_ms,
        first_funding_leg,
        first_funding_timestamp_ms,
        second_funding_timestamp_ms,
        funding_edge_bps,
        total_funding_edge_bps,
        first_stage_funding_edge_bps,
        second_stage_incremental_funding_edge_bps,
        stagger_gap_ms,
    }
}

fn hinted_pairs_for_symbol(
    symbol: &str,
    config: &AppConfig,
    hints: Option<&[OpportunityHint]>,
) -> Vec<(Venue, Venue)> {
    let symbol_key = normalize_symbol_key(symbol);
    let mut hinted_pairs = hints
        .map(|items| {
            items
                .iter()
                .filter(|hint| normalize_symbol_key(&hint.symbol) == symbol_key)
                .map(|hint| (hint.long_venue, hint.short_venue))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    hinted_pairs.sort();
    hinted_pairs.dedup();

    if !hinted_pairs.is_empty() {
        return hinted_pairs;
    }

    config
        .directed_pairs_or_all()
        .into_iter()
        .filter(|pair| pair.allows_symbol(symbol))
        .map(|pair| (pair.long, pair.short))
        .collect()
}

fn estimate_slippage_bps(snapshot: &SymbolMarketSnapshot, quantity: f64, taking_ask: bool) -> f64 {
    let top_size = if taking_ask {
        snapshot.ask_size
    } else {
        snapshot.bid_size
    };
    if top_size <= 0.0 || quantity <= 0.0 {
        return 500.0;
    }

    let depth_ratio = quantity / top_size;
    let impact_component = if depth_ratio <= 1.0 {
        depth_ratio * 0.8
    } else {
        0.8 + (depth_ratio - 1.0) * 15.0
    };
    impact_component
}

fn capped_quantity_by_top_book_depth(
    long_quote: &SymbolMarketSnapshot,
    short_quote: &SymbolMarketSnapshot,
    max_top_book_usage_ratio: f64,
) -> f64 {
    if max_top_book_usage_ratio <= 0.0 {
        return f64::INFINITY;
    }

    (long_quote.ask_size * max_top_book_usage_ratio)
        .min(short_quote.bid_size * max_top_book_usage_ratio)
        .max(0.0)
}

fn pair_id(symbol: &str, long_venue: Venue, short_venue: Venue) -> String {
    format!(
        "{}:{}->{}",
        symbol.to_ascii_lowercase(),
        long_venue,
        short_venue
    )
}

fn evaluate_transfer_preference(
    config: &AppConfig,
    transfer_statuses: Option<&TransferStatusView>,
    symbol: &str,
    long_venue: Venue,
    short_venue: Venue,
) -> (f64, Option<String>, Vec<String>) {
    let Some(transfer_statuses) = transfer_statuses else {
        return (
            config.strategy.transfer_unknown_bias_bps,
            Some("unknown".to_string()),
            vec!["transfer_status_unknown".to_string()],
        );
    };

    let asset = normalize_symbol_key(symbol);
    if asset.is_empty() {
        return (
            config.strategy.transfer_unknown_bias_bps,
            Some("unknown".to_string()),
            vec!["transfer_asset_unknown".to_string()],
        );
    }

    let long_status = transfer_statuses.asset_status(long_venue, &asset);
    let short_status = transfer_statuses.asset_status(short_venue, &asset);
    let all_known = long_status.is_some() && short_status.is_some();
    let all_healthy = long_status.map(|item| item.healthy()).unwrap_or(false)
        && short_status.map(|item| item.healthy()).unwrap_or(false);
    let any_degraded = long_status.map(|item| !item.healthy()).unwrap_or(false)
        || short_status.map(|item| !item.healthy()).unwrap_or(false);

    if all_known && all_healthy {
        return (
            config.strategy.transfer_healthy_bias_bps,
            Some("healthy".to_string()),
            Vec::new(),
        );
    }

    if any_degraded {
        let mut advisories = Vec::new();
        if long_status.map(|item| !item.healthy()).unwrap_or(false) {
            advisories.push(format!("transfer_status_degraded:{long_venue}"));
        }
        if short_status.map(|item| !item.healthy()).unwrap_or(false) {
            advisories.push(format!("transfer_status_degraded:{short_venue}"));
        }
        advisories.sort();
        advisories.dedup();
        return (
            config.strategy.transfer_degraded_bias_bps,
            Some("degraded".to_string()),
            advisories,
        );
    }

    let mut advisories = Vec::new();
    if long_status.is_none() {
        advisories.push(format!("transfer_status_unknown:{long_venue}"));
    }
    if short_status.is_none() {
        advisories.push(format!("transfer_status_unknown:{short_venue}"));
    }
    advisories.sort();
    advisories.dedup();

    (
        config.strategy.transfer_unknown_bias_bps,
        Some("unknown".to_string()),
        advisories,
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{
            AppConfig, PersistenceConfig, RuntimeConfig, RuntimeMode, StrategyConfig, VenueConfig,
        },
        market::MarketView,
        models::{FundingOpportunityType, Venue, VenueMarketSnapshot},
    };

    use super::{discover_candidates, estimate_slippage_bps};

    #[test]
    fn quantity_is_capped_by_top_book_depth_before_ranking() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                max_entry_notional: 1_000.0,
                max_top_book_usage_ratio: 0.5,
                min_funding_edge_bps: 0.0,
                min_expected_edge_bps: -1_000.0,
                min_worst_case_edge_bps: -1_000.0,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig::default(),
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 0.5,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 0.5,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: Vec::new(),
        };
        let market = MarketView::from_snapshots(vec![
            VenueMarketSnapshot {
                venue: Venue::Binance,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 99.9,
                    best_ask: 100.0,
                    bid_size: 10.0,
                    ask_size: 4.0,
                    mark_price: None,
                    funding_rate: -0.0001,
                    funding_timestamp_ms: 60_000,
                }],
            },
            VenueMarketSnapshot {
                venue: Venue::Okx,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.3,
                    best_ask: 100.4,
                    bid_size: 6.0,
                    ask_size: 10.0,
                    mark_price: None,
                    funding_rate: 0.0008,
                    funding_timestamp_ms: 60_000,
                }],
            },
        ]);

        let candidate = discover_candidates(&config, &market, None, None)
            .into_iter()
            .find(|item| item.long_venue == Venue::Binance && item.short_venue == Venue::Okx)
            .expect("candidate");

        assert!((candidate.quantity - 2.0).abs() < 1e-9);
        assert!(candidate
            .advisories
            .iter()
            .any(|item| item == "quantity_capped_by_top_book_depth"));
    }

    #[test]
    fn live_zero_notional_cap_falls_back_to_configured_max_entry_notional() {
        let config = AppConfig {
            runtime: RuntimeConfig {
                mode: RuntimeMode::Live,
                ..RuntimeConfig::default()
            },
            strategy: StrategyConfig {
                max_entry_notional: 120.0,
                live_max_entry_notional: 0.0,
                min_funding_edge_bps: 0.0,
                min_expected_edge_bps: -1_000.0,
                min_worst_case_edge_bps: -1_000.0,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig::default(),
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 0.5,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 0.5,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: Vec::new(),
        };
        let market = MarketView::from_snapshots(vec![
            VenueMarketSnapshot {
                venue: Venue::Binance,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: -0.0001,
                    funding_timestamp_ms: 60_000,
                }],
            },
            VenueMarketSnapshot {
                venue: Venue::Okx,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.2,
                    best_ask: 100.3,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: 0.0008,
                    funding_timestamp_ms: 60_000,
                }],
            },
        ]);

        let candidate = discover_candidates(&config, &market, None, None)
            .into_iter()
            .find(|item| item.long_venue == Venue::Binance && item.short_venue == Venue::Okx)
            .expect("candidate");

        assert!((candidate.entry_notional_quote - 120.0).abs() < 1e-9);
    }

    #[test]
    fn staggered_candidate_uses_first_stage_funding_edge_for_entry_gating() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                max_scan_minutes_before_funding: 0,
                min_scan_minutes_before_funding: 0,
                min_funding_edge_bps: 5.0,
                min_expected_edge_bps: -1_000.0,
                min_worst_case_edge_bps: -1_000.0,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig::default(),
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: vec![crate::config::DirectedPairConfig {
                long: Venue::Binance,
                short: Venue::Okx,
                symbols: vec!["BTCUSDT".to_string()],
            }],
        };
        let market = MarketView::from_snapshots(vec![
            VenueMarketSnapshot {
                venue: Venue::Binance,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: -0.0012,
                    funding_timestamp_ms: 60_000,
                }],
            },
            VenueMarketSnapshot {
                venue: Venue::Okx,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: -0.0020,
                    funding_timestamp_ms: 4 * 60 * 60 * 1_000,
                }],
            },
        ]);

        let candidate = discover_candidates(&config, &market, None, None)
            .into_iter()
            .find(|item| item.long_venue == Venue::Binance && item.short_venue == Venue::Okx)
            .expect("candidate");

        assert_eq!(
            candidate.opportunity_type,
            FundingOpportunityType::Staggered
        );
        assert!(candidate.is_tradeable());
        assert!((candidate.funding_edge_bps - 12.0).abs() < 1e-9);
        assert!((candidate.total_funding_edge_bps + 8.0).abs() < 1e-9);
        assert_eq!(candidate.funding_timestamp_ms, 60_000);
        assert_eq!(candidate.first_funding_timestamp_ms, 60_000);
        assert_eq!(candidate.second_funding_timestamp_ms, 4 * 60 * 60 * 1_000);
    }

    #[test]
    fn staggered_candidate_is_blocked_when_first_stage_expected_edge_is_negative() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                max_scan_minutes_before_funding: 0,
                min_scan_minutes_before_funding: 0,
                min_funding_edge_bps: 0.0,
                min_expected_edge_bps: 5.0,
                min_worst_case_edge_bps: -1_000.0,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig::default(),
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: vec![crate::config::DirectedPairConfig {
                long: Venue::Binance,
                short: Venue::Okx,
                symbols: vec!["BTCUSDT".to_string()],
            }],
        };
        let market = MarketView::from_snapshots(vec![
            VenueMarketSnapshot {
                venue: Venue::Binance,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: -0.0004,
                    funding_timestamp_ms: 60_000,
                }],
            },
            VenueMarketSnapshot {
                venue: Venue::Okx,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: 0.0020,
                    funding_timestamp_ms: 4 * 60 * 60 * 1_000,
                }],
            },
        ]);

        let candidate = discover_candidates(&config, &market, None, None)
            .into_iter()
            .find(|item| item.long_venue == Venue::Binance && item.short_venue == Venue::Okx)
            .expect("candidate");

        assert_eq!(
            candidate.opportunity_type,
            FundingOpportunityType::Staggered
        );
        assert!(!candidate.is_tradeable());
        assert!(candidate
            .blocked_reasons
            .iter()
            .any(|item| item == "expected_edge_below_floor"));
        assert!((candidate.funding_edge_bps - 4.0).abs() < 1e-9);
        assert!((candidate.total_funding_edge_bps - 24.0).abs() < 1e-9);
    }

    #[test]
    fn candidate_is_blocked_when_no_leg_settles_within_next_hour() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                max_scan_minutes_before_funding: 0,
                min_scan_minutes_before_funding: 0,
                min_funding_edge_bps: -1_000.0,
                min_expected_edge_bps: -1_000.0,
                min_worst_case_edge_bps: -1_000.0,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig::default(),
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: vec![crate::config::DirectedPairConfig {
                long: Venue::Binance,
                short: Venue::Okx,
                symbols: vec!["BTCUSDT".to_string()],
            }],
        };
        let market = MarketView::from_snapshots(vec![
            VenueMarketSnapshot {
                venue: Venue::Binance,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: -0.0005,
                    funding_timestamp_ms: 2 * 60 * 60 * 1_000,
                }],
            },
            VenueMarketSnapshot {
                venue: Venue::Okx,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.2,
                    best_ask: 100.2,
                    bid_size: 500.0,
                    ask_size: 500.0,
                    mark_price: None,
                    funding_rate: 0.0008,
                    funding_timestamp_ms: 4 * 60 * 60 * 1_000,
                }],
            },
        ]);

        let candidate = discover_candidates(&config, &market, None, None)
            .into_iter()
            .find(|item| item.long_venue == Venue::Binance && item.short_venue == Venue::Okx)
            .expect("candidate");

        assert!(!candidate.is_tradeable());
        assert!(candidate
            .blocked_reasons
            .iter()
            .any(|item| item == "no_near_term_settlement_leg"));
    }

    #[test]
    fn slippage_estimate_does_not_double_count_spread() {
        let snapshot = crate::models::SymbolMarketSnapshot {
            symbol: "BTCUSDT".to_string(),
            best_bid: 99.0,
            best_ask: 101.0,
            bid_size: 10.0,
            ask_size: 10.0,
            mark_price: None,
            funding_rate: 0.0,
            funding_timestamp_ms: 60_000,
        };

        let slippage_bps = estimate_slippage_bps(&snapshot, 5.0, true);

        assert!((slippage_bps - 0.4).abs() < 1e-9);
    }

    #[test]
    fn expected_edge_is_not_driven_by_entry_cross_direction() {
        let config = AppConfig {
            runtime: RuntimeConfig::default(),
            strategy: StrategyConfig {
                max_scan_minutes_before_funding: 0,
                min_scan_minutes_before_funding: 0,
                max_entry_notional: 100.0,
                min_funding_edge_bps: -1_000.0,
                min_expected_edge_bps: -1_000.0,
                min_worst_case_edge_bps: -1_000.0,
                exit_slippage_reserve_bps: 0.0,
                capital_buffer_bps: 0.0,
                execution_buffer_bps: 0.0,
                ..StrategyConfig::default()
            },
            persistence: PersistenceConfig::default(),
            venues: vec![
                VenueConfig {
                    venue: Venue::Binance,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
                VenueConfig {
                    venue: Venue::Okx,
                    enabled: true,
                    taker_fee_bps: 0.0,
                    max_notional: 1_000.0,
                    market_data_file: None,
                    live: Default::default(),
                },
            ],
            symbols: vec!["BTCUSDT".to_string()],
            directed_pairs: vec![crate::config::DirectedPairConfig {
                long: Venue::Binance,
                short: Venue::Okx,
                symbols: vec!["BTCUSDT".to_string()],
            }],
        };

        let negative_cross_market = MarketView::from_snapshots(vec![
            VenueMarketSnapshot {
                venue: Venue::Binance,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 50_000.0,
                    ask_size: 50_000.0,
                    mark_price: None,
                    funding_rate: -0.0005,
                    funding_timestamp_ms: 60_000,
                }],
            },
            VenueMarketSnapshot {
                venue: Venue::Okx,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 99.0,
                    best_ask: 99.0,
                    bid_size: 50_000.0,
                    ask_size: 50_000.0,
                    mark_price: None,
                    funding_rate: 0.0005,
                    funding_timestamp_ms: 60_000,
                }],
            },
        ]);
        let positive_cross_market = MarketView::from_snapshots(vec![
            VenueMarketSnapshot {
                venue: Venue::Binance,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 100.0,
                    best_ask: 100.0,
                    bid_size: 50_000.0,
                    ask_size: 50_000.0,
                    mark_price: None,
                    funding_rate: -0.0005,
                    funding_timestamp_ms: 60_000,
                }],
            },
            VenueMarketSnapshot {
                venue: Venue::Okx,
                observed_at_ms: 0,
                symbols: vec![crate::models::SymbolMarketSnapshot {
                    symbol: "BTCUSDT".to_string(),
                    best_bid: 101.0,
                    best_ask: 101.0,
                    bid_size: 50_000.0,
                    ask_size: 50_000.0,
                    mark_price: None,
                    funding_rate: 0.0005,
                    funding_timestamp_ms: 60_000,
                }],
            },
        ]);

        let negative_cross_candidate =
            discover_candidates(&config, &negative_cross_market, None, None)
                .into_iter()
                .find(|item| item.long_venue == Venue::Binance && item.short_venue == Venue::Okx)
                .expect("negative cross candidate");
        let positive_cross_candidate =
            discover_candidates(&config, &positive_cross_market, None, None)
                .into_iter()
                .find(|item| item.long_venue == Venue::Binance && item.short_venue == Venue::Okx)
                .expect("positive cross candidate");

        assert!(
            (negative_cross_candidate.expected_edge_bps
                - positive_cross_candidate.expected_edge_bps)
                .abs()
                < 0.5
        );
    }
}
