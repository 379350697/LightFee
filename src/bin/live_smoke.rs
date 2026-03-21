use std::{
    collections::BTreeMap,
    env,
    path::PathBuf,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use lightfee::{
    apply_cached_fee_snapshots_to_candidates, config::RuntimeMode, strategy::discover_candidates,
    AppConfig, AsterLiveAdapter, BinanceLiveAdapter, BitgetLiveAdapter, BybitLiveAdapter,
    CandidateOpportunity, ChillybotOpportunitySource, FeedgrabChillybotSource,
    FundingOpportunityType, GateLiveAdapter, MarketView, OkxLiveAdapter, OpportunityHintSource,
    OrderFill, OrderRequest, Side, TransferStatusSource, TransferStatusView, Venue, VenueAdapter,
};
use tokio::time::{sleep, Duration};

#[derive(Clone, Copy, Debug)]
struct LatencySample {
    local_roundtrip_ms: u128,
}

#[derive(Clone, Debug)]
enum PositionSelectionCheck {
    Flat,
    Open { long_size: f64, short_size: f64 },
    Error(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return Err(anyhow!(
            "usage: cargo run --bin live_smoke -- [--execute] <config-path>"
        ));
    }

    let execute = args.iter().any(|arg| arg == "--execute");
    let flatten_open = args.iter().any(|arg| arg == "--flatten-open");
    let config_arg = args
        .iter()
        .find(|arg| arg.as_str() != "--execute" && arg.as_str() != "--flatten-open")
        .ok_or_else(|| anyhow!("missing config path"))?;
    let config_path = PathBuf::from(config_arg);
    let config = AppConfig::load(&config_path)?;
    if !matches!(config.runtime.mode, RuntimeMode::Live) {
        return Err(anyhow!("live_smoke requires runtime.mode = \"live\""));
    }

    let adapters = build_live_adapters(&config).await?;
    let hint_source = build_hint_source(&config)?;
    let transfer_source = build_transfer_source(&config)?;

    let symbols = config.symbols.clone();
    if flatten_open {
        flatten_open_positions(&adapters, &symbols).await?;
        return Ok(());
    }

    let market = fetch_market(&adapters, &symbols).await?;
    let hints = fetch_hints(hint_source.as_ref(), &symbols).await;
    let transfer_view = fetch_transfer_view(transfer_source.as_ref(), &adapters, &symbols).await;
    let mut candidates =
        discover_candidates(&config, &market, hints.as_deref(), transfer_view.as_ref());
    let fee_snapshots = adapters
        .iter()
        .filter_map(|adapter| {
            adapter
                .cached_account_fee_snapshot()
                .map(|snapshot| (adapter.venue(), snapshot))
        })
        .collect::<BTreeMap<_, _>>();
    let _ = apply_cached_fee_snapshots_to_candidates(&config, &mut candidates, &fee_snapshots);

    println!("mode=live_smoke execute={execute}");
    println!("symbols={:?}", symbols);
    println!("candidate_count={}", candidates.len());

    let mut selected = None;
    let mut position_checks = BTreeMap::new();
    for candidate in candidates.iter().filter(|item| item.is_tradeable()) {
        let long_position = adapter(&adapters, candidate.long_venue)?
            .fetch_position(&candidate.symbol)
            .await;
        let short_position = adapter(&adapters, candidate.short_venue)?
            .fetch_position(&candidate.symbol)
            .await;
        match (long_position, short_position) {
            (Ok(long_position), Ok(short_position))
                if long_position.size.abs() <= 1e-9 && short_position.size.abs() <= 1e-9 =>
            {
                position_checks.insert(candidate.pair_id.clone(), PositionSelectionCheck::Flat);
                selected = Some(candidate.clone());
                break;
            }
            (Ok(long_position), Ok(short_position)) => {
                position_checks.insert(
                    candidate.pair_id.clone(),
                    PositionSelectionCheck::Open {
                        long_size: long_position.size,
                        short_size: short_position.size,
                    },
                );
                println!(
                    "skip pair={} reason=open_position long_size={} short_size={}",
                    candidate.pair_id, long_position.size, short_position.size
                );
            }
            (Err(error), _) | (_, Err(error)) => {
                position_checks.insert(
                    candidate.pair_id.clone(),
                    PositionSelectionCheck::Error(error.to_string()),
                );
                println!(
                    "skip pair={} reason=position_check_failed error={error:#}",
                    candidate.pair_id
                );
            }
        }
    }

    let candidate = match selected {
        Some(candidate) => candidate,
        None => {
            print_no_entry_diagnostics(&config, &market, &candidates, &position_checks);
            return Err(anyhow!("no flat tradeable candidate found"));
        }
    };
    let executable_quantity = common_executable_quantity(
        &adapters,
        candidate.long_venue,
        candidate.short_venue,
        &candidate.symbol,
        candidate.quantity,
    )
    .await?;
    if executable_quantity <= 0.0 {
        return Err(anyhow!(
            "quantity rounded to zero for {} on {} and {}",
            candidate.symbol,
            candidate.long_venue,
            candidate.short_venue
        ));
    }
    let executable_entry_notional =
        candidate.entry_notional_quote * (executable_quantity / candidate.quantity.max(1e-9));
    println!(
        "selected pair={} symbol={} long={} short={} requested_qty={} executable_qty={} per_leg_notional={:.4} gross_notional≈{:.4} transfer_state={:?} advisories={:?}",
        candidate.pair_id,
        candidate.symbol,
        candidate.long_venue,
        candidate.short_venue,
        candidate.quantity,
        executable_quantity,
        executable_entry_notional,
        executable_entry_notional * 2.0,
        candidate.transfer_state,
        candidate.advisories,
    );

    if !execute {
        println!("dry-run only; no orders submitted");
        return Ok(());
    }

    let request_id_seed = market.now_ms();
    let short_open = OrderRequest {
        symbol: candidate.symbol.clone(),
        side: Side::Sell,
        quantity: executable_quantity,
        reduce_only: false,
        client_order_id: smoke_order_id(request_id_seed, candidate.short_venue, "os"),
        price_hint: market
            .symbol(candidate.short_venue, &candidate.symbol)
            .map(|quote| quote.best_bid),
        mark_price_hint: market
            .symbol(candidate.short_venue, &candidate.symbol)
            .and_then(|quote| quote.mark_price),
        observed_at_ms: market.observed_at_ms(candidate.short_venue),
    };
    let long_open = OrderRequest {
        symbol: candidate.symbol.clone(),
        side: Side::Buy,
        quantity: executable_quantity,
        reduce_only: false,
        client_order_id: smoke_order_id(request_id_seed, candidate.long_venue, "ol"),
        price_hint: market
            .symbol(candidate.long_venue, &candidate.symbol)
            .map(|quote| quote.best_ask),
        mark_price_hint: market
            .symbol(candidate.long_venue, &candidate.symbol)
            .and_then(|quote| quote.mark_price),
        observed_at_ms: market.observed_at_ms(candidate.long_venue),
    };

    println!(
        "submitting open orders quantity={} symbol={}",
        executable_quantity, candidate.symbol
    );
    let (short_fill, short_latency) =
        timed_place_order(adapter(&adapters, candidate.short_venue)?, short_open)
            .await
            .with_context(|| format!("failed to open short leg on {}", candidate.short_venue))?;
    let long_fill =
        match timed_place_order(adapter(&adapters, candidate.long_venue)?, long_open).await {
            Ok(result) => result,
            Err(error) => {
                println!(
                    "long leg failed after short fill; compensating short leg on {}",
                    candidate.short_venue
                );
                let compensate = OrderRequest {
                    symbol: candidate.symbol.clone(),
                    side: Side::Buy,
                    quantity: short_fill.quantity,
                    reduce_only: true,
                    client_order_id: smoke_order_id(request_id_seed, candidate.short_venue, "cp"),
                    price_hint: Some(short_fill.average_price),
                    mark_price_hint: None,
                    observed_at_ms: Some(short_fill.filled_at_ms),
                };
                let _ = adapter(&adapters, candidate.short_venue)?
                    .place_order(compensate)
                    .await;
                return Err(error).context("failed to open long leg");
            }
        };
    let (long_fill, long_latency) = long_fill;

    print_fill("open_short", &short_fill, short_latency);
    print_fill("open_long", &long_fill, long_latency);

    sleep(Duration::from_millis(800)).await;

    let short_close = OrderRequest {
        symbol: candidate.symbol.clone(),
        side: Side::Buy,
        quantity: short_fill.quantity,
        reduce_only: true,
        client_order_id: smoke_order_id(request_id_seed, candidate.short_venue, "cs"),
        price_hint: Some(short_fill.average_price),
        mark_price_hint: None,
        observed_at_ms: Some(short_fill.filled_at_ms),
    };
    let long_close = OrderRequest {
        symbol: candidate.symbol.clone(),
        side: Side::Sell,
        quantity: long_fill.quantity,
        reduce_only: true,
        client_order_id: smoke_order_id(request_id_seed, candidate.long_venue, "cl"),
        price_hint: Some(long_fill.average_price),
        mark_price_hint: None,
        observed_at_ms: Some(long_fill.filled_at_ms),
    };

    println!("submitting immediate close orders");
    let (short_close_fill, short_close_latency) =
        timed_place_order(adapter(&adapters, candidate.short_venue)?, short_close)
            .await
            .with_context(|| format!("failed to close short leg on {}", candidate.short_venue))?;
    let (long_close_fill, long_close_latency) =
        timed_place_order(adapter(&adapters, candidate.long_venue)?, long_close)
            .await
            .with_context(|| format!("failed to close long leg on {}", candidate.long_venue))?;

    print_fill("close_short", &short_close_fill, short_close_latency);
    print_fill("close_long", &long_close_fill, long_close_latency);

    let total_fee = short_fill.fee_quote
        + long_fill.fee_quote
        + short_close_fill.fee_quote
        + long_close_fill.fee_quote;
    let max_latency_ms = [
        short_latency.local_roundtrip_ms,
        long_latency.local_roundtrip_ms,
        short_close_latency.local_roundtrip_ms,
        long_close_latency.local_roundtrip_ms,
    ]
    .into_iter()
    .max()
    .unwrap_or_default();
    let total_roundtrip_ms = short_latency.local_roundtrip_ms
        + long_latency.local_roundtrip_ms
        + short_close_latency.local_roundtrip_ms
        + long_close_latency.local_roundtrip_ms;
    println!(
        "latency_summary total_roundtrip_ms={} max_single_order_ms={}",
        total_roundtrip_ms, max_latency_ms
    );
    println!("smoke_test_completed total_fee_quote={total_fee:.8}");

    Ok(())
}

async fn build_live_adapters(config: &AppConfig) -> Result<Vec<Arc<dyn VenueAdapter>>> {
    let mut adapters: Vec<Arc<dyn VenueAdapter>> = Vec::new();
    for venue_config in config.enabled_venues() {
        let adapter: Arc<dyn VenueAdapter> = match venue_config.venue {
            Venue::Binance => Arc::new(
                BinanceLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
            Venue::Okx => {
                Arc::new(OkxLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?)
            }
            Venue::Bybit => Arc::new(
                BybitLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
            Venue::Bitget => Arc::new(
                BitgetLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
            Venue::Gate => Arc::new(
                GateLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
            Venue::Aster => Arc::new(
                AsterLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
            Venue::Hyperliquid => Arc::new(
                lightfee::HyperliquidLiveAdapter::new(
                    venue_config,
                    &config.runtime,
                    &config.symbols,
                )
                .await?,
            ),
        };
        adapters.push(adapter);
    }
    Ok(adapters)
}

fn build_hint_source(config: &AppConfig) -> Result<Option<Arc<dyn OpportunityHintSource>>> {
    match config.runtime.opportunity_source {
        lightfee::config::OpportunitySourceMode::ExchangeOnly => Ok(None),
        lightfee::config::OpportunitySourceMode::ChillybotFirst => {
            Ok(Some(Arc::new(ChillybotOpportunitySource::new(
                &config.runtime.chillybot_api_base,
                config.runtime.chillybot_timeout_ms,
            )?)))
        }
        lightfee::config::OpportunitySourceMode::ChillybotViaFeedgrab => {
            Ok(Some(Arc::new(FeedgrabChillybotSource::new(
                &config.runtime.chillybot_api_base,
                config.runtime.chillybot_timeout_ms,
            )?)))
        }
    }
}

fn build_transfer_source(config: &AppConfig) -> Result<Option<Arc<dyn TransferStatusSource>>> {
    match config.runtime.opportunity_source {
        lightfee::config::OpportunitySourceMode::ExchangeOnly => Ok(None),
        lightfee::config::OpportunitySourceMode::ChillybotFirst => {
            Ok(Some(Arc::new(ChillybotOpportunitySource::new(
                &config.runtime.chillybot_api_base,
                config.runtime.chillybot_timeout_ms,
            )?)))
        }
        lightfee::config::OpportunitySourceMode::ChillybotViaFeedgrab => {
            Ok(Some(Arc::new(FeedgrabChillybotSource::new(
                &config.runtime.chillybot_api_base,
                config.runtime.chillybot_timeout_ms,
            )?)))
        }
    }
}

async fn fetch_market(
    adapters: &[Arc<dyn VenueAdapter>],
    symbols: &[String],
) -> Result<MarketView> {
    let futures = adapters.iter().map(|adapter| {
        let adapter = Arc::clone(adapter);
        let symbols = symbols.to_vec();
        async move { adapter.fetch_market_snapshot(&symbols).await }
    });
    let snapshots = join_all(futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(MarketView::from_snapshots(snapshots))
}

async fn fetch_hints(
    source: Option<&Arc<dyn OpportunityHintSource>>,
    symbols: &[String],
) -> Option<Vec<lightfee::OpportunityHint>> {
    let source = source?.clone();
    source.fetch_hints(symbols).await.ok()
}

async fn fetch_transfer_view(
    source: Option<&Arc<dyn TransferStatusSource>>,
    adapters: &[Arc<dyn VenueAdapter>],
    symbols: &[String],
) -> Option<TransferStatusView> {
    let assets = symbols
        .iter()
        .map(|symbol| lightfee::opportunity_source::normalize_symbol_key(symbol))
        .filter(|asset| !asset.is_empty())
        .collect::<Vec<_>>();
    let venues = adapters
        .iter()
        .map(|adapter| adapter.venue())
        .collect::<Vec<_>>();

    if let Some(source) = source.cloned() {
        if let Ok(statuses) = source.fetch_transfer_statuses(&assets, &venues).await {
            if !statuses.is_empty() {
                return Some(TransferStatusView::from_statuses(statuses));
            }
        }
    }

    let futures = adapters.iter().map(|adapter| {
        let adapter = Arc::clone(adapter);
        let assets = assets.clone();
        async move { adapter.fetch_transfer_statuses(&assets).await }
    });
    let mut statuses = Vec::new();
    for result in join_all(futures).await {
        if let Ok(mut venue_statuses) = result {
            statuses.append(&mut venue_statuses);
        }
    }
    if statuses.is_empty() {
        None
    } else {
        Some(TransferStatusView::from_statuses(statuses))
    }
}

async fn common_executable_quantity(
    adapters: &[Arc<dyn VenueAdapter>],
    long_venue: Venue,
    short_venue: Venue,
    symbol: &str,
    desired_quantity: f64,
) -> Result<f64> {
    let long_adapter = adapter(adapters, long_venue)?;
    let short_adapter = adapter(adapters, short_venue)?;
    let long_quantity = long_adapter
        .normalize_quantity(symbol, desired_quantity)
        .await?;
    let short_quantity = short_adapter
        .normalize_quantity(symbol, desired_quantity)
        .await?;
    let common_quantity = long_quantity.min(short_quantity);
    if common_quantity <= 0.0 {
        return Ok(0.0);
    }

    let long_common = long_adapter
        .normalize_quantity(symbol, common_quantity)
        .await?;
    let short_common = short_adapter
        .normalize_quantity(symbol, common_quantity)
        .await?;
    Ok(long_common.min(short_common))
}

fn adapter(adapters: &[Arc<dyn VenueAdapter>], venue: Venue) -> Result<Arc<dyn VenueAdapter>> {
    adapters
        .iter()
        .find(|adapter| adapter.venue() == venue)
        .cloned()
        .ok_or_else(|| anyhow!("missing adapter for {venue}"))
}

fn print_fill(label: &str, fill: &OrderFill, latency: LatencySample) {
    println!(
        "{label} venue={} symbol={} side={:?} qty={} avg_price={} fee_quote={} order_id={} filled_at_ms={} local_roundtrip_ms={} quote_resolve_ms={} order_prepare_ms={} request_sign_ms={} submit_http_ms={} response_decode_ms={} private_fill_wait_ms={} submit_ack_ms={}",
        fill.venue,
        fill.symbol,
        fill.side,
        fill.quantity,
        fill.average_price,
        fill.fee_quote,
        fill.order_id,
        fill.filled_at_ms,
        latency.local_roundtrip_ms,
        fill.timing
            .as_ref()
            .and_then(|timing| timing.quote_resolve_ms)
            .unwrap_or_default(),
        fill.timing
            .as_ref()
            .and_then(|timing| timing.order_prepare_ms)
            .unwrap_or_default(),
        fill.timing
            .as_ref()
            .and_then(|timing| timing.request_sign_ms)
            .unwrap_or_default(),
        fill.timing
            .as_ref()
            .and_then(|timing| timing.submit_http_ms)
            .unwrap_or_default(),
        fill.timing
            .as_ref()
            .and_then(|timing| timing.response_decode_ms)
            .unwrap_or_default(),
        fill.timing
            .as_ref()
            .and_then(|timing| timing.private_fill_wait_ms)
            .unwrap_or_default(),
        fill.timing
            .as_ref()
            .and_then(|timing| timing.submit_ack_ms)
            .unwrap_or_default(),
    );
}

fn smoke_order_id(seed_ms: i64, venue: Venue, phase: &str) -> String {
    format!("smk{}{}{}", seed_ms, venue_code(venue), phase)
}

fn venue_code(venue: Venue) -> &'static str {
    match venue {
        Venue::Binance => "bn",
        Venue::Okx => "ok",
        Venue::Bybit => "by",
        Venue::Bitget => "bg",
        Venue::Gate => "gt",
        Venue::Aster => "as",
        Venue::Hyperliquid => "hl",
    }
}

async fn timed_place_order(
    adapter: Arc<dyn VenueAdapter>,
    request: OrderRequest,
) -> Result<(OrderFill, LatencySample)> {
    let started_at = Instant::now();
    let fill = adapter.place_order(request).await?;
    Ok((
        fill,
        LatencySample {
            local_roundtrip_ms: started_at.elapsed().as_millis(),
        },
    ))
}

async fn flatten_open_positions(
    adapters: &[Arc<dyn VenueAdapter>],
    symbols: &[String],
) -> Result<()> {
    let seed_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default();
    let mut flattened = 0_u32;
    for adapter in adapters {
        for symbol in symbols {
            let position = adapter.fetch_position(symbol).await?;
            if position.size.abs() <= 1e-9 {
                println!(
                    "flatten_skip venue={} symbol={} reason=flat",
                    adapter.venue(),
                    symbol
                );
                continue;
            }

            let side = if position.size > 0.0 {
                Side::Sell
            } else {
                Side::Buy
            };
            let request = OrderRequest {
                symbol: symbol.clone(),
                side,
                quantity: position.size.abs(),
                reduce_only: true,
                client_order_id: smoke_order_id(seed_ms, adapter.venue(), "fx"),
                price_hint: None,
                mark_price_hint: None,
                observed_at_ms: None,
            };
            let (fill, latency) = timed_place_order(adapter.clone(), request)
                .await
                .with_context(|| {
                    format!(
                        "failed to flatten open position on {} for {}",
                        adapter.venue(),
                        symbol
                    )
                })?;
            print_fill("flatten", &fill, latency);
            flattened += 1;
        }
    }

    println!("flatten_completed order_count={flattened}");
    Ok(())
}

fn print_no_entry_diagnostics(
    config: &AppConfig,
    market: &MarketView,
    candidates: &[CandidateOpportunity],
    position_checks: &BTreeMap<String, PositionSelectionCheck>,
) {
    let tradeable_count = candidates
        .iter()
        .filter(|candidate| candidate.is_tradeable())
        .count();
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
    println!(
        "no_entry_diagnostics reason={} candidate_count={} tradeable_count={} blocked_reason_counts={:?} advisory_counts={:?}",
        live_smoke_no_entry_reason(candidates, position_checks),
        candidates.len(),
        tradeable_count,
        blocked_reason_counts,
        advisory_counts,
    );

    for candidate in candidates {
        print_candidate_checklist(
            config,
            market,
            candidate,
            position_checks.get(&candidate.pair_id),
        );
    }
}

fn live_smoke_no_entry_reason(
    candidates: &[CandidateOpportunity],
    position_checks: &BTreeMap<String, PositionSelectionCheck>,
) -> &'static str {
    if candidates.is_empty() {
        return "no_candidates";
    }
    if candidates.iter().all(|candidate| !candidate.is_tradeable()) {
        return "no_tradeable_candidates";
    }
    if position_checks
        .values()
        .any(|status| matches!(status, PositionSelectionCheck::Flat))
    {
        return "flat_candidate_not_selected";
    }
    if position_checks
        .values()
        .any(|status| matches!(status, PositionSelectionCheck::Open { .. }))
    {
        return "tradeable_candidates_blocked_by_open_positions";
    }
    if position_checks
        .values()
        .any(|status| matches!(status, PositionSelectionCheck::Error(_)))
    {
        return "tradeable_candidates_position_check_failed";
    }
    "tradeable_candidates_not_selected"
}

fn print_candidate_checklist(
    config: &AppConfig,
    market: &MarketView,
    candidate: &CandidateOpportunity,
    position_check: Option<&PositionSelectionCheck>,
) {
    let remaining_ms = candidate
        .first_funding_timestamp_ms
        .saturating_sub(market.now_ms());
    let market_fresh_long = market.is_fresh(candidate.long_venue, config.runtime.max_market_age_ms);
    let market_fresh_short =
        market.is_fresh(candidate.short_venue, config.runtime.max_market_age_ms);
    let funding_not_passed = remaining_ms > 0;
    let inside_entry_window = funding_not_passed
        && lightfee::strategy::is_within_funding_scan_window_ms(config, remaining_ms);
    let stagger_gap_limit_ms = config
        .strategy
        .max_stagger_gap_minutes
        .saturating_mul(60_000);
    let stagger_gap_ok = candidate.opportunity_type != FundingOpportunityType::Staggered
        || config.strategy.max_stagger_gap_minutes <= 0
        || candidate.stagger_gap_ms <= stagger_gap_limit_ms;
    let order_size_positive = candidate.quantity > 0.0;
    let funding_edge_ok = candidate.funding_edge_bps >= config.strategy.min_funding_edge_bps;
    let expected_edge_ok = candidate.expected_edge_bps >= config.strategy.min_expected_edge_bps;
    let worst_case_edge_ok =
        candidate.worst_case_edge_bps >= config.strategy.min_worst_case_edge_bps;
    let (flat_position_clear, flat_position_detail) = match position_check {
        Some(PositionSelectionCheck::Flat) => (true, "flat".to_string()),
        Some(PositionSelectionCheck::Open {
            long_size,
            short_size,
        }) => (
            false,
            format!("long_size={} short_size={}", long_size, short_size),
        ),
        Some(PositionSelectionCheck::Error(error)) => {
            (false, format!("position_check_failed:{error}"))
        }
        None if candidate.is_tradeable() => (false, "position_not_checked".to_string()),
        None => (true, "candidate_blocked_before_position_check".to_string()),
    };

    println!(
        "candidate pair={} symbol={} long={} short={} type={:?} tradeable={} transfer_state={:?}",
        candidate.pair_id,
        candidate.symbol,
        candidate.long_venue,
        candidate.short_venue,
        candidate.opportunity_type,
        candidate.is_tradeable(),
        candidate.transfer_state,
    );
    println!(
        "  metrics quantity={:.8} entry_notional_quote={:.4} funding_edge_bps={:.4} first_stage_expected_edge_bps={:.4} expected_edge_bps={:.4} worst_case_edge_bps={:.4} ranking_edge_bps={:.4} entry_cross_bps={:.4} fee_bps={:.4} entry_slippage_bps={:.4}",
        candidate.quantity,
        candidate.entry_notional_quote,
        candidate.funding_edge_bps,
        candidate.first_stage_expected_edge_bps,
        candidate.expected_edge_bps,
        candidate.worst_case_edge_bps,
        candidate.ranking_edge_bps,
        candidate.entry_cross_bps,
        candidate.fee_bps,
        candidate.entry_slippage_bps,
    );
    for (label, ok, detail) in [
        (
            "market_fresh_long",
            market_fresh_long,
            format!(
                "venue={} max_age_ms={}",
                candidate.long_venue, config.runtime.max_market_age_ms
            ),
        ),
        (
            "market_fresh_short",
            market_fresh_short,
            format!(
                "venue={} max_age_ms={}",
                candidate.short_venue, config.runtime.max_market_age_ms
            ),
        ),
        (
            "funding_not_passed",
            funding_not_passed,
            format!("remaining_ms={remaining_ms}"),
        ),
        (
            "inside_entry_window",
            inside_entry_window,
            format!(
                "remaining_ms={remaining_ms} window={}..{}min",
                config.strategy.min_scan_minutes_before_funding,
                config.strategy.max_scan_minutes_before_funding
            ),
        ),
        (
            "stagger_gap_ok",
            stagger_gap_ok,
            format!(
                "opportunity_type={:?} gap_ms={} max_gap_ms={}",
                candidate.opportunity_type, candidate.stagger_gap_ms, stagger_gap_limit_ms
            ),
        ),
        (
            "order_size_positive",
            order_size_positive,
            format!("quantity={:.8}", candidate.quantity),
        ),
        (
            "funding_edge_ok",
            funding_edge_ok,
            format!(
                "{:.4}>={:.4}",
                candidate.funding_edge_bps, config.strategy.min_funding_edge_bps
            ),
        ),
        (
            "expected_edge_ok",
            expected_edge_ok,
            format!(
                "{:.4}>={:.4}",
                candidate.expected_edge_bps, config.strategy.min_expected_edge_bps
            ),
        ),
        (
            "worst_case_edge_ok",
            worst_case_edge_ok,
            format!(
                "{:.4}>={:.4}",
                candidate.worst_case_edge_bps, config.strategy.min_worst_case_edge_bps
            ),
        ),
        (
            "flat_position_clear",
            flat_position_clear,
            flat_position_detail,
        ),
    ] {
        let mark = if ok { "[x]" } else { "[ ]" };
        println!("  {} {} {}", mark, label, detail);
    }
    println!("  blocked_reasons={:?}", candidate.blocked_reasons);
    println!("  advisories={:?}", candidate.advisories);
}

fn count_strings(items: impl IntoIterator<Item = String>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for item in items {
        *counts.entry(item).or_insert(0) += 1;
    }
    counts
}
