use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use chrono::{Duration, Local, NaiveDate};
use lightfee::{
    analyze_daily_journal_file, config::RuntimeMode, AccountBalanceSnapshot, AppConfig,
    BalanceSnapshotFailure, BalanceSnapshotReport, BinanceLiveAdapter, BybitLiveAdapter,
    DailyProfitSummary, HyperliquidLiveAdapter, JsonlJournal, OkxLiveAdapter, ScriptedVenueAdapter,
    Venue, VenueAdapter,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct DailyReportOutput {
    report_date: String,
    generated_at_ms: i64,
    current_balance_snapshot: BalanceSnapshotReport,
    daily_profit_summary: Option<DailyProfitSummary>,
}

#[derive(Debug, Serialize)]
struct BalanceSnapshotEvent {
    total_equity_quote: Option<f64>,
    venues: Vec<AccountBalanceSnapshot>,
    failed_venues: Vec<BalanceSnapshotFailure>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return Err(anyhow!(
            "usage: cargo run --bin daily_report -- [--json] [--date YYYY-MM-DD] <config-path>"
        ));
    }

    let mut json_output = false;
    let mut date = None;
    let mut config_path = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--json" => {
                json_output = true;
                index += 1;
            }
            "--date" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow!("--date requires YYYY-MM-DD"))?;
                date = Some(
                    NaiveDate::parse_from_str(value, "%Y-%m-%d")
                        .with_context(|| format!("invalid --date value: {value}"))?,
                );
                index += 2;
            }
            value => {
                if config_path.is_none() {
                    config_path = Some(PathBuf::from(value));
                }
                index += 1;
            }
        }
    }
    let date = date.unwrap_or_else(|| Local::now().date_naive() - Duration::days(1));
    let config_path = config_path.ok_or_else(|| anyhow!("missing config path"))?;

    let config = AppConfig::load(&config_path)?;
    let journal = JsonlJournal::with_capacity(
        &config.persistence.event_log_path,
        config.runtime.journal_async_queue_capacity,
    );
    let generated_at_ms = chrono::Utc::now().timestamp_millis();
    let snapshot = fetch_balance_snapshot(&config, &config_path, generated_at_ms).await?;
    let snapshot_event = BalanceSnapshotEvent {
        total_equity_quote: snapshot.total_equity_quote,
        venues: snapshot.venues.clone(),
        failed_venues: snapshot.failed_venues.clone(),
    };
    journal.append_critical(generated_at_ms, "balance.snapshot", &snapshot_event)?;

    let analysis = analyze_daily_journal_file(Path::new(&config.persistence.event_log_path))?;
    let daily_profit_summary = analysis
        .daily_profit_summaries
        .into_iter()
        .find(|item| item.date == date.to_string());
    let output = DailyReportOutput {
        report_date: date.to_string(),
        generated_at_ms,
        current_balance_snapshot: analysis.current_balance_snapshot.unwrap_or(snapshot),
        daily_profit_summary,
    };

    if json_output {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_human_report(&output);
    }

    journal.shutdown()?;
    Ok(())
}

async fn fetch_balance_snapshot(
    config: &AppConfig,
    config_path: &PathBuf,
    observed_at_ms: i64,
) -> Result<BalanceSnapshotReport> {
    let mut venues = Vec::new();
    let mut failed_venues = Vec::new();

    for venue_config in config.enabled_venues() {
        match build_adapter_for_report(config, config_path, venue_config.venue).await {
            Ok(adapter) => {
                let venue = adapter.venue();
                let snapshot = adapter.fetch_account_balance_snapshot().await;
                let shutdown_result = adapter.shutdown().await;
                match snapshot {
                    Ok(Some(mut balance)) => {
                        balance.observed_at_ms = observed_at_ms;
                        venues.push(balance);
                    }
                    Ok(None) => failed_venues.push(BalanceSnapshotFailure {
                        venue,
                        error: "account balance snapshot unsupported".to_string(),
                    }),
                    Err(error) => failed_venues.push(BalanceSnapshotFailure {
                        venue,
                        error: error.to_string(),
                    }),
                }
                if let Err(error) = shutdown_result {
                    failed_venues.push(BalanceSnapshotFailure {
                        venue,
                        error: format!("shutdown_failed: {error}"),
                    });
                }
            }
            Err(error) => failed_venues.push(BalanceSnapshotFailure {
                venue: venue_config.venue,
                error: error.to_string(),
            }),
        }
    }

    let total_equity_quote = if venues.is_empty() {
        None
    } else {
        Some(venues.iter().map(|item| item.equity_quote).sum())
    };

    Ok(BalanceSnapshotReport {
        observed_at_ms,
        total_equity_quote,
        venues,
        failed_venues,
    })
}

async fn build_adapter_for_report(
    config: &AppConfig,
    config_path: &PathBuf,
    venue: Venue,
) -> Result<Arc<dyn VenueAdapter>> {
    let venue_config = config
        .venue(venue)
        .ok_or_else(|| anyhow!("venue {venue} is not enabled"))?;
    Ok(match config.runtime.mode {
        RuntimeMode::Paper => {
            let base_dir = config_path
                .parent()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            let data_file = venue_config.market_data_file.as_ref().with_context(|| {
                format!("venue {} missing market_data_file", venue_config.venue)
            })?;
            let path = if PathBuf::from(data_file).is_absolute() {
                PathBuf::from(data_file)
            } else {
                base_dir.join(data_file)
            };
            Arc::new(
                ScriptedVenueAdapter::from_file(
                    venue_config.venue,
                    venue_config.taker_fee_bps,
                    &path,
                )
                .await?,
            )
        }
        RuntimeMode::Live => match venue {
            Venue::Binance => Arc::new(
                BinanceLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
            Venue::Okx => {
                Arc::new(OkxLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?)
            }
            Venue::Bybit => Arc::new(
                BybitLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
            Venue::Hyperliquid => Arc::new(
                HyperliquidLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
        },
    })
}

fn print_human_report(report: &DailyReportOutput) {
    println!(
        "daily_report date={} generated_at_ms={} current_total_equity_quote={:?}",
        report.report_date,
        report.generated_at_ms,
        report.current_balance_snapshot.total_equity_quote
    );

    for venue in &report.current_balance_snapshot.venues {
        println!(
            "balance venue={} equity_quote={:.8} wallet_balance_quote={:?} available_balance_quote={:?} observed_at_ms={}",
            venue.venue,
            venue.equity_quote,
            venue.wallet_balance_quote,
            venue.available_balance_quote,
            venue.observed_at_ms,
        );
    }

    for failure in &report.current_balance_snapshot.failed_venues {
        println!(
            "balance_failed venue={} error={}",
            failure.venue, failure.error
        );
    }

    if let Some(summary) = &report.daily_profit_summary {
        println!(
            "daily_profit date={} realized_revenue_quote={:.8} partial_realized_revenue_quote={:.8} remaining_close_realized_revenue_quote={:.8} opened_position_count={} partial_close_count={} closed_position_count={} latest_total_equity_quote={:?}",
            summary.date,
            summary.realized_revenue_quote,
            summary.partial_realized_revenue_quote,
            summary.remaining_close_realized_revenue_quote,
            summary.opened_position_count,
            summary.partial_close_count,
            summary.closed_position_count,
            summary.latest_total_equity_quote,
        );
        for (venue, equity) in &summary.venue_equity_quote {
            println!(
                "daily_profit_balance venue={} equity_quote={:.8}",
                venue, equity
            );
        }
        for symbol in &summary.opened_symbol_revenues {
            println!(
                "daily_symbol symbol={} position_count={} partial_close_count={} closed_position_count={} realized_net_quote={:.8}",
                symbol.symbol,
                symbol.position_count,
                symbol.partial_close_count,
                symbol.closed_position_count,
                symbol.realized_net_quote,
            );
        }
    } else {
        println!(
            "daily_profit date={} realized_revenue_quote=0",
            report.report_date
        );
    }
}
