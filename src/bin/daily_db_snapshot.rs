use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use chrono::{Duration, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use lightfee::{
    config::RuntimeMode, AppConfig, AsterLiveAdapter, BalanceSnapshotFailure,
    BalanceSnapshotReport, BinanceLiveAdapter, BitgetLiveAdapter, BybitLiveAdapter, GateLiveAdapter,
    HyperliquidLiveAdapter, OkxLiveAdapter, ScriptedVenueAdapter, Venue, VenueAdapter,
};
use rusqlite::{params, Connection};

#[tokio::main]
async fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return Err(anyhow!(
            "usage: cargo run --bin daily_db_snapshot -- [--date YYYY-MM-DD] <config-path>"
        ));
    }

    let mut date = None;
    let mut config_path = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
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

    let report_date = date.unwrap_or_else(|| Local::now().date_naive());
    let config_path = config_path.ok_or_else(|| anyhow!("missing config path"))?;
    let config = AppConfig::load(&config_path)?;

    let observed_at_ms = Utc::now().timestamp_millis();
    let snapshot = fetch_balance_snapshot(&config, &config_path, observed_at_ms).await?;

    let (window_start_ms, window_end_ms) = shanghai_930_window(report_date)?;

    let db_path = Path::new("runtime").join("cache").join("live.sqlite3");
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(db_path)?;
    ensure_schema(&conn)?;

    let window_net_quote: f64 = conn.query_row(
        "SELECT COALESCE(SUM(COALESCE(net_quote,0)),0) FROM hourly_closed_reconcile WHERE close_event_ts_ms>=?1 AND close_event_ts_ms<?2",
        params![window_start_ms, window_end_ms],
        |row| row.get(0),
    )?;

    let venues_json = serde_json::to_string(&snapshot.venues)?;
    let failed_venues_json = serde_json::to_string(&snapshot.failed_venues)?;

    conn.execute(
        r#"
        INSERT INTO daily_930_reports(
          report_date, observed_at_ms, window_start_ms, window_end_ms,
          total_equity_quote, window_net_quote, venues_json, failed_venues_json
        ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(report_date) DO UPDATE SET
          observed_at_ms=excluded.observed_at_ms,
          window_start_ms=excluded.window_start_ms,
          window_end_ms=excluded.window_end_ms,
          total_equity_quote=excluded.total_equity_quote,
          window_net_quote=excluded.window_net_quote,
          venues_json=excluded.venues_json,
          failed_venues_json=excluded.failed_venues_json
        "#,
        params![
            report_date.to_string(),
            observed_at_ms,
            window_start_ms,
            window_end_ms,
            snapshot.total_equity_quote,
            window_net_quote,
            venues_json,
            failed_venues_json,
        ],
    )?;

    println!(
        "daily_930_saved date={} total_equity_quote={:?} window_net_quote={} window=[{}, {})",
        report_date,
        snapshot.total_equity_quote,
        window_net_quote,
        window_start_ms,
        window_end_ms
    );

    Ok(())
}

fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS daily_930_reports (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          report_date TEXT NOT NULL UNIQUE,
          observed_at_ms INTEGER NOT NULL,
          window_start_ms INTEGER NOT NULL,
          window_end_ms INTEGER NOT NULL,
          total_equity_quote REAL,
          window_net_quote REAL NOT NULL,
          venues_json TEXT NOT NULL,
          failed_venues_json TEXT NOT NULL
        );
        "#,
    )?;
    Ok(())
}

fn shanghai_930_window(report_date: NaiveDate) -> Result<(i64, i64)> {
    let sh = FixedOffset::east_opt(8 * 3600).ok_or_else(|| anyhow!("invalid +08 offset"))?;
    let end_naive = NaiveDateTime::new(
        report_date,
        NaiveTime::from_hms_opt(9, 30, 0).ok_or_else(|| anyhow!("invalid 09:30:00"))?,
    );
    let end = sh
        .from_local_datetime(&end_naive)
        .single()
        .ok_or_else(|| anyhow!("failed to resolve local 09:30 timestamp"))?;
    let start = end - Duration::days(1);
    Ok((start.timestamp_millis(), end.timestamp_millis()))
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
                HyperliquidLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
        },
    })
}
