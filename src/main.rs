use std::{env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use lightfee::{
    config::{OpportunitySourceMode, RuntimeMode},
    resilience::FailureBackoff,
    AppConfig, BinanceLiveAdapter, BybitLiveAdapter, ChillybotOpportunitySource, Engine,
    FeedgrabChillybotSource, HyperliquidLiveAdapter, OkxLiveAdapter, OpportunityHintSource,
    ScriptedVenueAdapter, TransferStatusSource, Venue, VenueAdapter,
};
use tokio::time;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let config_path = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/example.toml"));
    let config = AppConfig::load(&config_path)?;
    let adapters = build_adapters(&config, &config_path)
        .await
        .context("failed to build venue adapters")?;
    let (opportunity_source, transfer_status_source) = build_sources(&config)?;
    let mut engine = Engine::with_sources(
        config.clone(),
        adapters,
        opportunity_source,
        transfer_status_source,
    )
    .await?;

    if let Some(warmup_ms) = startup_market_warmup_ms(
        config.runtime.mode.clone(),
        engine.state().open_positions.len(),
        config.runtime.poll_interval_ms,
    ) {
        info!(
            warmup_ms,
            "waiting for live market warmup before first tick"
        );
        time::sleep(Duration::from_millis(warmup_ms)).await;
    }

    info!(
        poll_interval_ms = config.runtime.poll_interval_ms,
        mode = ?config.runtime.mode,
        "lightfee started"
    );

    let mut interval = time::interval(Duration::from_millis(config.runtime.poll_interval_ms));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut tick_backoff = FailureBackoff::new(
        config.runtime.tick_failure_backoff_initial_ms,
        config.runtime.tick_failure_backoff_max_ms,
        0x1F7A_11FE,
    );
    let mut backoff_until = None;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let now = time::Instant::now();
                if backoff_until.is_some_and(|until| now < until) {
                    continue;
                }
                backoff_until = None;
                match engine.tick().await {
                    Ok(()) => tick_backoff.on_success(),
                    Err(error) => {
                        let delay_ms = tick_backoff.on_failure_with_jitter();
                        backoff_until = Some(time::Instant::now() + Duration::from_millis(delay_ms));
                        error!(?error, backoff_ms = delay_ms, "engine tick failed");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received, shutting down");
                engine.shutdown().await?;
                break;
            }
        }
    }

    Ok(())
}

fn startup_market_warmup_ms(
    mode: RuntimeMode,
    active_position_count: usize,
    poll_interval_ms: u64,
) -> Option<u64> {
    if !matches!(mode, RuntimeMode::Live) || active_position_count > 0 {
        return None;
    }
    Some(poll_interval_ms.saturating_mul(3).clamp(3_000, 10_000))
}

async fn build_adapters(
    config: &AppConfig,
    config_path: &PathBuf,
) -> Result<Vec<Arc<dyn VenueAdapter>>> {
    match config.runtime.mode {
        RuntimeMode::Paper => build_paper_adapters(config, config_path).await,
        RuntimeMode::Live => build_live_adapters(config).await,
    }
}

async fn build_paper_adapters(
    config: &AppConfig,
    config_path: &PathBuf,
) -> Result<Vec<Arc<dyn VenueAdapter>>> {
    let base_dir = config_path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let mut adapters: Vec<Arc<dyn VenueAdapter>> = Vec::new();

    for venue_config in config.enabled_venues() {
        let data_file = venue_config
            .market_data_file
            .as_ref()
            .with_context(|| format!("venue {} missing market_data_file", venue_config.venue))?;
        let path = if PathBuf::from(data_file).is_absolute() {
            PathBuf::from(data_file)
        } else {
            base_dir.join(data_file)
        };

        let adapter =
            ScriptedVenueAdapter::from_file(venue_config.venue, venue_config.taker_fee_bps, &path)
                .await
                .with_context(|| {
                    format!("failed to load scripted data for {}", venue_config.venue)
                })?;
        adapters.push(Arc::new(adapter));
    }

    Ok(adapters)
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
            Venue::Hyperliquid => Arc::new(
                HyperliquidLiveAdapter::new(venue_config, &config.runtime, &config.symbols).await?,
            ),
        };
        adapters.push(adapter);
    }

    Ok(adapters)
}

fn build_sources(
    config: &AppConfig,
) -> Result<(
    Option<Arc<dyn OpportunityHintSource>>,
    Option<Arc<dyn TransferStatusSource>>,
)> {
    match config.runtime.opportunity_source {
        OpportunitySourceMode::ExchangeOnly => Ok((None, None)),
        OpportunitySourceMode::ChillybotFirst => {
            let source = Arc::new(ChillybotOpportunitySource::new(
                &config.runtime.chillybot_api_base,
                config.runtime.chillybot_timeout_ms,
            )?);
            let opportunity_source: Arc<dyn OpportunityHintSource> = source.clone();
            let transfer_status_source: Arc<dyn TransferStatusSource> = source;
            Ok((Some(opportunity_source), Some(transfer_status_source)))
        }
        OpportunitySourceMode::ChillybotViaFeedgrab => {
            let source = Arc::new(FeedgrabChillybotSource::new(
                &config.runtime.chillybot_api_base,
                config.runtime.chillybot_timeout_ms,
            )?);
            let opportunity_source: Arc<dyn OpportunityHintSource> = source.clone();
            let transfer_status_source: Arc<dyn TransferStatusSource> = source;
            Ok((Some(opportunity_source), Some(transfer_status_source)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::startup_market_warmup_ms;
    use lightfee::config::RuntimeMode;

    #[test]
    fn live_startup_warms_up_when_no_positions_are_open() {
        assert_eq!(
            startup_market_warmup_ms(RuntimeMode::Live, 0, 1_500),
            Some(4_500)
        );
    }

    #[test]
    fn live_startup_skips_warmup_when_positions_exist() {
        assert_eq!(startup_market_warmup_ms(RuntimeMode::Live, 1, 1_500), None);
    }

    #[test]
    fn paper_mode_skips_warmup() {
        assert_eq!(startup_market_warmup_ms(RuntimeMode::Paper, 0, 1_500), None);
    }
}
