use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::{task::JoinHandle, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, warn};

use crate::models::SymbolMarketSnapshot;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WsBookQuote {
    pub best_bid: f64,
    pub best_ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub observed_at_ms: i64,
}

#[derive(Clone, Debug, Default)]
struct WsSymbolState {
    quote: Option<WsBookQuote>,
    funding_rate: Option<f64>,
    funding_timestamp_ms: Option<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct WsMarketState {
    symbols: RwLock<HashMap<String, WsSymbolState>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl WsMarketState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn update_quote(
        &self,
        symbol: &str,
        best_bid: f64,
        best_ask: f64,
        bid_size: f64,
        ask_size: f64,
        observed_at_ms: i64,
    ) {
        let mut symbols = self.symbols.write().expect("lock");
        let state = symbols.entry(symbol.to_string()).or_default();
        state.quote = Some(WsBookQuote {
            best_bid,
            best_ask,
            bid_size,
            ask_size,
            observed_at_ms,
        });
    }

    pub(crate) fn update_funding(
        &self,
        symbol: &str,
        funding_rate: f64,
        funding_timestamp_ms: i64,
    ) {
        let mut symbols = self.symbols.write().expect("lock");
        let state = symbols.entry(symbol.to_string()).or_default();
        state.funding_rate = Some(funding_rate);
        state.funding_timestamp_ms = Some(funding_timestamp_ms);
    }

    pub(crate) fn quote(&self, symbol: &str) -> Option<WsBookQuote> {
        self.symbols
            .read()
            .expect("lock")
            .get(symbol)
            .and_then(|state| state.quote.clone())
    }

    pub(crate) fn funding(&self, symbol: &str) -> Option<(f64, i64)> {
        self.symbols
            .read()
            .expect("lock")
            .get(symbol)
            .and_then(|state| state.funding_rate.zip(state.funding_timestamp_ms))
    }

    pub(crate) fn snapshot(&self, symbol: &str) -> Option<SymbolMarketSnapshot> {
        let symbols = self.symbols.read().expect("lock");
        let state = symbols.get(symbol)?;
        let quote = state.quote.as_ref()?;
        let (funding_rate, funding_timestamp_ms) =
            state.funding_rate.zip(state.funding_timestamp_ms)?;
        Some(SymbolMarketSnapshot {
            symbol: symbol.to_string(),
            best_bid: quote.best_bid,
            best_ask: quote.best_ask,
            bid_size: quote.bid_size,
            ask_size: quote.ask_size,
            funding_rate,
            funding_timestamp_ms,
        })
    }

    pub(crate) fn set_worker(&self, worker: JoinHandle<()>) {
        self.worker.lock().expect("lock").replace(worker);
    }

    pub(crate) fn abort_worker(&self) {
        if let Some(worker) = self.worker.lock().expect("lock").take() {
            worker.abort();
        }
    }
}

pub(crate) fn spawn_ws_loop<F>(
    venue_name: &'static str,
    url: String,
    subscribe_messages: Vec<String>,
    state: Arc<WsMarketState>,
    handler: F,
) where
    F: Fn(&Arc<WsMarketState>, &str) -> Result<()> + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    let state_for_task = state.clone();
    let task = tokio::spawn(async move {
        loop {
            match connect_async(url.as_str()).await {
                Ok((mut socket, _)) => {
                    debug!(venue = venue_name, "market websocket connected");
                    let mut failed = false;
                    for message in &subscribe_messages {
                        if let Err(error) = socket.send(Message::Text(message.clone().into())).await
                        {
                            warn!(
                                venue = venue_name,
                                ?error,
                                "market websocket subscribe failed"
                            );
                            failed = true;
                            break;
                        }
                    }
                    if failed {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    while let Some(message) = socket.next().await {
                        match message {
                            Ok(Message::Text(text)) => {
                                if let Err(error) = handler(&state_for_task, text.as_ref()) {
                                    debug!(
                                        venue = venue_name,
                                        ?error,
                                        "market websocket message ignored"
                                    );
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                if let Err(error) = socket.send(Message::Pong(payload)).await {
                                    warn!(
                                        venue = venue_name,
                                        ?error,
                                        "market websocket pong failed"
                                    );
                                    break;
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                debug!(venue = venue_name, ?frame, "market websocket closed");
                                break;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(
                                    venue = venue_name,
                                    ?error,
                                    "market websocket receive failed"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        venue = venue_name,
                        ?error,
                        "market websocket connect failed"
                    );
                }
            }

            sleep(Duration::from_secs(1)).await;
        }
    });
    state.set_worker(task);
}

pub(crate) fn merged_quote_snapshot(
    symbol: &str,
    quote: WsBookQuote,
    funding_rate: f64,
    funding_timestamp_ms: i64,
) -> SymbolMarketSnapshot {
    SymbolMarketSnapshot {
        symbol: symbol.to_string(),
        best_bid: quote.best_bid,
        best_ask: quote.best_ask,
        bid_size: quote.bid_size,
        ask_size: quote.ask_size,
        funding_rate,
        funding_timestamp_ms,
    }
}

pub(crate) fn parse_text_message(raw: &str) -> Result<serde_json::Value> {
    serde_json::from_str(raw).context("failed to decode websocket json payload")
}

#[cfg(test)]
mod tests {
    use super::{merged_quote_snapshot, WsBookQuote, WsMarketState};

    #[test]
    fn market_state_merges_quote_and_funding_updates() {
        let state = WsMarketState::new();
        state.update_quote("ETHUSDT", 2140.0, 2141.0, 12.0, 11.0, 10);
        state.update_funding("ETHUSDT", 0.0001, 20);

        let snapshot = state.snapshot("ETHUSDT").expect("cached snapshot");
        assert_eq!(snapshot.best_bid, 2140.0);
        assert_eq!(snapshot.ask_size, 11.0);
        assert_eq!(snapshot.funding_rate, 0.0001);
        assert_eq!(snapshot.funding_timestamp_ms, 20);
    }

    #[test]
    fn merged_quote_snapshot_keeps_quote_depth() {
        let snapshot = merged_quote_snapshot(
            "ETHUSDT",
            WsBookQuote {
                best_bid: 2140.0,
                best_ask: 2141.0,
                bid_size: 9.0,
                ask_size: 8.0,
                observed_at_ms: 5,
            },
            0.0002,
            100,
        );
        assert_eq!(snapshot.bid_size, 9.0);
        assert_eq!(snapshot.funding_timestamp_ms, 100);
    }
}
