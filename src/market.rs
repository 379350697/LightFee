use std::collections::BTreeMap;

use crate::models::{SymbolMarketSnapshot, Venue, VenueMarketSnapshot};

#[derive(Clone, Debug, Default)]
pub struct MarketView {
    now_ms: i64,
    observed_at_by_venue: BTreeMap<Venue, i64>,
    symbols: BTreeMap<(Venue, String), SymbolMarketSnapshot>,
}

impl MarketView {
    pub fn from_snapshots(snapshots: Vec<VenueMarketSnapshot>) -> Self {
        let mut view = Self::default();

        for snapshot in snapshots {
            view.now_ms = view.now_ms.max(snapshot.observed_at_ms);
            view.observed_at_by_venue
                .insert(snapshot.venue, snapshot.observed_at_ms);
            for symbol in snapshot.symbols {
                view.symbols
                    .insert((snapshot.venue, symbol.symbol.clone()), symbol);
            }
        }

        view
    }

    pub fn empty(now_ms: i64) -> Self {
        Self {
            now_ms,
            ..Self::default()
        }
    }

    pub fn now_ms(&self) -> i64 {
        self.now_ms
    }

    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }

    pub fn symbol(&self, venue: Venue, symbol: &str) -> Option<&SymbolMarketSnapshot> {
        self.symbols.get(&(venue, symbol.to_string()))
    }

    pub fn observed_at_ms(&self, venue: Venue) -> Option<i64> {
        self.observed_at_by_venue.get(&venue).copied()
    }

    pub fn is_fresh(&self, venue: Venue, max_age_ms: i64) -> bool {
        self.observed_at_by_venue
            .get(&venue)
            .map(|observed_at| self.now_ms.saturating_sub(*observed_at) <= max_age_ms)
            .unwrap_or(false)
    }

    pub fn venue_symbol_snapshot(&self, venue: Venue, symbol: &str) -> Option<VenueMarketSnapshot> {
        Some(VenueMarketSnapshot {
            venue,
            observed_at_ms: self.observed_at_ms(venue)?,
            symbols: vec![self.symbol(venue, symbol)?.clone()],
        })
    }

    pub fn merge_snapshot(&mut self, snapshot: VenueMarketSnapshot) {
        self.now_ms = self.now_ms.max(snapshot.observed_at_ms);
        self.observed_at_by_venue
            .insert(snapshot.venue, snapshot.observed_at_ms);
        for symbol in snapshot.symbols {
            self.symbols
                .insert((snapshot.venue, symbol.symbol.clone()), symbol);
        }
    }
}
