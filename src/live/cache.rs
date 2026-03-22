use std::{
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{params, Connection, OptionalExtension};
use serde::{de::DeserializeOwned, Serialize};
use tracing::warn;

use crate::models::{AccountFeeSnapshot, AssetTransferStatus, Venue};

pub(crate) const SYMBOL_CACHE_TTL_MS: i64 = 6 * 60 * 60 * 1000;
pub(crate) const TRANSFER_CACHE_TTL_MS: i64 = 5 * 60 * 1000;

pub(crate) fn cache_is_fresh(updated_at_ms: i64, now_ms: i64, ttl_ms: i64) -> bool {
    now_ms.saturating_sub(updated_at_ms) <= ttl_ms.max(0)
}

pub(crate) fn load_json_cache<T: DeserializeOwned>(filename: &str) -> Option<T> {
    let path = cache_path(filename);
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

pub(crate) fn store_json_cache<T: Serialize>(filename: &str, value: &T) {
    let path = cache_path(filename);
    if let Err(error) = store_json_path(&path, value) {
        warn!(?error, path = %path.display(), "failed to persist live cache");
    }
}

pub(crate) fn load_account_fee_snapshot_cache(
    venue: Venue,
    filename: &str,
) -> Option<AccountFeeSnapshot> {
    load_account_fee_snapshot_sqlite(venue).or_else(|| load_json_cache(filename))
}

pub(crate) fn store_account_fee_snapshot_cache(filename: &str, snapshot: &AccountFeeSnapshot) {
    store_json_cache(filename, snapshot);
    if let Err(error) = store_account_fee_snapshot_sqlite(snapshot) {
        warn!(?error, venue = %snapshot.venue, "failed to persist live fee snapshot to sqlite");
    }
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub(crate) struct VenueTransferStatusCache {
    pub observed_at_ms: i64,
    pub statuses: Vec<AssetTransferStatus>,
}

pub(crate) fn transfer_cache_ttl_ms(runtime_ttl_ms: u64) -> i64 {
    (runtime_ttl_ms.max(TRANSFER_CACHE_TTL_MS as u64)).min(i64::MAX as u64) as i64
}

pub(crate) fn filter_transfer_statuses(
    cache: &VenueTransferStatusCache,
    wanted: &std::collections::BTreeSet<String>,
) -> Vec<AssetTransferStatus> {
    cache
        .statuses
        .iter()
        .filter(|status| wanted.is_empty() || wanted.contains(status.asset.as_str()))
        .cloned()
        .collect()
}

fn cache_path(filename: &str) -> PathBuf {
    Path::new("runtime").join("cache").join(filename)
}

fn sqlite_cache_path() -> PathBuf {
    Path::new("runtime").join("cache").join("live.sqlite3")
}

fn ensure_fee_snapshot_schema(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute(
        "CREATE TABLE IF NOT EXISTS account_fee_snapshots (
            venue TEXT PRIMARY KEY,
            taker_fee_bps REAL NOT NULL,
            maker_fee_bps REAL NOT NULL,
            taker_fee_pct REAL NOT NULL DEFAULT 0,
            maker_fee_pct REAL NOT NULL DEFAULT 0,
            observed_at_ms INTEGER NOT NULL,
            source TEXT NOT NULL
        )",
        [],
    )?;
    for (column, default_value) in [
        ("taker_fee_pct", "0"),
        ("maker_fee_pct", "0"),
    ] {
        let alter = format!(
            "ALTER TABLE account_fee_snapshots ADD COLUMN {column} REAL NOT NULL DEFAULT {default_value}"
        );
        match connection.execute(&alter, []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(message)))
                if message.to_ascii_lowercase().contains("duplicate column name") => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn open_sqlite_connection(path: &Path) -> rusqlite::Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    }
    let connection = Connection::open(path)?;
    ensure_fee_snapshot_schema(&connection)?;
    Ok(connection)
}

fn load_account_fee_snapshot_sqlite(venue: Venue) -> Option<AccountFeeSnapshot> {
    load_account_fee_snapshot_sqlite_from_path(&sqlite_cache_path(), venue)
        .map_err(|error| {
            warn!(?error, venue = %venue, "failed to load live fee snapshot from sqlite");
            error
        })
        .ok()
        .flatten()
}

fn load_account_fee_snapshot_sqlite_from_path(
    path: &Path,
    venue: Venue,
) -> anyhow::Result<Option<AccountFeeSnapshot>> {
    let connection = open_sqlite_connection(path)?;
    let venue_key = venue.as_str();
    let snapshot = connection
        .query_row(
            "SELECT taker_fee_bps, maker_fee_bps, observed_at_ms, source
             FROM account_fee_snapshots
             WHERE venue = ?1",
            [venue_key],
            |row| {
                Ok(AccountFeeSnapshot {
                    venue,
                    taker_fee_bps: row.get(0)?,
                    maker_fee_bps: row.get(1)?,
                    observed_at_ms: row.get(2)?,
                    source: row.get(3)?,
                })
            },
        )
        .optional()?;
    Ok(snapshot)
}

fn store_account_fee_snapshot_sqlite(snapshot: &AccountFeeSnapshot) -> anyhow::Result<()> {
    store_account_fee_snapshot_sqlite_to_path(&sqlite_cache_path(), snapshot)
}

fn store_account_fee_snapshot_sqlite_to_path(
    path: &Path,
    snapshot: &AccountFeeSnapshot,
) -> anyhow::Result<()> {
    let connection = open_sqlite_connection(path)?;
    connection.execute(
        "INSERT INTO account_fee_snapshots (
            venue,
            taker_fee_bps,
            maker_fee_bps,
            taker_fee_pct,
            maker_fee_pct,
            observed_at_ms,
            source
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(venue) DO UPDATE SET
            taker_fee_bps = excluded.taker_fee_bps,
            maker_fee_bps = excluded.maker_fee_bps,
            taker_fee_pct = excluded.taker_fee_pct,
            maker_fee_pct = excluded.maker_fee_pct,
            observed_at_ms = excluded.observed_at_ms,
            source = excluded.source",
        params![
            snapshot.venue.as_str(),
            snapshot.taker_fee_bps,
            snapshot.maker_fee_bps,
            snapshot.taker_fee_bps / 100.0,
            snapshot.maker_fee_bps / 100.0,
            snapshot.observed_at_ms,
            snapshot.source,
        ],
    )?;
    Ok(())
}

fn store_json_path<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    let payload = serde_json::to_vec(value)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))?;
    fs::write(&tmp_path, payload)?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rusqlite::{params, Connection};
    use tempfile::tempdir;

    use crate::models::{AccountFeeSnapshot, Venue};

    use super::{
        cache_is_fresh, filter_transfer_statuses, load_account_fee_snapshot_sqlite_from_path,
        store_account_fee_snapshot_sqlite_to_path, transfer_cache_ttl_ms, VenueTransferStatusCache,
    };

    #[test]
    fn transfer_cache_uses_minimum_floor_ttl() {
        assert_eq!(transfer_cache_ttl_ms(1_000), 300_000);
        assert_eq!(transfer_cache_ttl_ms(600_000), 600_000);
    }

    #[test]
    fn transfer_cache_filters_only_requested_assets() {
        let cache = VenueTransferStatusCache {
            observed_at_ms: 1_000,
            statuses: vec![
                crate::models::AssetTransferStatus {
                    venue: Venue::Binance,
                    asset: "BTC".to_string(),
                    deposit_enabled: true,
                    withdraw_enabled: true,
                    observed_at_ms: 1_000,
                    source: "binance".to_string(),
                },
                crate::models::AssetTransferStatus {
                    venue: Venue::Binance,
                    asset: "ETH".to_string(),
                    deposit_enabled: true,
                    withdraw_enabled: false,
                    observed_at_ms: 1_000,
                    source: "binance".to_string(),
                },
            ],
        };

        let wanted = BTreeSet::from(["BTC".to_string()]);
        let filtered = filter_transfer_statuses(&cache, &wanted);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].asset, "BTC");
        assert!(cache_is_fresh(cache.observed_at_ms, 5_000, 300_000));
        assert!(!cache_is_fresh(cache.observed_at_ms, 400_001, 300_000));
    }

    #[test]
    fn sqlite_fee_snapshot_round_trip_works_per_venue() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("live.sqlite3");
        let snapshot = AccountFeeSnapshot {
            venue: Venue::Gate,
            taker_fee_bps: 5.0,
            maker_fee_bps: 2.0,
            observed_at_ms: 1234,
            source: "gate_futures_fee".to_string(),
        };

        store_account_fee_snapshot_sqlite_to_path(&path, &snapshot).expect("store");
        let loaded = load_account_fee_snapshot_sqlite_from_path(&path, Venue::Gate).expect("load");

        assert_eq!(loaded, Some(snapshot));

        let connection = Connection::open(&path).expect("open sqlite");
        let (taker_fee_bps, maker_fee_bps, taker_fee_pct, maker_fee_pct): (f64, f64, f64, f64) =
            connection
                .query_row(
                    "SELECT taker_fee_bps, maker_fee_bps, taker_fee_pct, maker_fee_pct
                     FROM account_fee_snapshots
                     WHERE venue = ?1",
                    params![Venue::Gate.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("query fee snapshot row");
        assert_eq!(taker_fee_bps, 5.0);
        assert_eq!(maker_fee_bps, 2.0);
        assert!((taker_fee_pct - 0.05).abs() < 1e-12);
        assert!((maker_fee_pct - 0.02).abs() < 1e-12);

        assert_eq!(
            load_account_fee_snapshot_sqlite_from_path(&path, Venue::Binance).expect("load other"),
            None
        );
    }
}
