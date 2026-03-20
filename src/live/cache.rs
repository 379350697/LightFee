use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{de::DeserializeOwned, Serialize};
use tracing::warn;

use crate::models::AssetTransferStatus;

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

    use crate::models::Venue;

    use super::{
        cache_is_fresh, filter_transfer_statuses, transfer_cache_ttl_ms, VenueTransferStatusCache,
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
}
