use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{de::DeserializeOwned, Serialize};
use tracing::warn;

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
