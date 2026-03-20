use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Clone, Debug)]
pub struct FileStateStore {
    path: PathBuf,
}

impl FileStateStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn load<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let raw = fs::read_to_string(&self.path)
            .with_context(|| format!("failed to read snapshot {}", self.path.display()))?;
        let value = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse snapshot {}", self.path.display()))?;
        Ok(Some(value))
    }

    pub fn save<T: Serialize>(&self, value: &T) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }

        let tmp_path = self.path.with_extension("tmp");
        let raw = serde_json::to_vec_pretty(value)?;
        fs::write(&tmp_path, raw)
            .with_context(|| format!("failed to write snapshot {}", tmp_path.display()))?;
        fs::rename(&tmp_path, &self.path).with_context(|| {
            format!(
                "failed to atomically move snapshot {} -> {}",
                tmp_path.display(),
                self.path.display()
            )
        })?;
        Ok(())
    }
}
