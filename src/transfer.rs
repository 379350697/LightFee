use std::collections::BTreeMap;

use crate::models::{AssetTransferStatus, Venue};

#[derive(Clone, Debug, Default)]
pub struct TransferStatusView {
    by_asset: BTreeMap<(Venue, String), AssetTransferStatus>,
}

impl TransferStatusView {
    pub fn from_statuses(statuses: Vec<AssetTransferStatus>) -> Self {
        let mut by_asset = BTreeMap::new();
        for status in statuses {
            let key = (status.venue, status.asset.to_ascii_uppercase());
            by_asset
                .entry(key)
                .and_modify(|current: &mut AssetTransferStatus| {
                    current.deposit_enabled |= status.deposit_enabled;
                    current.withdraw_enabled |= status.withdraw_enabled;
                    current.observed_at_ms = current.observed_at_ms.max(status.observed_at_ms);
                    if current.source != status.source {
                        current.source = "mixed".to_string();
                    }
                })
                .or_insert_with(|| AssetTransferStatus {
                    asset: status.asset.to_ascii_uppercase(),
                    ..status
                });
        }

        Self { by_asset }
    }

    pub fn asset_status(&self, venue: Venue, asset: &str) -> Option<&AssetTransferStatus> {
        self.by_asset
            .get(&(venue, asset.trim().to_ascii_uppercase()))
    }
}
