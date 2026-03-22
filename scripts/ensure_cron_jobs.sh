#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/root/.openclaw/workspace-coder/logs"

HOURLY_JOB="25 * * * * /usr/bin/python3 ${ROOT_DIR}/scripts/hourly_close_reconcile.py >> ${LOG_DIR}/hourly-close-reconcile.log 2>&1"
DAILY_JOB="30 9 * * * cd ${ROOT_DIR} && ./target/release/daily_db_snapshot config/live.auto.toml >> ${LOG_DIR}/daily-db-snapshot.log 2>&1"

current="$(crontab -l 2>/dev/null || true)"
new="$current"

if ! printf '%s\n' "$current" | grep -Fq "$HOURLY_JOB"; then
  new="${new}${new:+$'\n'}${HOURLY_JOB}"
fi

if ! printf '%s\n' "$current" | grep -Fq "$DAILY_JOB"; then
  new="${new}${new:+$'\n'}${DAILY_JOB}"
fi

# only write when changed
if [ "$new" != "$current" ]; then
  printf '%s\n' "$new" | crontab -
  echo "[ensure_cron_jobs] cron updated"
else
  echo "[ensure_cron_jobs] cron already up-to-date"
fi
