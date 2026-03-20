#!/usr/bin/env bash
set -uo pipefail

bin_path="${LIGHTFEE_BIN:-./target/release/lightfee}"
config_path="${1:-config/live.example.toml}"
restart_sec="${LIGHTFEE_RESTART_SEC:-3}"
restart_max_sec="${LIGHTFEE_RESTART_MAX_SEC:-60}"
stable_run_reset_sec="${LIGHTFEE_STABLE_RUN_RESET_SEC:-120}"
current_restart_sec="$restart_sec"

while true; do
  started_at="$(date +%s)"
  "$bin_path" "$config_path"
  status=$?
  runtime_sec="$(( $(date +%s) - started_at ))"
  if [ "$runtime_sec" -ge "$stable_run_reset_sec" ]; then
    current_restart_sec="$restart_sec"
  else
    current_restart_sec="$(( current_restart_sec * 2 ))"
    if [ "$current_restart_sec" -gt "$restart_max_sec" ]; then
      current_restart_sec="$restart_max_sec"
    fi
  fi
  printf '%s lightfee exited with status=%s, restarting in %ss\n' \
    "$(date -Is)" "$status" "$current_restart_sec" >&2
  sleep "$current_restart_sec"
done
