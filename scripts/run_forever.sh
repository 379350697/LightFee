#!/usr/bin/env bash
set -uo pipefail

bin_path="${LIGHTFEE_BIN:-./target/release/lightfee}"
config_path="${1:-config/live.example.toml}"
restart_sec="${LIGHTFEE_RESTART_SEC:-3}"

while true; do
  "$bin_path" "$config_path"
  status=$?
  printf '%s lightfee exited with status=%s, restarting in %ss\n' \
    "$(date -Is)" "$status" "$restart_sec" >&2
  sleep "$restart_sec"
done
