# LightFee

LightFee is a lightweight unattended funding-arbitrage engine for the four-venue same-symbol workflow:

- long the lower-funding venue
- short the higher-funding venue
- evaluate net edge after fees, slippage, and buffers
- keep aligned funding-settlement arbitrage and also detect staggered settlement opportunities
- enter with a narrow taker-only execution path
- scan only near settlement, when funding comparisons are meaningful
- hold through funding
- exit automatically on funding capture, trailing giveback, hard stop, or recovery failure

This repository intentionally avoids the heavyweight control-plane shape from larger trading platforms. The hot path lives in one Rust service with a small set of modules:

- `config`: TOML runtime and strategy settings
- `venue`: a narrow exchange adapter trait
- `strategy`: candidate discovery and edge scoring
- `engine`: unattended supervisor, execution state machine, and restart recovery
- `store` + `journal`: state snapshotting and JSONL event logging
- `simulator`: scripted paper adapters for local verification

## Quick start

```bash
cargo test
cargo run -- config/example.toml
cargo run --bin journal_report -- runtime/events.jsonl
```

The bundled config runs in `paper` mode using scripted venue feeds from `config/demo/`.

## Design choices

- single binary crate instead of a multi-crate platform
- taker-only v1 execution path for deterministic unattended behavior
- scan/entry discovery limited to the last 15 to 5 minutes before funding by default
- up to two concurrent hedged symbols, selected by edge plus execution-risk balance
- temporary live per-leg notional cap of 30 USDT until broader live soak testing is complete
- staggered funding opportunities are scored on first-stage realizable edge by default, with configurable continuation into a second stage
- submit-time quote expiry checks, top-of-book size capping, and short venue cooldowns after uncertain order outcomes
- fail-closed restart recovery with venue position reconciliation
- async JSONL event journal with bounded non-blocking telemetry and sync-flushed critical recovery markers
- JSON snapshot store instead of a database
- exchange-specific behavior isolated behind `VenueAdapter`

## Journal semantics

The journal is designed for unattended operation:

- hot-path telemetry is appended asynchronously on a background writer thread
- non-critical telemetry uses a bounded queue and is dropped with counters instead of growing memory without bound
- critical state transitions such as `entry.opened`, `exit.closed`, and `recovery.*` are flushed synchronously
- each record carries `seq`, `run_id`, `ts_ms`, `kind`, and `payload`
- execution logs include common executable quantity normalization, per-leg order submissions, fills, failures, and local round-trip latency
- runtime write-path counters are exposed in memory via `metrics_snapshot()` without adding blocking work to order execution

For offline analysis, use `journal_report`:

```bash
cargo run --bin journal_report -- runtime/events.jsonl
cargo run --bin journal_report -- --json runtime/events.jsonl
```

It summarizes:

- per-venue order counts and failure rate
- per-venue latency `p50/p95/p99/max`
- recovery event counts
- fail-closed reason distribution
- runtime gate blocks, venue cooldown counts, and first-leg risk-factor distribution
- recent per-position trade replays including candidate selection, entry plan, per-leg fills/failures, and warnings/errors
- heuristic optimization recommendations with evidence, so recurring latency/timeout/depth issues surface directly in the report

That gives us low-overhead logging during normal trading while still making restart recovery deterministic.

## What is not in v1

- real exchange REST/WebSocket connectors
- GUI or control plane
- multi-symbol portfolio optimizer
- maker queue management

Those can be added later without changing the strategy/runtime boundary.

## Unattended recovery

LightFee now uses three recovery layers for live mode:

- immediate snapshot save after `entry.opened` and `exit.closed`
- JSONL journal replay when the snapshot is missing but the event log still exists
- live position discovery when local state is gone but the exchanges still show a single balanced hedge

Additional unattended guards keep long-running deployments calmer:

- private position caches expire and fall back to exchange truth instead of being trusted forever
- the main tick loop backs off after consecutive full-cycle failures instead of hammering dependencies
- market/private WebSocket reconnects use exponential backoff and mark the stream unhealthy after repeated failures
- the bundled shell/systemd supervisors now rate-limit crash loops instead of restarting at a fixed cadence forever

If live exposure is unbalanced or ambiguous on restart, the engine goes `fail_closed` instead of guessing.

Current unattended entry behavior is intentionally conservative:

- outside the configured funding scan window, the engine still manages/reconciles open positions but skips opportunity discovery
- inside the window, it ranks tradeable candidates and opens at most `max_concurrent_positions`
- candidates with the same symbol as an already-open hedge are skipped
- live mode clamps per-leg notional with `live_max_entry_notional` even if `max_entry_notional` is higher
- staggered opportunities use the first upcoming funding event for entry timing and, by default, exit after the first captured stage
- `staggered_exit_mode = "evaluate_second_stage"` allows carrying into the later leg when the second stage still adds positive funding edge

For process-level auto-restart, use a tiny external supervisor instead of adding a heavy control plane:

- `scripts/run_forever.sh` for a shell-based restart loop
- `deploy/systemd/lightfee.service` for Linux servers using `systemd`

## Live smoke

The `live_smoke` binary is meant for small real-money validation:

- opens the two legs
- immediately submits reduce-only closes
- prints per-order local round-trip latency
- supports `--flatten-open` to close any residual position without opening a new one
