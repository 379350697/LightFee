# Funding Window And Dual-Position Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restrict opportunity scanning to the last 15 to 5 minutes before settlement, allow up to two concurrent hedged symbols, and temporarily cap live per-leg notional at 30 USDT.

**Architecture:** Keep the existing per-tick engine, but split the tick into two modes: manage/recover always, scan/enter only inside a funding scan window. Replace the single `open_position` state with a bounded collection of independently managed hedged positions, then select at most two distinct symbols per scan using a risk-adjusted ranking built from existing edge and venue-health signals.

**Tech Stack:** Rust, Tokio, Serde, existing live adapters, existing JSONL journal/state snapshot system.

---

### Task 1: Add the new engine-state and config tests first

**Files:**
- Modify: `tests/engine_flow.rs`
- Modify: `src/config.rs`
- Test: `tests/engine_flow.rs`

- [ ] **Step 1: Write a failing test for scan-window gating**

Add a test that sets market timestamps outside `hh:45:00~hh:55:00`, verifies `scan.completed` is skipped for entry discovery, and confirms open-position management still runs.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `cargo test --test engine_flow scan_window`
Expected: FAIL because the engine still scans on every tick.

- [ ] **Step 3: Write a failing test for holding two distinct symbols**

Add a test that creates tradeable candidates for at least three symbols, runs the engine, and verifies only the best two distinct symbols can become active positions while the third remains unopened.

- [ ] **Step 4: Run the focused test to verify it fails**

Run: `cargo test --test engine_flow dual_symbol`
Expected: FAIL because the engine only supports a single `open_position`.

- [ ] **Step 5: Write a failing test for the 30 USDT live cap**

Add a test that configures higher notional, marks runtime as live, and verifies effective requested entry notional is clamped to 30 USDT per leg before quantity normalization.

- [ ] **Step 6: Run the focused test to verify it fails**

Run: `cargo test --test engine_flow live_notional_cap`
Expected: FAIL because live mode currently uses the configured notional unchanged.


### Task 2: Teach config and strategy about the funding scan window and live cap

**Files:**
- Modify: `src/config.rs`
- Modify: `src/strategy.rs`
- Modify: `config/example.toml`
- Modify: `config/live.example.toml`
- Modify: `config/live.smoke.toml`
- Modify: `config/live.smoke.exec.toml`
- Modify: `config/live.smoke.binance.exec.toml`
- Modify: `config/live.smoke.bybit.exec.toml`

- [ ] **Step 1: Add config fields for the scan window and temporary live cap**

Introduce defaults and serde fields for:
- scan-window start minutes before funding
- scan-window end minutes before funding
- max concurrent positions
- live per-leg notional cap

- [ ] **Step 2: Apply the cap only where it belongs**

Update candidate sizing so runtime live mode uses `min(configured_max_entry_notional, live_cap)` while paper mode keeps the configured value.

- [ ] **Step 3: Add strategy helpers for scan-window eligibility**

Implement a helper that determines whether a quote’s settlement timestamp is inside the allowed scan window and expose it in a way engine tests can exercise directly.

- [ ] **Step 4: Run focused tests**

Run: `cargo test --test engine_flow scan_window live_notional_cap`
Expected: PASS.


### Task 3: Replace single-position state with a bounded multi-position set

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/lib.rs`
- Test: `tests/engine_flow.rs`

- [ ] **Step 1: Refactor engine state from one optional position to a keyed collection**

Use a small bounded map/vector keyed by `position_id` while preserving serialization and snapshot comparison behavior.

- [ ] **Step 2: Update recovery and persistence logic**

Teach journal replay, live recovery probing, reconcile, and persistence to handle zero, one, or two independently tracked positions.

- [ ] **Step 3: Update position management**

Run `manage_open_position` per active position, preserve existing exit logic, and keep fail-closed behavior if any managed position becomes inconsistent.

- [ ] **Step 4: Run focused tests**

Run: `cargo test --test engine_flow dual_symbol restart_recovers_open_position_from_journal_when_snapshot_missing`
Expected: PASS.


### Task 4: Select and open up to two symbols per scan

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/analysis.rs`
- Modify: `src/bin/journal_report.rs`
- Test: `tests/engine_flow.rs`

- [ ] **Step 1: Add a candidate selector for up to two distinct symbols**

Reuse existing ranking, then enforce:
- tradeable only
- distinct symbols
- skip symbols already active
- stop at configured `max_concurrent_positions`

- [ ] **Step 2: Preserve risk-first entry sequencing per symbol**

For each selected candidate, keep the current `leg_risk_score` and compensation flow unchanged.

- [ ] **Step 3: Update journals and offline replay**

Ensure reports can represent multiple simultaneous positions in the same run without assuming a single active trade.

- [ ] **Step 4: Run focused tests**

Run: `cargo test --test engine_flow dual_symbol`
Expected: PASS.


### Task 5: Update docs and run full verification

**Files:**
- Modify: `README.md`
- Modify: `config/example.toml`
- Modify: `config/live.example.toml`

- [ ] **Step 1: Document the new operating model**

Describe:
- scan only in the last 15 to 5 minutes before funding
- up to two concurrent symbols
- temporary 30 USDT live cap per leg

- [ ] **Step 2: Run formatting and all tests**

Run: `cargo fmt --check`
Expected: PASS

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/config.rs src/strategy.rs src/engine.rs src/lib.rs tests/engine_flow.rs src/analysis.rs src/bin/journal_report.rs README.md config/example.toml config/live.example.toml config/live.smoke.toml config/live.smoke.exec.toml config/live.smoke.binance.exec.toml config/live.smoke.bybit.exec.toml docs/superpowers/plans/2026-03-20-funding-window-dual-position.md
git commit -m "feat: add funding scan window and dual position engine"
```
