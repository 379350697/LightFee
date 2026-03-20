# Staggered Funding Arbitrage Design

Date: 2026-03-20

## Goal

Keep the existing aligned funding-arbitrage path and add a second class of opportunities where the two legs do not settle funding at the same time.

The new logic should allow trades such as:

- leg A settles funding in the next hour
- leg B settles funding in four or eight hours
- the system enters because leg A alone offers enough near-term net funding edge

This expands opportunity capture without forcing the engine to rely on distant or uncertain later funding payments.

## Current Gap

Today candidate discovery compresses both legs into a single `funding_timestamp_ms` by taking the earlier of the two venues. That works for aligned settlements but loses important structure for staggered opportunities:

- it does not distinguish which leg settles first
- it does not model the first capturable funding leg separately
- it cannot reason about a second stage where the later leg may or may not still be worth holding for

As a result, some valid staggered funding trades are either ignored or scored inaccurately.

## Scope

This change adds:

- candidate discovery for staggered funding opportunities
- first-stage funding capture scoring
- configurable exit behavior after the first leg settles
- journal/reporting visibility for aligned vs staggered trades

This change does not add:

- multi-stage probabilistic forecasting
- carry trades held for many settlement cycles
- extra live network requests on the trading hot path

## Opportunity Types

Candidate generation will classify each pair into one of two types:

- `aligned`
  - both legs have the same next funding timestamp, or timestamps are close enough to be treated as the same event
- `staggered`
  - the legs have materially different next funding timestamps and one leg clearly settles first

The aligned path preserves current behavior.

The staggered path adds a new scoring and lifecycle model.

## Data Model Changes

### CandidateOpportunity

Replace the single settlement view with per-leg funding timing:

- `long_funding_timestamp_ms`
- `short_funding_timestamp_ms`
- `first_funding_leg`
- `first_funding_timestamp_ms`
- `second_funding_timestamp_ms`
- `opportunity_type`

Keep existing fields that remain useful:

- `funding_edge_bps`
- `expected_edge_bps`
- `worst_case_edge_bps`
- `ranking_edge_bps`

Add staggered-specific economics:

- `first_stage_funding_edge_bps`
- `first_stage_expected_edge_bps`
- `second_stage_incremental_funding_edge_bps`
- `stagger_gap_ms`

### OpenPosition

Persist enough data to manage staged exits after restart:

- `opportunity_type`
- `first_funding_leg`
- `first_funding_timestamp_ms`
- `second_funding_timestamp_ms`
- `first_stage_funding_captured`
- `second_stage_enabled_at_entry`
- `exit_after_first_stage`

## Candidate Discovery

### Aligned Opportunities

Keep the current flow:

- compare long/short funding rates
- score combined entry edge
- use the shared settlement event

### Staggered Opportunities

For each directed pair:

1. Read each leg's next funding timestamp independently.
2. Identify which leg settles first.
3. Compute first-stage capturable funding using only the earlier leg.
4. Subtract:
   - taker fees
   - estimated entry slippage
   - reserved exit slippage
   - execution buffer
   - capital buffer
5. Only keep the candidate if first-stage net edge is above configured floors.

This means a staggered opportunity is allowed even when the later leg has not reached its own settlement window yet.

## Scoring Rules

### Default Rule

The engine will score staggered opportunities by first-stage net edge, not by total theoretical carry across both future settlements.

Rationale:

- it is the nearest realizable cash flow
- it avoids optimistic assumptions about later funding persistence
- it keeps holding time short by default

### Ranking

For aligned opportunities, existing ranking remains the base path.

For staggered opportunities:

- primary score: `first_stage_expected_edge_bps`
- secondary score: `worst_case_edge_bps`
- tertiary adjustments:
  - transfer preference bias
  - venue health bias
  - leg-risk ordering inputs already present in the engine

The engine should compare aligned and staggered candidates in one shared candidate set so the best opportunities can still win globally.

## Entry Window Behavior

Aligned opportunities keep the current window semantics.

Staggered opportunities use the first settlement leg for entry timing:

- scan window is evaluated against `first_funding_timestamp_ms`
- if the first leg is outside the allowed pre-funding window, the staggered candidate is blocked
- the later leg's settlement time does not block entry on its own

## Exit Policy

Add a configurable exit mode for staggered positions.

### Default

`after_first_stage`

Behavior:

- once the first funding leg has settled
- and the configured post-funding hold has elapsed
- the engine prefers to exit and lock in the first-stage result

### Optional Later Mode

`evaluate_second_stage`

Behavior:

- after first-stage capture, keep the position alive
- re-evaluate whether the second stage still has positive net expected value
- if not, exit
- if yes, continue until the later stage or an existing stop/profit rule triggers

This mode is opt-in. The default remains conservative.

## Position Lifecycle

### Aligned

No meaningful lifecycle change.

### Staggered

Lifecycle becomes:

1. enter before first settlement
2. wait for first settlement
3. mark first-stage funding captured
4. branch:
   - default: exit after first stage
   - optional: continue to second-stage evaluation

Existing stop-loss, trailing, venue failure, fail-closed, and compensation logic remains active throughout.

## Risk Controls

Staggered opportunities should be stricter than aligned ones.

Add configuration for:

- maximum allowed funding timestamp gap
- minimum first-stage edge
- optional maximum first-stage hold time

Existing controls still apply:

- stale market blocking
- top-of-book size cap
- live max per-leg notional
- cached flat guard
- venue cooldowns
- Hyperliquid submit-ack gate
- leg-risk-first order sequencing

## Logging And Reporting

Journal events should include:

- `opportunity_type`
- `first_funding_leg`
- `first_funding_timestamp_ms`
- `second_funding_timestamp_ms`
- `first_stage_expected_edge_bps`
- `second_stage_incremental_funding_edge_bps`
- chosen staggered exit mode

For open positions and exits, record:

- whether exit happened after first-stage capture
- whether second-stage continuation was enabled
- which stage was actually captured

Offline reporting should later be able to answer:

- are staggered opportunities profitable after fees and slippage
- which venues/symbols work best for staggered trades
- does second-stage continuation help or hurt compared with default first-stage exits

## Recovery Semantics

Restart recovery must preserve staged funding state.

Recovered positions need enough metadata to know:

- whether the trade is aligned or staggered
- whether first-stage capture already occurred
- whether the configured policy says to exit now or continue

If recovery metadata is incomplete for a staggered position, the engine should fail closed rather than guessing.

## Testing Plan

Add tests before implementation for:

- aligned funding opportunities still behave as before
- staggered candidate appears when first-stage net funding edge is positive
- staggered candidate is blocked when first-stage net edge is below floor
- scan window for staggered candidates keys off the first settlement leg
- default mode exits after first-stage capture
- optional continuation mode keeps the position alive into second-stage evaluation
- restart recovery preserves staggered position state

## Recommended Implementation Order

1. Extend config and models for staggered funding metadata and exit mode.
2. Add failing strategy tests for staggered candidate discovery.
3. Implement staggered candidate generation while preserving aligned behavior.
4. Add failing engine tests for first-stage capture and default exit.
5. Implement staged position lifecycle in the engine.
6. Extend journal/report metadata.
7. Add recovery tests for staggered positions.

## Recommendation

Implement the conservative staggered model first:

- keep aligned opportunities unchanged
- allow staggered entry only when first-stage net edge is already positive
- default to exiting after first-stage capture

That adds real new opportunity coverage while keeping holding risk and behavioral complexity under control.
