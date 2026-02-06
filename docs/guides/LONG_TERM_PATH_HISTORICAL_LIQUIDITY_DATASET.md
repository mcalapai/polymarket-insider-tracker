# Long-Term Path: Historical Liquidity Dataset for Backtests

## 1. Scope
This guide defines the long-term production path to make liquidity-aware backtests fully historical and reproducible by building a durable liquidity time-series dataset.

Primary objective:
- Build continuous, query-agnostic historical liquidity coverage (`condition_id`, `asset_id`, `computed_at`) so `size_anomaly` can run historically without synthetic fallbacks.

Secondary objective:
- Add observability and quality gates so scan/backtest can enforce minimum data coverage before using liquidity-dependent signals.

## 2. Current State
What exists today:
- Liquidity snapshots are computed from current orderbook depth and stored in `liquidity_snapshots`.
- Snapshot computation is tied to hot markets in live runtime.
- Historical scans require time-aligned snapshots but do not backfill them.

Where this is implemented:
- `src/polymarket_insider_tracker/ingestor/metadata_sync.py` (`_sync_liquidity_for_hot_markets`).
- `src/polymarket_insider_tracker/pipeline.py` (marks hot markets on live trades).
- `src/polymarket_insider_tracker/storage/models.py` (`liquidity_snapshots` model).
- `src/polymarket_insider_tracker/scan/runner.py` (historical `get_latest_before` lookup).

Root gap:
- No dedicated collector for broad, continuous, historical liquidity capture.
- No coverage SLA or quality thresholds per market/time window.

## 3. Intended State
Target behavior after implementation:
- A continuously collected liquidity dataset exists for active tradable markets and is persisted at fixed cadence.
- Each scan/backtest can measure coverage before running liquidity-dependent detectors.
- Liquidity-aware backtests use only snapshots at-or-before trade timestamps.
- Missing historical coverage is handled by explicit policy, not implicit assumptions.

Historical invariants:
- No future leakage.
- No substitution with present-time orderbook depth in historically pure mode.
- Reproducible replay given same snapshot tables and config.

## 4. Step-by-Step Implementation Guide

### Step 1: Define collection universe precisely
1. Use `simplified-markets`/market metadata to define eligible markets for collection.
2. Restrict default universe to tradable markets:
   - `active=true`
   - `closed=false`
   - `accepting_orders=true`
   - `enable_order_book=true`
3. Persist market eligibility decisions in logs/metrics for auditability.

### Step 2: Introduce a dedicated liquidity collection loop
1. Reuse existing `MarketMetadataSync` scheduling mechanics; do not create parallel duplicate abstractions.
2. Extend sync to support two sources of market candidates:
   - hot markets (existing behavior),
   - periodic active-market sweep (new behavior).
3. Add bounded concurrency and strict timeout/retry behavior per orderbook request.

### Step 3: Standardize snapshot cadence and granularity
1. Add config for snapshot cadence (for example every 1-5 minutes).
2. Normalize timestamps to fixed buckets (`computed_at` floor to cadence boundary) for deterministic joins.
3. Upsert by `(condition_id, asset_id, computed_at)` exactly once per bucket.

### Step 4: Compute and store canonical liquidity features
1. Keep the existing canonical fields:
   - `visible_book_depth_usdc`
   - `mid_price`
   - `rolling_24h_volume_usdc` (if available from rolling trade cache)
2. Compute `visible_book_depth_usdc` consistently using one method (current code path in `compute_visible_book_depth_usdc`).
3. Never store ambiguous variants without explicit version tags.

### Step 5: Add coverage metadata and quality gates
1. Add a daily/hourly coverage job that computes:
   - expected snapshots per market/token,
   - observed snapshots,
   - coverage ratio.
2. Persist coverage metrics in DB (new table, for example `liquidity_coverage`).
3. Expose coverage thresholds to scan/backtest:
   - market-level minimum coverage for enabling `size_anomaly`,
   - run-level minimum coverage for claiming liquidity-aware results.

### Step 6: Integrate coverage-aware detector enablement
1. In scan/backtest replay, before per-trade scoring:
   - check market/token coverage over the run lookback window.
2. If coverage is below threshold:
   - disable `size_anomaly` for that market/time segment with explicit reason,
   - continue remaining detectors.
3. Record enable/disable decisions in run metadata.

### Step 7: Add operational backfill workflow (forward-only realism)
1. Accept that deep historical orderbook depth cannot be reconstructed from public trade-only endpoints.
2. Provide controlled backfill only for periods where source snapshots exist (if any internal archives exist).
3. For dates before collection start:
   - mark liquidity coverage as unavailable,
   - avoid pretending liquidity-aware backtests are complete.

### Step 8: Add observability and SLOs
1. Metrics:
   - snapshot ingest rate,
   - snapshot lag,
   - per-market coverage ratio,
   - orderbook fetch error rates.
2. Alerts:
   - sustained coverage drop,
   - collector lag beyond threshold,
   - repeated endpoint failures.
3. Dashboards:
   - coverage heatmap by market and time.

### Step 9: Testing strategy
1. Unit tests for snapshot bucketing, idempotent upserts, and coverage computation.
2. Integration tests for collector loop with mocked orderbook responses and failure retries.
3. Replay tests validating that liquidity-aware backtest results are stable across reruns.

### Step 10: Cleanup and migration
1. Remove assumptions that hot-market snapshots alone are sufficient for historical replay.
2. Align docs/config around one canonical historical-liquidity pipeline.
3. Add Alembic migrations for any new coverage/metadata tables and indexes.

## 5. Validation Plan
Dataset validation:
1. Run collector for a fixed period (for example 7 days) and verify expected cadence coverage.
2. Confirm each active tradable market/token has near-continuous snapshots in that window.

Backtest validation:
1. Run identical backtest twice over same window and confirm deterministic outputs.
2. Verify `size_anomaly` participation aligns with measured coverage thresholds.

Operational validation:
1. Simulate endpoint degradation and confirm retries, bounded failures, and no unbounded queue growth.
2. Verify coverage alerts trigger and clear correctly.

## 6. Rollout Plan
1. Phase 1: deploy collector in shadow mode, write snapshots and coverage metrics only.
2. Phase 2: enable coverage-gated `size_anomaly` in scan/backtest while keeping robust skip path.
3. Phase 3: publish run-quality labels (`liquidity_coverage_ok=true/false`) in artifacts and downstream reports.
4. Phase 4: treat low-coverage liquidity-aware backtests as invalid for strategy evaluation.

## 7. Definition of Done
- Continuous liquidity snapshot collection is running with observable coverage SLAs.
- Scan/backtest can decide liquidity-detector eligibility from measured coverage, not assumptions.
- Liquidity-aware historical backtests are reproducible and explicitly labeled by data quality.
- All behavior is policy-driven and auditable through persisted run metadata.

---

Your task is to perform a thorough refactor/feature implementation for this, following the `docs/guides/LONG_TERM_PATH_HISTORICAL_LIQUIDITY_DATASET.md` guide (also above):

1. Read the document sections for more context.
2. Execute every single step under the implementation guide section(s) - make a new step in your internal plan for each subsection (i.e. 5.1, 5.2, etc.).

This is a large change. Implement it end to end, completely, with robust, scalable and efficient code. You must:
- Follow KISS, DRY and YAGNI principles
- Clean up ALL legacy code, do not have legacy fallbacks, and remove redundant code along the way
- Integrate cleanly with the existing code and design.
- Do not use brittle pattern matching, heuristics or fallbacks.
- Keep this as a plan in your internal planning, and execute it to completion, no matter how long it takes.
- Anything you're unsure of - decide on the best long-term, prod-ready solution.
- In general, consider production-ready best practices.
- Remember our goals: NO query specific logic (must be query agnostic, generalisable), not language specific, and NOT dependent on any user input apart from an NL query (i.e. the requirements from retrieval section of AGENTS.md)