# Robust Backtest Path: Missing Historical Liquidity

## 1. Scope
This guide defines the production fix for scan/backtest failures caused by missing historical liquidity snapshots. It targets the root cause in historical replay behavior, not operational workarounds.

Primary objective:
- Backtests must continue for supported strategies (`fresh_wallet`, `funding`, `sniper/coentry`, `pre_move`) even when historical orderbook depth is unavailable for some trades.

Secondary objective:
- Preserve historical integrity by never injecting present-time liquidity into historical scoring unless explicitly requested.

## 2. Current State
Current behavior in scan/backtest:
- Trade history loads successfully from market trade endpoints.
- Replay requests a liquidity snapshot at-or-before each trade timestamp.
- If no snapshot exists, scan raises and terminates the entire run.

Where this is enforced:
- `src/polymarket_insider_tracker/scan/runner.py` (historical liquidity lookup and hard failure path).
- `src/polymarket_insider_tracker/storage/repos.py` (`LiquiditySnapshotRepository.get_latest_before`).

Important system constraints:
- Historical liquidity snapshots are not guaranteed for all markets/assets.
- Present-time orderbook substitution is not part of historically pure replay behavior.

Failure symptom:
- `ScanError: Missing historical liquidity snapshot for market=... asset=...`

## 3. Intended State
Target behavior after implementation:
- Missing historical liquidity does not crash scan/backtest by default.
- For trades lacking historical liquidity:
  - `size_anomaly` is skipped for that trade.
  - Other signals continue to run and score normally.
  - Missing-input reason is persisted and reported.
- Historical integrity is preserved by default:
  - No present-time orderbook substitution in historical runs unless explicitly enabled.

Policy model (explicit, deterministic):
- `required`: fail trade/run when liquidity is missing.
- `skip_size_signal`: continue trade processing without `size_anomaly` (default for historical scan/backtest).

## 4. Step-by-Step Implementation Guide

### Step 1: Replace boolean toggle with explicit policy
1. In `src/polymarket_insider_tracker/config.py`:
   - Add `SCAN_HISTORICAL_LIQUIDITY_POLICY` enum-like setting with allowed values:
     - `required`
     - `skip_size_signal`
   - Set default to `skip_size_signal` for scan/backtest.
2. Keep config parsing strict; invalid values must fail at startup.

### Step 2: Refactor liquidity handling in scan replay
1. In `src/polymarket_insider_tracker/scan/runner.py`:
   - Move liquidity resolution into a dedicated helper (single source of truth).
   - For each trade:
     - Attempt `liq_repo.get_latest_before(condition_id, asset_id, as_of=trade_ts)`.
     - Branch by policy:
       - `required`: raise `ScanError`.
       - `skip_size_signal`: set `size_signal=None`; continue.
2. Remove implicit coupling between “can’t compute liquidity” and “abort entire replay” for non-`required` policies.

### Step 3: Add first-class data quality telemetry for replay
1. Track counters during scan:
   - `trades_total`
   - `trades_with_hist_liquidity`
   - `trades_missing_hist_liquidity`
   - `trades_size_signal_skipped`
   - `trades_size_signal_computed`
2. Persist these counters in run artifacts:
   - Extend `params_json` payload for backtest/scan run metadata in `backtest_runs`.
3. Include the counters in JSON output and logs.

### Step 4: Persist missing-input provenance per trade
1. For trades where `size_anomaly` is skipped due to missing historical liquidity:
   - Persist a deterministic audit record (either:
     - new `trade_signals` type like `size_anomaly_missing_input`, or
     - include in `features_json` with explicit `missing_reason` field).
2. Ensure downstream analysis can filter these trades without inferring from nulls.

### Step 5: Keep scoring deterministic with partial signals
1. Confirm `SignalBundle` and `RiskScorer` already handle absent signals without errors.
2. Ensure the scoring path does not silently re-weight other signals in a way that changes semantics unintentionally.
3. Document expected behavior: missing `size_anomaly` contributes zero weight for that trade.

### Step 6: Update CLI/operator UX
1. In scan logs:
   - Emit a final coverage summary for historical liquidity.
2. In `--config-check`:
   - Validate policy value and explain implications.
3. In docs:
   - Clarify that `skip_size_signal` is historically pure and default.

### Step 7: Add targeted tests
1. Unit tests for policy branching:
   - `required` raises.
   - `skip_size_signal` continues.
2. Integration test:
   - Seed trades without matching `liquidity_snapshots`.
   - Assert run completes and emits coverage counters.
3. Regression test:
   - Ensure existing runs with complete liquidity behave unchanged.

### Step 8: Cleanup
1. Remove legacy assumptions that every replayed trade must have historical depth.
2. Remove references that imply boolean-only fallback behavior.
3. Keep one policy mechanism only (no dual flag logic).

## 5. Validation Plan
Functional validation:
1. Run scan with `SCAN_HISTORICAL_LIQUIDITY_POLICY=skip_size_signal` on a dataset known to have gaps.
2. Verify run completes and output includes trades flagged by non-size detectors.
3. Verify coverage metrics report non-zero missing liquidity.

Correctness validation:
1. Confirm no call to current orderbook path under `skip_size_signal`.
2. Confirm `required` still fails early for strict audit use cases.

Performance validation:
1. Ensure added branching/telemetry has negligible replay overhead.
2. Ensure DB writes for audit metadata are batched, not per-trade unbounded chatter.

## 6. Rollout Plan
1. Ship with default `skip_size_signal` in scan/backtest.
2. Keep `required` available for strict environments.
3. Keep the policy surface minimal: `required` and `skip_size_signal`.
4. Add release note explaining behavior change from “fail whole run” to “continue with explicit missing-input audit”.

## 7. Definition of Done
- Historical scan/backtest no longer fails globally due to partial liquidity snapshot gaps under default policy.
- Historical integrity is preserved by default.
- Coverage/missing-input metrics are visible in logs and persisted outputs.
- Tests cover all policy branches and the missing-liquidity replay path.
