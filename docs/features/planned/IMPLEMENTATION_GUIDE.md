# Planned Features: Production Implementation Guide

This document is a **developer handoff** for finishing the repo’s insider-trade detection capabilities and adding a **historical scan/backtest** mode. It is written against the current code in `src/polymarket_insider_tracker/` and prioritizes:

1. **Clean, scalable, production-ready code** (no brittle heuristics, no “best-effort” fallbacks).
2. **KISS/DRY/YAGNI** while still being correct and operationally robust.
3. Reuse existing modules whenever possible; only add new code where the repo is currently missing a required component.
4. Remove/clean up legacy code paths that silently degrade correctness.
5. No “legacy compatibility” shims: if a behavior is wrong, replace it and update config/docs accordingly.
6. Query-agnostic and generalizable: detectors operate on market/trade/order data, not on hand-coded market-specific logic.
7. User input should be limited to a single **natural-language query** for historical scanning; everything else is configuration.

---

## Cross-Cutting Decisions (Apply to All Sections)

### Strictness: No silent fallbacks
Current code contains multiple “return None / default 0 / create minimal metadata” behaviors that make results look valid while being wrong. The intended end-state is:

- **If required data is missing**, the system should fail fast at startup (`--config-check`) or fail the specific unit of work with a clear error and metric.
- Detectors should not silently “guess” missing values.

Concrete places to remove/replace fallbacks:
- `profiler/chain.py` returns `None` for first tx (`get_first_transaction`) and returns `0` for nonce failures in batch mode.
- `detector/size_anomaly.py` fabricates metadata when missing.
- `ingestor/models.py` derives “category” via keyword lists (brittle).

### Single source of truth: structured inputs
To keep logic clean and scalable, standardize the inputs that every detector needs:

- A **trade** (who, what market, how much, when, side, price).
- A **market snapshot** (liquidity metrics, token mapping, current state).
- A **wallet snapshot** (nonce/tx count *as-of time*, age/first-seen, balances *as-of time*, funding origin).

The current `TradeEvent` model exists (`ingestor/models.py`), but market/wallet snapshots are incomplete.

### Persistence: use existing storage layer
The repo already has SQLAlchemy models and repos:
- Models: `storage/models.py`
- Repos: `storage/repos.py`

The pipeline currently **does not write** to Postgres. The intended end-state is:
- Persist market snapshots, trades, wallet snapshots, funding transfers, and computed signals/assessments.
- Keep Redis for caching/dedup/rolling stats, but treat Postgres as durable truth.

### Configuration: validated at startup
Use `config.py` for strict configuration (Pydantic). Any feature that requires extra external capability (archive node, embeddings, etc.) must:
- Add explicit settings.
- Validate them in `--config-check`.
- Refuse to run if missing.

### Database migrations
Any new persistence requirement must be backed by Alembic migrations:
- Add/modify SQLAlchemy models in `storage/models.py`.
- Generate a revision in `alembic/versions/` and keep it minimal and reviewable.
- Add an integration test that exercises the migration and the relevant repository methods.

### Acceptance criteria
Each feature section below ends with “Done when…”. Treat those as the definition of done for implementation and review.

---

## 1) Fresh Wallet Detection (Finish & Make Correct)

### Current state
- Detector exists: `detector/fresh_wallet.py`
- Uses `WalletAnalyzer.analyze()` and flags trades above a fixed size (`DEFAULT_MIN_TRADE_SIZE = $1,000`) and nonce <= 5.
- Wallet “age” is often `None` because the chain client **cannot actually compute first transaction**:
  - `PolygonClient.get_first_transaction()` logs a warning and returns `None` (`profiler/chain.py`).

### Intended state (behavior)
Fresh-wallet detection must be correct **at trade time**, not “at scan time”:

- `nonce_as_of_trade`: transaction count as of a block close to the trade timestamp (or another defensible time anchor).
- `first_funding_at`: timestamp of the wallet’s earliest inbound **USDC transfer** (see Funding section). This is the repo’s canonical definition of “wallet age” for this detector (deterministic and auditable).
- Deterministic confidence score based on those snapshots.

No age/nonce “unknown” states in production runs:
- If the system cannot compute wallet age and nonce as-of time, it must **refuse to run**.

### Implementation steps
1. **Define “as-of time” semantics for wallet snapshots**
   - Use the trade’s timestamp from `TradeEvent.timestamp`.
   - Introduce “as-of block” resolution:
     - Add `PolygonClient.get_block_number_at_or_before(timestamp)` (binary search on `eth_getBlockByNumber` timestamps; requires an RPC endpoint that supports historical blocks reliably).
   - Add `PolygonClient.get_transaction_count(address, *, block_number)` that queries `eth_getTransactionCount(address, block)` and caches by `(address, block_number)` (do not reuse the “latest nonce” cache key).

2. **Replace `get_first_transaction` stub with a production mechanism**
   Remove the “first transaction” concept from the production path and use the **USDC transfer index** as the source of truth:
   - The Funding feature (Section 4) introduces a persisted `erc20_transfers` index and provides the earliest inbound USDC transfer per wallet.
   - `WalletAnalyzer` must treat “wallet age” as `as_of - first_funding_at` and require `first_funding_at` to exist for any wallet it analyzes in production mode.

3. **Refactor `WalletAnalyzer` to produce a time-aware wallet snapshot**
   - Change `WalletAnalyzer.analyze(address)` to accept `as_of: datetime`.
   - Store and return a new immutable DTO, e.g. `WalletSnapshot` (or extend `WalletProfile` carefully) with:
     - `nonce_as_of`, `first_seen_at`, `age_hours_as_of`, `matic_balance_as_of`, `usdc_balance_as_of`
   - Ensure caching keys include `as_of`/block to avoid mixing “now” with “then”.

4. **Update `FreshWalletDetector` to use the snapshot**
   - Remove reliance on `profile.age_hours is None` semantics.
   - Make thresholds configurable (settings), but avoid brittle magic numbers:
     - Prefer percentiles/z-scores relative to a rolling global baseline for “what is fresh” (details in the “Statistical baselines” section below).

5. **Persist wallet snapshots**
   - Wire `storage/repos.WalletRepository.upsert()` into the pipeline:
     - In `pipeline._on_trade`, after wallet snapshot creation, upsert it in Postgres.
   - This enables historical scans/backtests to reuse computed snapshots.

6. **Clean up legacy**
   - Delete `PolygonClient.get_first_transaction()` and any codepaths depending on it.
   - Remove any code paths that default unknowns to zero (e.g., `get_transaction_counts` currently writes `0` on exception).

### Files you will touch
- `src/polymarket_insider_tracker/profiler/chain.py` (time→block, nonce-as-of, strict error handling)
- `src/polymarket_insider_tracker/profiler/analyzer.py` (as-of snapshots, caching keys)
- `src/polymarket_insider_tracker/profiler/models.py` (snapshot DTOs if needed)
- `src/polymarket_insider_tracker/detector/fresh_wallet.py` (strict inputs, updated scoring)
- `src/polymarket_insider_tracker/pipeline.py` (pass `as_of` to wallet analysis, persist snapshot)
- `src/polymarket_insider_tracker/storage/models.py` and `src/polymarket_insider_tracker/storage/repos.py` (persist snapshots)
- `alembic/versions/*` (schema changes)

### Done when…
- Running live mode produces fresh-wallet signals that are computed from wallet state **as-of trade time**.
- There is no “age unknown” path in production execution (missing capabilities fail at startup).
- Wallet snapshot persistence is in place and backtest mode reuses it.

---

## 2) Unusual Sizing / Liquidity Impact (Make Real, Remove “Niche Heuristics”)

### Current state
- `detector/size_anomaly.py` supports:
  - volume impact if `daily_volume` is provided
  - book impact if `book_depth` is provided
  - otherwise it falls back to a “niche market” heuristic using categories and a hard-coded set.
- Pipeline calls `SizeAnomalyDetector.analyze(trade)` without providing volume/depth (`pipeline.py`), so in practice it mainly flags by the niche heuristic.
- Market metadata caching (`MarketMetadataSync`) does not store liquidity metrics.

### Intended state (behavior)
Liquidity/size anomaly must be computed from **measured market microstructure**, not category keywords:

- `volume_impact = trade_notional / rolling_24h_volume_notional`
- `book_impact = trade_notional / visible_depth_notional` (defined precisely)

No category-based niche logic. No fabricated metadata.

### Implementation steps
1. **Define required liquidity inputs and where they live**
   - Extend `MarketMetadata` to include liquidity metrics (do not overload “category”):
     - `rolling_24h_volume_usdc`
     - `visible_book_depth_usdc` (defined below)
     - `mid_price` (optional but useful)
     - `last_liquidity_update_at`

2. **Implement a Market Liquidity service**
   Reuse existing `MarketMetadataSync` as the scheduling shell (don’t create a parallel daemon):
   - Add a “hot market registry” in Redis:
     - When a trade arrives, record the market id into a Redis set with a TTL.
   - In the metadata sync loop, only compute liquidity for “hot markets” to keep load bounded.

3. **Rolling 24h volume**
   - Compute from ingested trades (preferred) rather than relying on API fields you can’t validate.
   - Add persistent storage for trades (new table/model required; keep it minimal):
     - `trades` table: `(market_id, ts, wallet, side, price, size, notional, trade_id)`
   - Maintain a rolling window volume in Redis for fast access:
     - Use a time-bucketed approach (e.g., per-minute buckets) to allow efficient 24h sums without scanning.

4. **Order book depth**
   - Define “visible depth” in a deterministic, query-agnostic way:
     - Example: sum notional of top `N` levels on both sides, or within a `max_slippage_bps` band around mid.
   - Fetch orderbook via `ClobClient.get_orderbook(token_id)` and compute depth from `Orderbook.bids/asks` (`ingestor/models.py`).
   - Store computed depth per market in Redis + Postgres snapshots.

5. **Make `SizeAnomalyDetector` strict**
   - Remove optional parameters `daily_volume` and `book_depth` from the public API; require them.
   - Remove `_create_minimal_metadata` and any “niche” heuristics and category dependencies.
   - If liquidity inputs are missing, raise a domain error and count it as a pipeline error (don’t silently return `None`).

6. **Wire liquidity into the pipeline**
   - In `Pipeline._detect_size_anomaly`, fetch liquidity snapshot for the market from Redis/DB and pass it to the detector.

7. **Clean up legacy**
   - Remove `NICHE_PRONE_CATEGORIES` and the “niche market” multiplier/base score logic.
   - Remove keyword-based category derivation from `ingestor/models.py` if it’s only used for this feature (see cleanup section).

### Files you will touch
- `src/polymarket_insider_tracker/ingestor/models.py` (remove derived category; extend market metadata for liquidity)
- `src/polymarket_insider_tracker/ingestor/metadata_sync.py` (hot market tracking + liquidity snapshots)
- `src/polymarket_insider_tracker/ingestor/clob_client.py` (orderbook helpers)
- `src/polymarket_insider_tracker/detector/size_anomaly.py` (strict inputs, remove niche logic)
- `src/polymarket_insider_tracker/pipeline.py` (supply liquidity snapshot)
- `src/polymarket_insider_tracker/storage/models.py` (trades + liquidity snapshot tables)
- `alembic/versions/*`

### Done when…
- `SizeAnomalyDetector` never uses category/niche heuristics.
- Liquidity inputs are always available at runtime (or the pipeline fails explicitly).
- Live alerts include measured volume/book impact factors.

---

## 3) Sniper / DBSCAN Cluster Detection (Integrate Properly)

### Current state
- `detector/sniper.py` implements `SniperDetector` with DBSCAN and an internal entry recording API.
- It is not used anywhere in `Pipeline`.
- It assumes an “entry delta” from market creation time, but the system does not have a reliable market-creation timestamp.

### Intended state (behavior)
Detect coordinated wallets that systematically enter markets **early** and/or in a correlated way.

To avoid reliance on missing “market creation” timestamps, define entry timing relative to a measurable anchor:
- `market_first_observed_trade_at` (first trade timestamp observed/recorded by our system).
- Optionally, `market_first_observed_order_at` if order events are ingested (see spoofing section).

### Implementation steps
1. **Persist market entry events**
   Add a minimal table/model for entries:
   - `market_entries`: `(market_id, wallet, ts, notional, entry_rank, entry_delta_seconds, created_at)`

2. **Compute the anchor timestamp**
   - For each market, maintain `first_trade_at` as the min trade timestamp in Postgres.
   - When processing a trade:
     - If market has no `first_trade_at`, set it to this trade’s timestamp.
     - Compute `entry_delta_seconds = trade.ts - first_trade_at` (0 for the first trade).

3. **Integrate `SniperDetector` in `Pipeline`**
   - Add a `SniperDetector` instance to `Pipeline` initialization.
   - In `_on_trade`, call:
     - `sniper.record_entry(trade, market_first_trade_at=first_trade_at)`
   - Run clustering on a schedule (not on every trade):
     - e.g., every `X` seconds or every `Y` new entries.
   - When new clusters are found, create a `SniperClusterSignal` and include it in scoring (extend `SignalBundle` and `RiskScorer`).

4. **Make clustering reproducible and bounded**
   - Ensure feature vectors are stable and normalized.
   - Store clusters in Postgres so clustering can be resumed after restart.
   - Keep a bounded window (e.g., last 7 days) to avoid unbounded memory growth.
   - Add a second, simpler coordination signal that does not depend on DBSCAN:
     - “Co-entry correlation”: wallets that repeatedly appear in the first `N` entrants of the same markets within a short window.
     - This should be computed via deterministic counts in SQL/Redis and used as a low-weight signal (DBSCAN remains higher-level).

5. **Clean up**
   - Remove UUID/hash-based “cluster id” generation if it’s not stable across restarts; derive from deterministic data (e.g., sorted wallet addresses + date bucket).

### Files you will touch
- `src/polymarket_insider_tracker/detector/sniper.py` (anchor semantics, persistence hooks)
- `src/polymarket_insider_tracker/detector/models.py` (signal model wiring if missing)
- `src/polymarket_insider_tracker/detector/scorer.py` (add sniper weight)
- `src/polymarket_insider_tracker/pipeline.py` (record entries, schedule clustering)
- `src/polymarket_insider_tracker/storage/models.py` (market entries + clusters tables)
- `alembic/versions/*`

### Done when…
- Clustering runs periodically and emits `SniperClusterSignal` objects.
- Signals are deterministic and survive restarts (persisted clusters + dedup).
- Sniper signals affect `RiskScorer` and appear in alerts/backtest output.

---

## 4) Funding-Chain Tracing + Wallet Graph (Wire It In, Make It Scalable)

### Current state
- `profiler/funding.py` implements `FundingTracer` that tries to find the first USDC transfer by calling `eth_getLogs` from block 0 → latest.
- Storage models/repos exist for funding transfers and wallet relationships (`storage/models.py`, `storage/repos.py`).
- None of this is invoked by the pipeline; there is no persistence of funding chain results.

### Intended state (behavior)
For any wallet flagged by other detectors (or for a configurable sample), compute:
- Funding chain up to `N` hops based on USDC transfers.
- Origin classification via `EntityRegistry` (CEX/bridge/unknown) with a deterministic suspiciousness score.
- Persist transfers and derived relationships.
 - Cluster wallets by shared funding origin and timing proximity (graph features), enabling network-style “related wallet” detection without any market-specific heuristics.

No “scan from genesis for every wallet” behavior. Must be incremental and bounded.

### Implementation steps
1. **Define when funding tracing runs**
   To control cost, trace only for:
   - Wallets that trigger a risk assessment above some threshold, OR
   - Wallets that are “fresh” AND trade notional exceeds a configured high-water mark.

2. **Build an on-demand transfer index (recommended)**
   Replace “from_block=0” log scans with an indexed approach:
   - Create a table `erc20_transfers` keyed by `(token, to_address, block_number, tx_hash, log_index)`.
   - Add an async job that can backfill transfers for a given `to_address` over a bounded lookback:
     - Use a fixed, configured lookback window (e.g., 180 days). If the wallet cannot be traced within that window, **fail the trace** and surface the limitation explicitly (do not silently accept partial chains and do not auto-expand).
   - Once indexed, `FundingTracer.get_first_usdc_transfer()` becomes a DB query for the earliest transfer record.

3. **Make `FundingTracer` strict**
   - If the transfer index cannot produce a first transfer within the required window, return an explicit error state (and fail the trace) rather than returning `None`.

4. **Persist results**
   - Use `FundingRepository` to upsert transfers.
   - Use `RelationshipRepository` to upsert edges:
     - `wallet_a -> wallet_b` with `relationship_type="funded_by"` and confidence derived from trace certainty.
   - Add additional relationship types derived deterministically from indexed transfers:
     - `relationship_type="shares_funder"`: two wallets whose earliest inbound USDC transfer comes from the same `from_address` within a tight time band.
     - `relationship_type="funding_burst"`: multiple new wallets funded by the same source within a short interval.

5. **Wire into alerts**
   - Extend `AlertFormatter` to include origin summary (CEX/bridge/unknown + hop count).
   - Include funding suspiciousness as an additional weighted signal in `RiskScorer`.

6. **Clean up**
   - Remove direct access to `polygon_client._w3` internals inside `FundingTracer` (currently it reaches into private fields).
   - Add a first-class `PolygonClient.get_logs(...)` API and use it everywhere.

### Files you will touch
- `src/polymarket_insider_tracker/profiler/funding.py` (strict on-demand indexing, no genesis scans)
- `src/polymarket_insider_tracker/profiler/entities.py` (entity registry data strategy; keep it deterministic and auditable)
- `src/polymarket_insider_tracker/profiler/chain.py` (public log API; strict retry semantics)
- `src/polymarket_insider_tracker/detector/scorer.py` (funding signal weight)
- `src/polymarket_insider_tracker/alerter/formatter.py` (include origin summary)
- `src/polymarket_insider_tracker/storage/models.py` and `src/polymarket_insider_tracker/storage/repos.py` (transfer index + relationships)
- `alembic/versions/*`

### Done when…
- Funding traces run only when justified and complete within bounded cost.
- Traces are persisted (transfers + relationships) and reusable for backtests.
- No code path performs unbounded `eth_getLogs` scans from genesis.

---

## 5) Event/Timing Correlation (No External News Dependency)

### Current state
- Not implemented.

### Intended state (behavior)
Detect trades that are “informational” by measuring **subsequent market movement** after the trade, without requiring any external news feed:

- Compute future price move over fixed horizons (e.g., 15m, 1h, 4h).
- Mark trades that consistently precede outsized moves as suspicious.
- Use this as a feature in historical scanning/backtesting and optionally in live alerts (careful with feedback loops).

### Implementation steps
1. **Persist trades and price series**
   - Ensure trades are stored (see liquidity section).
   - Maintain per-market price series in Postgres (or derived from trades).

2. **Define a movement metric**
   - Use log returns or absolute delta in implied probability.
   - Normalize by recent volatility (rolling stddev) to avoid “big move” thresholds.

3. **Implement a detector**
   - Add a new detector in `detector/` (keep it small and pure):
     - Input: `TradeEvent` + price series reference
     - Output: `PreMoveSignal` with `z_score` or percentile.

4. **Wire into risk scoring**
   - Extend `SignalBundle` and `RiskScorer` weights.
   - Persist the computed signal for offline evaluation.

### Done when…
- Historical scans can rank “informational trades” by normalized future move.
- Live mode can optionally include the signal without requiring any external news feed.

---

## 6) Distributional / Statistical Anomalies (Benford, Outlier Sizing, etc.)

### Current state
- Not implemented.

### Intended state (behavior)
Provide a general anomaly layer that flags behavior inconsistent with the market’s typical microstructure without hard-coded thresholds:

- Trade size outliers relative to the market’s recent distribution.
- Digit/distribution irregularities (Benford-style checks) as a weak signal, not a primary alert driver.

### Implementation steps
1. **Build rolling baselines**
   - Maintain per-market rolling distributions in Redis (e.g., quantile sketches / histograms).
   - Persist daily aggregates in Postgres for auditability.

2. **Add detectors**
   - `TradeSizeOutlierDetector`: z-score/percentile on notional size.
   - `DigitDistributionDetector`: compute first-digit distribution over a window and compare to expected baseline; treat as low-weight.
   - `TradeSlicingDetector` (iceberg-style behavior):
     - Detects when a wallet accumulates a large notional position via many small trades within a rolling window.
     - Score should be based on divergence from the wallet’s own baseline and/or the market’s baseline (avoid fixed “k trades in t minutes” rules).

3. **Avoid brittle parameters**
   - Do not hard-code “> $X” thresholds.
   - Use rolling percentiles (e.g., > 99.5th percentile) or robust z-scores.

### Done when…
- Distribution-based signals are computed from rolling baselines and persisted.
- These signals can be enabled with low weight and do not dominate alerts.

---

## 7) Spoofing / Order-to-Trade Ratio / Time-to-Cancel (Requires Order Event Ingestion)

### Current state
- The WebSocket client ingests **only trades** (`ingestor/websocket.py`).
- There is no order event model, no persistence of orders/cancels, and no related detectors.

### Intended state (behavior)
Detect manipulative order behavior using **order lifecycle events**:

- High order-to-trade ratios.
- Rapid cancel after moving the book.
- Repeated short-lived orders clustered by wallet.

### Implementation steps
1. **Confirm available order event feeds**
   - Polymarket CLOB WebSockets expose two channels:
     - `market` channel (public): `book`, `price_change`, `last_trade_price` (order book/price deltas).
     - `user` channel (authenticated): order updates for **your own account** (placements/updates/cancellations/fills).
   - For insider/spoofing detection across arbitrary wallets, the `user` channel is not sufficient (it is user-scoped).
     The production approach is therefore:
     - ingest `market` channel deltas (`book` + `price_change`) for globally-observable lifecycle changes, and
     - attribute orders by fetching order details via Level-2 HTTP endpoints (requires L2 auth).
   - Keep `TradeStreamHandler` for the trade activity feed and add a separate CLOB market stream handler for book deltas
     (this matches Polymarket’s channel separation and keeps responsibilities clear).

2. **Add models**
   - Add `OrderEvent` / `CancelEvent` dataclasses in `ingestor/models.py` (or a new file under `ingestor/` if models become too large).
   - Add persistence tables:
     - `orders`, `order_events`, `cancels` (minimal columns: ids, wallet, market, side, price, size, ts, status).

3. **Ingest and persist**
   - In the stream handler, route events to callbacks:
     - `on_trade`, `on_order`, `on_cancel`
   - Persist all events in Postgres (async session).

4. **Implement detectors**
   - `OrderToTradeRatioDetector` (per wallet per market over rolling windows).
   - `RapidCancelDetector` (cancel within `T` seconds after placement; T derived from baseline distributions).
   - `BookImpactWithoutFillDetector` (order moves best bid/ask but cancels quickly without execution).

5. **Wire into scoring**
   - Extend `SignalBundle` and `RiskScorer` accordingly.

### Done when…
- Order lifecycle events are ingested and persisted.
- Spoofing-related detectors produce signals with baseline-derived thresholds.

---

## 8) Historical Scan / Backtest Feature (Natural-Language Query Only)

### Current state
- No backtest scripts exist despite README mention.
- `ClobClient` wrapper supports markets and orderbooks but does not expose trade history helpers.
- `py-clob-client` provides methods that can fetch market trades (`get_market_trades_events`) and user trade history (`get_trades`), but there is no integration.

### Intended state (behavior)
Add a CLI subcommand that takes **only**:
- `--query "<natural language>"` (required)

And produces:
- Ranked list of suspicious historical trades/wallets/markets with full explanation (signals + confidence + funding origin where applicable).

No requirement for the user to provide market IDs, wallet addresses, date ranges, etc. Those are derived.

### Implementation steps
1. **Market retrieval from NL query (production approach)**
   Avoid brittle keyword matching and do semantic retrieval:
   - Add a `markets` table to Postgres with: `condition_id`, `question`, `description`, `tokens`, `active/closed`, `end_date`, and an `embedding` vector.
   - Use `pgvector` for vector search.
   - Standardize on a **local, multilingual embedding model** (so the system is not language-specific) and enforce it via configuration. Do not implement a “provider fallback” chain.
   - Implement:
     - `MarketIndexer`: periodically sync markets (reuse `MarketMetadataSync` scheduling) and compute/store embeddings.
     - `MarketSearch`: embed query and return top K markets.

2. **Historical trade fetch**
   - Extend `ingestor/clob_client.py` wrapper to expose a single method:
     - `get_market_trades(condition_id) -> list[TradeEvent]` (normalized).
   - Persist fetched trades to Postgres to avoid refetching.

3. **Replay engine (offline pipeline)**
   - Add a “replay” execution mode that:
     - Iterates trades in timestamp order.
     - For each trade:
       - Computes market liquidity snapshot at that time (from stored trades + stored orderbook snapshots if available).
       - Computes wallet snapshot as-of trade time (requires strict wallet snapshot implementation).
       - Runs detectors and scoring.
     - Writes assessments/signals to Postgres and outputs a report.

4. **Output format**
   - Produce a deterministic, developer-friendly output:
     - JSONL by default (one record per flagged trade), plus optional pretty text summary.
   - Include:
     - trade id, market id, wallet, size, price, timestamp
     - detector signals + confidence factors
     - risk score and whether it would alert

5. **Evaluation & metrics**
   - Add an offline evaluation utility:
     - Compute “hit rate” based on subsequent price move (from the event correlation section).
   - Store backtest runs in a `backtest_runs` table with parameters, version, and summary stats.

6. **Clean up README**
   - Replace the README’s `scripts/backtest.py` mention with the real CLI usage once implemented.

7. **CLI surface (argparse)**
   - Convert the current flags-only CLI (`__main__.py`) into subcommands using `argparse` subparsers:
     - `run` (default) → current live pipeline behavior
     - `scan --query "..."`
     - `train-model` (Section 9)
   - Keep `--config-check` and `--log-level` global options.
   - Ensure each subcommand has deterministic exit codes and machine-readable output option (JSONL).

### Done when…
- `python -m polymarket_insider_tracker scan --query "..."` runs end-to-end and produces a ranked report.
- Scan requires no user-provided IDs/ranges and is fully reproducible (persisted run metadata).

---

## 9) Machine Learning Layer (Production-Safe, Minimal, and Auditable)

This section covers the “ML + unsupervised/supervised” items without over-engineering.

### Current state
- Unsupervised clustering exists (`detector/sniper.py` via DBSCAN), but is not integrated.
- No feature store, no model training, no audit trail.

### Intended state (behavior)
- **Unsupervised**: clustering remains a detector (already designed as such).
- **Supervised**: use a simple, auditable model (logistic regression / gradient-boosted trees) trained on **self-supervised labels** derived from future market movement (Section 5), not on hand-curated market-specific labels.

### Implementation steps
1. **Persist a feature row per trade**
   - Add a `trade_features` table keyed by `trade_id` with:
     - wallet snapshot fields
     - liquidity impacts
     - order-flow features (if present)
     - cluster membership features
2. **Define self-supervised labels**
   - Create labels like: “did the market move by > X stddev within 1h in the trade’s direction?”
   - Store labels with the horizon and normalization method for auditability.
3. **Train offline**
   - Implement a CLI subcommand `train-model` that:
     - extracts features + labels
     - fits a simple model
     - writes a versioned artifact to disk and a metadata row to Postgres
4. **Serve in live mode**
   - Load the latest “blessed” model at startup.
   - Output a `ModelScoreSignal` as *one component* of risk scoring (never the only gate).

### Done when…
- The model pipeline is fully reproducible (dataset version + feature schema + artifact hash).
- Live mode can incorporate a model score without changing any detector logic.

---

## 10) Legacy/Old Code Cleanup Checklist

This repo currently includes logic that violates the desired principles. Remove or replace as part of implementing the features above:

- Remove keyword-based “category derivation”:
  - `_CATEGORY_KEYWORDS` and `derive_category()` in `ingestor/models.py`.
  - Any “niche category” logic in detectors.
- Remove fabricated metadata fallback:
  - `SizeAnomalyDetector._create_minimal_metadata` and the “create minimal metadata” path.
- Remove stubbed wallet age logic:
  - `PolygonClient.get_first_transaction()` must be replaced with a strict implementation.
- Remove silent error-to-default conversions:
  - Any “return 0 / return None on exception” in core signal inputs (nonce, liquidity, transfer history).
- Ensure WebSocket endpoint configuration is coherent:
  - `TradeStreamHandler.DEFAULT_WS_HOST` and `Settings.polymarket.ws_url` must point to the actual feed used.

---

## Appendix A: Where to Wire Things (Existing Code Touchpoints)

- CLI entry / command surface: `src/polymarket_insider_tracker/__main__.py`
- Main orchestration: `src/polymarket_insider_tracker/pipeline.py`
- Trade ingestion: `src/polymarket_insider_tracker/ingestor/websocket.py`
- Market metadata caching: `src/polymarket_insider_tracker/ingestor/metadata_sync.py`
- CLOB API wrapper: `src/polymarket_insider_tracker/ingestor/clob_client.py`
- Wallet profiling: `src/polymarket_insider_tracker/profiler/analyzer.py`
- Polygon RPC client: `src/polymarket_insider_tracker/profiler/chain.py`
- Funding tracing: `src/polymarket_insider_tracker/profiler/funding.py`
- Detectors: `src/polymarket_insider_tracker/detector/*`
- Risk aggregation: `src/polymarket_insider_tracker/detector/scorer.py`
- Storage: `src/polymarket_insider_tracker/storage/models.py`, `src/polymarket_insider_tracker/storage/repos.py`

---

Your task is to perform a thorough refactor/feature implementation for this, following the `docs/features/planned/IMPLEMENTATION_GUIDE.md` guide (also above):

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
