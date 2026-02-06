"""Historical scan/backtest runner.

This module implements the `scan --query "..."` command:
- Semantic market retrieval
- Historical trade fetch (bounded)
- Offline replay using the same detectors/scoring as live mode
- Deterministic JSONL report output
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Awaitable, Callable, TypeVar

from py_clob_client.clob_types import ApiCreds
from redis.asyncio import Redis

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.digit_distribution import (
    DigitDistributionDetector,
    DigitDistributionDetectorError,
)
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.models import (
    CoEntryCorrelationSignal,
    FundingChainSignal,
    PreMoveSignal,
    RiskAssessment,
    SniperClusterSignal,
)
from polymarket_insider_tracker.detector.pre_move import PreMoveDetector, PreMoveDetectorError
from polymarket_insider_tracker.detector.scorer import DEFAULT_WEIGHTS, RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.size_anomaly import LiquidityInputs, SizeAnomalyDetector
from polymarket_insider_tracker.detector.sniper import SniperDetector
from polymarket_insider_tracker.detector.trade_size_outlier import (
    TradeSizeOutlierDetector,
    TradeSizeOutlierDetectorError,
)
from polymarket_insider_tracker.detector.trade_slicing import (
    TradeSlicingDetector,
    TradeSlicingDetectorError,
)
from polymarket_insider_tracker.ingestor.baselines import (
    RollingDigitConfig,
    RollingDigitDistributionCache,
    RollingHistogramCache,
    RollingHistogramConfig,
)
from polymarket_insider_tracker.ingestor.clob_client import (
    ClobClient,
    ClobClientError,
    ClobClientNotFoundError,
    RetryError,
)
from polymarket_insider_tracker.ingestor.flow import RollingFlowConfig, RollingWalletMarketFlowCache
from polymarket_insider_tracker.ingestor.liquidity import now_utc
from polymarket_insider_tracker.ingestor.volume import RollingVolumeCache, RollingVolumeConfig
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer, WalletAnalyzerError
from polymarket_insider_tracker.profiler.chain import PolygonClient, RPCError
from polymarket_insider_tracker.profiler.funding import FundingTraceError, FundingTracer
from polymarket_insider_tracker.scan.embeddings import EmbeddingConfig, SentenceTransformerEmbeddingProvider
from polymarket_insider_tracker.scan.market_indexer import MarketIndexer
from polymarket_insider_tracker.scan.search import MarketSearch, MarketSearchConfig
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.repos import (
    BacktestRunDTO,
    BacktestRunRepository,
    LiquidityCoverageDTO,
    LiquidityCoverageRepository,
    LiquiditySnapshotRepository,
    MarketRepository,
    MarketPriceBarDTO,
    MarketPriceBarRepository,
    TradeDTO,
    TradeRepository,
    TradeFeatureRepository,
    TradeSignalDTO,
    TradeSignalRepository,
    WalletSnapshotDTO,
    WalletSnapshotRepository,
)
from polymarket_insider_tracker.training.features import build_trade_features

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class ScanResult:
    run_id: str
    output_path: Path
    total_trades_considered: int
    total_records_written: int


class ScanError(RuntimeError):
    pass


@dataclass(frozen=True)
class LiquidityCoverageDecision:
    condition_id: str
    asset_id: str
    coverage_ratio: float
    availability_status: str
    enabled: bool
    reason: str | None


def _json_default(x: object) -> str:
    if isinstance(x, (datetime,)):
        return x.isoformat()
    return str(x)


def _walk_exception_chain(error: BaseException) -> list[str]:
    seen: set[int] = set()
    out: list[str] = []
    cur: BaseException | None = error
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        out.append(str(cur))
        nxt = cur.__cause__
        if nxt is None:
            nxt = cur.__context__
        cur = nxt
    return out


def _is_rpc_history_constraint(error: BaseException) -> bool:
    message = " | ".join(_walk_exception_chain(error)).lower()
    return any(
        token in message
        for token in (
            "block range is too large",
            "history has been pruned",
            "-32062",
            "-32701",
        )
    )

def _chunked(items: list[T], size: int) -> list[list[T]]:
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _analyze_optional_signal(
    *,
    detector_name: str,
    analyze_fn: Callable[[], Awaitable[object | None]],
    skippable_errors: tuple[type[Exception], ...],
) -> tuple[object | None, dict[str, object] | None]:
    try:
        signal = await analyze_fn()
        return signal, None
    except skippable_errors as e:
        return None, {
            "reason": "detector_prerequisites_unavailable",
            "detector": detector_name,
            "error_type": type(e).__name__,
            "error_message": str(e),
        }


def _build_liquidity_coverage_decisions(
    coverage_rows: list[LiquidityCoverageDTO],
    *,
    min_market_coverage_ratio: float,
) -> dict[tuple[str, str], LiquidityCoverageDecision]:
    decisions: dict[tuple[str, str], LiquidityCoverageDecision] = {}
    for row in coverage_rows:
        key = (row.condition_id, row.asset_id)
        ratio = float(row.coverage_ratio)
        enabled = True
        reason: str | None = None
        if row.availability_status != "measured":
            enabled = False
            reason = "coverage_unavailable_before_collection_start"
        elif ratio < min_market_coverage_ratio:
            enabled = False
            reason = "coverage_below_market_threshold"
        decisions[key] = LiquidityCoverageDecision(
            condition_id=row.condition_id,
            asset_id=row.asset_id,
            coverage_ratio=ratio,
            availability_status=row.availability_status,
            enabled=enabled,
            reason=reason,
        )
    return decisions


async def run_scan(
    *,
    settings: Settings,
    query: str,
    output_path: Path | None = None,
) -> ScanResult:
    settings.validate_requirements(command="scan")
    if not query.strip():
        raise ScanError("--query is required")

    run_id = str(uuid.uuid4())
    output_path = output_path or (settings.scan.artifacts_dir / f"scan_{run_id}.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    redis = Redis.from_url(settings.redis.url)
    db = DatabaseManager(settings.database.url, async_mode=True)
    polygon: PolygonClient | None = None
    try:
        started_at = datetime.now(UTC)
        # Prefix all rolling caches with a per-run namespace for determinism.
        prefix = f"polymarket:scan:{run_id}:"
        volume_cache = RollingVolumeCache(
            redis,
            config=RollingVolumeConfig(
                window=timedelta(hours=settings.liquidity.volume_window_hours),
                key_prefix=f"{prefix}volume:",
            ),
        )
        hist_cache = RollingHistogramCache(
            redis,
            config=RollingHistogramConfig(
                window=timedelta(hours=settings.baseline.window_hours),
                bucket_seconds=settings.baseline.bucket_seconds,
                cleanup_batch_size=settings.baseline.cleanup_batch_size,
                key_prefix=f"{prefix}hist:",
            ),
        )
        digit_cache = RollingDigitDistributionCache(
            redis,
            config=RollingDigitConfig(
                window=timedelta(hours=settings.baseline.window_hours),
                bucket_seconds=settings.baseline.bucket_seconds,
                cleanup_batch_size=settings.baseline.cleanup_batch_size,
                key_prefix=f"{prefix}digits:",
            ),
        )
        flow_cache = RollingWalletMarketFlowCache(
            redis,
            config=RollingFlowConfig(
                window=timedelta(minutes=30),
                bucket_seconds=settings.baseline.bucket_seconds,
                cleanup_batch_size=settings.baseline.cleanup_batch_size,
                key_prefix=f"{prefix}flow:",
            ),
        )

        private_key = (
            settings.polymarket.clob_private_key.get_secret_value()
            if settings.polymarket.clob_private_key
            else None
        )
        api_creds: ApiCreds | None = None
        if (
            settings.polymarket.clob_api_key
            and settings.polymarket.clob_api_secret
            and settings.polymarket.clob_api_passphrase
        ):
            api_creds = ApiCreds(
                api_key=settings.polymarket.clob_api_key.get_secret_value(),
                api_secret=settings.polymarket.clob_api_secret.get_secret_value(),
                api_passphrase=settings.polymarket.clob_api_passphrase.get_secret_value(),
            )
        clob = ClobClient(
            host=settings.polymarket.clob_host,
            data_api_host=settings.polymarket.data_api_host,
            chain_id=settings.polymarket.clob_chain_id,
            private_key=private_key,
            api_creds=api_creds,
            signature_type=settings.polymarket.clob_signature_type,
            funder=settings.polymarket.clob_funder,
        )

        embedder = SentenceTransformerEmbeddingProvider(
            config=EmbeddingConfig(
                model_name_or_path=settings.scan.embedding_model or "",
                device=settings.scan.embedding_device,
                expected_dim=settings.scan.embedding_dim or 0,
            )
        )

        async with db.get_async_session() as session:
            repo = MarketRepository(session)
            row_count, max_updated_at = await repo.get_index_state()
            now = datetime.now(UTC)
            should_index = settings.scan.index_force or row_count == 0 or max_updated_at is None
            if not should_index and settings.scan.index_max_age_hours == 0:
                should_index = True
            if not should_index and max_updated_at is not None:
                age = now - max_updated_at
                should_index = age > timedelta(hours=settings.scan.index_max_age_hours)

            if should_index:
                logger.info(
                    "Indexing markets for semantic search (existing_rows=%d last_updated_at=%s active_only=%s)",
                    row_count,
                    max_updated_at.isoformat() if max_updated_at else None,
                    settings.scan.index_active_only,
                )
                from polymarket_insider_tracker.scan.market_indexer import MarketIndexerConfig

                indexer = MarketIndexer(
                    clob_client=clob,
                    embedder=embedder,
                    config=MarketIndexerConfig(
                        active_only=settings.scan.index_active_only,
                        chunk_size=settings.scan.index_chunk_size,
                        embed_batch_size=settings.scan.index_embed_batch_size,
                        commit_every_chunks=settings.scan.index_commit_every_chunks,
                    ),
                )
                indexed = await indexer.run_once(session=session)
                logger.info("Market index updated (indexed=%d)", indexed)
            else:
                logger.info(
                    "Skipping market indexing (rows=%d last_updated_at=%s max_age_hours=%d)",
                    row_count,
                    max_updated_at.isoformat() if max_updated_at else None,
                    settings.scan.index_max_age_hours,
                )

        async with db.get_async_session() as session:
            search = MarketSearch(
                embedder=embedder,
                config=MarketSearchConfig(
                    top_k=settings.scan.top_k_markets,
                    active_only=settings.scan.index_active_only,
                ),
            )
            markets = await search.search(session=session, query=query)

        if not markets:
            raise ScanError("No markets found for query")

        cutoff = datetime.now(UTC) - timedelta(days=settings.scan.lookback_days)

        # Fetch + persist trades
        trades: list[TradeDTO] = []
        skipped_markets = 0
        async with db.get_async_session() as session:
            trade_repo = TradeRepository(session)
            price_repo = MarketPriceBarRepository(session)

            for m in markets:
                try:
                    raw_trades = await asyncio.to_thread(
                        clob.get_market_trades,
                        m.condition_id,
                        max_trades=settings.scan.max_trades_per_market,
                    )
                except ClobClientNotFoundError as e:
                    skipped_markets += 1
                    logger.warning("Skipping market with no trades endpoint (market=%s): %s", m.condition_id, e)
                    continue
                except (ClobClientError, RetryError) as e:
                    skipped_markets += 1
                    logger.warning("Skipping market trades fetch failure (market=%s): %s", m.condition_id, e)
                    continue
                # Bound work deterministically.
                raw_trades = [t for t in raw_trades if t.timestamp >= cutoff]
                raw_trades = sorted(raw_trades, key=lambda t: t.timestamp)[: settings.scan.max_trades_per_market]
                trade_dtos: list[TradeDTO] = []
                bars: dict[tuple[str, datetime], MarketPriceBarDTO] = {}
                for t in raw_trades:
                    trade_dtos.append(
                        TradeDTO(
                            trade_id=t.trade_id,
                            market_id=t.market_id,
                            asset_id=t.asset_id,
                            wallet_address=t.wallet_address,
                            side=t.side,
                            outcome=t.outcome,
                            outcome_index=t.outcome_index,
                            price=t.price,
                            size=t.size,
                            notional_usdc=t.notional_value,
                            ts=t.timestamp,
                        )
                    )

                    bucket = t.timestamp.replace(second=0, microsecond=0)
                    key = (t.market_id, bucket)
                    existing = bars.get(key)
                    if existing is None:
                        bars[key] = MarketPriceBarDTO(
                            market_id=t.market_id,
                            bucket_start=bucket,
                            first_trade_ts=t.timestamp,
                            last_trade_ts=t.timestamp,
                            open_price=t.price,
                            high_price=t.price,
                            low_price=t.price,
                            close_price=t.price,
                        )
                    else:
                        if t.timestamp < existing.first_trade_ts:
                            existing.first_trade_ts = t.timestamp
                            existing.open_price = t.price
                        if t.timestamp > existing.last_trade_ts:
                            existing.last_trade_ts = t.timestamp
                            existing.close_price = t.price
                        if t.price > existing.high_price:
                            existing.high_price = t.price
                        if t.price < existing.low_price:
                            existing.low_price = t.price

                # Bulk persist in bounded chunks.
                for chunk in _chunked(trade_dtos, 5_000):
                    await trade_repo.upsert_many(chunk)
                for chunk in _chunked(list(bars.values()), 10_000):
                    await price_repo.upsert_many(chunk)

                trades.extend(trade_dtos)

            await session.commit()

        if not trades:
            raise ScanError(
                "No trades fetched for retrieved markets "
                f"(retrieved_markets={len(markets)} skipped_markets={skipped_markets})."
            )

        # Replay in timestamp order.
        trades.sort(key=lambda t: t.ts)
        total_considered = 0
        written = 0

        polygon = PolygonClient(
            settings.polygon.rpc_url,
            fallback_rpc_url=settings.polygon.fallback_rpc_url,
            redis=redis,
        )
        funding = FundingTracer(
            polygon,
            token_addresses=list(settings.funding.usdc_contract_addresses),
            lookback_days=settings.funding.lookback_days,
            logs_chunk_size_blocks=settings.funding.logs_chunk_size_blocks,
            max_hops=settings.funding.max_hops,
        )

        async def first_funding_at_provider(address: str, as_of: datetime) -> datetime:
            async with db.get_async_session() as session:
                first = await funding.ensure_first_inbound_transfer(session, address=address, as_of=as_of)
                if first.timestamp > as_of:
                    raise WalletAnalyzerError("first funding transfer is after as_of")
                return first.timestamp

        wallet_analyzer = WalletAnalyzer(
            polygon,
            redis=redis,
            usdc_addresses=tuple(settings.funding.usdc_contract_addresses),
            first_funding_at_provider=first_funding_at_provider,
        )

        fresh = FreshWalletDetector(
            min_trade_size=settings.fresh_wallet.min_trade_notional_usdc,
            max_nonce=settings.fresh_wallet.max_nonce,
            max_age_hours=settings.fresh_wallet.max_age_hours,
        )
        size_anomaly = SizeAnomalyDetector(
            volume_threshold=settings.size_anomaly.volume_threshold,
            book_threshold=settings.size_anomaly.book_threshold,
        )
        sniper = SniperDetector(
            entry_threshold_seconds=settings.sniper.entry_threshold_seconds,
            min_cluster_size=settings.sniper.min_cluster_size,
            eps=settings.sniper.eps,
            min_samples=settings.sniper.min_samples,
            min_entries_per_wallet=settings.sniper.min_entries_per_wallet,
            min_markets_in_common=settings.sniper.min_markets_in_common,
            coentry_top_n=settings.sniper.coentry_top_n,
            coentry_min_markets=settings.sniper.coentry_min_markets,
            coentry_min_shared_markets=settings.sniper.coentry_min_shared_markets,
        )
        outlier = TradeSizeOutlierDetector(hist_cache)
        digits = DigitDistributionDetector(digit_cache)
        slicing = TradeSlicingDetector(flow_cache, hist_cache)
        # Offline scans/backtests must not deduplicate.
        scorer_trade_time = RiskScorer(redis, dedup_window_seconds=0, key_prefix=f"{prefix}dedup:")
        future_weights = DEFAULT_WEIGHTS.copy()
        future_weights["pre_move"] = float(settings.scan.pre_move_weight)
        scorer_with_future = RiskScorer(
            redis,
            weights=future_weights,
            dedup_window_seconds=0,
            key_prefix=f"{prefix}dedup_future:",
        )

        # Pre-move computed as a second pass after all price bars are present.
        pre_move = PreMoveDetector()

        # Build entry events for sniper/co-entry (time-consistent: clustering runs periodically
        # over entries observed up to the current trade time).
        from polymarket_insider_tracker.detector.sniper import MarketEntry as SniperMarketEntry

        entries: list[SniperMarketEntry] = []
        market_first_trade: dict[str, datetime] = {}
        market_wallet_seen: set[tuple[str, str]] = set()
        market_entry_rank: dict[str, int] = {}
        entries_window_start_idx = 0
        last_sniper_run_at: datetime | None = None
        wallet_sniper: dict[str, SniperClusterSignal] = {}
        wallet_coentry: dict[str, CoEntryCorrelationSignal] = {}

        async with db.get_async_session() as session:
            liq_repo = LiquiditySnapshotRepository(session)
            signal_repo = TradeSignalRepository(session)
            feature_repo = TradeFeatureRepository(session)
            wallet_repo = WalletSnapshotRepository(session)
            price_repo = MarketPriceBarRepository(session)

            funding_cache: dict[str, FundingChainSignal] = {}
            bundles_by_trade: dict[str, SignalBundle] = {}
            trade_time_assessments: dict[str, RiskAssessment] = {}
            pre_move_signals: dict[str, PreMoveSignal] = {}
            future_assessments: dict[str, RiskAssessment] = {}

            async def compute_funding_signal(*, wallet_address: str, as_of: datetime) -> FundingChainSignal:
                chain = await funding.trace(session, address=wallet_address, as_of=as_of)
                await funding.persist_relationships(
                    session,
                    chain,
                    shares_funder_time_band_minutes=settings.funding.shares_funder_time_band_minutes,
                    funding_burst_min_wallets=settings.funding.funding_burst_min_wallets,
                )
                suspiciousness = funding.get_suspiciousness_score(chain)
                return FundingChainSignal(
                    wallet_address=wallet_address.lower(),
                    origin_address=chain.origin_address.lower(),
                    origin_type=chain.origin_type,
                    hop_count=chain.hop_count,
                    suspiciousness=suspiciousness,
                    confidence=suspiciousness,
                )

            async def maybe_run_sniper(*, now: datetime) -> None:
                nonlocal entries_window_start_idx, last_sniper_run_at, wallet_sniper, wallet_coentry
                if not entries:
                    return
                if last_sniper_run_at is not None:
                    if (now - last_sniper_run_at).total_seconds() < float(settings.sniper.run_interval_seconds):
                        return

                last_sniper_run_at = now
                window_start = now - timedelta(days=settings.sniper.window_days)
                while entries_window_start_idx < len(entries) and entries[entries_window_start_idx].timestamp < window_start:
                    entries_window_start_idx += 1
                window_entries = entries[entries_window_start_idx:]
                if not window_entries:
                    wallet_sniper = {}
                    wallet_coentry = {}
                    return

                clusters, cluster_signals = await asyncio.to_thread(
                    sniper.detect_clusters,
                    window_entries,
                    window_start=window_start,
                    window_end=now,
                )
                _ = clusters  # clusters are persisted in live mode; scan uses wallet-level signals only.
                coentry_signals = await asyncio.to_thread(
                    sniper.detect_coentry,
                    window_entries,
                    window_start=window_start,
                    window_end=now,
                )

                best_sniper: dict[str, SniperClusterSignal] = {}
                for s in cluster_signals:
                    prev = best_sniper.get(s.wallet_address)
                    if prev is None or s.confidence > prev.confidence:
                        best_sniper[s.wallet_address] = s
                best_co: dict[str, CoEntryCorrelationSignal] = {}
                for s in coentry_signals:
                    prev = best_co.get(s.wallet_address)
                    if prev is None or s.confidence > prev.confidence:
                        best_co[s.wallet_address] = s

                wallet_sniper = best_sniper
                wallet_coentry = best_co

            run_window_start = trades[0].ts
            run_window_end = trades[-1].ts
            market_asset_pairs = sorted({(t.market_id, t.asset_id) for t in trades})

            coverage_repo = LiquidityCoverageRepository(session)
            coverage_rows = await coverage_repo.compute_and_upsert_window(
                pairs=market_asset_pairs,
                window_start=run_window_start,
                window_end=run_window_end,
                cadence_seconds=settings.liquidity.snapshot_cadence_seconds,
                computed_at=now_utc(),
            )
            coverage_decisions = _build_liquidity_coverage_decisions(
                coverage_rows,
                min_market_coverage_ratio=settings.liquidity.market_min_coverage_ratio,
            )
            disabled_pair_records = [
                {
                    "condition_id": d.condition_id,
                    "asset_id": d.asset_id,
                    "coverage_ratio": d.coverage_ratio,
                    "availability_status": d.availability_status,
                    "reason": d.reason,
                }
                for d in coverage_decisions.values()
                if not d.enabled
            ]
            run_coverage_ratio = 0.0
            if coverage_rows:
                run_coverage_ratio = sum(float(r.coverage_ratio) for r in coverage_rows) / len(coverage_rows)
            liquidity_coverage_ok = run_coverage_ratio >= settings.liquidity.run_min_coverage_ratio

            logger.info(
                "Liquidity coverage summary window=[%s,%s] pairs=%d disabled=%d avg_ratio=%.4f run_ok=%s",
                run_window_start.isoformat(),
                run_window_end.isoformat(),
                len(coverage_rows),
                len(disabled_pair_records),
                run_coverage_ratio,
                liquidity_coverage_ok,
            )

            liquidity_telemetry: dict[str, int] = {
                "trades_total": 0,
                "trades_with_hist_liquidity": 0,
                "trades_missing_hist_liquidity": 0,
                "trades_size_signal_skipped": 0,
                "trades_size_signal_computed": 0,
                "trades_size_signal_disabled_by_coverage": 0,
            }
            enrichment_telemetry: dict[str, int] = {
                "fresh_wallet_failures": 0,
                "funding_trace_failures": 0,
                "fresh_wallet_disabled_by_rpc_history": 0,
                "funding_disabled_by_rpc_history": 0,
            }
            fresh_wallet_enabled = True
            fresh_wallet_disabled_reason: str | None = None
            funding_trace_enabled = True
            funding_trace_disabled_reason: str | None = None

            commit_every = 1000
            for idx, t in enumerate(trades, start=1):
                liquidity_telemetry["trades_total"] += 1
                total_considered += 1
                # Update rolling caches (deterministic for this replay).
                await volume_cache.record_trade_notional(t.market_id, ts=t.ts, notional_usdc=t.notional_usdc)
                await hist_cache.record(market_id=t.market_id, ts=t.ts, notional_usdc=t.notional_usdc)
                await digit_cache.record(market_id=t.market_id, ts=t.ts, notional_usdc=t.notional_usdc)
                await flow_cache.record(
                    wallet=t.wallet_address, market_id=t.market_id, ts=t.ts, notional_usdc=t.notional_usdc
                )

                # Sniper entry bookkeeping (first trade per wallet per market).
                if t.market_id not in market_first_trade:
                    market_first_trade[t.market_id] = t.ts
                    market_entry_rank[t.market_id] = 0
                key = (t.market_id, t.wallet_address.lower())
                if key not in market_wallet_seen:
                    market_wallet_seen.add(key)
                    market_entry_rank[t.market_id] += 1
                    entries.append(
                        SniperMarketEntry(
                            wallet_address=t.wallet_address.lower(),
                            market_id=t.market_id,
                            entry_delta_seconds=(t.ts - market_first_trade[t.market_id]).total_seconds(),
                            position_size=t.notional_usdc,
                            entry_rank=market_entry_rank[t.market_id],
                            timestamp=t.ts,
                        )
                    )

                await maybe_run_sniper(now=t.ts)

                # Recreate TradeEvent for detector compatibility.
                from polymarket_insider_tracker.ingestor.models import TradeEvent

                trade_event = TradeEvent(
                    market_id=t.market_id,
                    trade_id=t.trade_id,
                    wallet_address=t.wallet_address,
                    side="BUY" if t.side.upper() == "BUY" else "SELL",
                    outcome=t.outcome,
                    outcome_index=t.outcome_index,
                    price=t.price,
                    size=t.size,
                    timestamp=t.ts,
                    asset_id=t.asset_id,
                )

                fresh_signal = None
                fresh_missing_payload: dict[str, object] | None = None
                if t.notional_usdc >= settings.fresh_wallet.min_trade_notional_usdc:
                    if not fresh_wallet_enabled:
                        fresh_missing_payload = {
                            "reason": fresh_wallet_disabled_reason
                            or "wallet_snapshot_unavailable_for_run",
                        }
                    else:
                        try:
                            wallet_snapshot = await wallet_analyzer.analyze(t.wallet_address, as_of=t.ts)
                            await wallet_repo.upsert(
                                WalletSnapshotDTO(
                                    address=wallet_snapshot.address,
                                    as_of_block_number=wallet_snapshot.as_of_block_number,
                                    as_of=wallet_snapshot.as_of,
                                    nonce_as_of=wallet_snapshot.nonce_as_of,
                                    first_funding_at=wallet_snapshot.first_funding_at,
                                    age_hours_as_of=Decimal(str(wallet_snapshot.age_hours_as_of)),
                                    matic_balance_wei_as_of=wallet_snapshot.matic_balance_wei_as_of,
                                    usdc_balance_units_as_of=wallet_snapshot.usdc_balance_units_as_of,
                                    computed_at=wallet_snapshot.computed_at,
                                )
                            )
                            fresh_signal = await fresh.analyze(trade_event, wallet_snapshot=wallet_snapshot)
                        except (WalletAnalyzerError, FundingTraceError, RPCError) as e:
                            enrichment_telemetry["fresh_wallet_failures"] += 1
                            reason = "wallet_snapshot_unavailable"
                            if _is_rpc_history_constraint(e):
                                reason = "rpc_history_unavailable_for_wallet_snapshot"
                                if fresh_wallet_enabled:
                                    fresh_wallet_enabled = False
                                    fresh_wallet_disabled_reason = reason
                                    enrichment_telemetry["fresh_wallet_disabled_by_rpc_history"] += 1
                                    logger.warning(
                                        "Disabling fresh_wallet for remainder of scan: %s",
                                        str(e),
                                    )
                            fresh_missing_payload = {
                                "reason": reason,
                                "error_type": type(e).__name__,
                                "error_message": str(e),
                            }

                size_signal = None
                size_missing_payload: dict[str, object] | None = None
                coverage_decision = coverage_decisions.get((t.market_id, t.asset_id))
                if coverage_decision is None:
                    coverage_decision = LiquidityCoverageDecision(
                        condition_id=t.market_id,
                        asset_id=t.asset_id,
                        coverage_ratio=0.0,
                        availability_status="unavailable",
                        enabled=False,
                        reason="coverage_not_computed_for_market_asset",
                    )

                if not coverage_decision.enabled:
                    liquidity_telemetry["trades_size_signal_skipped"] += 1
                    liquidity_telemetry["trades_size_signal_disabled_by_coverage"] += 1
                    size_missing_payload = {
                        "reason": coverage_decision.reason,
                        "coverage_ratio": coverage_decision.coverage_ratio,
                        "availability_status": coverage_decision.availability_status,
                    }
                else:
                    liq = await liq_repo.get_latest_before(
                        condition_id=t.market_id,
                        asset_id=t.asset_id,
                        as_of=t.ts,
                    )
                    if liq is None:
                        liquidity_telemetry["trades_missing_hist_liquidity"] += 1
                        if settings.scan.historical_liquidity_policy == "required":
                            raise ScanError(
                                "Missing historical liquidity snapshot for "
                                f"market={t.market_id} asset={t.asset_id} at ts={t.ts.isoformat()}"
                            )
                        liquidity_telemetry["trades_size_signal_skipped"] += 1
                        size_missing_payload = {
                            "reason": "missing_historical_snapshot_at_trade_time",
                            "policy": settings.scan.historical_liquidity_policy,
                            "coverage_ratio": coverage_decision.coverage_ratio,
                        }
                    else:
                        liquidity_telemetry["trades_with_hist_liquidity"] += 1
                        rolling_volume = await volume_cache.get_rolling_volume(t.market_id, as_of=t.ts)
                        size_signal = await size_anomaly.analyze(
                            trade_event,
                            liquidity=LiquidityInputs(
                                rolling_24h_volume_usdc=rolling_volume,
                                visible_book_depth_usdc=liq.visible_book_depth_usdc,
                            ),
                        )
                        liquidity_telemetry["trades_size_signal_computed"] += 1

                missing_input_signals: list[tuple[str, dict[str, object]]] = []
                if fresh_missing_payload is not None:
                    missing_input_signals.append(("fresh_wallet_missing_input", fresh_missing_payload))
                if size_missing_payload is not None:
                    missing_input_signals.append(("size_anomaly_missing_input", size_missing_payload))

                outlier_signal_obj, outlier_missing = await _analyze_optional_signal(
                    detector_name="trade_size_outlier",
                    analyze_fn=lambda: outlier.analyze(trade_event),
                    skippable_errors=(TradeSizeOutlierDetectorError,),
                )
                outlier_signal = outlier_signal_obj
                if outlier_missing is not None:
                    missing_input_signals.append(("trade_size_outlier_missing_input", outlier_missing))

                digit_signal_obj, digit_missing = await _analyze_optional_signal(
                    detector_name="digit_distribution",
                    analyze_fn=lambda: digits.analyze(trade_event),
                    skippable_errors=(DigitDistributionDetectorError,),
                )
                digit_signal = digit_signal_obj
                if digit_missing is not None:
                    missing_input_signals.append(("digit_distribution_missing_input", digit_missing))

                slicing_signal_obj, slicing_missing = await _analyze_optional_signal(
                    detector_name="trade_slicing",
                    analyze_fn=lambda: slicing.analyze(trade_event),
                    skippable_errors=(TradeSlicingDetectorError,),
                )
                slicing_signal = slicing_signal_obj
                if slicing_missing is not None:
                    missing_input_signals.append(("trade_slicing_missing_input", slicing_missing))

                wallet = t.wallet_address.lower()
                sniper_signal = wallet_sniper.get(wallet)
                coentry_signal = wallet_coentry.get(wallet)

                bundle = SignalBundle(
                    trade_event=trade_event,
                    fresh_wallet_signal=fresh_signal,
                    size_anomaly_signal=size_signal,
                    sniper_cluster_signal=sniper_signal,
                    coentry_signal=coentry_signal,
                    trade_size_outlier_signal=outlier_signal,
                    digit_distribution_signal=digit_signal,
                    trade_slicing_signal=slicing_signal,
                )
                assessment = await scorer_trade_time.assess(bundle)

                # On-demand funding trace for high-risk candidates (strict: trace must succeed if triggered).
                should_trace = assessment.weighted_score >= settings.funding.trace_min_score
                if not should_trace and fresh_signal is not None:
                    should_trace = float(t.notional_usdc) >= settings.funding.trace_high_water_notional_usdc

                if should_trace:
                    if not funding_trace_enabled:
                        missing_input_signals.append(
                            (
                                "funding_missing_input",
                                {
                                    "reason": funding_trace_disabled_reason
                                    or "funding_trace_unavailable_for_run",
                                },
                            )
                        )
                    else:
                        try:
                            cached = funding_cache.get(wallet)
                            funding_signal = cached
                            if funding_signal is None:
                                funding_signal = await compute_funding_signal(wallet_address=wallet, as_of=t.ts)
                                funding_cache[wallet] = funding_signal
                            bundle = dataclasses.replace(bundle, funding_signal=funding_signal)
                            assessment = await scorer_trade_time.assess(bundle)
                        except (FundingTraceError, RPCError) as e:
                            enrichment_telemetry["funding_trace_failures"] += 1
                            reason = "funding_trace_unavailable"
                            if _is_rpc_history_constraint(e):
                                reason = "rpc_history_unavailable_for_funding_trace"
                                if funding_trace_enabled:
                                    funding_trace_enabled = False
                                    funding_trace_disabled_reason = reason
                                    enrichment_telemetry["funding_disabled_by_rpc_history"] += 1
                                    logger.warning(
                                        "Disabling funding trace for remainder of scan: %s",
                                        str(e),
                                    )
                            missing_input_signals.append(
                                (
                                    "funding_missing_input",
                                    {
                                        "reason": reason,
                                        "error_type": type(e).__name__,
                                        "error_message": str(e),
                                    },
                                )
                            )

                await feature_repo.upsert(
                    build_trade_features(bundle=bundle, assessment=assessment, computed_at=now_utc())
                )

                # Persist signals (audit).
                computed_at = now_utc()
                for signal_type, payload in missing_input_signals:
                    await signal_repo.upsert(
                        TradeSignalDTO(
                            trade_id=t.trade_id,
                            signal_type=signal_type,
                            confidence=Decimal("0"),
                            payload_json=json.dumps(payload, default=_json_default),
                            computed_at=computed_at,
                        )
                    )
                for signal_type, signal in (
                    ("fresh_wallet", fresh_signal),
                    ("size_anomaly", size_signal),
                    ("sniper_cluster", bundle.sniper_cluster_signal),
                    ("coentry", bundle.coentry_signal),
                    ("funding", bundle.funding_signal),
                    ("trade_size_outlier", outlier_signal),
                    ("digit_distribution", digit_signal),
                    ("trade_slicing", slicing_signal),
                ):
                    if signal is None:
                        continue
                    await signal_repo.upsert(
                        TradeSignalDTO(
                            trade_id=t.trade_id,
                            signal_type=signal_type,
                            confidence=Decimal(str(signal.confidence)),
                            payload_json=json.dumps(signal.to_dict(), default=_json_default),
                            computed_at=computed_at,
                        )
                    )

                bundles_by_trade[t.trade_id] = bundle
                trade_time_assessments[t.trade_id] = assessment

                if idx % commit_every == 0:
                    await session.commit()

            await session.commit()

            # Pre-move evaluation pass (writes as separate signal rows for audit) and
            # computes "with-future" assessments for ranking only.
            hits = 0
            evaluated = 0
            for t in trades:
                from polymarket_insider_tracker.ingestor.models import TradeEvent

                trade_event = TradeEvent(
                    market_id=t.market_id,
                    trade_id=t.trade_id,
                    wallet_address=t.wallet_address,
                    side="BUY" if t.side.upper() == "BUY" else "SELL",
                    outcome=t.outcome,
                    outcome_index=t.outcome_index,
                    price=t.price,
                    size=t.size,
                    timestamp=t.ts,
                    asset_id=t.asset_id,
                )
                try:
                    signal = await pre_move.analyze(trade_event, prices=price_repo)
                except PreMoveDetectorError:
                    continue
                pre_move_signals[t.trade_id] = signal
                await signal_repo.upsert(
                    TradeSignalDTO(
                        trade_id=t.trade_id,
                        signal_type="pre_move",
                        confidence=Decimal(str(signal.confidence)),
                        payload_json=json.dumps(signal.to_dict(), default=_json_default),
                        computed_at=now_utc(),
                    )
                )

                base = trade_time_assessments.get(t.trade_id)
                bundle = bundles_by_trade.get(t.trade_id)
                if base is None or bundle is None:
                    continue
                bundle_future = dataclasses.replace(bundle, pre_move_signal=signal)
                future = await scorer_with_future.assess(bundle_future)
                future_assessments[t.trade_id] = future

                # Hit rate is computed for trade-time alerts only (avoid circularity).
                if base.should_alert:
                    evaluated += 1
                    if float(signal.max_z_score) >= float(settings.model.label_z_threshold):
                        hits += 1
            await session.commit()

            hit_rate: Decimal | None = None
            if evaluated > 0:
                hit_rate = Decimal(str(hits / evaluated))

            flagged_records: list[dict[str, object]] = []
            for t in trades:
                base = trade_time_assessments.get(t.trade_id)
                if base is None:
                    continue
                future = future_assessments.get(t.trade_id, base)
                if not future.should_alert:
                    continue

                pre = pre_move_signals.get(t.trade_id)
                bundle = bundles_by_trade[t.trade_id]
                pair_decision = coverage_decisions.get((t.market_id, t.asset_id))

                flagged_records.append(
                    {
                        "run_id": run_id,
                        "trade_id": t.trade_id,
                        "market_id": t.market_id,
                        "asset_id": t.asset_id,
                        "wallet_address": t.wallet_address.lower(),
                        "ts": t.ts.isoformat(),
                        "side": t.side,
                        "price": str(t.price),
                        "size": str(t.size),
                        "notional_usdc": str(t.notional_usdc),
                        "risk_score_trade_time": base.weighted_score,
                        "risk_score_with_future": future.weighted_score,
                        "would_alert_trade_time": base.should_alert,
                        "would_alert_with_future": future.should_alert,
                        "run_quality": {"liquidity_coverage_ok": liquidity_coverage_ok},
                        "liquidity_size_anomaly": {
                            "enabled_by_coverage": bool(pair_decision.enabled) if pair_decision else False,
                            "coverage_ratio": pair_decision.coverage_ratio if pair_decision else 0.0,
                            "availability_status": pair_decision.availability_status
                            if pair_decision
                            else "unavailable",
                            "missing_input": bundle.size_anomaly_signal is None,
                        },
                        "signals": {
                            "fresh_wallet": bundle.fresh_wallet_signal.to_dict()
                            if bundle.fresh_wallet_signal
                            else None,
                            "size_anomaly": bundle.size_anomaly_signal.to_dict()
                            if bundle.size_anomaly_signal
                            else None,
                            "sniper_cluster": bundle.sniper_cluster_signal.to_dict()
                            if bundle.sniper_cluster_signal
                            else None,
                            "coentry": bundle.coentry_signal.to_dict() if bundle.coentry_signal else None,
                            "funding": bundle.funding_signal.to_dict() if bundle.funding_signal else None,
                            "trade_size_outlier": bundle.trade_size_outlier_signal.to_dict()
                            if bundle.trade_size_outlier_signal
                            else None,
                            "digit_distribution": bundle.digit_distribution_signal.to_dict()
                            if bundle.digit_distribution_signal
                            else None,
                            "trade_slicing": bundle.trade_slicing_signal.to_dict()
                            if bundle.trade_slicing_signal
                            else None,
                            "pre_move": pre.to_dict() if pre else None,
                        },
                    }
                )

            flagged_records.sort(
                key=lambda r: (
                    float(r.get("risk_score_with_future") or 0.0),
                    float(r.get("risk_score_trade_time") or 0.0),
                ),
                reverse=True,
            )
            with output_path.open("w", encoding="utf-8") as f:
                for rec in flagged_records:
                    f.write(json.dumps(rec, default=_json_default) + "\n")
            written = len(flagged_records)

            finished_at = datetime.now(UTC)
            run_repo = BacktestRunRepository(session)
            await run_repo.insert(
                BacktestRunDTO(
                    run_id=run_id,
                    command="scan",
                    query=query,
                    started_at=started_at,
                    finished_at=finished_at,
                    lookback_days=settings.scan.lookback_days,
                    top_k_markets=settings.scan.top_k_markets,
                    markets_considered=len(markets),
                    trades_considered=total_considered,
                    flagged_trades=written,
                    hit_rate=hit_rate,
                    output_path=str(output_path),
                    params_json=json.dumps(
                        {
                            "lookback_days": settings.scan.lookback_days,
                            "top_k_markets": settings.scan.top_k_markets,
                            "historical_liquidity_policy": settings.scan.historical_liquidity_policy,
                            "pre_move_weight": settings.scan.pre_move_weight,
                            "liquidity_coverage": {
                                "window_start": run_window_start.isoformat(),
                                "window_end": run_window_end.isoformat(),
                                "snapshot_cadence_seconds": settings.liquidity.snapshot_cadence_seconds,
                                "market_min_coverage_ratio": settings.liquidity.market_min_coverage_ratio,
                                "run_min_coverage_ratio": settings.liquidity.run_min_coverage_ratio,
                                "run_coverage_ratio": run_coverage_ratio,
                                "liquidity_coverage_ok": liquidity_coverage_ok,
                                "pairs_total": len(coverage_rows),
                                "pairs_disabled": len(disabled_pair_records),
                                "disabled_pairs": disabled_pair_records,
                            },
                            "liquidity_telemetry": liquidity_telemetry,
                            "enrichment_telemetry": enrichment_telemetry,
                        }
                    ),
                )
            )
            await session.commit()

        return ScanResult(
            run_id=run_id,
            output_path=output_path,
            total_trades_considered=total_considered,
            total_records_written=written,
        )
    finally:
        if polygon is not None:
            await polygon.aclose()
        await redis.aclose()
        await db.dispose_async()
