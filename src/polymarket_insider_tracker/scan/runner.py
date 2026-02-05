"""Historical scan/backtest runner.

This module implements the `scan --query "..."` command:
- Semantic market retrieval
- Historical trade fetch (bounded)
- Offline replay using the same detectors/scoring as live mode
- Deterministic JSONL report output
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from redis.asyncio import Redis

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.detector.digit_distribution import DigitDistributionDetector
from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.pre_move import PreMoveDetector
from polymarket_insider_tracker.detector.scorer import RiskScorer, SignalBundle
from polymarket_insider_tracker.detector.size_anomaly import LiquidityInputs, SizeAnomalyDetector
from polymarket_insider_tracker.detector.sniper import SniperDetector
from polymarket_insider_tracker.detector.trade_size_outlier import TradeSizeOutlierDetector
from polymarket_insider_tracker.detector.trade_slicing import TradeSlicingDetector
from polymarket_insider_tracker.ingestor.baselines import (
    RollingDigitConfig,
    RollingDigitDistributionCache,
    RollingHistogramCache,
    RollingHistogramConfig,
)
from polymarket_insider_tracker.ingestor.clob_client import ClobClient
from polymarket_insider_tracker.ingestor.flow import RollingFlowConfig, RollingWalletMarketFlowCache
from polymarket_insider_tracker.ingestor.liquidity import compute_visible_book_depth_usdc, now_utc
from polymarket_insider_tracker.ingestor.volume import RollingVolumeCache, RollingVolumeConfig
from polymarket_insider_tracker.profiler.analyzer import WalletAnalyzer, WalletAnalyzerError
from polymarket_insider_tracker.profiler.chain import PolygonClient
from polymarket_insider_tracker.profiler.funding import FundingTracer
from polymarket_insider_tracker.scan.embeddings import EmbeddingConfig, SentenceTransformerEmbeddingProvider
from polymarket_insider_tracker.scan.market_indexer import MarketIndexer
from polymarket_insider_tracker.scan.search import MarketSearch, MarketSearchConfig
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.repos import (
    BacktestRunDTO,
    BacktestRunRepository,
    LiquiditySnapshotRepository,
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


@dataclass(frozen=True)
class ScanResult:
    run_id: str
    output_path: Path
    total_trades_considered: int
    total_records_written: int


class ScanError(RuntimeError):
    pass


def _json_default(x: object) -> str:
    if isinstance(x, (datetime,)):
        return x.isoformat()
    return str(x)


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

        clob = ClobClient(host=settings.polymarket.clob_host, chain_id=settings.polymarket.clob_chain_id)

        embedder = SentenceTransformerEmbeddingProvider(
            config=EmbeddingConfig(
                model_name_or_path=settings.scan.embedding_model or "",
                device=settings.scan.embedding_device,
                expected_dim=settings.scan.embedding_dim or 0,
            )
        )

        async with db.get_async_session() as session:
            indexer = MarketIndexer(clob_client=clob, embedder=embedder)
            indexed = await indexer.run_once(session=session)
            await session.commit()
            logger.info("Indexed %d markets for semantic search", indexed)

        async with db.get_async_session() as session:
            search = MarketSearch(embedder=embedder, config=MarketSearchConfig(top_k=settings.scan.top_k_markets))
            markets = await search.search(session=session, query=query)

        if not markets:
            raise ScanError("No markets found for query")

        cutoff = datetime.now(UTC) - timedelta(days=settings.scan.lookback_days)

        # Fetch + persist trades
        trades: list[TradeDTO] = []
        async with db.get_async_session() as session:
            trade_repo = TradeRepository(session)
            price_repo = MarketPriceBarRepository(session)

            for m in markets:
                raw_trades = await asyncio.to_thread(clob.get_market_trades, m.condition_id)
                # Bound work deterministically.
                raw_trades = [t for t in raw_trades if t.timestamp >= cutoff]
                raw_trades = sorted(raw_trades, key=lambda t: t.timestamp)[: settings.scan.max_trades_per_market]
                for t in raw_trades:
                    dto = TradeDTO(
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
                    await trade_repo.upsert(dto)
                    await price_repo.upsert_trade_price(market_id=t.market_id, ts=t.timestamp, price=t.price)
                    trades.append(dto)

            await session.commit()

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
        scorer = RiskScorer(redis)

        # Pre-move computed as a second pass after all price bars are present.
        pre_move = PreMoveDetector()

        # Build entry events for sniper/co-entry.
        from polymarket_insider_tracker.detector.sniper import MarketEntry as SniperMarketEntry

        entries: list[SniperMarketEntry] = []
        market_first_trade: dict[str, datetime] = {}
        market_wallet_seen: set[tuple[str, str]] = set()
        market_entry_rank: dict[str, int] = {}

        async with db.get_async_session() as session:
            liq_repo = LiquiditySnapshotRepository(session)
            signal_repo = TradeSignalRepository(session)
            feature_repo = TradeFeatureRepository(session)
            wallet_repo = WalletSnapshotRepository(session)
            price_repo = MarketPriceBarRepository(session)

            flagged_records: list[dict[str, object]] = []
            for t in trades:
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

                # Liquidity inputs (strict).
                liq = await liq_repo.get_latest_before(
                    condition_id=t.market_id,
                    asset_id=t.asset_id,
                    as_of=t.ts,
                )
                if liq is None:
                    if not settings.scan.allow_non_historical_book_depth:
                        raise ScanError(
                            f"Missing historical liquidity snapshot for market={t.market_id} asset={t.asset_id}"
                        )
                    # Explicitly non-historical: compute from current book.
                    ob = await asyncio.to_thread(clob.get_orderbook, t.asset_id)
                    depth, _mid = compute_visible_book_depth_usdc(
                        ob,
                        max_slippage_bps=settings.liquidity.depth_max_slippage_bps,
                    )
                    visible_depth = depth
                else:
                    visible_depth = liq.visible_book_depth_usdc

                rolling_volume = await volume_cache.get_rolling_volume(t.market_id, as_of=t.ts)
                size_signal = await size_anomaly.analyze(
                    trade_event,
                    liquidity=LiquidityInputs(
                        rolling_24h_volume_usdc=rolling_volume,
                        visible_book_depth_usdc=visible_depth,
                    ),
                )

                outlier_signal = await outlier.analyze(trade_event)
                digit_signal = await digits.analyze(trade_event)
                slicing_signal = await slicing.analyze(trade_event)

                # Sniper/co-entry computed after full pass; placeholders for now.
                bundle = SignalBundle(
                    trade_event=trade_event,
                    fresh_wallet_signal=fresh_signal,
                    size_anomaly_signal=size_signal,
                    trade_size_outlier_signal=outlier_signal,
                    digit_distribution_signal=digit_signal,
                    trade_slicing_signal=slicing_signal,
                )
                assessment = await scorer.assess(bundle)

                await feature_repo.upsert(
                    build_trade_features(bundle=bundle, assessment=assessment, computed_at=now_utc())
                )

                # Persist signals (audit).
                computed_at = now_utc()
                for signal_type, signal in (
                    ("fresh_wallet", fresh_signal),
                    ("size_anomaly", size_signal),
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

                if assessment.should_alert:
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
                            "risk_score": assessment.weighted_score,
                            "signals": {
                                "fresh_wallet": fresh_signal.to_dict() if fresh_signal else None,
                                "size_anomaly": size_signal.to_dict() if size_signal else None,
                                "trade_size_outlier": outlier_signal.to_dict() if outlier_signal else None,
                                "digit_distribution": digit_signal.to_dict() if digit_signal else None,
                                "trade_slicing": slicing_signal.to_dict() if slicing_signal else None,
                            },
                        }
                    )

            await session.commit()

            flagged_records.sort(key=lambda r: float(r.get("risk_score") or 0.0), reverse=True)
            with output_path.open("w", encoding="utf-8") as f:
                for rec in flagged_records:
                    f.write(json.dumps(rec, default=_json_default) + "\n")
            written = len(flagged_records)

            # Compute sniper/co-entry and enrich stored signals in Redis for optional second stage.
            if entries:
                window_end = datetime.now(UTC)
                window_start = cutoff
                clusters, cluster_signals = await asyncio.to_thread(
                    sniper.detect_clusters, entries, window_start=window_start, window_end=window_end
                )
                coentry_signals = await asyncio.to_thread(
                    sniper.detect_coentry, entries, window_start=window_start, window_end=window_end
                )
                # Store in Redis under scan namespace for potential future extensions.
                ttl = int(timedelta(days=1).total_seconds())
                for s in cluster_signals:
                    await redis.set(f"{prefix}sniper:{s.wallet_address}", json.dumps(s.to_dict()), ex=ttl)
                for s in coentry_signals:
                    await redis.set(f"{prefix}coentry:{s.wallet_address}", json.dumps(s.to_dict()), ex=ttl)

            # Pre-move evaluation pass (writes as separate signal rows for audit).
            flagged_ids = {str(r["trade_id"]) for r in flagged_records}
            hits = 0
            evaluated = 0
            for t in trades:
                from polymarket_insider_tracker.detector.pre_move import PreMoveDetectorError
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
                if t.trade_id in flagged_ids:
                    evaluated += 1
                    if float(signal.max_z_score) >= 2.0:
                        hits += 1
                await signal_repo.upsert(
                    TradeSignalDTO(
                        trade_id=t.trade_id,
                        signal_type="pre_move",
                        confidence=Decimal(str(signal.confidence)),
                        payload_json=json.dumps(signal.to_dict(), default=_json_default),
                        computed_at=now_utc(),
                    )
                )
            await session.commit()

            hit_rate: Decimal | None = None
            if evaluated > 0:
                hit_rate = Decimal(str(hits / evaluated))

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
                            "allow_non_historical_book_depth": settings.scan.allow_non_historical_book_depth,
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
        await redis.aclose()
        await db.dispose_async()
