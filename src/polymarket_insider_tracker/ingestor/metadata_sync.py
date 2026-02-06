"""Market metadata synchronizer with Redis caching.

This module provides a background sync service that keeps market metadata
up-to-date in Redis, with cache-first lookups for fast access.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from prometheus_client import Counter, Gauge
from redis.asyncio import Redis

from .clob_client import ClobClient
from .liquidity import (
    LiquiditySnapshot,
    compute_visible_book_depth_usdc,
    floor_to_cadence_bucket,
    now_utc,
)
from .models import MarketMetadata

logger = logging.getLogger(__name__)


# Default configuration
DEFAULT_SYNC_INTERVAL_SECONDS = 300  # 5 minutes
DEFAULT_CACHE_TTL_SECONDS = 600  # 10 minutes
DEFAULT_REDIS_KEY_PREFIX = "polymarket:market:"
DEFAULT_HOT_MARKET_TTL_SECONDS = 3600
DEFAULT_LIQUIDITY_CACHE_TTL_SECONDS = 60
DEFAULT_DEPTH_MAX_SLIPPAGE_BPS = 200
DEFAULT_SNAPSHOT_CADENCE_SECONDS = 300
DEFAULT_ACTIVE_SWEEP_INTERVAL_SECONDS = 300
DEFAULT_ACTIVE_SWEEP_BATCH_SIZE = 250
DEFAULT_ORDERBOOK_TIMEOUT_SECONDS = 10.0
DEFAULT_ORDERBOOK_MAX_RETRIES = 2
DEFAULT_COLLECTION_MAX_CONCURRENCY = 10
DEFAULT_COVERAGE_JOB_INTERVAL_SECONDS = 3600
DEFAULT_COVERAGE_WINDOW_HOURS = 24

HOT_MARKETS_ZSET_KEY = "polymarket:hot_markets"
LIQUIDITY_KEY_PREFIX = "polymarket:liquidity:"

# Prometheus metrics
LIQUIDITY_SNAPSHOT_INGEST_TOTAL = Counter(
    "liquidity_snapshot_ingest_total",
    "Total number of liquidity snapshots computed and cached",
)
LIQUIDITY_SNAPSHOT_LAG_SECONDS = Gauge(
    "liquidity_snapshot_lag_seconds",
    "Lag between collection time and snapshot bucket timestamp",
)
LIQUIDITY_ORDERBOOK_FETCH_ERROR_TOTAL = Counter(
    "liquidity_orderbook_fetch_error_total",
    "Orderbook fetch failures during liquidity collection",
)
LIQUIDITY_ELIGIBLE_MARKETS = Gauge(
    "liquidity_eligible_markets",
    "Current eligible tradable markets count",
)
LIQUIDITY_INELIGIBLE_MARKETS = Gauge(
    "liquidity_ineligible_markets",
    "Current ineligible markets count",
)
LIQUIDITY_MARKET_CYCLE_COVERAGE_RATIO = Gauge(
    "liquidity_market_cycle_coverage_ratio",
    "Per-market snapshot success ratio per collector cycle",
    ["condition_id"],
)
LIQUIDITY_COVERAGE_JOB_TOTAL = Counter(
    "liquidity_coverage_job_total",
    "Total number of liquidity coverage jobs executed",
)
LIQUIDITY_COVERAGE_JOB_ERROR_TOTAL = Counter(
    "liquidity_coverage_job_error_total",
    "Total number of liquidity coverage job failures",
)


class SyncState(StrEnum):
    """State of the metadata synchronizer."""

    STOPPED = "stopped"
    STARTING = "starting"
    SYNCING = "syncing"
    IDLE = "idle"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class SyncStats:
    """Statistics for the metadata sync process."""

    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    markets_cached: int = 0
    last_sync_time: datetime | None = None
    last_sync_duration_seconds: float = 0.0
    last_error: str | None = None


# Type aliases for callbacks
StateCallback = Callable[[SyncState], None]
SyncCallback = Callable[[SyncStats], None]
LiquidityCallback = Callable[[LiquiditySnapshot], Awaitable[None]]
CoverageCallback = Callable[[datetime, datetime, int, list[tuple[str, str]]], Awaitable[None]]


class MetadataSyncError(Exception):
    """Base exception for metadata sync errors."""


class MarketMetadataSync:
    """Background service that syncs market metadata to Redis."""

    def __init__(
        self,
        redis: Redis,
        clob_client: ClobClient,
        *,
        sync_interval_seconds: int = DEFAULT_SYNC_INTERVAL_SECONDS,
        cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
        key_prefix: str = DEFAULT_REDIS_KEY_PREFIX,
        hot_market_ttl_seconds: int = DEFAULT_HOT_MARKET_TTL_SECONDS,
        liquidity_cache_ttl_seconds: int = DEFAULT_LIQUIDITY_CACHE_TTL_SECONDS,
        depth_max_slippage_bps: int = DEFAULT_DEPTH_MAX_SLIPPAGE_BPS,
        snapshot_cadence_seconds: int = DEFAULT_SNAPSHOT_CADENCE_SECONDS,
        active_sweep_interval_seconds: int = DEFAULT_ACTIVE_SWEEP_INTERVAL_SECONDS,
        active_sweep_batch_size: int = DEFAULT_ACTIVE_SWEEP_BATCH_SIZE,
        orderbook_timeout_seconds: float = DEFAULT_ORDERBOOK_TIMEOUT_SECONDS,
        orderbook_max_retries: int = DEFAULT_ORDERBOOK_MAX_RETRIES,
        collection_max_concurrency: int = DEFAULT_COLLECTION_MAX_CONCURRENCY,
        coverage_job_interval_seconds: int = DEFAULT_COVERAGE_JOB_INTERVAL_SECONDS,
        coverage_window_hours: int = DEFAULT_COVERAGE_WINDOW_HOURS,
        on_state_change: StateCallback | None = None,
        on_sync_complete: SyncCallback | None = None,
        on_liquidity_snapshot: LiquidityCallback | None = None,
        on_liquidity_coverage: CoverageCallback | None = None,
    ) -> None:
        self._redis = redis
        self._clob = clob_client
        self._sync_interval = sync_interval_seconds
        self._cache_ttl = cache_ttl_seconds
        self._key_prefix = key_prefix
        self._hot_market_ttl_seconds = hot_market_ttl_seconds
        self._liquidity_cache_ttl_seconds = liquidity_cache_ttl_seconds
        self._depth_max_slippage_bps = depth_max_slippage_bps
        self._snapshot_cadence_seconds = snapshot_cadence_seconds
        self._active_sweep_interval_seconds = active_sweep_interval_seconds
        self._active_sweep_batch_size = active_sweep_batch_size
        self._orderbook_timeout_seconds = orderbook_timeout_seconds
        self._orderbook_max_retries = orderbook_max_retries
        self._collection_max_concurrency = collection_max_concurrency
        self._coverage_job_interval_seconds = coverage_job_interval_seconds
        self._coverage_window_hours = coverage_window_hours
        self._on_state_change = on_state_change
        self._on_sync_complete = on_sync_complete
        self._on_liquidity_snapshot = on_liquidity_snapshot
        self._on_liquidity_coverage = on_liquidity_coverage

        self._state = SyncState.STOPPED
        self._stats = SyncStats()
        self._sync_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

        self._eligible_tradable_market_ids: list[str] = []
        self._sweep_cursor = 0
        self._last_active_sweep_at: datetime | None = None
        self._last_coverage_job_at: datetime | None = None
        self._orderbook_failures_by_asset: dict[str, int] = {}
        self._recent_cycle_coverage: deque[float] = deque(maxlen=3)

    @staticmethod
    def _is_tradable_market(metadata: MarketMetadata) -> bool:
        return (
            metadata.active
            and not metadata.closed
            and metadata.accepting_orders
            and metadata.enable_order_book
        )

    async def mark_hot_market(self, condition_id: str) -> None:
        """Mark a market as 'hot' for bounded liquidity computation."""
        expires_at = now_utc().timestamp() + float(self._hot_market_ttl_seconds)
        await self._redis.zadd(HOT_MARKETS_ZSET_KEY, {condition_id: expires_at})

    async def _prune_hot_markets(self) -> None:
        now_ts = now_utc().timestamp()
        await self._redis.zremrangebyscore(HOT_MARKETS_ZSET_KEY, min="-inf", max=now_ts)

    async def get_hot_markets(self, *, limit: int = 1000) -> list[str]:
        """Get currently-hot markets, pruning expired entries."""
        await self._prune_hot_markets()
        now_ts = now_utc().timestamp()
        markets = await self._redis.zrangebyscore(
            HOT_MARKETS_ZSET_KEY,
            min=now_ts,
            max="+inf",
            start=0,
            num=limit,
        )
        result: list[str] = []
        for m in markets:
            if isinstance(m, bytes):
                result.append(m.decode())
            else:
                result.append(str(m))
        return result

    def _liquidity_key(self, condition_id: str, asset_id: str) -> str:
        return f"{LIQUIDITY_KEY_PREFIX}{condition_id}:{asset_id}"

    async def set_liquidity_snapshot(self, snapshot: LiquiditySnapshot) -> None:
        await self._redis.setex(
            self._liquidity_key(snapshot.condition_id, snapshot.asset_id),
            self._liquidity_cache_ttl_seconds,
            snapshot.to_json(),
        )

    async def get_liquidity_snapshot(
        self, condition_id: str, asset_id: str
    ) -> LiquiditySnapshot | None:
        raw = await self._redis.get(self._liquidity_key(condition_id, asset_id))
        if not raw:
            return None
        try:
            if isinstance(raw, bytes):
                raw = raw.decode()
            return LiquiditySnapshot.from_json(str(raw))
        except Exception as e:
            logger.warning("Failed to parse liquidity snapshot for %s/%s: %s", condition_id, asset_id, e)
            return None

    @property
    def state(self) -> SyncState:
        return self._state

    @property
    def stats(self) -> SyncStats:
        return self._stats

    def _set_state(self, new_state: SyncState) -> None:
        old_state = self._state
        self._state = new_state
        if self._on_state_change and old_state != new_state:
            try:
                self._on_state_change(new_state)
            except Exception as e:
                logger.warning("State change callback failed: %s", e)

    async def start(self) -> None:
        """Start the background sync service."""
        if self._state != SyncState.STOPPED:
            logger.warning("Cannot start sync: already in state %s", self._state)
            return

        self._set_state(SyncState.STARTING)
        self._stop_event.clear()

        try:
            await self._sync_all_markets()
        except Exception as e:
            logger.error("Initial sync failed: %s", e)
            self._set_state(SyncState.ERROR)
            self._stats.last_error = str(e)
            raise MetadataSyncError(f"Failed to start: initial sync failed: {e}") from e

        self._sync_task = asyncio.create_task(self._sync_loop())
        self._set_state(SyncState.IDLE)
        logger.info("Market metadata sync started")

    async def stop(self) -> None:
        """Stop the background sync service."""
        if self._state == SyncState.STOPPED:
            return

        self._set_state(SyncState.STOPPING)
        self._stop_event.set()

        if self._sync_task:
            self._sync_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sync_task
            self._sync_task = None

        self._set_state(SyncState.STOPPED)
        logger.info("Market metadata sync stopped")

    async def _sync_loop(self) -> None:
        """Background loop that periodically syncs markets."""
        while not self._stop_event.is_set():
            try:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._sync_interval)
                    break
                except TimeoutError:
                    pass

                if self._stop_event.is_set():
                    break

                await self._sync_all_markets()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Sync loop error: %s", e)
                self._stats.failed_syncs += 1
                self._stats.last_error = str(e)
                self._set_state(SyncState.ERROR)

    def _next_active_sweep_markets(self) -> list[str]:
        if not self._eligible_tradable_market_ids:
            return []
        n = len(self._eligible_tradable_market_ids)
        size = min(self._active_sweep_batch_size, n)
        start = self._sweep_cursor % n

        result: list[str] = []
        for i in range(size):
            result.append(self._eligible_tradable_market_ids[(start + i) % n])

        self._sweep_cursor = (start + size) % n
        return result

    async def _sync_all_markets(self) -> None:
        """Fetch all markets, cache metadata, and collect liquidity snapshots."""
        self._set_state(SyncState.SYNCING)
        start_time = datetime.now(UTC)
        self._stats.total_syncs += 1

        try:
            markets = await asyncio.to_thread(self._clob.get_markets, False)

            cached_count = 0
            tradable_count = 0
            ineligible_count = 0
            metadata_by_condition: dict[str, MarketMetadata] = {}
            tradable_ids: list[str] = []

            for market in markets:
                try:
                    metadata = MarketMetadata.from_market(market)
                    metadata_by_condition[metadata.condition_id] = metadata
                    await self._cache_market(metadata)
                    cached_count += 1

                    if self._is_tradable_market(metadata):
                        tradable_count += 1
                        tradable_ids.append(metadata.condition_id)
                    else:
                        ineligible_count += 1
                except Exception as e:
                    logger.warning("Failed to cache market %s: %s", getattr(market, "condition_id", "?"), e)

            self._eligible_tradable_market_ids = sorted(set(tradable_ids))
            LIQUIDITY_ELIGIBLE_MARKETS.set(tradable_count)
            LIQUIDITY_INELIGIBLE_MARKETS.set(ineligible_count)

            logger.info(
                "Market eligibility audit: total=%d tradable=%d ineligible=%d",
                len(markets),
                tradable_count,
                ineligible_count,
            )

            await self._sync_liquidity_snapshots(metadata_by_condition=metadata_by_condition)

            end_time = datetime.now(UTC)
            self._stats.successful_syncs += 1
            self._stats.markets_cached = cached_count
            self._stats.last_sync_time = end_time
            self._stats.last_sync_duration_seconds = (end_time - start_time).total_seconds()
            self._stats.last_error = None

            self._set_state(SyncState.IDLE)
            logger.info(
                "Synced %d markets in %.2fs",
                cached_count,
                self._stats.last_sync_duration_seconds,
            )

            if self._on_sync_complete:
                try:
                    self._on_sync_complete(self._stats)
                except Exception as e:
                    logger.warning("Sync complete callback failed: %s", e)

        except Exception as e:
            self._stats.failed_syncs += 1
            self._stats.last_error = str(e)
            self._set_state(SyncState.ERROR)
            logger.error("Market sync failed: %s", e)
            raise

    async def _fetch_orderbook_with_retry(self, asset_id: str):
        last_error: Exception | None = None
        for attempt in range(self._orderbook_max_retries + 1):
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(self._clob.get_orderbook, asset_id),
                    timeout=self._orderbook_timeout_seconds,
                )
            except Exception as e:
                last_error = e
                LIQUIDITY_ORDERBOOK_FETCH_ERROR_TOTAL.inc()
                failures = self._orderbook_failures_by_asset.get(asset_id, 0) + 1
                self._orderbook_failures_by_asset[asset_id] = failures
                if failures >= 3:
                    logger.warning(
                        "Repeated orderbook fetch failures (asset=%s failures=%d latest_error=%s)",
                        asset_id,
                        failures,
                        e,
                    )
                if attempt >= self._orderbook_max_retries:
                    break
                await asyncio.sleep(min(0.25 * (2**attempt), 2.0))

        assert last_error is not None
        raise last_error

    async def _sync_liquidity_snapshots(
        self,
        *,
        metadata_by_condition: dict[str, MarketMetadata],
    ) -> None:
        hot_markets = await self.get_hot_markets()

        now = now_utc()
        sweep_markets: list[str] = []
        if self._last_active_sweep_at is None or (
            (now - self._last_active_sweep_at).total_seconds() >= self._active_sweep_interval_seconds
        ):
            sweep_markets = self._next_active_sweep_markets()
            self._last_active_sweep_at = now

        candidate_market_ids = list(dict.fromkeys([*hot_markets, *sweep_markets]))
        if not candidate_market_ids:
            return

        bucket_ts = floor_to_cadence_bucket(now, cadence_seconds=self._snapshot_cadence_seconds)
        lag_seconds = (now - bucket_ts).total_seconds()
        LIQUIDITY_SNAPSHOT_LAG_SECONDS.set(lag_seconds)
        if lag_seconds > (2 * self._snapshot_cadence_seconds):
            logger.warning(
                "Liquidity collector lag exceeded threshold lag=%.2fs cadence=%ss",
                lag_seconds,
                self._snapshot_cadence_seconds,
            )

        sem = asyncio.Semaphore(self._collection_max_concurrency)

        attempted_by_market: dict[str, int] = {}
        success_by_market: dict[str, int] = {}
        candidate_pairs: list[tuple[str, str]] = []

        async def sync_token(condition_id: str, asset_id: str) -> None:
            attempted_by_market[condition_id] = attempted_by_market.get(condition_id, 0) + 1
            candidate_pairs.append((condition_id, asset_id))
            async with sem:
                orderbook = await self._fetch_orderbook_with_retry(asset_id)
                depth, mid = compute_visible_book_depth_usdc(
                    orderbook,
                    max_slippage_bps=self._depth_max_slippage_bps,
                )
                snapshot = LiquiditySnapshot(
                    condition_id=condition_id,
                    asset_id=asset_id,
                    rolling_24h_volume_usdc=None,
                    visible_book_depth_usdc=depth,
                    mid_price=mid,
                    computed_at=bucket_ts,
                )
                await self.set_liquidity_snapshot(snapshot)
                if self._on_liquidity_snapshot:
                    await self._on_liquidity_snapshot(snapshot)
                LIQUIDITY_SNAPSHOT_INGEST_TOTAL.inc()
                self._orderbook_failures_by_asset.pop(asset_id, None)
                success_by_market[condition_id] = success_by_market.get(condition_id, 0) + 1

        tasks: list[asyncio.Task[None]] = []
        for condition_id in candidate_market_ids:
            metadata = metadata_by_condition.get(condition_id)
            if metadata is None:
                metadata = await self.get_market(condition_id)
            if metadata is None:
                continue
            if not self._is_tradable_market(metadata):
                continue
            for token in metadata.tokens:
                tasks.append(asyncio.create_task(sync_token(condition_id, token.token_id)))

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                logger.warning("Liquidity snapshot collection failed: %s", result)

        for condition_id, attempted in attempted_by_market.items():
            success = success_by_market.get(condition_id, 0)
            ratio = float(success / attempted) if attempted > 0 else 0.0
            LIQUIDITY_MARKET_CYCLE_COVERAGE_RATIO.labels(condition_id=condition_id).set(ratio)
            if attempted > 0 and ratio < 0.5:
                logger.warning(
                    "Low liquidity cycle coverage market=%s success=%d attempted=%d ratio=%.3f",
                    condition_id,
                    success,
                    attempted,
                    ratio,
                )
            self._recent_cycle_coverage.append(ratio)

        if len(self._recent_cycle_coverage) == self._recent_cycle_coverage.maxlen:
            avg = sum(self._recent_cycle_coverage) / len(self._recent_cycle_coverage)
            if avg < 0.5:
                logger.warning(
                    "Sustained liquidity coverage drop avg_ratio=%.3f window=%d cycles",
                    avg,
                    len(self._recent_cycle_coverage),
                )

        await self._maybe_run_coverage_job(candidate_pairs=candidate_pairs)

    async def _maybe_run_coverage_job(self, *, candidate_pairs: list[tuple[str, str]]) -> None:
        if self._on_liquidity_coverage is None:
            return

        now = now_utc()
        if self._last_coverage_job_at is not None:
            elapsed = (now - self._last_coverage_job_at).total_seconds()
            if elapsed < self._coverage_job_interval_seconds:
                return

        if not candidate_pairs:
            return

        window_end = floor_to_cadence_bucket(now, cadence_seconds=self._snapshot_cadence_seconds)
        window_start = window_end - timedelta(hours=self._coverage_window_hours)
        unique_pairs = sorted(set(candidate_pairs))

        LIQUIDITY_COVERAGE_JOB_TOTAL.inc()
        try:
            await self._on_liquidity_coverage(
                window_start,
                window_end,
                self._snapshot_cadence_seconds,
                unique_pairs,
            )
            self._last_coverage_job_at = now
            logger.info(
                "Liquidity coverage job completed pairs=%d window_start=%s window_end=%s",
                len(unique_pairs),
                window_start.isoformat(),
                window_end.isoformat(),
            )
        except Exception as e:
            LIQUIDITY_COVERAGE_JOB_ERROR_TOTAL.inc()
            logger.warning("Liquidity coverage job failed: %s", e)

    async def _cache_market(self, metadata: MarketMetadata) -> None:
        key = f"{self._key_prefix}{metadata.condition_id}"
        value = json.dumps(metadata.to_dict())
        await self._redis.setex(key, self._cache_ttl, value)

    async def get_market(self, condition_id: str) -> MarketMetadata | None:
        key = f"{self._key_prefix}{condition_id}"
        cached = await self._redis.get(key)

        if cached:
            try:
                data = json.loads(cached)
                return MarketMetadata.from_dict(data)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning("Failed to parse cached market %s: %s", condition_id, e)

        try:
            market = await asyncio.to_thread(self._clob.get_market, condition_id)
            if market:
                metadata = MarketMetadata.from_market(market)
                await self._cache_market(metadata)
                return metadata
        except Exception as e:
            logger.warning("Failed to fetch market %s: %s", condition_id, e)

        return None

    async def invalidate_market(self, condition_id: str) -> bool:
        key = f"{self._key_prefix}{condition_id}"
        deleted = await self._redis.delete(key)
        return int(deleted) > 0

    async def force_sync(self) -> None:
        await self._sync_all_markets()
