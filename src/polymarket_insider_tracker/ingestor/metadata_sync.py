"""Market metadata synchronizer with Redis caching.

This module provides a background sync service that keeps market metadata
up-to-date in Redis, with cache-first lookups for fast access.
"""

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from collections.abc import Awaitable

from redis.asyncio import Redis

from .clob_client import ClobClient
from .liquidity import (
    LiquiditySnapshot,
    compute_visible_book_depth_usdc,
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

HOT_MARKETS_ZSET_KEY = "polymarket:hot_markets"
LIQUIDITY_KEY_PREFIX = "polymarket:liquidity:"


class SyncState(str, Enum):
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


class MetadataSyncError(Exception):
    """Base exception for metadata sync errors."""

    pass


class MarketMetadataSync:
    """Background service that syncs market metadata to Redis.

    This service:
    - Fetches all markets from the CLOB API on startup
    - Refreshes the cache every sync_interval_seconds (default: 5 minutes)
    - Stores market metadata in Redis with TTL-based expiration
    - Provides cache-first lookups via get_market()

    Example:
        ```python
        redis = Redis.from_url("redis://localhost:6379")
        clob = ClobClient()

        sync = MarketMetadataSync(redis=redis, clob_client=clob)
        await sync.start()

        # Get market metadata (cache-first)
        metadata = await sync.get_market("0x1234...")

        await sync.stop()
        ```
    """

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
        on_state_change: StateCallback | None = None,
        on_sync_complete: SyncCallback | None = None,
        on_liquidity_snapshot: LiquidityCallback | None = None,
    ) -> None:
        """Initialize the metadata sync service.

        Args:
            redis: Redis async client for caching.
            clob_client: CLOB API client for fetching markets.
            sync_interval_seconds: Interval between syncs (default: 300 / 5 min).
            cache_ttl_seconds: TTL for cached entries (default: 600 / 10 min).
            key_prefix: Redis key prefix for market data.
            on_state_change: Callback for state changes.
            on_sync_complete: Callback after each sync completes.
        """
        self._redis = redis
        self._clob = clob_client
        self._sync_interval = sync_interval_seconds
        self._cache_ttl = cache_ttl_seconds
        self._key_prefix = key_prefix
        self._hot_market_ttl_seconds = hot_market_ttl_seconds
        self._liquidity_cache_ttl_seconds = liquidity_cache_ttl_seconds
        self._depth_max_slippage_bps = depth_max_slippage_bps
        self._on_state_change = on_state_change
        self._on_sync_complete = on_sync_complete
        self._on_liquidity_snapshot = on_liquidity_snapshot

        self._state = SyncState.STOPPED
        self._stats = SyncStats()
        self._sync_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def mark_hot_market(self, condition_id: str) -> None:
        """Mark a market as 'hot' for bounded liquidity computation."""
        expires_at = now_utc().timestamp() + float(self._hot_market_ttl_seconds)
        await self._redis.zadd(HOT_MARKETS_ZSET_KEY, {condition_id: expires_at})

    async def _prune_hot_markets(self) -> None:
        now_ts = now_utc().timestamp()
        await self._redis.zremrangebyscore(HOT_MARKETS_ZSET_KEY, min="-inf", max=now_ts)

    async def get_hot_markets(self, *, limit: int = 500) -> list[str]:
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
        """Current sync state."""
        return self._state

    @property
    def stats(self) -> SyncStats:
        """Current sync statistics."""
        return self._stats

    def _set_state(self, new_state: SyncState) -> None:
        """Update state and notify callback."""
        old_state = self._state
        self._state = new_state
        if self._on_state_change and old_state != new_state:
            try:
                self._on_state_change(new_state)
            except Exception as e:
                logger.warning(f"State change callback failed: {e}")

    async def start(self) -> None:
        """Start the background sync service.

        This will:
        1. Perform an initial sync of all markets
        2. Start a background task to periodically refresh
        """
        if self._state != SyncState.STOPPED:
            logger.warning(f"Cannot start sync: already in state {self._state}")
            return

        self._set_state(SyncState.STARTING)
        self._stop_event.clear()

        # Perform initial sync
        try:
            await self._sync_all_markets()
        except Exception as e:
            logger.error(f"Initial sync failed: {e}")
            self._set_state(SyncState.ERROR)
            self._stats.last_error = str(e)
            raise MetadataSyncError(f"Failed to start: initial sync failed: {e}") from e

        # Start background sync loop
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
                # Wait for next sync interval or stop event
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._sync_interval,
                    )
                    # Stop event was set
                    break
                except TimeoutError:
                    # Timeout - time to sync
                    pass

                if self._stop_event.is_set():
                    break

                await self._sync_all_markets()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sync loop error: {e}")
                self._stats.failed_syncs += 1
                self._stats.last_error = str(e)
                self._set_state(SyncState.ERROR)
                # Continue running - will retry on next interval

    async def _sync_all_markets(self) -> None:
        """Fetch all markets and cache them in Redis."""
        self._set_state(SyncState.SYNCING)
        start_time = datetime.now(UTC)
        self._stats.total_syncs += 1

        try:
            # Fetch markets from CLOB API (runs in thread pool for sync API)
            markets = await asyncio.to_thread(self._clob.get_markets, True)

            # Cache each market in Redis
            cached_count = 0
            for market in markets:
                try:
                    metadata = MarketMetadata.from_market(market)
                    await self._cache_market(metadata)
                    cached_count += 1
                except Exception as e:
                    logger.warning(f"Failed to cache market {market.condition_id}: {e}")

            # Update liquidity snapshots for hot markets (bounded work)
            await self._sync_liquidity_for_hot_markets()

            # Update stats
            end_time = datetime.now(UTC)
            self._stats.successful_syncs += 1
            self._stats.markets_cached = cached_count
            self._stats.last_sync_time = end_time
            self._stats.last_sync_duration_seconds = (end_time - start_time).total_seconds()
            self._stats.last_error = None

            self._set_state(SyncState.IDLE)
            logger.info(
                f"Synced {cached_count} markets in {self._stats.last_sync_duration_seconds:.2f}s"
            )

            # Notify callback
            if self._on_sync_complete:
                try:
                    self._on_sync_complete(self._stats)
                except Exception as e:
                    logger.warning(f"Sync complete callback failed: {e}")

        except Exception as e:
            self._stats.failed_syncs += 1
            self._stats.last_error = str(e)
            self._set_state(SyncState.ERROR)
            logger.error(f"Market sync failed: {e}")
            raise

    async def _sync_liquidity_for_hot_markets(self) -> None:
        hot_markets = await self.get_hot_markets()
        if not hot_markets:
            return

        semaphore = asyncio.Semaphore(10)

        async def sync_one(condition_id: str) -> None:
            async with semaphore:
                metadata = await self.get_market(condition_id)
                if metadata is None:
                    return
                for token in metadata.tokens:
                    orderbook = await asyncio.to_thread(self._clob.get_orderbook, token.token_id)
                    depth, mid = compute_visible_book_depth_usdc(
                        orderbook,
                        max_slippage_bps=self._depth_max_slippage_bps,
                    )
                    snapshot = LiquiditySnapshot(
                        condition_id=condition_id,
                        asset_id=token.token_id,
                        rolling_24h_volume_usdc=None,
                        visible_book_depth_usdc=depth,
                        mid_price=mid,
                        computed_at=now_utc(),
                    )
                    await self.set_liquidity_snapshot(snapshot)
                    if self._on_liquidity_snapshot:
                        await self._on_liquidity_snapshot(snapshot)

        results = await asyncio.gather(*(sync_one(m) for m in hot_markets), return_exceptions=True)
        for market_id, result in zip(hot_markets, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning("Liquidity sync failed for %s: %s", market_id, result)

    async def _cache_market(self, metadata: MarketMetadata) -> None:
        """Cache a single market metadata in Redis.

        Args:
            metadata: The market metadata to cache.
        """
        key = f"{self._key_prefix}{metadata.condition_id}"
        value = json.dumps(metadata.to_dict())
        await self._redis.setex(key, self._cache_ttl, value)

    async def get_market(self, condition_id: str) -> MarketMetadata | None:
        """Get market metadata with cache-first lookup.

        This first checks Redis cache. If not found or expired,
        it fetches from the CLOB API and caches the result.

        Args:
            condition_id: The market condition ID.

        Returns:
            MarketMetadata if found, None otherwise.
        """
        # Try cache first
        key = f"{self._key_prefix}{condition_id}"
        cached = await self._redis.get(key)

        if cached:
            try:
                data = json.loads(cached)
                return MarketMetadata.from_dict(data)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to parse cached market {condition_id}: {e}")

        # Cache miss - fetch from API
        try:
            market = await asyncio.to_thread(self._clob.get_market, condition_id)
            if market:
                metadata = MarketMetadata.from_market(market)
                await self._cache_market(metadata)
                return metadata
        except Exception as e:
            logger.warning(f"Failed to fetch market {condition_id}: {e}")

        return None

    async def invalidate_market(self, condition_id: str) -> bool:
        """Invalidate (delete) a cached market.

        Args:
            condition_id: The market condition ID to invalidate.

        Returns:
            True if the key was deleted, False if it didn't exist.
        """
        key = f"{self._key_prefix}{condition_id}"
        deleted = await self._redis.delete(key)
        return int(deleted) > 0

    async def force_sync(self) -> None:
        """Force an immediate sync of all markets.

        This can be called to refresh the cache outside of the
        normal sync interval.
        """
        await self._sync_all_markets()
