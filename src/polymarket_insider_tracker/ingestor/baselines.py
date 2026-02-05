"""Rolling statistical baselines (Redis-backed).

Implements a bounded rolling histogram per market using:
- Per-bucket hashes (counts by bin)
- A per-market aggregate hash for the rolling window
- A per-market zset of bucket ids to evict expired buckets lazily

These baselines are used by distributional anomaly detectors without relying on
hard-coded thresholds.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


def _epoch_seconds(ts: datetime) -> int:
    return int(ts.timestamp())


def _bucket_start(ts: datetime, *, bucket_seconds: int) -> datetime:
    if ts.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    epoch = _epoch_seconds(ts)
    start = (epoch // bucket_seconds) * bucket_seconds
    return datetime.fromtimestamp(start, tz=UTC)


@dataclass(frozen=True)
class RollingHistogramConfig:
    window: timedelta
    bucket_seconds: int
    cleanup_batch_size: int = 100
    key_prefix: str = "polymarket:baseline:hist:"
    # log10-space bins for notional USDC
    min_log10: float = -2.0
    max_log10: float = 7.0
    bins: int = 36

    @property
    def bin_width(self) -> float:
        return (self.max_log10 - self.min_log10) / self.bins


class RollingHistogramCache:
    """Rolling histogram for trade notional distributions per market."""

    def __init__(self, redis: Redis, *, config: RollingHistogramConfig) -> None:
        self._redis = redis
        self._cfg = config
        self._prefix = config.key_prefix

    def _agg_key(self, market_id: str) -> str:
        return f"{self._prefix}agg:{market_id}"

    def _buckets_key(self, market_id: str) -> str:
        return f"{self._prefix}buckets:{market_id}"

    def _bucket_key(self, market_id: str, bucket: datetime) -> str:
        return f"{self._prefix}bucket:{market_id}:{_epoch_seconds(bucket)}"

    def _bin_index(self, value_usdc: Decimal) -> int:
        v = float(value_usdc)
        v = max(1e-9, v)
        x = math.log10(v)
        idx = int((x - self._cfg.min_log10) / self._cfg.bin_width)
        return max(0, min(self._cfg.bins - 1, idx))

    async def record(self, *, market_id: str, ts: datetime, notional_usdc: Decimal) -> None:
        bucket = _bucket_start(ts, bucket_seconds=self._cfg.bucket_seconds)
        bucket_id = _epoch_seconds(bucket)
        b = self._bin_index(notional_usdc)
        field = f"b{b}"

        bucket_key = self._bucket_key(market_id, bucket)
        agg_key = self._agg_key(market_id)
        buckets_key = self._buckets_key(market_id)

        pipe = self._redis.pipeline()
        pipe.hincrby(bucket_key, field, 1)
        pipe.hincrby(agg_key, field, 1)
        pipe.zadd(buckets_key, {str(bucket_id): bucket_id})
        pipe.expire(bucket_key, int(self._cfg.window.total_seconds()) + self._cfg.bucket_seconds)
        await pipe.execute()

    async def _evict_expired(self, *, market_id: str, as_of: datetime) -> None:
        window_start = as_of - self._cfg.window
        cutoff = _epoch_seconds(_bucket_start(window_start, bucket_seconds=self._cfg.bucket_seconds))
        buckets_key = self._buckets_key(market_id)
        expired = await self._redis.zrangebyscore(
            buckets_key,
            min=0,
            max=cutoff - 1,
            start=0,
            num=self._cfg.cleanup_batch_size,
        )
        if not expired:
            return

        agg_key = self._agg_key(market_id)
        pipe = self._redis.pipeline()
        for raw in expired:
            bucket_id = int(raw.decode() if isinstance(raw, bytes) else raw)
            bucket_key = f"{self._prefix}bucket:{market_id}:{bucket_id}"
            counts = await self._redis.hgetall(bucket_key)
            for k, v in counts.items():
                k_str = k.decode() if isinstance(k, bytes) else str(k)
                v_int = int(v.decode() if isinstance(v, bytes) else v)
                pipe.hincrby(agg_key, k_str, -v_int)
            pipe.delete(bucket_key)
            pipe.zrem(buckets_key, str(bucket_id))
        await pipe.execute()

    async def estimate_quantile(
        self,
        *,
        market_id: str,
        as_of: datetime,
        q: float,
    ) -> Decimal | None:
        if not (0.0 < q < 1.0):
            raise ValueError("q must be in (0, 1)")
        await self._evict_expired(market_id=market_id, as_of=as_of)

        counts = await self._redis.hgetall(self._agg_key(market_id))
        if not counts:
            return None
        total = 0
        by_bin = [0] * self._cfg.bins
        for k, v in counts.items():
            k_str = k.decode() if isinstance(k, bytes) else str(k)
            if not k_str.startswith("b"):
                continue
            idx = int(k_str[1:])
            if 0 <= idx < self._cfg.bins:
                c = int(v.decode() if isinstance(v, bytes) else v)
                if c <= 0:
                    continue
                by_bin[idx] = c
                total += c
        if total <= 0:
            return None

        target = int(math.ceil(q * total))
        cumulative = 0
        for idx, c in enumerate(by_bin):
            cumulative += c
            if cumulative >= target:
                # Return bin midpoint in linear space.
                x0 = self._cfg.min_log10 + idx * self._cfg.bin_width
                x1 = x0 + self._cfg.bin_width
                mid = (x0 + x1) / 2.0
                return Decimal(str(10 ** mid))
        return None

    async def get_histogram_counts(
        self,
        *,
        market_id: str,
        as_of: datetime,
    ) -> dict[int, int]:
        """Return current rolling bin counts (after lazy eviction)."""
        await self._evict_expired(market_id=market_id, as_of=as_of)
        counts = await self._redis.hgetall(self._agg_key(market_id))
        by_bin: dict[int, int] = {i: 0 for i in range(self._cfg.bins)}
        for k, v in counts.items():
            k_str = k.decode() if isinstance(k, bytes) else str(k)
            if not k_str.startswith("b"):
                continue
            idx = int(k_str[1:])
            if 0 <= idx < self._cfg.bins:
                by_bin[idx] = max(0, int(v.decode() if isinstance(v, bytes) else v))
        return by_bin

    async def total_samples(self, *, market_id: str, as_of: datetime) -> int:
        counts = await self.get_histogram_counts(market_id=market_id, as_of=as_of)
        return sum(counts.values())

    async def estimate_percentile(
        self,
        *,
        market_id: str,
        as_of: datetime,
        value: Decimal,
    ) -> float | None:
        """Estimate percentile rank for a value in the current rolling histogram.

        Returns a float in [0, 1], or None if insufficient data.
        """
        await self._evict_expired(market_id=market_id, as_of=as_of)
        counts_raw = await self._redis.hgetall(self._agg_key(market_id))
        if not counts_raw:
            return None
        by_bin = [0] * self._cfg.bins
        total = 0
        for k, v in counts_raw.items():
            k_str = k.decode() if isinstance(k, bytes) else str(k)
            if not k_str.startswith("b"):
                continue
            idx = int(k_str[1:])
            if 0 <= idx < self._cfg.bins:
                c = int(v.decode() if isinstance(v, bytes) else v)
                if c <= 0:
                    continue
                by_bin[idx] = c
                total += c
        if total <= 0:
            return None
        idx = self._bin_index(value)
        below = sum(by_bin[:idx])
        in_bin = by_bin[idx]
        # Mid-bin interpolation for stability.
        rank = (below + 0.5 * in_bin) / float(total)
        return max(0.0, min(1.0, rank))


@dataclass(frozen=True)
class RollingDigitConfig:
    window: timedelta
    bucket_seconds: int
    cleanup_batch_size: int = 100
    key_prefix: str = "polymarket:baseline:digits:"


class RollingDigitDistributionCache:
    """Rolling first-digit distribution for trade notionals (weak signal)."""

    def __init__(self, redis: Redis, *, config: RollingDigitConfig) -> None:
        self._redis = redis
        self._cfg = config
        self._prefix = config.key_prefix

    def _agg_key(self, market_id: str) -> str:
        return f"{self._prefix}agg:{market_id}"

    def _buckets_key(self, market_id: str) -> str:
        return f"{self._prefix}buckets:{market_id}"

    def _bucket_key(self, market_id: str, bucket: datetime) -> str:
        return f"{self._prefix}bucket:{market_id}:{_epoch_seconds(bucket)}"

    def _first_digit(self, value_usdc: Decimal) -> int:
        v = abs(float(value_usdc))
        if v <= 0:
            return 0
        # Normalize into [1, 10) then take int part.
        exp = math.floor(math.log10(v))
        leading = v / (10 ** exp)
        d = int(leading)
        return d if 1 <= d <= 9 else 0

    async def record(self, *, market_id: str, ts: datetime, notional_usdc: Decimal) -> None:
        bucket = _bucket_start(ts, bucket_seconds=self._cfg.bucket_seconds)
        bucket_id = _epoch_seconds(bucket)
        d = self._first_digit(notional_usdc)
        if d == 0:
            return
        field = f"d{d}"

        bucket_key = self._bucket_key(market_id, bucket)
        agg_key = self._agg_key(market_id)
        buckets_key = self._buckets_key(market_id)

        pipe = self._redis.pipeline()
        pipe.hincrby(bucket_key, field, 1)
        pipe.hincrby(agg_key, field, 1)
        pipe.zadd(buckets_key, {str(bucket_id): bucket_id})
        pipe.expire(bucket_key, int(self._cfg.window.total_seconds()) + self._cfg.bucket_seconds)
        await pipe.execute()

    async def _evict_expired(self, *, market_id: str, as_of: datetime) -> None:
        window_start = as_of - self._cfg.window
        cutoff = _epoch_seconds(_bucket_start(window_start, bucket_seconds=self._cfg.bucket_seconds))
        buckets_key = self._buckets_key(market_id)
        expired = await self._redis.zrangebyscore(
            buckets_key,
            min=0,
            max=cutoff - 1,
            start=0,
            num=self._cfg.cleanup_batch_size,
        )
        if not expired:
            return

        agg_key = self._agg_key(market_id)
        pipe = self._redis.pipeline()
        for raw in expired:
            bucket_id = int(raw.decode() if isinstance(raw, bytes) else raw)
            bucket_key = f"{self._prefix}bucket:{market_id}:{bucket_id}"
            counts = await self._redis.hgetall(bucket_key)
            for k, v in counts.items():
                k_str = k.decode() if isinstance(k, bytes) else str(k)
                v_int = int(v.decode() if isinstance(v, bytes) else v)
                pipe.hincrby(agg_key, k_str, -v_int)
            pipe.delete(bucket_key)
            pipe.zrem(buckets_key, str(bucket_id))
        await pipe.execute()

    async def get_distribution(self, *, market_id: str, as_of: datetime) -> dict[int, int]:
        await self._evict_expired(market_id=market_id, as_of=as_of)
        counts = await self._redis.hgetall(self._agg_key(market_id))
        dist: dict[int, int] = {d: 0 for d in range(1, 10)}
        for k, v in counts.items():
            k_str = k.decode() if isinstance(k, bytes) else str(k)
            if not k_str.startswith("d"):
                continue
            d = int(k_str[1:])
            if 1 <= d <= 9:
                dist[d] = max(0, int(v.decode() if isinstance(v, bytes) else v))
        return dist
