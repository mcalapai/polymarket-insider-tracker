"""Rolling wallet/market flow windows (Redis-backed).

Used for detecting trade slicing / iceberg-style behavior without hard-coded
"k trades in t minutes" rules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN

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
class RollingFlowConfig:
    window: timedelta
    bucket_seconds: int
    cleanup_batch_size: int = 100
    key_prefix: str = "polymarket:flow:"


class RollingWalletMarketFlowCache:
    """Rolling window sum/count of notionals for (wallet, market)."""

    def __init__(self, redis: Redis, *, config: RollingFlowConfig) -> None:
        self._redis = redis
        self._cfg = config
        self._prefix = config.key_prefix

    def _agg_key(self, wallet: str, market_id: str) -> str:
        return f"{self._prefix}agg:{wallet.lower()}:{market_id}"

    def _buckets_key(self, wallet: str, market_id: str) -> str:
        return f"{self._prefix}buckets:{wallet.lower()}:{market_id}"

    def _bucket_key(self, wallet: str, market_id: str, bucket: datetime) -> str:
        return f"{self._prefix}bucket:{wallet.lower()}:{market_id}:{_epoch_seconds(bucket)}"

    def _to_micros(self, value_usdc: Decimal) -> int:
        micros = (value_usdc * Decimal("1000000")).quantize(Decimal("1"), rounding=ROUND_DOWN)
        return int(micros)

    async def record(self, *, wallet: str, market_id: str, ts: datetime, notional_usdc: Decimal) -> None:
        bucket = _bucket_start(ts, bucket_seconds=self._cfg.bucket_seconds)
        bucket_id = _epoch_seconds(bucket)
        micros = self._to_micros(notional_usdc)

        bucket_key = self._bucket_key(wallet, market_id, bucket)
        agg_key = self._agg_key(wallet, market_id)
        buckets_key = self._buckets_key(wallet, market_id)

        pipe = self._redis.pipeline()
        pipe.hincrby(bucket_key, "sum_micros", micros)
        pipe.hincrby(bucket_key, "count", 1)
        pipe.hincrby(agg_key, "sum_micros", micros)
        pipe.hincrby(agg_key, "count", 1)
        pipe.zadd(buckets_key, {str(bucket_id): bucket_id})
        pipe.expire(bucket_key, int(self._cfg.window.total_seconds()) + self._cfg.bucket_seconds)
        await pipe.execute()

    async def _evict_expired(self, *, wallet: str, market_id: str, as_of: datetime) -> None:
        window_start = as_of - self._cfg.window
        cutoff = _epoch_seconds(_bucket_start(window_start, bucket_seconds=self._cfg.bucket_seconds))
        buckets_key = self._buckets_key(wallet, market_id)
        expired = await self._redis.zrangebyscore(
            buckets_key,
            min=0,
            max=cutoff - 1,
            start=0,
            num=self._cfg.cleanup_batch_size,
        )
        if not expired:
            return

        agg_key = self._agg_key(wallet, market_id)
        pipe = self._redis.pipeline()
        for raw in expired:
            bucket_id = int(raw.decode() if isinstance(raw, bytes) else raw)
            bucket_key = f"{self._prefix}bucket:{wallet.lower()}:{market_id}:{bucket_id}"
            data = await self._redis.hgetall(bucket_key)
            sum_raw = data.get(b"sum_micros") or data.get("sum_micros")  # type: ignore[arg-type]
            count_raw = data.get(b"count") or data.get("count")  # type: ignore[arg-type]
            sum_micros = int(sum_raw.decode() if isinstance(sum_raw, bytes) else (sum_raw or 0))
            count = int(count_raw.decode() if isinstance(count_raw, bytes) else (count_raw or 0))
            if sum_micros:
                pipe.hincrby(agg_key, "sum_micros", -sum_micros)
            if count:
                pipe.hincrby(agg_key, "count", -count)
            pipe.delete(bucket_key)
            pipe.zrem(buckets_key, str(bucket_id))
        await pipe.execute()

    async def get_window(self, *, wallet: str, market_id: str, as_of: datetime) -> tuple[Decimal, int]:
        await self._evict_expired(wallet=wallet, market_id=market_id, as_of=as_of)
        data = await self._redis.hgetall(self._agg_key(wallet, market_id))
        if not data:
            return (Decimal("0"), 0)
        sum_raw = data.get(b"sum_micros") or data.get("sum_micros")
        count_raw = data.get(b"count") or data.get("count")
        sum_micros = int(sum_raw.decode() if isinstance(sum_raw, bytes) else sum_raw or 0)
        count = int(count_raw.decode() if isinstance(count_raw, bytes) else count_raw or 0)
        return (Decimal(sum_micros) / Decimal("1000000"), max(0, count))
