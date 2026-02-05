"""Rolling order lifecycle counters (Redis-backed)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

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
class RollingOrderCountConfig:
    window: timedelta
    bucket_seconds: int
    cleanup_batch_size: int = 100
    key_prefix: str = "polymarket:orders:count:"


class RollingOrderCountCache:
    """Rolling window counters for (wallet, market) order lifecycle events."""

    def __init__(self, redis: Redis, *, config: RollingOrderCountConfig) -> None:
        self._redis = redis
        self._cfg = config
        self._prefix = config.key_prefix

    def _agg_key(self, wallet: str, market_id: str) -> str:
        return f"{self._prefix}agg:{wallet.lower()}:{market_id}"

    def _buckets_key(self, wallet: str, market_id: str) -> str:
        return f"{self._prefix}buckets:{wallet.lower()}:{market_id}"

    def _bucket_key(self, wallet: str, market_id: str, bucket: datetime) -> str:
        return f"{self._prefix}bucket:{wallet.lower()}:{market_id}:{_epoch_seconds(bucket)}"

    async def record(self, *, wallet: str, market_id: str, ts: datetime, kind: str) -> None:
        bucket = _bucket_start(ts, bucket_seconds=self._cfg.bucket_seconds)
        bucket_id = _epoch_seconds(bucket)

        bucket_key = self._bucket_key(wallet, market_id, bucket)
        agg_key = self._agg_key(wallet, market_id)
        buckets_key = self._buckets_key(wallet, market_id)

        pipe = self._redis.pipeline()
        pipe.hincrby(bucket_key, kind, 1)
        pipe.hincrby(agg_key, kind, 1)
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
            for k, v in data.items():
                k_str = k.decode() if isinstance(k, bytes) else str(k)
                v_int = int(v.decode() if isinstance(v, bytes) else v)
                if v_int:
                    pipe.hincrby(agg_key, k_str, -v_int)
            pipe.delete(bucket_key)
            pipe.zrem(buckets_key, str(bucket_id))
        await pipe.execute()

    async def get_window(self, *, wallet: str, market_id: str, as_of: datetime) -> dict[str, int]:
        await self._evict_expired(wallet=wallet, market_id=market_id, as_of=as_of)
        data = await self._redis.hgetall(self._agg_key(wallet, market_id))
        if not data:
            return {}
        out: dict[str, int] = {}
        for k, v in data.items():
            k_str = k.decode() if isinstance(k, bytes) else str(k)
            v_int = int(v.decode() if isinstance(v, bytes) else v)
            if v_int:
                out[k_str] = max(0, v_int)
        return out
