"""Rolling volume cache for size anomaly detection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from redis.asyncio import Redis


class VolumeCacheError(Exception):
    """Raised when rolling volume cannot be computed."""


@dataclass(frozen=True)
class RollingVolumeConfig:
    window: timedelta
    key_prefix: str = "polymarket:volume:"
    ttl_seconds: int = 60 * 60 * 48  # 48h


class RollingVolumeCache:
    """Time-bucketed rolling 24h volume cache.

    Stores per-minute notional volume per market in a Redis hash:
      key = {prefix}{market_id}
      field = bucket_start_epoch_seconds
      value = notional_usdc (string float)
    """

    def __init__(self, redis: Redis, *, config: RollingVolumeConfig) -> None:
        self._redis = redis
        self._config = config

    def _key(self, market_id: str) -> str:
        return f"{self._config.key_prefix}{market_id}"

    @staticmethod
    def _bucket_start(ts: datetime) -> int:
        if ts.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        t = int(ts.timestamp())
        return (t // 60) * 60

    async def record_trade_notional(self, market_id: str, *, ts: datetime, notional_usdc: Decimal) -> None:
        bucket = self._bucket_start(ts)
        key = self._key(market_id)
        # Redis stores floats; use string representation.
        await self._redis.hincrbyfloat(key, str(bucket), float(notional_usdc))
        await self._redis.expire(key, self._config.ttl_seconds)

    async def get_rolling_volume(self, market_id: str, *, as_of: datetime) -> Decimal:
        """Sum buckets over the configured window."""
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")

        key = self._key(market_id)
        raw = await self._redis.hgetall(key)
        if not raw:
            raise VolumeCacheError(f"No volume buckets for market {market_id}")

        cutoff = int((as_of - self._config.window).timestamp())
        total = Decimal(0)

        for field, value in raw.items():
            field_str = field.decode() if isinstance(field, bytes) else str(field)
            value_str = value.decode() if isinstance(value, bytes) else str(value)
            try:
                bucket = int(field_str)
                if bucket < cutoff:
                    continue
                total += Decimal(value_str)
            except Exception:
                # If a corrupted entry exists, fail loudly (no silent fallbacks).
                raise VolumeCacheError(f"Corrupt volume bucket for {market_id}: {field_str}={value_str}")

        return total

    async def prune_old_buckets(self, market_id: str, *, as_of: datetime) -> int:
        """Remove buckets older than the window; returns removed count."""
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        key = self._key(market_id)
        raw = await self._redis.hkeys(key)
        if not raw:
            return 0

        cutoff = int((as_of - self._config.window).timestamp())
        to_delete: list[str] = []
        for field in raw:
            field_str = field.decode() if isinstance(field, bytes) else str(field)
            try:
                if int(field_str) < cutoff:
                    to_delete.append(field_str)
            except ValueError:
                to_delete.append(field_str)

        if not to_delete:
            return 0
        return int(await self._redis.hdel(key, *to_delete))

