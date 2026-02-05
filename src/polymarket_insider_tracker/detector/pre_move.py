"""Event/timing correlation detector (no external news dependency).

Computes whether a trade precedes an outsized favorable move over fixed horizons,
normalized by recent volatility in logit space.

This is intended primarily for historical scan/backtests; live mode cannot know
future prices.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

from polymarket_insider_tracker.detector.models import PreMoveSignal
from polymarket_insider_tracker.ingestor.models import TradeEvent


class PriceSeriesProvider(Protocol):
    async def get_close_at_or_after(self, *, market_id: str, ts: datetime) -> tuple[datetime, Decimal] | None:
        raise NotImplementedError

    async def list_close_series(
        self,
        *,
        market_id: str,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, Decimal]]:
        raise NotImplementedError


class PreMoveDetectorError(Exception):
    """Raised when pre-move signal cannot be computed strictly."""


@dataclass(frozen=True)
class PreMoveConfig:
    horizons: tuple[timedelta, ...] = (timedelta(minutes=15), timedelta(hours=1), timedelta(hours=4))
    volatility_lookback: timedelta = timedelta(hours=24)
    min_points: int = 30
    epsilon: float = 1e-6


def _clip_prob(p: float, eps: float) -> float:
    return max(eps, min(1.0 - eps, p))


def _logit(p: float) -> float:
    return math.log(p / (1.0 - p))


def _stddev(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    mean = sum(xs) / len(xs)
    var = sum((x - mean) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)


class PreMoveDetector:
    def __init__(self, *, config: PreMoveConfig | None = None) -> None:
        self._cfg = config or PreMoveConfig()

    async def analyze(
        self,
        trade: TradeEvent,
        *,
        prices: PriceSeriesProvider,
    ) -> PreMoveSignal:
        if trade.timestamp.tzinfo is None:
            raise PreMoveDetectorError("trade.timestamp must be timezone-aware")

        start = trade.timestamp - self._cfg.volatility_lookback
        series = await prices.list_close_series(
            market_id=trade.market_id,
            start=start,
            end=trade.timestamp,
        )
        if len(series) < self._cfg.min_points:
            raise PreMoveDetectorError("Insufficient price history for volatility normalization")

        logits: list[float] = []
        for _ts, px in series:
            p = _clip_prob(float(px), self._cfg.epsilon)
            logits.append(_logit(p))
        returns = [logits[i] - logits[i - 1] for i in range(1, len(logits))]
        vol = _stddev(returns)
        if vol <= 0:
            raise PreMoveDetectorError("Zero volatility in lookback window")

        direction = 1.0 if trade.side.upper() == "BUY" else -1.0
        p0 = _logit(_clip_prob(float(trade.price), self._cfg.epsilon))

        z_scores: dict[int, float] = {}
        for horizon in self._cfg.horizons:
            future = await prices.get_close_at_or_after(
                market_id=trade.market_id,
                ts=trade.timestamp + horizon,
            )
            if future is None:
                continue
            _f_ts, p_future = future
            p1 = _logit(_clip_prob(float(p_future), self._cfg.epsilon))
            delta = direction * (p1 - p0)
            z_scores[int(horizon.total_seconds())] = float(delta / vol)

        if not z_scores:
            raise PreMoveDetectorError("No future prices available for configured horizons")

        max_z = max(z_scores.values())
        # Map z-score to [0, 1] in a smooth, monotonic way.
        confidence = max(0.0, min(1.0, (max_z - 1.5) / 4.0))

        horizons_seconds = tuple(int(h.total_seconds()) for h in self._cfg.horizons)
        return PreMoveSignal(
            trade_event=trade,
            horizons_seconds=horizons_seconds,
            z_scores_by_horizon=z_scores,
            max_z_score=max_z,
            confidence=confidence,
        )

