"""Tests for SniperDetector coordination signals."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polymarket_insider_tracker.detector.models import CoEntryCorrelationSignal, SniperClusterSignal
from polymarket_insider_tracker.detector.sniper import MarketEntry, SniperDetector


def _entry(
    *,
    wallet: str,
    market: str,
    ts: datetime,
    delta_seconds: float,
    rank: int = 1,
    notional: Decimal = Decimal("1000"),
) -> MarketEntry:
    return MarketEntry(
        wallet_address=wallet,
        market_id=market,
        entry_delta_seconds=delta_seconds,
        position_size=notional,
        entry_rank=rank,
        timestamp=ts,
    )


class TestSniperDetectorInit:
    def test_default_parameters(self) -> None:
        detector = SniperDetector()
        assert detector.entry_threshold_seconds == 300
        assert detector.min_cluster_size == 3
        assert detector.min_entries_per_wallet == 2


class TestDetectClusters:
    def test_returns_empty_with_insufficient_wallets(self) -> None:
        detector = SniperDetector(min_cluster_size=3, min_entries_per_wallet=2)
        now = datetime.now(UTC)
        entries = [
            _entry(wallet="0x1", market="m1", ts=now, delta_seconds=30.0),
            _entry(wallet="0x1", market="m2", ts=now + timedelta(seconds=1), delta_seconds=25.0),
            _entry(wallet="0x2", market="m1", ts=now, delta_seconds=30.0),
            _entry(wallet="0x2", market="m2", ts=now + timedelta(seconds=1), delta_seconds=25.0),
        ]
        clusters, signals = detector.detect_clusters(entries, window_start=now, window_end=now)
        assert clusters == []
        assert signals == []

    def test_excludes_wallets_with_insufficient_entries(self) -> None:
        detector = SniperDetector(min_cluster_size=2, min_entries_per_wallet=3, eps=2.0, min_samples=2)
        now = datetime.now(UTC)
        # Two wallets but only 2 entries each; should be excluded.
        entries = []
        for wallet in ("0xa", "0xb"):
            entries.append(_entry(wallet=wallet, market="m1", ts=now, delta_seconds=15.0))
            entries.append(_entry(wallet=wallet, market="m2", ts=now + timedelta(seconds=1), delta_seconds=18.0))

        clusters, signals = detector.detect_clusters(entries, window_start=now, window_end=now)
        assert clusters == []
        assert signals == []

    def test_detects_cluster_and_emits_signals(self) -> None:
        detector = SniperDetector(
            entry_threshold_seconds=300,
            min_cluster_size=3,
            min_entries_per_wallet=2,
            eps=2.0,
            min_samples=2,
            min_markets_in_common=1,
        )
        now = datetime.now(UTC)
        wallets = [f"0x{i}" for i in range(5)]
        markets = ["m1", "m2", "m3"]
        entries: list[MarketEntry] = []

        for m in markets:
            for w in wallets:
                entries.append(
                    _entry(
                        wallet=w,
                        market=m,
                        ts=now + timedelta(seconds=len(entries)),
                        delta_seconds=30.0,
                        rank=1,
                        notional=Decimal("1500"),
                    )
                )

        clusters, signals = detector.detect_clusters(entries, window_start=now, window_end=now)
        assert len(clusters) >= 1
        assert len(signals) >= len(wallets)
        assert all(isinstance(s, SniperClusterSignal) for s in signals)

        # Deterministic cluster id across ordering.
        clusters2, _signals2 = detector.detect_clusters(list(reversed(entries)), window_start=now, window_end=now)
        assert {c.cluster_id for c in clusters} == {c.cluster_id for c in clusters2}


class TestDetectCoEntry:
    def test_returns_empty_when_not_enough_markets(self) -> None:
        detector = SniperDetector(coentry_min_markets=3, coentry_min_shared_markets=2)
        now = datetime.now(UTC)
        entries = [
            _entry(wallet="0xa", market="m1", ts=now, delta_seconds=10.0),
            _entry(wallet="0xb", market="m1", ts=now + timedelta(seconds=1), delta_seconds=12.0),
            _entry(wallet="0xa", market="m2", ts=now + timedelta(seconds=2), delta_seconds=11.0),
            _entry(wallet="0xb", market="m2", ts=now + timedelta(seconds=3), delta_seconds=13.0),
        ]
        signals = detector.detect_coentry(entries, window_start=now, window_end=now)
        assert signals == []

    def test_emits_signal_for_repeated_partner(self) -> None:
        detector = SniperDetector(coentry_top_n=10, coentry_min_markets=3, coentry_min_shared_markets=2)
        now = datetime.now(UTC)
        entries: list[MarketEntry] = []
        for i, market in enumerate(["m1", "m2", "m3", "m4"]):
            # wallet a and b appear in top-N together
            entries.append(_entry(wallet="0xa", market=market, ts=now + timedelta(seconds=i * 10), delta_seconds=10.0))
            entries.append(
                _entry(wallet="0xb", market=market, ts=now + timedelta(seconds=i * 10 + 1), delta_seconds=12.0)
            )
            # additional noise entrant
            entries.append(
                _entry(wallet=f"0xnoise{i}", market=market, ts=now + timedelta(seconds=i * 10 + 2), delta_seconds=20.0)
            )

        signals = detector.detect_coentry(entries, window_start=now, window_end=now)
        assert any(isinstance(s, CoEntryCorrelationSignal) for s in signals)
        a_signal = next(s for s in signals if s.wallet_address == "0xa")
        assert a_signal.top_partner_wallet == "0xb"
        assert a_signal.shared_markets >= 2


class TestSniperClusterSignalModel:
    def test_properties_and_serialization(self) -> None:
        signal = SniperClusterSignal(
            wallet_address="0xabc",
            cluster_id="sniper_deadbeef",
            cluster_size=4,
            avg_entry_delta_seconds=12.5,
            markets_in_common=2,
            confidence=0.8,
        )
        assert signal.is_high_confidence is True
        d = signal.to_dict()
        assert d["wallet_address"] == "0xabc"


@pytest.mark.parametrize(
    ("confidence", "is_high", "is_very_high"),
    [
        (0.9, True, True),
        (0.75, True, False),
        (0.3, False, False),
    ],
)
def test_coentry_signal_confidence_properties(confidence: float, is_high: bool, is_very_high: bool) -> None:
    signal = CoEntryCorrelationSignal(
        wallet_address="0xa",
        top_partner_wallet="0xb",
        shared_markets=3,
        markets_considered=4,
        confidence=confidence,
    )
    assert signal.is_high_confidence is is_high
    assert signal.is_very_high_confidence is is_very_high

