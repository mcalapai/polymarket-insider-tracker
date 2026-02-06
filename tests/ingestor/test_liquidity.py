"""Tests for liquidity utility helpers."""

from datetime import UTC, datetime

import pytest

from polymarket_insider_tracker.ingestor.liquidity import floor_to_cadence_bucket


def test_floor_to_cadence_bucket() -> None:
    ts = datetime(2026, 2, 6, 12, 7, 42, tzinfo=UTC)
    bucket = floor_to_cadence_bucket(ts, cadence_seconds=300)

    assert bucket == datetime(2026, 2, 6, 12, 5, 0, tzinfo=UTC)


def test_floor_to_cadence_bucket_requires_timezone() -> None:
    ts = datetime(2026, 2, 6, 12, 7, 42)
    with pytest.raises(ValueError, match="timezone-aware"):
        floor_to_cadence_bucket(ts, cadence_seconds=300)
