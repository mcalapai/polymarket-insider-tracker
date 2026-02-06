"""Tests for scan replay liquidity coverage gating."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polymarket_insider_tracker.scan.runner import (
    _analyze_optional_signal,
    _build_liquidity_coverage_decisions,
    _is_rpc_history_constraint,
)
from polymarket_insider_tracker.storage.repos import LiquidityCoverageDTO


def _row(*, ratio: str, status: str) -> LiquidityCoverageDTO:
    ts = datetime(2026, 2, 6, 12, 0, tzinfo=UTC)
    return LiquidityCoverageDTO(
        condition_id="m1",
        asset_id="a1",
        window_start=ts,
        window_end=ts,
        cadence_seconds=300,
        expected_snapshots=1,
        observed_snapshots=1,
        coverage_ratio=Decimal(ratio),
        availability_status=status,
        computed_at=ts,
    )


def test_coverage_decision_enables_when_measured_and_above_threshold() -> None:
    decisions = _build_liquidity_coverage_decisions([_row(ratio="0.95", status="measured")], min_market_coverage_ratio=0.8)
    d = decisions[("m1", "a1")]

    assert d.enabled is True
    assert d.reason is None


def test_coverage_decision_disables_unavailable() -> None:
    decisions = _build_liquidity_coverage_decisions([_row(ratio="1.0", status="unavailable")], min_market_coverage_ratio=0.8)
    d = decisions[("m1", "a1")]

    assert d.enabled is False
    assert d.reason == "coverage_unavailable_before_collection_start"


def test_coverage_decision_disables_below_threshold() -> None:
    decisions = _build_liquidity_coverage_decisions([_row(ratio="0.50", status="measured")], min_market_coverage_ratio=0.8)
    d = decisions[("m1", "a1")]

    assert d.enabled is False
    assert d.reason == "coverage_below_market_threshold"


def test_coverage_decisions_are_deterministic() -> None:
    rows = [_row(ratio="0.90", status="measured")]
    a = _build_liquidity_coverage_decisions(rows, min_market_coverage_ratio=0.8)
    b = _build_liquidity_coverage_decisions(rows, min_market_coverage_ratio=0.8)

    assert a == b


@pytest.mark.asyncio
async def test_analyze_optional_signal_returns_missing_payload_on_skippable_error() -> None:
    class _DetectorError(Exception):
        pass

    async def _raise() -> object:
        raise _DetectorError("not ready")

    signal, missing = await _analyze_optional_signal(
        detector_name="x",
        analyze_fn=_raise,
        skippable_errors=(_DetectorError,),
    )
    assert signal is None
    assert missing is not None
    assert missing["reason"] == "detector_prerequisites_unavailable"
    assert missing["detector"] == "x"


@pytest.mark.asyncio
async def test_analyze_optional_signal_returns_signal_on_success() -> None:
    async def _ok() -> object:
        return {"confidence": 0.9}

    signal, missing = await _analyze_optional_signal(
        detector_name="x",
        analyze_fn=_ok,
        skippable_errors=(RuntimeError,),
    )
    assert signal == {"confidence": 0.9}
    assert missing is None


def test_is_rpc_history_constraint_detects_block_range_and_pruned_history() -> None:
    assert _is_rpc_history_constraint(RuntimeError("Block range is too large"))
    assert _is_rpc_history_constraint(RuntimeError("History has been pruned for this block"))


def test_is_rpc_history_constraint_inspects_exception_chain() -> None:
    try:
        try:
            raise RuntimeError("History has been pruned for this block")
        except RuntimeError as inner:
            raise RuntimeError("RPC failed") from inner
    except RuntimeError as outer:
        assert _is_rpc_history_constraint(outer)


def test_is_rpc_history_constraint_false_for_unrelated_errors() -> None:
    assert _is_rpc_history_constraint(RuntimeError("connection timeout")) is False
