"""Feature extraction for model training/serving."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.detector.models import RiskAssessment
from polymarket_insider_tracker.storage.repos import TradeFeatureDTO


def _d(x: float | None) -> Decimal | None:
    if x is None:
        return None
    return Decimal(str(x))


def build_trade_features(
    *,
    bundle: SignalBundle,
    assessment: RiskAssessment,
    computed_at: datetime | None = None,
) -> TradeFeatureDTO:
    computed_at = computed_at or datetime.now(UTC)

    wallet_snapshot = bundle.fresh_wallet_signal.wallet_snapshot if bundle.fresh_wallet_signal else None
    size = bundle.size_anomaly_signal if bundle.size_anomaly_signal else None
    model_score = bundle.model_score_signal.score if getattr(bundle, "model_score_signal", None) else None

    features = {
        "trade_id": bundle.trade_event.trade_id,
        "market_id": bundle.trade_event.market_id,
        "wallet_address": bundle.trade_event.wallet_address.lower(),
        "ts": bundle.trade_event.timestamp.isoformat(),
        "side": bundle.trade_event.side,
        "price": str(bundle.trade_event.price),
        "notional_usdc": str(bundle.trade_event.notional_value),
        "risk_score": assessment.weighted_score,
        "signals_triggered": assessment.signals_triggered,
        "should_alert": assessment.should_alert,
        "model_score": model_score,
    }

    return TradeFeatureDTO(
        trade_id=bundle.trade_event.trade_id,
        computed_at=computed_at,
        trade_notional_usdc=bundle.trade_event.notional_value,
        wallet_nonce_as_of=wallet_snapshot.nonce_as_of if wallet_snapshot else None,
        wallet_age_hours_as_of=Decimal(str(wallet_snapshot.age_hours_as_of)) if wallet_snapshot else None,
        wallet_usdc_balance_units_as_of=wallet_snapshot.usdc_balance_units_as_of if wallet_snapshot else None,
        wallet_matic_balance_wei_as_of=wallet_snapshot.matic_balance_wei_as_of if wallet_snapshot else None,
        volume_impact=_d(size.volume_impact) if size else None,
        book_impact=_d(size.book_impact) if size else None,
        fresh_wallet_confidence=_d(bundle.fresh_wallet_signal.confidence) if bundle.fresh_wallet_signal else None,
        size_anomaly_confidence=_d(bundle.size_anomaly_signal.confidence) if bundle.size_anomaly_signal else None,
        sniper_cluster_confidence=_d(bundle.sniper_cluster_signal.confidence) if bundle.sniper_cluster_signal else None,
        coentry_confidence=_d(bundle.coentry_signal.confidence) if bundle.coentry_signal else None,
        funding_confidence=_d(bundle.funding_signal.confidence) if bundle.funding_signal else None,
        trade_size_outlier_confidence=_d(bundle.trade_size_outlier_signal.confidence)
        if bundle.trade_size_outlier_signal
        else None,
        digit_distribution_confidence=_d(bundle.digit_distribution_signal.confidence)
        if bundle.digit_distribution_signal
        else None,
        trade_slicing_confidence=_d(bundle.trade_slicing_signal.confidence) if bundle.trade_slicing_signal else None,
        order_to_trade_ratio_confidence=_d(bundle.order_to_trade_ratio_signal.confidence)
        if bundle.order_to_trade_ratio_signal
        else None,
        rapid_cancel_confidence=_d(bundle.rapid_cancel_signal.confidence) if bundle.rapid_cancel_signal else None,
        book_impact_without_fill_confidence=_d(bundle.book_impact_without_fill_signal.confidence)
        if bundle.book_impact_without_fill_signal
        else None,
        model_score=_d(model_score),
        features_json=json.dumps(features),
    )
