"""Model serving utilities for live scoring."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from joblib import load

from polymarket_insider_tracker.detector.scorer import SignalBundle
from polymarket_insider_tracker.training.runner import FEATURE_COLUMNS, _to_float


class ModelServingError(RuntimeError):
    pass


@dataclass(frozen=True)
class LoadedModel:
    artifact_path: Path
    feature_columns: tuple[str, ...]
    model: Any


def load_model_from_artifact(*, artifact_path: Path, schema_json: str) -> LoadedModel:
    try:
        schema = json.loads(schema_json)
        cols = tuple(schema.get("feature_columns") or FEATURE_COLUMNS)
        if not cols:
            raise ValueError("missing feature_columns")
    except Exception as e:
        raise ModelServingError(f"Invalid schema_json: {e}") from e

    try:
        model = load(artifact_path)
    except Exception as e:
        raise ModelServingError(f"Failed to load model artifact: {e}") from e

    return LoadedModel(artifact_path=artifact_path, feature_columns=cols, model=model)


def predict_proba(bundle: SignalBundle, *, loaded: LoadedModel) -> float:
    # Build feature vector in the exact training column order.
    feature_map = {
        "wallet_nonce_as_of": bundle.fresh_wallet_signal.wallet_snapshot.nonce_as_of
        if bundle.fresh_wallet_signal
        else None,
        "wallet_age_hours_as_of": bundle.fresh_wallet_signal.wallet_snapshot.age_hours_as_of
        if bundle.fresh_wallet_signal
        else None,
        "wallet_usdc_balance_units_as_of": bundle.fresh_wallet_signal.wallet_snapshot.usdc_balance_units_as_of
        if bundle.fresh_wallet_signal
        else None,
        "wallet_matic_balance_wei_as_of": bundle.fresh_wallet_signal.wallet_snapshot.matic_balance_wei_as_of
        if bundle.fresh_wallet_signal
        else None,
        "trade_notional_usdc": bundle.trade_event.notional_value,
        "volume_impact": bundle.size_anomaly_signal.volume_impact if bundle.size_anomaly_signal else None,
        "book_impact": bundle.size_anomaly_signal.book_impact if bundle.size_anomaly_signal else None,
        "fresh_wallet_confidence": bundle.fresh_wallet_signal.confidence if bundle.fresh_wallet_signal else None,
        "size_anomaly_confidence": bundle.size_anomaly_signal.confidence if bundle.size_anomaly_signal else None,
        "sniper_cluster_confidence": bundle.sniper_cluster_signal.confidence if bundle.sniper_cluster_signal else None,
        "coentry_confidence": bundle.coentry_signal.confidence if bundle.coentry_signal else None,
        "funding_confidence": bundle.funding_signal.confidence if bundle.funding_signal else None,
        "trade_size_outlier_confidence": bundle.trade_size_outlier_signal.confidence
        if bundle.trade_size_outlier_signal
        else None,
        "digit_distribution_confidence": bundle.digit_distribution_signal.confidence
        if bundle.digit_distribution_signal
        else None,
        "trade_slicing_confidence": bundle.trade_slicing_signal.confidence if bundle.trade_slicing_signal else None,
        "order_to_trade_ratio_confidence": bundle.order_to_trade_ratio_signal.confidence
        if bundle.order_to_trade_ratio_signal
        else None,
        "rapid_cancel_confidence": bundle.rapid_cancel_signal.confidence if bundle.rapid_cancel_signal else None,
        "book_impact_without_fill_confidence": bundle.book_impact_without_fill_signal.confidence
        if bundle.book_impact_without_fill_signal
        else None,
    }

    row = [_to_float(feature_map.get(c)) for c in loaded.feature_columns]
    proba = loaded.model.predict_proba([row])[0][1]
    return float(proba)

