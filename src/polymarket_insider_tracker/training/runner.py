"""Model training entrypoints (self-supervised, auditable)."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import numpy as np
from joblib import dump
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.preprocessing import StandardScaler
import sqlalchemy as sa

from polymarket_insider_tracker.config import Settings
from polymarket_insider_tracker.storage.database import DatabaseManager
from polymarket_insider_tracker.storage.models import TradeFeatureModel, TradeLabelModel
from polymarket_insider_tracker.storage.repos import ModelArtifactDTO, ModelArtifactRepository
from polymarket_insider_tracker.training.labels import LabelConfig, generate_labels_from_pre_move


@dataclass(frozen=True)
class TrainingResult:
    model_id: str
    artifact_path: Path
    metrics: dict[str, float]


def _to_float(x: object) -> float:
    if x is None:
        return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0


FEATURE_COLUMNS: tuple[str, ...] = (
    "wallet_nonce_as_of",
    "wallet_age_hours_as_of",
    "wallet_usdc_balance_units_as_of",
    "wallet_matic_balance_wei_as_of",
    "trade_notional_usdc",
    "volume_impact",
    "book_impact",
    "fresh_wallet_confidence",
    "size_anomaly_confidence",
    "sniper_cluster_confidence",
    "coentry_confidence",
    "funding_confidence",
    "trade_size_outlier_confidence",
    "digit_distribution_confidence",
    "trade_slicing_confidence",
    "order_to_trade_ratio_confidence",
    "rapid_cancel_confidence",
    "book_impact_without_fill_confidence",
)


async def train_model(*, settings: Settings) -> None:  # noqa: ARG001
    settings.validate_requirements(command="train-model")

    db = DatabaseManager(settings.database.url, async_mode=True)
    model_id = str(uuid.uuid4())
    trained_at = datetime.now(UTC)
    artifacts_dir = settings.model.artifacts_dir
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifacts_dir / f"model_{model_id}.joblib"

    label_cfg = LabelConfig(
        label_type="pre_move_z",
        horizon_seconds=settings.model.label_horizon_seconds,
        z_threshold=settings.model.label_z_threshold,
    )

    try:
        async with db.get_async_session() as session:
            # Generate/update labels from pre-move signals.
            await generate_labels_from_pre_move(session=session, config=label_cfg)

            # Load features + labels.
            cols = [getattr(TradeFeatureModel, c) for c in FEATURE_COLUMNS]
            stmt = (
                sa.select(TradeFeatureModel.trade_id, *cols, TradeLabelModel.label)
                .join(
                    TradeLabelModel,
                    (TradeLabelModel.trade_id == TradeFeatureModel.trade_id)
                    & (TradeLabelModel.label_type == label_cfg.label_type)
                    & (TradeLabelModel.horizon_seconds == label_cfg.horizon_seconds),
                )
                .order_by(TradeFeatureModel.trade_id.asc())
            )
            result = await session.execute(stmt)
            rows = result.all()
            if len(rows) < 200:
                raise RuntimeError("Insufficient labeled feature rows for training")

            X: list[list[float]] = []
            y: list[int] = []
            for row in rows:
                # row = (trade_id, f1..fn, label)
                feats = [_to_float(v) for v in row[1:-1]]
                X.append(feats)
                y.append(1 if bool(row[-1]) else 0)

            X_arr = np.asarray(X, dtype=np.float64)
            y_arr = np.asarray(y, dtype=np.int64)

            X_train, X_test, y_train, y_test = train_test_split(
                X_arr, y_arr, test_size=0.2, random_state=42, stratify=y_arr
            )

            model = SkPipeline(
                steps=[
                    ("scaler", StandardScaler(with_mean=True, with_std=True)),
                    ("clf", LogisticRegression(max_iter=200, n_jobs=None)),
                ]
            )
            model.fit(X_train, y_train)

            y_pred = model.predict(X_test)
            acc = float(accuracy_score(y_test, y_pred))
            try:
                y_proba = model.predict_proba(X_test)[:, 1]
                auc = float(roc_auc_score(y_test, y_proba))
            except Exception:
                auc = float("nan")

            metrics = {"accuracy": acc, "roc_auc": auc, "n_rows": float(len(rows))}
            dump(model, artifact_path)

            repo = ModelArtifactRepository(session)
            if settings.model.auto_bless:
                await repo.unbless_all()
            await repo.insert(
                ModelArtifactDTO(
                    model_id=model_id,
                    algorithm="logreg",
                    label_type=label_cfg.label_type,
                    horizon_seconds=label_cfg.horizon_seconds,
                    z_threshold=Decimal(str(label_cfg.z_threshold)),
                    artifact_path=str(artifact_path),
                    metrics_json=json.dumps(metrics),
                    schema_json=json.dumps({"feature_columns": FEATURE_COLUMNS}),
                    blessed=bool(settings.model.auto_bless),
                    trained_at=trained_at,
                )
            )
            await session.commit()

        return None
    finally:
        await db.dispose_async()
