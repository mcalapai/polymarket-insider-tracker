"""Sniper cluster detection using DBSCAN clustering.

This module identifies wallets that exhibit coordinated "sniper" behavior -
consistently entering markets within minutes of an observable anchor.
"""

from __future__ import annotations

import logging
import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import numpy as np
from sklearn.cluster import DBSCAN

from polymarket_insider_tracker.detector.models import CoEntryCorrelationSignal, SniperClusterSignal

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class MarketEntry:
    """Record of a wallet's entry into a market.

    Attributes:
        wallet_address: The wallet that entered the market.
        market_id: The market condition ID.
        entry_delta_seconds: Time between market creation and wallet entry.
        position_size: Size of the initial position in USDC.
        timestamp: When the entry occurred.
    """

    wallet_address: str
    market_id: str
    entry_delta_seconds: float
    position_size: Decimal
    entry_rank: int
    timestamp: datetime


@dataclass
class ClusterInfo:
    """Information about a detected sniper cluster.

    Attributes:
        cluster_id: Unique identifier for this cluster.
        wallet_addresses: Set of wallet addresses in the cluster.
        avg_entry_delta: Average entry delay in seconds across the cluster.
        markets_in_common: Number of markets where cluster members overlap.
        created_at: When this cluster was first detected.
    """

    cluster_id: str
    wallet_addresses: set[str]
    avg_entry_delta: float
    markets_in_common: int
    confidence: float
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class SniperDetector:
    """Detects sniper clusters using DBSCAN clustering algorithm.

    The detector tracks wallet entries across markets and periodically
    runs DBSCAN clustering to identify groups of wallets with similar
    timing patterns (consistently entering markets early after creation).

    Attributes:
        entry_threshold_seconds: Maximum seconds after market creation to be
            considered a "sniper" entry (default 300 = 5 minutes).
        min_cluster_size: Minimum wallets to form a cluster (default 3).
        eps: DBSCAN epsilon parameter for neighborhood distance (default 0.5).
        min_samples: DBSCAN minimum samples for core point (default 2).
    """

    def __init__(
        self,
        *,
        entry_threshold_seconds: int = 300,
        min_cluster_size: int = 3,
        eps: float = 0.65,
        min_samples: int = 3,
        min_entries_per_wallet: int = 2,
        min_markets_in_common: int = 1,
        coentry_top_n: int = 10,
        coentry_min_markets: int = 3,
        coentry_min_shared_markets: int = 2,
    ) -> None:
        """Initialize the sniper detector.

        Args:
            entry_threshold_seconds: Max seconds for sniper entry (default 300).
            min_cluster_size: Minimum cluster size (default 3).
            eps: DBSCAN epsilon (default 0.5).
            min_samples: DBSCAN min samples (default 2).
            min_entries_per_wallet: Minimum entries to include wallet (default 2).
        """
        self.entry_threshold_seconds = entry_threshold_seconds
        self.min_cluster_size = min_cluster_size
        self.eps = eps
        self.min_samples = min_samples
        self.min_entries_per_wallet = min_entries_per_wallet
        self.min_markets_in_common = min_markets_in_common

        self.coentry_top_n = coentry_top_n
        self.coentry_min_markets = coentry_min_markets
        self.coentry_min_shared_markets = coentry_min_shared_markets

    def detect_clusters(
        self,
        entries: list[MarketEntry],
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> tuple[list[ClusterInfo], list[SniperClusterSignal]]:
        """Detect sniper clusters from persisted market entries.

        This is a pure function over the provided entries; callers are expected
        to supply a bounded time window (e.g., last N days) to keep runtime and
        memory bounded.
        """
        wallet_entries: dict[str, list[MarketEntry]] = defaultdict(list)
        for entry in entries:
            if entry.entry_delta_seconds < 0 or entry.entry_delta_seconds > self.entry_threshold_seconds:
                continue
            wallet_entries[entry.wallet_address.lower()].append(entry)

        eligible_wallets = [
            wallet
            for wallet, w_entries in wallet_entries.items()
            if len(w_entries) >= self.min_entries_per_wallet
        ]
        if len(eligible_wallets) < self.min_cluster_size:
            return ([], [])

        features, wallet_index = self._build_wallet_feature_matrix(wallet_entries, eligible_wallets)
        if features.size == 0:
            return ([], [])

        clustering = DBSCAN(
            eps=self.eps,
            min_samples=self.min_samples,
            metric="euclidean",
        ).fit(features)

        clusters = self._labels_to_clusters(clustering.labels_, wallet_index)
        cluster_infos: list[ClusterInfo] = []
        signals: list[SniperClusterSignal] = []

        for wallets in clusters.values():
            if len(wallets) < self.min_cluster_size:
                continue

            markets_in_common = self._markets_in_common(wallet_entries, wallets)
            if markets_in_common < self.min_markets_in_common:
                continue

            avg_delta = self._avg_entry_delta(wallet_entries, wallets)
            confidence = self._calculate_confidence(
                wallets,
                avg_entry_delta_seconds=avg_delta,
                markets_in_common=markets_in_common,
            )
            cluster_id = self._deterministic_cluster_id(wallets)
            info = ClusterInfo(
                cluster_id=cluster_id,
                wallet_addresses=set(wallets),
                avg_entry_delta=avg_delta,
                markets_in_common=markets_in_common,
                confidence=confidence,
                created_at=datetime.now(UTC),
            )
            cluster_infos.append(info)

            for wallet in wallets:
                signals.append(
                    SniperClusterSignal(
                        wallet_address=wallet,
                        cluster_id=cluster_id,
                        cluster_size=len(wallets),
                        avg_entry_delta_seconds=avg_delta,
                        markets_in_common=markets_in_common,
                        confidence=confidence,
                        timestamp=datetime.now(UTC),
                    )
                )

        logger.info(
            "Sniper clustering: window=%s..%s entries=%d eligible_wallets=%d clusters=%d",
            window_start.isoformat(),
            window_end.isoformat(),
            len(entries),
            len(eligible_wallets),
            len(cluster_infos),
        )
        return (cluster_infos, signals)

    def detect_coentry(
        self,
        entries: list[MarketEntry],
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> list[CoEntryCorrelationSignal]:
        """Deterministic co-entry coordination signal.

        This does not rely on DBSCAN: it looks for wallets that repeatedly appear
        among the first N entrants of the same markets.
        """
        market_entries: dict[str, list[MarketEntry]] = defaultdict(list)
        for entry in entries:
            if entry.entry_delta_seconds < 0 or entry.entry_delta_seconds > self.entry_threshold_seconds:
                continue
            market_entries[entry.market_id].append(entry)

        wallet_markets_in_top_n: dict[str, set[str]] = defaultdict(set)
        co_counts: dict[tuple[str, str], int] = defaultdict(int)

        for market_id, m_entries in market_entries.items():
            m_entries_sorted = sorted(m_entries, key=lambda e: (e.timestamp, e.entry_rank))
            top_wallets = [e.wallet_address.lower() for e in m_entries_sorted[: self.coentry_top_n]]
            unique = list(dict.fromkeys(top_wallets))  # stable de-dupe
            for wallet in unique:
                wallet_markets_in_top_n[wallet].add(market_id)
            for i in range(len(unique)):
                for j in range(i + 1, len(unique)):
                    a, b = unique[i], unique[j]
                    if a > b:
                        a, b = b, a
                    co_counts[(a, b)] += 1

        signals: list[CoEntryCorrelationSignal] = []
        for wallet, markets in wallet_markets_in_top_n.items():
            if len(markets) < self.coentry_min_markets:
                continue

            best_partner: str | None = None
            best_shared = 0
            for (a, b), shared in co_counts.items():
                if a == wallet and shared > best_shared:
                    best_partner, best_shared = b, shared
                elif b == wallet and shared > best_shared:
                    best_partner, best_shared = a, shared

            if best_partner is None or best_shared < self.coentry_min_shared_markets:
                continue

            confidence = round(min(1.0, best_shared / max(5.0, float(len(markets)))), 3)
            signals.append(
                CoEntryCorrelationSignal(
                    wallet_address=wallet,
                    top_partner_wallet=best_partner,
                    shared_markets=best_shared,
                    markets_considered=len(markets),
                    confidence=confidence,
                    timestamp=datetime.now(UTC),
                )
            )

        logger.info(
            "Co-entry correlation: window=%s..%s markets=%d signals=%d",
            window_start.isoformat(),
            window_end.isoformat(),
            len(market_entries),
            len(signals),
        )
        return signals

    def _calculate_confidence(
        self,
        cluster_wallets: list[str],
        *,
        avg_entry_delta_seconds: float,
        markets_in_common: int,
    ) -> float:
        """Calculate confidence score for a cluster.

        Higher confidence when:
        - Larger cluster size
        - Lower average entry delta (faster entries)
        - More markets in common

        Args:
            cluster_wallets: Wallets in the cluster.
            stats: Cluster statistics dict.

        Returns:
            Confidence score from 0.0 to 1.0.
        """
        # Size factor: more wallets = higher confidence
        size_factor = min(1.0, len(cluster_wallets) / 10.0)

        # Speed factor: faster entries = higher confidence
        # 0 seconds = 1.0, 300 seconds = 0.0
        speed_factor = max(0.0, 1.0 - (avg_entry_delta_seconds / self.entry_threshold_seconds))

        # Overlap factor: more markets in common = higher confidence
        overlap_factor = min(1.0, markets_in_common / 5.0)

        # Weighted combination
        confidence = 0.3 * size_factor + 0.4 * speed_factor + 0.3 * overlap_factor

        return round(min(1.0, confidence), 3)

    def _build_wallet_feature_matrix(
        self,
        wallet_entries: dict[str, list[MarketEntry]],
        eligible_wallets: list[str],
    ) -> tuple[np.ndarray, dict[int, str]]:
        rows: list[list[float]] = []
        wallet_index: dict[int, str] = {}
        for idx, wallet in enumerate(eligible_wallets):
            entries = wallet_entries[wallet]
            deltas = np.array([e.entry_delta_seconds for e in entries], dtype=float)
            ranks = np.array([e.entry_rank for e in entries], dtype=float)
            sizes = np.array([max(float(e.position_size), 1.0) for e in entries], dtype=float)

            mean_delta = float(deltas.mean())
            std_delta = float(deltas.std()) if len(deltas) > 1 else 0.0
            mean_rank = float(ranks.mean())
            mean_log_size = float(np.log10(sizes).mean())
            markets_count = float(len({e.market_id for e in entries}))
            entries_count = float(len(entries))

            # Normalize raw magnitudes into comparable ranges before standardization.
            rows.append(
                [
                    mean_delta / max(1.0, float(self.entry_threshold_seconds)),
                    std_delta / max(1.0, float(self.entry_threshold_seconds)),
                    mean_rank / 100.0,
                    mean_log_size / 5.0,
                    markets_count / 50.0,
                    entries_count / 50.0,
                ]
            )
            wallet_index[idx] = wallet

        mat = np.array(rows, dtype=float)
        if mat.size == 0:
            return mat, wallet_index

        # Deterministic feature scaling (z-score).
        means = mat.mean(axis=0)
        stds = mat.std(axis=0)
        stds[stds == 0] = 1.0
        mat = (mat - means) / stds
        return mat, wallet_index

    def _labels_to_clusters(self, labels: np.ndarray, wallet_index: dict[int, str]) -> dict[int, list[str]]:
        clusters: dict[int, list[str]] = defaultdict(list)
        for row_idx, label in enumerate(labels):
            if int(label) == -1:
                continue
            clusters[int(label)].append(wallet_index[row_idx])
        return clusters

    def _markets_in_common(self, wallet_entries: dict[str, list[MarketEntry]], wallets: list[str]) -> int:
        sets = [{e.market_id for e in wallet_entries[w]} for w in wallets]
        if len(sets) < 2:
            return 0
        return len(set.intersection(*sets))

    def _avg_entry_delta(self, wallet_entries: dict[str, list[MarketEntry]], wallets: list[str]) -> float:
        deltas: list[float] = []
        for w in wallets:
            deltas.extend([e.entry_delta_seconds for e in wallet_entries[w]])
        if not deltas:
            return float(self.entry_threshold_seconds)
        return float(sum(deltas) / len(deltas))

    def _deterministic_cluster_id(self, wallets: list[str]) -> str:
        ordered = sorted({w.lower() for w in wallets})
        material = "|".join(ordered).encode("utf-8")
        digest = hashlib.sha256(material).hexdigest()
        return f"sniper_{digest[:24]}"
