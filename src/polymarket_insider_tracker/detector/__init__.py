"""Anomaly detection layer - Suspicious activity identification."""

from polymarket_insider_tracker.detector.fresh_wallet import FreshWalletDetector
from polymarket_insider_tracker.detector.models import FreshWalletSignal

__all__ = ["FreshWalletDetector", "FreshWalletSignal"]
