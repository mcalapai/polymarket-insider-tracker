"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def sample_market_id() -> str:
    """Sample market ID for testing."""
    return "0x1234567890abcdef1234567890abcdef12345678"
