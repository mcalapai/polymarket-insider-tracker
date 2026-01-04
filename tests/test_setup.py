"""Test that the project setup is working correctly."""

import polymarket_insider_tracker


def test_version() -> None:
    """Test that version is defined."""
    assert polymarket_insider_tracker.__version__ == "0.1.0"


def test_import_modules() -> None:
    """Test that all submodules can be imported."""
    from polymarket_insider_tracker import ingestor
    from polymarket_insider_tracker import profiler
    from polymarket_insider_tracker import detector
    from polymarket_insider_tracker import alerter
    from polymarket_insider_tracker import storage

    # Just verify imports work
    assert ingestor is not None
    assert profiler is not None
    assert detector is not None
    assert alerter is not None
    assert storage is not None
