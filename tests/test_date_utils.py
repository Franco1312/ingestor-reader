"""Tests for date utilities."""

from datetime import datetime

import pytz

from src.infrastructure.utils.date_utils import to_naive


class TestToNaive:
    """Tests for to_naive function."""

    def test_none_returns_none(self):
        """Test that None input returns None."""
        assert to_naive(None) is None

    def test_naive_datetime_returns_same(self):
        """Test that naive datetime returns unchanged."""
        naive_dt = datetime(2025, 1, 15, 10, 30, 0)
        result = to_naive(naive_dt)
        assert result == naive_dt
        assert result.tzinfo is None

    def test_aware_datetime_returns_naive(self):
        """Test that aware datetime returns naive version."""
        tz = pytz.timezone("America/Argentina/Buenos_Aires")
        aware_dt = tz.localize(datetime(2025, 1, 15, 10, 30, 0))
        result = to_naive(aware_dt)
        assert result.tzinfo is None
        assert result == datetime(2025, 1, 15, 10, 30, 0)

    def test_utc_datetime_returns_naive(self):
        """Test that UTC datetime returns naive version."""
        utc_dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=pytz.UTC)
        result = to_naive(utc_dt)
        assert result.tzinfo is None
        assert result == datetime(2025, 1, 15, 10, 30, 0)

