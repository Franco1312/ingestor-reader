"""Tests for BCRA Infomondia normalizer."""

from datetime import datetime

import pytest
import pytz

from src.infrastructure.plugins.normalizers.bcra_infomondia_normalizer import (
    BcraInfomondiaNormalizer,
)


class TestBcraInfomondiaNormalizer:
    """Tests for BcraInfomondiaNormalizer class."""

    @pytest.fixture
    def normalizer(self):
        """Create a normalizer instance."""
        return BcraInfomondiaNormalizer()

    def test_normalize_empty_data(self, normalizer, sample_normalizer_config):
        """Test normalizing empty data."""
        result = normalizer.normalize([], sample_normalizer_config)
        assert result == []

    def test_normalize_basic_data(self, normalizer, sample_normalizer_config):
        """Test basic normalization."""
        data = [
            {
                "internal_series_code": "TEST_SERIES",
                "obs_time": datetime(2025, 1, 15, 10, 30, 0),
                "value": 100.5,
                "unit": "test_unit",
                "frequency": "D",
            }
        ]
        
        result = normalizer.normalize(data, sample_normalizer_config)
        
        assert len(result) == 1
        assert result[0]["internal_series_code"] == "TEST_SERIES"
        assert result[0]["value"] == 100.5
        assert result[0]["unit"] == "test_unit"
        assert result[0]["frequency"] == "D"
        assert isinstance(result[0]["obs_time"], datetime)
        assert result[0]["obs_time"].tzinfo is not None

    def test_normalize_with_timezone(self, normalizer):
        """Test normalization with specific timezone."""
        config = {
            "normalize": {
                "timezone": "America/Argentina/Buenos_Aires",
                "primary_keys": ["obs_time", "internal_series_code"],
            }
        }
        
        data = [
            {
                "internal_series_code": "TEST_SERIES",
                "obs_time": datetime(2025, 1, 15, 10, 30, 0),
                "value": 100.5,
            }
        ]
        
        result = normalizer.normalize(data, config)
        assert result[0]["obs_time"].tzinfo is not None
        assert str(result[0]["obs_time"].tzinfo) == "America/Argentina/Buenos_Aires"

    def test_normalize_filters_missing_series_code(self, normalizer, sample_normalizer_config):
        """Test that data points without series_code are filtered."""
        data = [
            {"obs_time": datetime(2025, 1, 15), "value": 100.5},
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": 200.5},
        ]
        
        result = normalizer.normalize(data, sample_normalizer_config)
        assert len(result) == 1
        assert result[0]["internal_series_code"] == "TEST_SERIES"

    def test_normalize_filters_invalid_datetime(self, normalizer, sample_normalizer_config):
        """Test that data points with invalid datetime are filtered."""
        data = [
            {"internal_series_code": "TEST_SERIES", "obs_time": None, "value": 100.5},
            {"internal_series_code": "TEST_SERIES", "obs_time": "invalid", "value": 200.5},
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": 300.5},
        ]
        
        result = normalizer.normalize(data, sample_normalizer_config)
        assert len(result) == 1
        assert result[0]["value"] == 300.5

    def test_normalize_filters_invalid_value(self, normalizer, sample_normalizer_config):
        """Test that data points with invalid value are filtered."""
        data = [
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": None},
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": "invalid"},
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": 100.5},
        ]
        
        result = normalizer.normalize(data, sample_normalizer_config)
        assert len(result) == 1
        assert result[0]["value"] == 100.5

    def test_normalize_value_conversion(self, normalizer, sample_normalizer_config):
        """Test value normalization for different types."""
        data = [
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15), "value": 100},
            {"internal_series_code": "SERIES_2", "obs_time": datetime(2025, 1, 15), "value": 200.5},
            {"internal_series_code": "SERIES_3", "obs_time": datetime(2025, 1, 15), "value": "300.5"},
            {"internal_series_code": "SERIES_4", "obs_time": datetime(2025, 1, 15), "value": "1234.56"},  # Without comma
        ]
        
        result = normalizer.normalize(data, sample_normalizer_config)
        assert len(result) == 4
        assert result[0]["value"] == 100.0
        assert result[1]["value"] == 200.5
        assert result[2]["value"] == 300.5
        assert result[3]["value"] == 1234.56

    def test_normalize_deduplication(self, normalizer):
        """Test deduplication based on primary keys."""
        config = {
            "normalize": {
                "timezone": "UTC",
                "primary_keys": ["obs_time", "internal_series_code"],
            }
        }
        
        data = [
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15), "value": 100.5},
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15), "value": 200.5},  # Duplicate
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 16), "value": 300.5},  # Different date
        ]
        
        result = normalizer.normalize(data, config)
        assert len(result) == 2  # One duplicate removed

    def test_normalize_no_deduplication(self, normalizer):
        """Test that deduplication is skipped when primary_keys not specified."""
        config = {
            "normalize": {
                "timezone": "UTC",
                "primary_keys": [],
            }
        }
        
        data = [
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15), "value": 100.5},
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15), "value": 200.5},
        ]
        
        result = normalizer.normalize(data, config)
        assert len(result) == 2  # No deduplication

    def test_normalize_aware_datetime_conversion(self, normalizer):
        """Test conversion of aware datetime to target timezone."""
        config = {
            "normalize": {
                "timezone": "America/Argentina/Buenos_Aires",
                "primary_keys": [],
            }
        }
        
        utc_tz = pytz.UTC
        utc_dt = utc_tz.localize(datetime(2025, 1, 15, 15, 0, 0))
        
        data = [
            {"internal_series_code": "TEST_SERIES", "obs_time": utc_dt, "value": 100.5},
        ]
        
        result = normalizer.normalize(data, config)
        assert result[0]["obs_time"].tzinfo is not None
        assert str(result[0]["obs_time"].tzinfo) == "America/Argentina/Buenos_Aires"

    def test_normalize_value_empty_string_after_cleaning(self, normalizer, sample_normalizer_config):
        """Test that empty string after cleaning returns None (line 104)."""
        data = [
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": "   "},  # Only spaces
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": ","},  # Only comma
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": "100.5"},  # Valid
        ]
        
        result = normalizer.normalize(data, sample_normalizer_config)
        # Only the valid value should pass
        assert len(result) == 1
        assert result[0]["value"] == 100.5

    def test_normalize_value_invalid_type(self, normalizer, sample_normalizer_config):
        """Test that invalid value types return None (line 110)."""
        data = [
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": [1, 2, 3]},  # List
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": {"key": "value"}},  # Dict
            {"internal_series_code": "TEST_SERIES", "obs_time": datetime(2025, 1, 15), "value": 100.5},  # Valid
        ]
        
        result = normalizer.normalize(data, sample_normalizer_config)
        # Only the valid value should pass
        assert len(result) == 1
        assert result[0]["value"] == 100.5

