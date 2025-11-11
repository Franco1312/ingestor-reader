"""Tests for BCRA Infomondia transformer."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
import pytz

from src.infrastructure.plugins.transformers.bcra_infomondia_transformer import (
    BcraInfomondiaTransformer,
)
from tests.builders import ConfigBuilder, DataPointListBuilder


class TestBcraInfomondiaTransformer:
    """Tests for BcraInfomondiaTransformer class."""

    @pytest.fixture
    def transformer(self):
        """Create a transformer instance."""
        return BcraInfomondiaTransformer()

    @pytest.fixture
    def sample_transformer_config(self):
        """Sample transformer configuration with series map."""
        return (
            ConfigBuilder()
            .with_series("TEST_SERIES", unit="test_unit", frequency="D")
            .with_series("ANOTHER_SERIES", unit="another_unit", frequency="M")
            .build()
        )

    def test_transform_empty_data(self, transformer, sample_transformer_config):
        """Test transforming empty data."""
        result = transformer.transform([], sample_transformer_config)
        assert result == []

    @patch("src.infrastructure.plugins.transformers.bcra_infomondia_transformer.datetime")
    def test_transform_adds_unit_and_frequency_from_config(
        self, mock_datetime, transformer, sample_transformer_config
    ):
        """Test that unit and frequency come from config, not from input data."""
        # Mock collection date
        mock_collection_date = datetime(2025, 1, 20, 12, 0, 0, tzinfo=UTC)
        # Mock datetime.now to return our date when called with UTC
        mock_datetime.now = lambda tz=None: mock_collection_date

        # Input data (unit and frequency may be different or missing)
        data = (
            DataPointListBuilder()
            .add(
                "TEST_SERIES",
                datetime(2025, 1, 15, 0, 0, 0, tzinfo=pytz.UTC),
                100.5,
                unit="wrong_unit",  # Should be replaced by config
                frequency="wrong_frequency",  # Should be replaced by config
            )
            .build()
        )

        result = transformer.transform(data, sample_transformer_config)

        assert len(result) == 1
        assert result[0]["obs_time"] == data[0]["obs_time"]
        assert result[0]["internal_series_code"] == "TEST_SERIES"
        assert result[0]["value"] == 100.5
        assert result[0]["unit"] == "test_unit"  # From config
        assert result[0]["frequency"] == "D"  # From config
        assert result[0]["collection_date"] == mock_collection_date

    @patch("src.infrastructure.plugins.transformers.bcra_infomondia_transformer.datetime")
    def test_transform_adds_collection_date(
        self, mock_datetime, transformer, sample_transformer_config
    ):
        """Test that collection_date is added to all data points."""
        # Mock collection date
        mock_collection_date = datetime(2025, 1, 20, 12, 0, 0, tzinfo=UTC)
        # Mock datetime.now to return our date when called with UTC
        mock_datetime.now = lambda tz=None: mock_collection_date

        data = (
            DataPointListBuilder()
            .add("TEST_SERIES", datetime(2025, 1, 15, 0, 0, 0, tzinfo=pytz.UTC), 100.5)
            .build()
        )

        result = transformer.transform(data, sample_transformer_config)

        assert len(result) == 1
        assert result[0]["collection_date"] == mock_collection_date

    @patch("src.infrastructure.plugins.transformers.bcra_infomondia_transformer.datetime")
    def test_transform_multiple_series(self, mock_datetime, transformer, sample_transformer_config):
        """Test transforming data with multiple series."""
        # Mock collection date
        mock_collection_date = datetime(2025, 1, 20, 12, 0, 0, tzinfo=UTC)
        # Mock datetime.now to return our date when called with UTC
        mock_datetime.now = lambda tz=None: mock_collection_date

        data = (
            DataPointListBuilder()
            .add("TEST_SERIES", datetime(2025, 1, 15, 0, 0, 0, tzinfo=pytz.UTC), 100.5)
            .add("ANOTHER_SERIES", datetime(2025, 1, 16, 0, 0, 0, tzinfo=pytz.UTC), 200.5)
            .build()
        )

        result = transformer.transform(data, sample_transformer_config)

        assert len(result) == 2

        # First series
        assert result[0]["internal_series_code"] == "TEST_SERIES"
        assert result[0]["unit"] == "test_unit"
        assert result[0]["frequency"] == "D"
        assert result[0]["collection_date"] == mock_collection_date

        # Second series
        assert result[1]["internal_series_code"] == "ANOTHER_SERIES"
        assert result[1]["unit"] == "another_unit"
        assert result[1]["frequency"] == "M"
        assert result[1]["collection_date"] == mock_collection_date

    @patch("src.infrastructure.plugins.transformers.bcra_infomondia_transformer.datetime")
    def test_transform_series_not_in_config(
        self, mock_datetime, transformer, sample_transformer_config
    ):
        """Test transforming data for series not in config."""
        # Mock collection date
        mock_collection_date = datetime(2025, 1, 20, 12, 0, 0, tzinfo=UTC)
        # Mock datetime.now to return our date when called with UTC
        mock_datetime.now = lambda tz=None: mock_collection_date

        data = (
            DataPointListBuilder()
            .add("UNKNOWN_SERIES", datetime(2025, 1, 15, 0, 0, 0, tzinfo=pytz.UTC), 100.5)
            .build()
        )

        result = transformer.transform(data, sample_transformer_config)

        assert len(result) == 1
        assert result[0]["internal_series_code"] == "UNKNOWN_SERIES"
        assert result[0]["value"] == 100.5
        assert result[0]["unit"] is None  # Not in config
        assert result[0]["frequency"] is None  # Not in config
        assert result[0]["collection_date"] == mock_collection_date

    @patch("src.infrastructure.plugins.transformers.bcra_infomondia_transformer.datetime")
    def test_transform_preserves_obs_time_and_value(
        self, mock_datetime, transformer, sample_transformer_config
    ):
        """Test that obs_time and value are preserved from input."""
        # Mock collection date
        mock_collection_date = datetime(2025, 1, 20, 12, 0, 0, tzinfo=UTC)
        # Mock datetime.now to return our date when called with UTC
        mock_datetime.now = lambda tz=None: mock_collection_date

        obs_time = datetime(2025, 1, 15, 10, 30, 0, tzinfo=pytz.UTC)
        value = 1234.56

        data = DataPointListBuilder().add("TEST_SERIES", obs_time, value).build()

        result = transformer.transform(data, sample_transformer_config)

        assert len(result) == 1
        assert result[0]["obs_time"] == obs_time
        assert result[0]["value"] == value

    @patch("src.infrastructure.plugins.transformers.bcra_infomondia_transformer.datetime")
    def test_transform_with_empty_parse_config(self, mock_datetime, transformer):
        """Test transforming with empty parse_config."""
        # Mock collection date
        mock_collection_date = datetime(2025, 1, 20, 12, 0, 0, tzinfo=UTC)
        # Mock datetime.now to return our date when called with UTC
        mock_datetime.now = lambda tz=None: mock_collection_date

        config = ConfigBuilder().build()
        data = (
            DataPointListBuilder()
            .add("TEST_SERIES", datetime(2025, 1, 15, 0, 0, 0, tzinfo=pytz.UTC), 100.5)
            .build()
        )

        result = transformer.transform(data, config)

        assert len(result) == 1
        assert result[0]["unit"] is None
        assert result[0]["frequency"] is None
        assert result[0]["collection_date"] == mock_collection_date
