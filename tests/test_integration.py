"""Integration tests for the complete ETL flow."""

from datetime import datetime
from unittest.mock import Mock

import pytest

from src.infrastructure.plugins.normalizers.bcra_infomondia_normalizer import (
    BcraInfomondiaNormalizer,
)
from src.infrastructure.plugins.parsers.bcra_infomondia_parser import BcraInfomondiaParser
from tests.builders import ConfigBuilder, ETLUseCaseBuilder, StateManagerBuilder


class TestETLIntegration:
    """Integration tests for complete ETL flow."""

    @pytest.fixture
    def temp_state_file(self, tmp_path):
        """Create a temporary state file."""
        return str(tmp_path / "test_state.json")

    @pytest.fixture
    def sample_config(self):
        """Sample ETL configuration."""
        return (
            ConfigBuilder()
            .with_series(
                "TEST_SERIES",
                unit="test_unit",
                frequency="D",
                sheet="TEST_SHEET",
                header_row=1,
                skip_rows_after_header=0,
                date_col="A",
                value_col="B",
                drop_na=False,
            )
            .with_normalize_config("UTC", ["obs_time", "internal_series_code"])
            .build()
        )

    def test_full_etl_flow_with_mock_extractor(
        self, sample_excel_bytes, sample_config, temp_state_file
    ):
        """Test complete ETL flow with mocked extractor."""
        extractor = Mock()
        extractor.extract = Mock(return_value=sample_excel_bytes)
        
        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser(BcraInfomondiaParser())
            .with_normalizer(BcraInfomondiaNormalizer())
            .with_state_manager(state_config={"kind": "file", "state_file": temp_state_file})
            .build()
        )
        
        # First run
        result1 = etl.execute(sample_config)
        assert len(result1) > 0
        assert all("obs_time" in item for item in result1)
        assert all("value" in item for item in result1)
        
        # Verify state was saved
        saved_date = etl.state_manager.get_last_date("TEST_SERIES")
        assert saved_date is not None
        
        # Second run - should filter based on saved date
        result2 = etl.execute(sample_config)
        # Should have fewer or equal items (filtered by min_date)
        assert len(result2) <= len(result1)

    def test_etl_flow_without_state_manager(self, sample_excel_bytes, sample_config):
        """Test ETL flow without state manager."""
        extractor = Mock()
        extractor.extract = Mock(return_value=sample_excel_bytes)
        
        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser(BcraInfomondiaParser())
            .with_normalizer(BcraInfomondiaNormalizer())
            .build()
        )
        
        result = etl.execute(sample_config)
        assert len(result) > 0

    def test_etl_flow_incremental_updates(self, sample_excel_bytes, sample_config, temp_state_file):
        """Test that incremental updates work correctly."""
        extractor = Mock()
        extractor.extract = Mock(return_value=sample_excel_bytes)
        
        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser(BcraInfomondiaParser())
            .with_normalizer(BcraInfomondiaNormalizer())
            .with_state_manager(state_config={"kind": "file", "state_file": temp_state_file})
            .build()
        )
        
        # First run - full extraction
        result1 = etl.execute(sample_config)
        first_run_count = len(result1)
        
        # Get saved date
        saved_date = etl.state_manager.get_last_date("TEST_SERIES")
        assert saved_date is not None
        
        # Second run - should filter
        result2 = etl.execute(sample_config)
        
        # Verify filtering occurred
        if first_run_count > 0:
            # All dates in second run should be > saved_date
            for item in result2:
                obs_time = item["obs_time"]
                if isinstance(obs_time, datetime):
                    obs_time_naive = obs_time.replace(tzinfo=None) if obs_time.tzinfo else obs_time
                    assert obs_time_naive > saved_date or obs_time_naive == saved_date

