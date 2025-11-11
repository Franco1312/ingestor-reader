"""Tests specifically for incremental update flow."""

from datetime import datetime
from unittest.mock import Mock

import pytest

from src.infrastructure.plugins.normalizers.bcra_infomondia_normalizer import (
    BcraInfomondiaNormalizer,
)
from src.infrastructure.plugins.parsers.bcra_infomondia_parser import BcraInfomondiaParser
from tests.builders import ConfigBuilder, ETLUseCaseBuilder, StateManagerBuilder


class TestIncrementalUpdates:
    """Tests for incremental update functionality."""

    @pytest.fixture
    def temp_state_file(self, tmp_path):
        """Create a temporary state file."""
        return str(tmp_path / "incremental_state.json")

    @pytest.fixture
    def sample_config_multiple_series(self):
        """Config with multiple series."""
        return (
            ConfigBuilder()
            .with_series(
                "SERIES_1",
                sheet="TEST_SHEET",
                header_row=1,
                skip_rows_after_header=0,
                date_col="A",
                value_col="B",
                drop_na=False,
            )
            .with_series(
                "SERIES_2",
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

    def test_incremental_first_run_processes_all(self, sample_excel_bytes, sample_parser_config, temp_state_file):
        """Test that first run processes all data."""
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
        
        result = etl.execute(sample_parser_config)
        
        # Should process all data on first run
        assert len(result) > 0
        
        # State should be saved
        saved_date = etl.state_manager.get_last_date("TEST_SERIES")
        assert saved_date is not None

    def test_incremental_second_run_filters_old_data(
        self, sample_excel_bytes, sample_parser_config, temp_state_file
    ):
        """Test that second run filters data before saved date."""
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
        result1 = etl.execute(sample_parser_config)
        first_count = len(result1)
        
        # Get saved date
        saved_date = etl.state_manager.get_last_date("TEST_SERIES")
        assert saved_date is not None
        
        # Second run
        result2 = etl.execute(sample_parser_config)
        second_count = len(result2)
        
        # Should filter old data
        assert second_count < first_count
        
        # All dates in second run should be >= saved_date
        for item in result2:
            obs_time = item["obs_time"]
            if isinstance(obs_time, datetime):
                obs_time_naive = obs_time.replace(tzinfo=None) if obs_time.tzinfo else obs_time
                assert obs_time_naive >= saved_date

    def test_incremental_multiple_series_uses_series_dates(
        self, sample_excel_bytes, sample_config_multiple_series, temp_state_file
    ):
        """Test that with multiple series, uses minimum date for filtering."""
        extractor = Mock()
        extractor.extract = Mock(return_value=sample_excel_bytes)
        
        state_manager = StateManagerBuilder().with_file(temp_state_file).build()
        # Manually set different dates to test series-specific filtering
        state_manager.save_dates_from_data([
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 5)},
            {"internal_series_code": "SERIES_2", "obs_time": datetime(2025, 1, 3)},  # Earlier date
        ])
        
        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser(BcraInfomondiaParser())
            .with_normalizer(BcraInfomondiaNormalizer())
            .with_state_manager(state_manager)
            .build()
        )
        
        # Get series last dates before running ETL
        series_last_dates = state_manager.get_series_last_dates(sample_config_multiple_series)
        assert series_last_dates["SERIES_1"] == datetime(2025, 1, 5)
        assert series_last_dates["SERIES_2"] == datetime(2025, 1, 3)
        
        # Run ETL - should filter by series-specific dates
        result = etl.execute(sample_config_multiple_series)
        
        # Verify all dates in result are > their series' last date (filtered correctly)
        for item in result:
            obs_time = item["obs_time"]
            series_code = item["internal_series_code"]
            if isinstance(obs_time, datetime) and series_code in series_last_dates:
                obs_time_naive = obs_time.replace(tzinfo=None) if obs_time.tzinfo else obs_time
                assert obs_time_naive > series_last_dates[series_code]

    def test_incremental_saves_max_date_per_series(
        self, sample_excel_bytes, sample_parser_config, temp_state_file
    ):
        """Test that saves maximum date for each series."""
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
        
        # Run ETL
        result = etl.execute(sample_parser_config)
        
        # Find max date in result
        max_date_in_result = None
        for item in result:
            obs_time = item.get("obs_time")
            if isinstance(obs_time, datetime):
                obs_time_naive = obs_time.replace(tzinfo=None) if obs_time.tzinfo else obs_time
                if max_date_in_result is None or obs_time_naive > max_date_in_result:
                    max_date_in_result = obs_time_naive
        
        # Verify saved date is the maximum
        saved_date = etl.state_manager.get_last_date("TEST_SERIES")
        assert saved_date == max_date_in_result

    def test_incremental_filters_at_boundary_date(
        self, sample_excel_bytes, sample_parser_config, temp_state_file
    ):
        """Test that filtering works correctly at boundary (date == last_date)."""
        extractor = Mock()
        extractor.extract = Mock(return_value=sample_excel_bytes)
        
        state_manager = StateManagerBuilder().with_file(temp_state_file).build()
        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser(BcraInfomondiaParser())
            .with_normalizer(BcraInfomondiaNormalizer())
            .with_state_manager(state_manager)
            .build()
        )
        
        # First run
        etl.execute(sample_parser_config)
        
        # Manually set a specific date
        boundary_date = datetime(2025, 1, 5)
        state_manager.save_dates_from_data([
            {"internal_series_code": "TEST_SERIES", "obs_time": boundary_date},
        ])
        
        # Second run
        result2 = etl.execute(sample_parser_config)
        
        # Dates <= boundary_date should be filtered
        for item in result2:
            obs_time = item["obs_time"]
            if isinstance(obs_time, datetime):
                obs_time_naive = obs_time.replace(tzinfo=None) if obs_time.tzinfo else obs_time
                # Parser filters date <= last_date, so only > should remain
                assert obs_time_naive > boundary_date

    def test_incremental_partial_series_coverage(
        self, sample_excel_bytes, sample_config_multiple_series, temp_state_file
    ):
        """Test incremental update when only some series have saved dates."""
        extractor = Mock()
        extractor.extract = Mock(return_value=sample_excel_bytes)
        
        state_manager = StateManagerBuilder().with_file(temp_state_file).build()
        # Save date for only one series BEFORE running ETL
        state_manager.save_dates_from_data([
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 5)},
            # SERIES_2 has no saved date
        ])
        
        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser(BcraInfomondiaParser())
            .with_normalizer(BcraInfomondiaNormalizer())
            .with_state_manager(state_manager)
            .build()
        )
        
        # Get series last dates before running ETL
        series_last_dates = state_manager.get_series_last_dates(sample_config_multiple_series)
        assert series_last_dates["SERIES_1"] == datetime(2025, 1, 5)
        assert "SERIES_2" not in series_last_dates  # No saved date for SERIES_2
        
        # Run ETL
        result = etl.execute(sample_config_multiple_series)
        
        # SERIES_1 dates should be > 2025-01-05 (filtered correctly)
        # SERIES_2 should process all data (no saved date)
        for item in result:
            obs_time = item.get("obs_time")
            series_code = item.get("internal_series_code")
            if isinstance(obs_time, datetime) and series_code in series_last_dates:
                obs_time_naive = obs_time.replace(tzinfo=None) if obs_time.tzinfo else obs_time
                assert obs_time_naive > series_last_dates[series_code]

    def test_incremental_no_state_manager_processes_all(
        self, sample_excel_bytes, sample_parser_config
    ):
        """Test that without state manager, all data is processed every time."""
        extractor = Mock()
        extractor.extract = Mock(return_value=sample_excel_bytes)
        
        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser(BcraInfomondiaParser())
            .with_normalizer(BcraInfomondiaNormalizer())
            # No state_manager
            .build()
        )
        
        # First run
        result1 = etl.execute(sample_parser_config)
        first_count = len(result1)
        
        # Second run
        result2 = etl.execute(sample_parser_config)
        second_count = len(result2)
        
        # Should process same amount of data
        assert first_count == second_count

