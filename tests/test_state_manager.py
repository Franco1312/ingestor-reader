"""Tests for StateManager."""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from tests.builders import StateManagerBuilder


class TestStateManager:
    """Tests for StateManager class."""

    @pytest.fixture
    def temp_state_file(self):
        """Create a temporary state file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name
        yield temp_path
        Path(temp_path).unlink(missing_ok=True)

    @pytest.fixture
    def state_manager(self, temp_state_file):
        """Create a StateManager instance with temp file."""
        return StateManagerBuilder().with_file(temp_state_file).build()

    def test_get_last_date_nonexistent_series(self, state_manager):
        """Test getting last date for non-existent series."""
        assert state_manager.get_last_date("NONEXISTENT") is None

    def test_save_and_get_last_date(self, state_manager):
        """Test saving and getting last date."""
        series_code = "TEST_SERIES"
        date = datetime(2025, 1, 15, 10, 30, 0)
        
        # Save date
        state_manager.save_dates_from_data([{
            "internal_series_code": series_code,
            "obs_time": date,
        }])
        
        # Get date
        result = state_manager.get_last_date(series_code)
        assert result == date
        assert result.tzinfo is None

    def test_save_aware_datetime_converts_to_naive(self, state_manager):
        """Test that aware datetime is saved as naive."""
        import pytz
        tz = pytz.timezone("America/Argentina/Buenos_Aires")
        aware_date = tz.localize(datetime(2025, 1, 15, 10, 30, 0))
        
        state_manager.save_dates_from_data([{
            "internal_series_code": "TEST_SERIES",
            "obs_time": aware_date,
        }])
        
        result = state_manager.get_last_date("TEST_SERIES")
        assert result.tzinfo is None
        assert result == datetime(2025, 1, 15, 10, 30, 0)

    def test_save_dates_from_data_multiple_series(self, state_manager):
        """Test saving dates for multiple series."""
        data = [
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 10)},
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15)},
            {"internal_series_code": "SERIES_2", "obs_time": datetime(2025, 1, 12)},
        ]
        
        state_manager.save_dates_from_data(data)
        
        assert state_manager.get_last_date("SERIES_1") == datetime(2025, 1, 15)
        assert state_manager.get_last_date("SERIES_2") == datetime(2025, 1, 12)

    def test_save_dates_from_data_empty_list(self, state_manager):
        """Test saving empty data list."""
        state_manager.save_dates_from_data([])
        # Should not raise error

    def test_save_dates_from_data_invalid_data(self, state_manager):
        """Test saving data with invalid entries."""
        data = [
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 10)},
            {"internal_series_code": None, "obs_time": datetime(2025, 1, 15)},
            {"internal_series_code": "SERIES_2", "obs_time": None},
            {"internal_series_code": "SERIES_3", "obs_time": "not a date"},
        ]
        
        state_manager.save_dates_from_data(data)
        
        assert state_manager.get_last_date("SERIES_1") == datetime(2025, 1, 10)
        assert state_manager.get_last_date("SERIES_2") is None
        assert state_manager.get_last_date("SERIES_3") is None

    def test_get_series_last_dates_no_series(self, state_manager):
        """Test getting series last dates with no series in config."""
        config = {"parse_config": {"series_map": []}}
        result = state_manager.get_series_last_dates(config)
        assert result == {}

    def test_get_series_last_dates_no_saved_dates(self, state_manager):
        """Test getting series last dates when no dates are saved."""
        config = {
            "parse_config": {
                "series_map": [
                    {"internal_series_code": "SERIES_1"},
                    {"internal_series_code": "SERIES_2"},
                ]
            }
        }
        result = state_manager.get_series_last_dates(config)
        assert result == {}

    def test_get_series_last_dates_with_saved_dates(self, state_manager):
        """Test getting series last dates from saved dates."""
        # Save dates for multiple series
        state_manager.save_dates_from_data([
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15)},
            {"internal_series_code": "SERIES_2", "obs_time": datetime(2025, 1, 10)},
            {"internal_series_code": "SERIES_3", "obs_time": datetime(2025, 1, 20)},
        ])
        
        config = {
            "parse_config": {
                "series_map": [
                    {"internal_series_code": "SERIES_1"},
                    {"internal_series_code": "SERIES_2"},
                    {"internal_series_code": "SERIES_3"},
                ]
            }
        }
        
        result = state_manager.get_series_last_dates(config)
        assert result == {
            "SERIES_1": datetime(2025, 1, 15),
            "SERIES_2": datetime(2025, 1, 10),
            "SERIES_3": datetime(2025, 1, 20),
        }

    def test_get_series_last_dates_partial_series(self, state_manager):
        """Test getting series last dates when only some series have saved dates."""
        state_manager.save_dates_from_data([
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15)},
        ])
        
        config = {
            "parse_config": {
                "series_map": [
                    {"internal_series_code": "SERIES_1"},
                    {"internal_series_code": "SERIES_2"},  # No saved date
                ]
            }
        }
        
        result = state_manager.get_series_last_dates(config)
        assert result == {"SERIES_1": datetime(2025, 1, 15)}

    def test_persistence(self, temp_state_file):
        """Test that state persists across instances."""
        # First instance
        manager1 = StateManagerBuilder().with_file(temp_state_file).build()
        manager1.save_dates_from_data([
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15)},
        ])
        
        # Second instance
        manager2 = StateManagerBuilder().with_file(temp_state_file).build()
        assert manager2.get_last_date("SERIES_1") == datetime(2025, 1, 15)

    def test_load_handles_invalid_json(self, temp_state_file):
        """Test that loading handles invalid JSON gracefully."""
        # Write invalid JSON to file
        with open(temp_state_file, "w", encoding="utf-8") as f:
            f.write("invalid json content {")
        
        state_manager = StateManagerBuilder().with_file(temp_state_file).build()
        
        # Should return empty dict instead of raising error
        result = state_manager.get_last_date("SERIES_1")
        assert result is None
        
        # Should still be able to save
        state_manager.save_dates_from_data([
            {"internal_series_code": "SERIES_1", "obs_time": datetime(2025, 1, 15)},
        ])
        assert state_manager.get_last_date("SERIES_1") == datetime(2025, 1, 15)

