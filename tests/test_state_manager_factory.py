"""Tests for StateManagerFactory."""

import pytest

from src.infrastructure.state_managers.file_state_manager import FileStateManager
from src.infrastructure.state_managers.s3_state_manager import S3StateManager
from src.infrastructure.state_managers.state_manager_factory import StateManagerFactory


class TestStateManagerFactory:
    """Tests for StateManagerFactory class."""

    def test_create_none_returns_none(self):
        """Test that creating with None returns None."""
        result = StateManagerFactory.create(None)
        assert result is None

    def test_create_file_with_full_config(self):
        """Test creating FileStateManager with full config."""
        config = {"kind": "file", "state_file": "custom_state.json"}
        result = StateManagerFactory.create(config)

        assert isinstance(result, FileStateManager)
        # Verify it's a FileStateManager by checking it implements the interface
        assert hasattr(result, "get_series_last_dates")
        assert hasattr(result, "save_dates_from_data")
        assert hasattr(result, "get_last_date")

    def test_create_file_with_default_state_file(self):
        """Test creating FileStateManager with default state_file."""
        config = {"kind": "file"}
        result = StateManagerFactory.create(config)

        assert isinstance(result, FileStateManager)
        assert hasattr(result, "get_series_last_dates")

    def test_create_file_without_kind_defaults_to_file(self):
        """Test that missing kind defaults to 'file'."""
        config = {"state_file": "test.json"}
        result = StateManagerFactory.create(config)

        assert isinstance(result, FileStateManager)
        assert hasattr(result, "get_series_last_dates")

    def test_create_s3_with_full_config(self):
        """Test creating S3StateManager with full config."""
        config = {
            "kind": "s3",
            "bucket": "my-bucket",
            "key": "state/state.json",
            "aws_access_key_id": "test-key",
            "aws_secret_access_key": "test-secret",
        }
        result = StateManagerFactory.create(config)

        assert isinstance(result, S3StateManager)
        assert hasattr(result, "get_series_last_dates")
        assert hasattr(result, "save_dates_from_data")
        assert hasattr(result, "get_last_date")

    def test_create_s3_with_minimal_config(self):
        """Test creating S3StateManager with minimal config."""
        config = {
            "kind": "s3",
            "bucket": "my-bucket",
            "key": "state.json",
        }
        result = StateManagerFactory.create(config)

        assert isinstance(result, S3StateManager)
        assert hasattr(result, "get_series_last_dates")

    def test_create_s3_missing_bucket_raises_error(self):
        """Test that missing bucket raises ValueError."""
        config = {"kind": "s3", "key": "state.json"}

        with pytest.raises(ValueError, match="S3 state manager requires 'bucket' and 'key'"):
            StateManagerFactory.create(config)

    def test_create_s3_missing_key_raises_error(self):
        """Test that missing key raises ValueError."""
        config = {"kind": "s3", "bucket": "my-bucket"}

        with pytest.raises(ValueError, match="S3 state manager requires 'bucket' and 'key'"):
            StateManagerFactory.create(config)

    def test_create_unknown_kind_raises_error(self):
        """Test that unknown kind raises ValueError."""
        config = {"kind": "unknown"}

        with pytest.raises(ValueError, match="Unknown state manager kind: unknown"):
            StateManagerFactory.create(config)

    def test_create_empty_dict_returns_none(self):
        """Test that empty dict returns None (treated as not configured)."""
        result = StateManagerFactory.create({})

        assert result is None
