"""Tests for LockManagerFactory."""

import pytest

from src.infrastructure.lock_managers.lock_manager_factory import LockManagerFactory


class TestLockManagerFactory:
    """Tests for LockManagerFactory class."""

    def test_create_none_returns_none(self):
        """Test that creating with None returns None."""
        result = LockManagerFactory.create(None)
        assert result is None

    def test_create_empty_dict_returns_none(self):
        """Test that creating with empty dict returns None."""
        result = LockManagerFactory.create({})
        assert result is None

    def test_create_dynamodb_with_table_name(self):
        """Test creating DynamoDB lock manager with table_name."""
        config = {"kind": "dynamodb", "table_name": "test_locks"}
        result = LockManagerFactory.create(config)

        assert result is not None
        assert hasattr(result, "acquire")
        assert hasattr(result, "release")

    def test_create_dynamodb_with_region(self):
        """Test creating DynamoDB lock manager with region."""
        config = {
            "kind": "dynamodb",
            "table_name": "test_locks",
            "region_name": "us-west-2",
        }
        result = LockManagerFactory.create(config)

        assert result is not None

    def test_create_dynamodb_with_credentials(self):
        """Test creating DynamoDB lock manager with credentials."""
        config = {
            "kind": "dynamodb",
            "table_name": "test_locks",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
        }
        result = LockManagerFactory.create(config)

        assert result is not None

    def test_create_dynamodb_missing_table_name(self):
        """Test that missing table_name raises ValueError."""
        config = {"kind": "dynamodb"}

        with pytest.raises(ValueError) as exc_info:
            LockManagerFactory.create(config)

        assert "table_name" in str(exc_info.value)

    def test_create_missing_kind(self):
        """Test that missing kind raises ValueError."""
        config = {"table_name": "test_locks"}

        with pytest.raises(ValueError) as exc_info:
            LockManagerFactory.create(config)

        assert "kind" in str(exc_info.value)

    def test_create_unknown_kind(self):
        """Test that unknown kind raises ValueError."""
        config = {"kind": "unknown", "table_name": "test_locks"}

        with pytest.raises(ValueError) as exc_info:
            LockManagerFactory.create(config)

        assert "Unknown lock manager kind" in str(exc_info.value)

    def test_create_all_dynamodb_parameters(self):
        """Test creating DynamoDB lock manager with all parameters."""
        config = {
            "kind": "dynamodb",
            "table_name": "test_locks",
            "region_name": "us-east-1",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
        }
        result = LockManagerFactory.create(config)

        assert result is not None
