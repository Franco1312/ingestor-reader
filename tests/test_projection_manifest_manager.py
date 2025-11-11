"""Tests for ProjectionManifestManager."""

import json
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from src.infrastructure.projections.projection_manifest_manager import (
    ProjectionManifestManager,
)


class TestProjectionManifestManager:
    """Tests for ProjectionManifestManager class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def manifest_manager(self, mock_s3_client):
        """Create ProjectionManifestManager instance."""
        return ProjectionManifestManager(bucket="test-bucket", s3_client=mock_s3_client)

    def test_is_version_projected_returns_false_when_manifest_not_exists(
        self, manifest_manager, mock_s3_client
    ):
        """Test that is_version_projected returns False when manifest doesn't exist."""
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.get_object.side_effect = ClientError(error_response, "GetObject")

        result = manifest_manager.is_version_projected("test_dataset", "v20240115_143022")

        assert result is False

    def test_is_version_projected_returns_false_when_version_not_in_list(
        self, manifest_manager, mock_s3_client
    ):
        """Test that is_version_projected returns False when version is not in list."""
        manifest = {
            "projected_versions": ["v20240114_120000", "v20240116_150000"],
            "last_projection_date": "2024-01-16T15:00:00Z",
            "last_projected_version": "v20240116_150000",
        }

        class MockBody:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        mock_s3_client.get_object.return_value = {
            "Body": MockBody(json.dumps(manifest).encode("utf-8"))
        }

        result = manifest_manager.is_version_projected("test_dataset", "v20240115_143022")

        assert result is False

    def test_is_version_projected_returns_true_when_version_in_list(
        self, manifest_manager, mock_s3_client
    ):
        """Test that is_version_projected returns True when version is in list."""
        manifest = {
            "projected_versions": ["v20240114_120000", "v20240115_143022", "v20240116_150000"],
            "last_projection_date": "2024-01-16T15:00:00Z",
            "last_projected_version": "v20240116_150000",
        }

        class MockBody:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        mock_s3_client.get_object.return_value = {
            "Body": MockBody(json.dumps(manifest).encode("utf-8"))
        }

        result = manifest_manager.is_version_projected("test_dataset", "v20240115_143022")

        assert result is True

    def test_add_projected_version_creates_manifest_when_not_exists(
        self, manifest_manager, mock_s3_client
    ):
        """Test that add_projected_version creates manifest when it doesn't exist."""
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.get_object.side_effect = ClientError(error_response, "GetObject")

        manifest_manager.add_projected_version("test_dataset", "v20240115_143022")

        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert call_args[1]["Key"] == "datasets/test_dataset/projections/manifest.json"

        saved_manifest = json.loads(call_args[1]["Body"].decode("utf-8"))
        assert "v20240115_143022" in saved_manifest["projected_versions"]
        assert saved_manifest["last_projected_version"] == "v20240115_143022"
        assert saved_manifest["last_projection_date"] is not None

    def test_add_projected_version_appends_to_existing_manifest(
        self, manifest_manager, mock_s3_client
    ):
        """Test that add_projected_version appends version to existing manifest."""
        existing_manifest = {
            "projected_versions": ["v20240114_120000"],
            "last_projection_date": "2024-01-14T12:00:00Z",
            "last_projected_version": "v20240114_120000",
        }

        class MockBody:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        mock_s3_client.get_object.return_value = {
            "Body": MockBody(json.dumps(existing_manifest).encode("utf-8"))
        }

        manifest_manager.add_projected_version("test_dataset", "v20240115_143022")

        call_args = mock_s3_client.put_object.call_args
        saved_manifest = json.loads(call_args[1]["Body"].decode("utf-8"))
        assert len(saved_manifest["projected_versions"]) == 2
        assert "v20240114_120000" in saved_manifest["projected_versions"]
        assert "v20240115_143022" in saved_manifest["projected_versions"]
        assert saved_manifest["last_projected_version"] == "v20240115_143022"

    def test_add_projected_version_does_not_duplicate_version(
        self, manifest_manager, mock_s3_client
    ):
        """Test that add_projected_version does not add duplicate versions."""
        existing_manifest = {
            "projected_versions": ["v20240115_143022"],
            "last_projection_date": "2024-01-15T14:30:22Z",
            "last_projected_version": "v20240115_143022",
        }

        class MockBody:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        mock_s3_client.get_object.return_value = {
            "Body": MockBody(json.dumps(existing_manifest).encode("utf-8"))
        }

        manifest_manager.add_projected_version("test_dataset", "v20240115_143022")

        call_args = mock_s3_client.put_object.call_args
        saved_manifest = json.loads(call_args[1]["Body"].decode("utf-8"))
        assert len(saved_manifest["projected_versions"]) == 1
        assert saved_manifest["projected_versions"].count("v20240115_143022") == 1

    def test_add_projected_version_updates_last_projection_metadata(
        self, manifest_manager, mock_s3_client
    ):
        """Test that add_projected_version updates last projection date and version."""
        existing_manifest = {
            "projected_versions": ["v20240114_120000"],
            "last_projection_date": "2024-01-14T12:00:00Z",
            "last_projected_version": "v20240114_120000",
        }

        class MockBody:
            def __init__(self, data):
                self._data = data

            def read(self):
                return self._data

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        mock_s3_client.get_object.return_value = {
            "Body": MockBody(json.dumps(existing_manifest).encode("utf-8"))
        }

        manifest_manager.add_projected_version("test_dataset", "v20240115_143022")

        call_args = mock_s3_client.put_object.call_args
        saved_manifest = json.loads(call_args[1]["Body"].decode("utf-8"))
        assert saved_manifest["last_projected_version"] == "v20240115_143022"
        assert saved_manifest["last_projection_date"] is not None
        assert saved_manifest["last_projection_date"] != "2024-01-14T12:00:00Z"

