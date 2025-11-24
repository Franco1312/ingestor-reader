"""Tests for StagingManager."""

from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from src.infrastructure.projections.staging_manager import StagingManager


class TestStagingManager:
    """Tests for StagingManager class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def staging_manager(self, mock_s3_client):
        """Create StagingManager instance."""
        return StagingManager(bucket="test-bucket", s3_client=mock_s3_client)

    @pytest.fixture
    def staging_manager_parallel(self, mock_s3_client):
        """Create StagingManager instance with parallel workers."""
        return StagingManager(bucket="test-bucket", s3_client=mock_s3_client, copy_workers=3)

    def test_copy_from_version_copies_single_file(self, staging_manager, mock_s3_client):
        """Test that copy_from_version copies a single JSON file from version to staging."""
        dataset_id = "test_dataset"
        version_id = "v20240115_143022"
        json_files = ["SERIES_1/year=2024/month=01/data.json"]

        # Mock S3 copy operation
        mock_s3_client.copy_object = Mock()

        result = staging_manager.copy_from_version(version_id, dataset_id, json_files)

        # Verify copy was called with correct source and destination
        expected_source = f"datasets/{dataset_id}/versions/{version_id}/data/{json_files[0]}"
        expected_dest = f"datasets/{dataset_id}/staging/{json_files[0]}"

        mock_s3_client.copy_object.assert_called_once_with(
            CopySource={"Bucket": "test-bucket", "Key": expected_source},
            Bucket="test-bucket",
            Key=expected_dest,
        )

        # Verify return value
        assert result == [f"datasets/{dataset_id}/staging/{json_files[0]}"]

    def test_copy_from_version_copies_multiple_files(self, staging_manager, mock_s3_client):
        """Test that copy_from_version copies multiple JSON files."""
        dataset_id = "test_dataset"
        version_id = "v20240115_143022"
        json_files = [
            "SERIES_1/year=2024/month=01/data.json",
            "SERIES_2/year=2024/month=02/data.json",
        ]

        mock_s3_client.copy_object = Mock()

        result = staging_manager.copy_from_version(version_id, dataset_id, json_files)

        # Verify all files were copied
        assert mock_s3_client.copy_object.call_count == 2
        assert len(result) == 2

    def test_list_staging_partitions_returns_empty_list_when_no_partitions(
        self, staging_manager, mock_s3_client
    ):
        """Test that list_staging_partitions returns empty list when staging is empty."""
        dataset_id = "test_dataset"

        # Mock empty S3 response
        mock_s3_client.list_objects_v2.return_value = {"Contents": []}

        result = staging_manager.list_staging_partitions(dataset_id)

        assert result == []
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket",
            Prefix=f"datasets/{dataset_id}/staging/",
        )

    def test_list_staging_partitions_extracts_partitions(self, staging_manager, mock_s3_client):
        """Test that list_staging_partitions extracts unique partition paths."""
        dataset_id = "test_dataset"

        # Mock S3 response with multiple files in same partition
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.json"},
                {
                    "Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/part-00001.json"
                },
                {"Key": f"datasets/{dataset_id}/staging/SERIES_2/year=2024/month=02/data.json"},
            ]
        }

        result = staging_manager.list_staging_partitions(dataset_id)

        # Should return unique partition paths
        expected_partitions = [
            "SERIES_1/year=2024/month=01",
            "SERIES_2/year=2024/month=02",
        ]
        assert sorted(result) == sorted(expected_partitions)

    def test_clear_staging_deletes_all_files(self, staging_manager, mock_s3_client):
        """Test that clear_staging deletes all files in staging."""
        dataset_id = "test_dataset"

        # Mock S3 response with files
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.json"},
                {"Key": f"datasets/{dataset_id}/staging/SERIES_2/year=2024/month=02/data.json"},
            ]
        }
        mock_s3_client.delete_object = Mock()

        staging_manager.clear_staging(dataset_id)

        # Verify delete was called for each file
        assert mock_s3_client.delete_object.call_count == 2

    def test_clear_staging_handles_empty_staging(self, staging_manager, mock_s3_client):
        """Test that clear_staging handles empty staging gracefully."""
        dataset_id = "test_dataset"

        # Mock empty S3 response
        mock_s3_client.list_objects_v2.return_value = {"Contents": []}
        mock_s3_client.delete_object = Mock()

        staging_manager.clear_staging(dataset_id)

        # Verify delete was not called
        mock_s3_client.delete_object.assert_not_called()

    def test_list_staging_partitions_handles_response_without_contents_key(
        self, staging_manager, mock_s3_client
    ):
        """Test that list_staging_partitions handles response without Contents key."""
        dataset_id = "test_dataset"

        # Mock S3 response without Contents key
        mock_s3_client.list_objects_v2.return_value = {}

        result = staging_manager.list_staging_partitions(dataset_id)

        assert result == []

    def test_list_staging_partitions_handles_nosuchkey_error(self, staging_manager, mock_s3_client):
        """Test that list_staging_partitions handles NoSuchKey ClientError."""
        dataset_id = "test_dataset"

        # Mock ClientError with NoSuchKey
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.list_objects_v2.side_effect = ClientError(error_response, "ListObjectsV2")

        result = staging_manager.list_staging_partitions(dataset_id)

        assert result == []

    def test_list_staging_partitions_raises_other_client_errors(
        self, staging_manager, mock_s3_client
    ):
        """Test that list_staging_partitions raises non-NoSuchKey ClientErrors."""
        dataset_id = "test_dataset"

        # Mock ClientError with different error code
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_s3_client.list_objects_v2.side_effect = ClientError(error_response, "ListObjectsV2")

        with pytest.raises(ClientError):
            staging_manager.list_staging_partitions(dataset_id)

    def test_clear_staging_handles_nosuchkey_error(self, staging_manager, mock_s3_client):
        """Test that clear_staging handles NoSuchKey ClientError."""
        dataset_id = "test_dataset"

        # Mock ClientError with NoSuchKey
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.list_objects_v2.side_effect = ClientError(error_response, "ListObjectsV2")

        # Should not raise, should return gracefully
        staging_manager.clear_staging(dataset_id)

    def test_list_staging_partitions_filters_keys_without_prefix(
        self, staging_manager, mock_s3_client
    ):
        """Test that list_staging_partitions filters out keys that don't start with prefix."""
        dataset_id = "test_dataset"

        # Mock S3 response with a key that doesn't match the prefix
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.json"},
                {"Key": f"datasets/{dataset_id}/other/SERIES_2/year=2024/month=02/data.json"},
            ]
        }

        result = staging_manager.list_staging_partitions(dataset_id)

        # Should only return partition for key that matches prefix
        assert result == ["SERIES_1/year=2024/month=01"]

    def test_copy_from_version_uses_parallel_workers(
        self, staging_manager_parallel, mock_s3_client
    ):
        """Test that copy_from_version uses parallel workers when configured."""
        from unittest.mock import patch

        dataset_id = "test_dataset"
        version_id = "v20240115_143022"
        json_files = [
            "SERIES_1/year=2024/month=01/data.json",
            "SERIES_2/year=2024/month=02/data.json",
            "SERIES_3/year=2024/month=03/data.json",
        ]

        mock_s3_client.copy_object = Mock()

        with patch(
            "src.infrastructure.projections.staging_manager.ThreadPoolExecutor"
        ) as mock_executor_class:
            mock_executor = Mock()
            mock_executor.__enter__ = Mock(return_value=mock_executor)
            mock_executor.__exit__ = Mock(return_value=None)

            # Mock as_completed to return futures
            from concurrent.futures import Future

            futures = []
            for json_file in json_files:
                future = Future()
                future.set_result(f"datasets/{dataset_id}/staging/{json_file}")
                futures.append(future)

            def submit_side_effect(func, *args):
                future = Future()
                future.set_result(func(*args))
                return future

            mock_executor.submit = Mock(side_effect=submit_side_effect)
            mock_executor_class.return_value = mock_executor

            with patch(
                "src.infrastructure.projections.staging_manager.as_completed",
                return_value=futures,
            ):
                result = staging_manager_parallel.copy_from_version(version_id, dataset_id, json_files)

            # Verify ThreadPoolExecutor was created with correct workers
            mock_executor_class.assert_called_once_with(max_workers=3)
            assert len(result) == 3

    def test_copy_from_version_sequential_with_one_worker(
        self, staging_manager, mock_s3_client
    ):
        """Test that copy_from_version works sequentially with 1 worker."""
        dataset_id = "test_dataset"
        version_id = "v20240115_143022"
        json_files = [
            "SERIES_1/year=2024/month=01/data.json",
            "SERIES_2/year=2024/month=02/data.json",
        ]

        mock_s3_client.copy_object = Mock()

        result = staging_manager.copy_from_version(version_id, dataset_id, json_files)

        # Verify all files were copied (sequential or parallel, result should be same)
        assert mock_s3_client.copy_object.call_count == 2
        assert len(result) == 2
