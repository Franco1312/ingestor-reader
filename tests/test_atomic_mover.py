"""Tests for AtomicProjectionMover."""

from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from src.infrastructure.projections.atomic_mover import AtomicProjectionMover


class TestAtomicProjectionMover:
    """Tests for AtomicProjectionMover class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def atomic_mover(self, mock_s3_client):
        """Create AtomicProjectionMover instance."""
        return AtomicProjectionMover(bucket="test-bucket", s3_client=mock_s3_client)

    def test_move_staging_to_projections_copies_all_files(
        self, atomic_mover, mock_s3_client
    ):
        """Test that move_staging_to_projections copies all files from staging to projections."""
        dataset_id = "test_dataset"

        # Mock staging files
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.parquet"},
                {"Key": f"datasets/{dataset_id}/staging/SERIES_2/year=2024/month=02/data.parquet"},
            ]
        }
        mock_s3_client.copy_object = Mock()
        mock_s3_client.delete_object = Mock()

        atomic_mover.move_staging_to_projections(dataset_id)

        # Verify copy was called for each file
        assert mock_s3_client.copy_object.call_count == 2

        # Verify copy source and destination
        copy_calls = mock_s3_client.copy_object.call_args_list
        assert copy_calls[0][1]["CopySource"]["Key"] == f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.parquet"
        assert copy_calls[0][1]["Key"] == f"datasets/{dataset_id}/projections/SERIES_1/year=2024/month=01/data.parquet"

    def test_move_staging_to_projections_deletes_staging_after_successful_copy(
        self, atomic_mover, mock_s3_client
    ):
        """Test that staging files are deleted after successful copy."""
        dataset_id = "test_dataset"

        staging_files = [
            {"Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.parquet"},
        ]

        mock_s3_client.list_objects_v2.return_value = {"Contents": staging_files}
        mock_s3_client.copy_object = Mock()
        mock_s3_client.delete_object = Mock()

        atomic_mover.move_staging_to_projections(dataset_id)

        # Verify delete was called for staging file
        assert mock_s3_client.delete_object.call_count == 1
        mock_s3_client.delete_object.assert_called_with(
            Bucket="test-bucket",
            Key=f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.parquet",
        )

    def test_move_staging_to_projections_handles_empty_staging(
        self, atomic_mover, mock_s3_client
    ):
        """Test that move_staging_to_projections handles empty staging gracefully."""
        dataset_id = "test_dataset"

        mock_s3_client.list_objects_v2.return_value = {"Contents": []}
        mock_s3_client.copy_object = Mock()
        mock_s3_client.delete_object = Mock()

        atomic_mover.move_staging_to_projections(dataset_id)

        # Verify no copy or delete operations
        mock_s3_client.copy_object.assert_not_called()
        mock_s3_client.delete_object.assert_not_called()

    def test_move_staging_to_projections_rolls_back_on_copy_failure(
        self, atomic_mover, mock_s3_client
    ):
        """Test that move_staging_to_projections rolls back on copy failure."""
        dataset_id = "test_dataset"

        staging_files = [
            {"Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.parquet"},
            {"Key": f"datasets/{dataset_id}/staging/SERIES_2/year=2024/month=02/data.parquet"},
        ]

        mock_s3_client.list_objects_v2.return_value = {"Contents": staging_files}

        # First copy succeeds, second fails
        copy_call_count = 0

        def copy_side_effect(**_kwargs):  # noqa: N803
            nonlocal copy_call_count
            copy_call_count += 1
            if copy_call_count == 2:  # Second copy fails
                error_response = {"Error": {"Code": "AccessDenied"}}
                raise ClientError(error_response, "CopyObject")

        mock_s3_client.copy_object.side_effect = copy_side_effect
        mock_s3_client.delete_object = Mock()

        with pytest.raises(ClientError):
            atomic_mover.move_staging_to_projections(dataset_id)

        # Verify rollback: delete was called for the first copied file
        assert mock_s3_client.delete_object.call_count == 1
        # Should delete the projection file that was copied before the failure
        delete_call = mock_s3_client.delete_object.call_args
        assert "projections" in delete_call[1]["Key"]
        assert "staging" not in delete_call[1]["Key"]

    def test_move_staging_to_projections_handles_delete_failure_gracefully(
        self, atomic_mover, mock_s3_client
    ):
        """Test that move_staging_to_projections handles delete failure gracefully."""
        from unittest.mock import patch

        dataset_id = "test_dataset"

        staging_files = [
            {"Key": f"datasets/{dataset_id}/staging/SERIES_1/year=2024/month=01/data.parquet"},
        ]

        mock_s3_client.list_objects_v2.return_value = {"Contents": staging_files}
        mock_s3_client.copy_object = Mock()

        # Make _delete_files raise an exception to test error handling in _delete_staging_after_successful_copy
        with patch.object(atomic_mover, "_delete_files") as mock_delete_files:
            mock_delete_files.side_effect = Exception("Delete failed")

            # Should complete successfully even if delete fails (data is already in projections)
            atomic_mover.move_staging_to_projections(dataset_id)

            # Verify copy was successful
            assert mock_s3_client.copy_object.called
            # Verify delete was attempted
            assert mock_delete_files.called

    def test_list_staging_files_handles_response_without_contents(
        self, atomic_mover, mock_s3_client
    ):
        """Test that _list_s3_files handles response without Contents key."""
        mock_s3_client.list_objects_v2.return_value = {}

        result = atomic_mover._list_s3_files("datasets/test_dataset/staging/")  # noqa: SLF001

        assert result == []

    def test_list_staging_files_handles_nosuchkey_error(
        self, atomic_mover, mock_s3_client
    ):
        """Test that _list_s3_files handles NoSuchKey ClientError."""
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.list_objects_v2.side_effect = ClientError(
            error_response, "ListObjectsV2"
        )

        result = atomic_mover._list_s3_files("datasets/test_dataset/staging/")  # noqa: SLF001

        assert result == []

    def test_rollback_handles_delete_failure(self, atomic_mover, mock_s3_client):
        """Test that rollback handles delete failures gracefully."""
        copied_files = [
            "datasets/test_dataset/projections/SERIES_1/year=2024/month=01/data.parquet",
        ]

        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_s3_client.delete_object.side_effect = ClientError(
            error_response, "DeleteObject"
        )

        atomic_mover._delete_files(copied_files)  # noqa: SLF001

        assert mock_s3_client.delete_object.called

    def test_list_staging_files_raises_other_client_errors(
        self, atomic_mover, mock_s3_client
    ):
        """Test that _list_s3_files raises non-NoSuchKey ClientErrors."""
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_s3_client.list_objects_v2.side_effect = ClientError(
            error_response, "ListObjectsV2"
        )

        with pytest.raises(ClientError):
            atomic_mover._list_s3_files("datasets/test_dataset/staging/")  # noqa: SLF001

