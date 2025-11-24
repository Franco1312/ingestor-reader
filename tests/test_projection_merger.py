"""Tests for ProjectionMerger."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import json
import pytest

from src.infrastructure.projections.projection_merger import ProjectionMerger
from tests.builders import DataPointBuilder


class TestProjectionMerger:
    """Tests for ProjectionMerger class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def projection_merger(self, mock_s3_client):
        """Create ProjectionMerger instance."""
        return ProjectionMerger(bucket="test-bucket", s3_client=mock_s3_client)

    @pytest.fixture
    def projection_merger_parallel(self, mock_s3_client):
        """Create ProjectionMerger instance with parallel workers."""
        return ProjectionMerger(bucket="test-bucket", s3_client=mock_s3_client, merge_workers=3)

    @pytest.fixture
    def sample_data_staging(self):
        """Create sample data for staging."""
        return [
            {
                **DataPointBuilder()
                .with_series_code("SERIES_1")
                .with_obs_time(datetime(2024, 1, 15, 12, 0, 0))
                .with_value(100.0)
                .with_unit("unit1")
                .with_frequency("D")
                .build(),
                "collection_date": datetime(2024, 1, 15, 14, 25, 0),
            },
            {
                **DataPointBuilder()
                .with_series_code("SERIES_1")
                .with_obs_time(datetime(2024, 1, 16, 12, 0, 0))
                .with_value(101.0)
                .with_unit("unit1")
                .with_frequency("D")
                .build(),
                "collection_date": datetime(2024, 1, 16, 14, 25, 0),
            },
        ]

    @pytest.fixture
    def sample_data_projections(self):
        """Create sample data for projections (existing data)."""
        return [
            {
                **DataPointBuilder()
                .with_series_code("SERIES_1")
                .with_obs_time(datetime(2024, 1, 14, 12, 0, 0))
                .with_value(99.0)
                .with_unit("unit1")
                .with_frequency("D")
                .build(),
                "collection_date": datetime(2024, 1, 14, 14, 25, 0),
            },
        ]

    def _create_json_file(self, data: list, file_path: Path) -> None:
        """Helper to create a JSON file from data."""
        # Serialize datetime objects to ISO format strings
        json_data = []
        for item in data:
            json_item = {}
            for key, value in item.items():
                if isinstance(value, datetime):
                    json_item[key] = value.isoformat()
                else:
                    json_item[key] = value
            json_data.append(json_item)
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

    def test_merge_partition_with_no_existing_projection(
        self, projection_merger, mock_s3_client, sample_data_staging
    ):
        """Test merge when no existing projection data exists."""
        dataset_id = "test_dataset"
        partition_path = "SERIES_1/year=2024/month=01"

        staging_key = f"datasets/{dataset_id}/staging/{partition_path}/data.json"

        # Mock S3: staging exists, projections don't
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_file = Path(tmpdir) / "staging.json"
            self._create_json_file(sample_data_staging, staging_file)

            def download_file(_Bucket, Key, Filename):  # noqa: N803
                if Key == staging_key:
                    import shutil

                    shutil.copy(staging_file, Filename)

            mock_s3_client.download_file.side_effect = download_file

            def head_object(**kwargs):  # noqa: N803
                Key = kwargs.get("Key")
                if Key == staging_key:
                    return {}
                from botocore.exceptions import ClientError

                error_response = {"Error": {"Code": "404"}}
                raise ClientError(error_response, "HeadObject")

            mock_s3_client.head_object.side_effect = head_object

            # Mock upload
            mock_s3_client.upload_file = Mock()

            projection_merger.merge_partition(dataset_id, partition_path)

            # Verify upload was called (merged data written back to staging)
            assert mock_s3_client.upload_file.called

    def test_merge_partition_removes_duplicates(
        self, projection_merger, mock_s3_client, sample_data_staging, sample_data_projections
    ):
        """Test that merge removes duplicates based on obs_time and internal_series_code."""
        dataset_id = "test_dataset"
        partition_path = "SERIES_1/year=2024/month=01"

        staging_key = f"datasets/{dataset_id}/staging/{partition_path}/data.json"
        projections_key = f"datasets/{dataset_id}/projections/{partition_path}/data.json"

        # Add duplicate: same obs_time and series_code as existing projection
        duplicate_data = [
            {
                **DataPointBuilder()
                .with_series_code("SERIES_1")
                .with_obs_time(datetime(2024, 1, 14, 12, 0, 0))  # Same as in projections
                .with_value(999.0)  # Different value (should be ignored)
                .with_unit("unit1")
                .with_frequency("D")
                .build(),
                "collection_date": datetime(2024, 1, 14, 14, 25, 0),
            },
        ]
        staging_with_duplicate = sample_data_staging + duplicate_data

        with tempfile.TemporaryDirectory() as tmpdir:
            staging_file = Path(tmpdir) / "staging.json"
            projections_file = Path(tmpdir) / "projections.json"
            self._create_json_file(staging_with_duplicate, staging_file)
            self._create_json_file(sample_data_projections, projections_file)

            def download_file(_Bucket, Key, Filename):  # noqa: N803
                import shutil

                if Key == staging_key:
                    shutil.copy(staging_file, Filename)
                elif Key == projections_key:
                    shutil.copy(projections_file, Filename)

            mock_s3_client.download_file.side_effect = download_file
            mock_s3_client.head_object.return_value = {}

            uploaded_files = []
            uploaded_content = []

            def capture_upload(LocalFilename, _Bucket, Key):  # noqa: N803
                uploaded_files.append((LocalFilename, Key))
                # Read file content before it's deleted
                if Path(LocalFilename).exists():
                    with open(LocalFilename, encoding="utf-8") as f:
                        data = json.load(f)
                        uploaded_content.append(data)

            mock_s3_client.upload_file.side_effect = capture_upload

            projection_merger.merge_partition(dataset_id, partition_path)

            # Verify upload was called
            assert mock_s3_client.upload_file.called
            assert len(uploaded_files) == 1
            assert len(uploaded_content) == 1

            # Verify duplicate was removed by reading the captured content
            data = uploaded_content[0]
            # Should have 3 rows: 1 from projections + 2 from staging (duplicate removed)
            assert len(data) == 3

            # Verify the original value (99.0) is kept, not the duplicate (999.0)
            # Find row with obs_time = 2024-01-14
            target_time_str = datetime(2024, 1, 14, 12, 0, 0).isoformat()

            found_value = None
            for item in data:
                obs_time = item.get("obs_time")
                if isinstance(obs_time, str):
                    obs_time_dt = datetime.fromisoformat(obs_time.replace("Z", "+00:00"))
                    if obs_time_dt.replace(tzinfo=None) == datetime(2024, 1, 14, 12, 0, 0) and item.get("internal_series_code") == "SERIES_1":
                        found_value = item.get("value")
                        break

            assert found_value == 99.0  # Original value, not 999.0

    def test_merge_partition_appends_new_data(
        self, projection_merger, mock_s3_client, sample_data_staging, sample_data_projections
    ):
        """Test that merge appends new data from staging to projections."""
        dataset_id = "test_dataset"
        partition_path = "SERIES_1/year=2024/month=01"

        staging_key = f"datasets/{dataset_id}/staging/{partition_path}/data.json"
        projections_key = f"datasets/{dataset_id}/projections/{partition_path}/data.json"

        with tempfile.TemporaryDirectory() as tmpdir:
            staging_file = Path(tmpdir) / "staging.json"
            projections_file = Path(tmpdir) / "projections.json"
            self._create_json_file(sample_data_staging, staging_file)
            self._create_json_file(sample_data_projections, projections_file)

            def download_file(_Bucket, Key, Filename):  # noqa: N803
                import shutil

                if Key == staging_key:
                    shutil.copy(staging_file, Filename)
                elif Key == projections_key:
                    shutil.copy(projections_file, Filename)

            mock_s3_client.download_file.side_effect = download_file
            mock_s3_client.head_object.return_value = {}

            uploaded_content = []

            def capture_upload(LocalFilename, _Bucket, _Key):  # noqa: N803
                # Read file content before it's deleted
                if Path(LocalFilename).exists():
                    with open(LocalFilename, encoding="utf-8") as f:
                        data = json.load(f)
                        uploaded_content.append(data)

            mock_s3_client.upload_file.side_effect = capture_upload

            projection_merger.merge_partition(dataset_id, partition_path)

            # Verify merged data contains both old and new
            assert len(uploaded_content) > 0
            data = uploaded_content[0]
            # Should have 3 rows: 1 from projections + 2 from staging
            assert len(data) == 3

    def test_merge_partition_handles_empty_staging(self, projection_merger, mock_s3_client):
        """Test merge when staging is empty (should keep projections as-is)."""
        dataset_id = "test_dataset"
        partition_path = "SERIES_1/year=2024/month=01"

        staging_key = f"datasets/{dataset_id}/staging/{partition_path}/data.json"
        projections_key = f"datasets/{dataset_id}/projections/{partition_path}/data.json"

        # Mock: staging doesn't exist, projections exist
        def head_object(**kwargs):  # noqa: N803
            Key = kwargs.get("Key")
            if Key == staging_key:
                from botocore.exceptions import ClientError

                error_response = {"Error": {"Code": "404"}}
                raise ClientError(error_response, "HeadObject")
            return {}

        mock_s3_client.head_object.side_effect = head_object

        with tempfile.TemporaryDirectory() as tmpdir:
            projections_file = Path(tmpdir) / "projections.json"
            sample_data = [
                {
                    **DataPointBuilder()
                    .with_series_code("SERIES_1")
                    .with_obs_time(datetime(2024, 1, 14, 12, 0, 0))
                    .with_value(99.0)
                    .with_unit("unit1")
                    .with_frequency("D")
                    .build(),
                    "collection_date": datetime(2024, 1, 14, 14, 25, 0),
                },
            ]
            self._create_json_file(sample_data, projections_file)

            def download_file(_Bucket, Key, Filename):  # noqa: N803
                import shutil

                if Key == projections_key:
                    shutil.copy(projections_file, Filename)

            mock_s3_client.download_file.side_effect = download_file
            mock_s3_client.upload_file = Mock()

            projection_merger.merge_partition(dataset_id, partition_path)

            # Should still upload (projections data written to staging)
            assert mock_s3_client.upload_file.called

    def test_merge_all_partitions_calls_merge_for_each_partition(
        self, projection_merger
    ):
        """Test that merge_all_partitions calls merge_partition for each partition."""

        dataset_id = "test_dataset"

        # Mock StagingManager to return multiple partitions
        with patch(
            "src.infrastructure.projections.projection_merger.StagingManager"
        ) as mock_staging_manager_class:
            mock_staging_manager = Mock()
            mock_staging_manager.list_staging_partitions.return_value = [
                "SERIES_1/year=2024/month=01",
                "SERIES_2/year=2024/month=02",
            ]
            mock_staging_manager_class.return_value = mock_staging_manager

            with patch.object(
                projection_merger, "merge_partition"
            ) as mock_merge_partition:
                projection_merger.merge_all_partitions(dataset_id)

                # Verify merge_partition was called for each partition
                assert mock_merge_partition.call_count == 2
                mock_merge_partition.assert_any_call(
                    dataset_id, "SERIES_1/year=2024/month=01"
                )
                mock_merge_partition.assert_any_call(
                    dataset_id, "SERIES_2/year=2024/month=02"
                )

    def test_s3_object_exists_raises_non_404_errors(
        self, projection_merger, mock_s3_client
    ):
        """Test that _s3_object_exists raises non-404 ClientErrors."""
        from botocore.exceptions import ClientError

        s3_key = "test-key"
        error_response = {"Error": {"Code": "AccessDenied"}}

        mock_s3_client.head_object.side_effect = ClientError(
            error_response, "HeadObject"
        )

        with pytest.raises(ClientError):
            projection_merger._s3_object_exists(s3_key)  # noqa: SLF001

    def test_merge_partition_handles_both_files_missing(
        self, projection_merger, mock_s3_client
    ):
        """Test merge when both staging and projections files don't exist."""
        dataset_id = "test_dataset"
        partition_path = "SERIES_1/year=2024/month=01"

        # Mock: both files don't exist
        def head_object(**_kwargs):  # noqa: N803
            from botocore.exceptions import ClientError

            error_response = {"Error": {"Code": "404"}}
            raise ClientError(error_response, "HeadObject")

        mock_s3_client.head_object.side_effect = head_object
        mock_s3_client.upload_file = Mock()

        # Should not raise, should complete without uploading
        projection_merger.merge_partition(dataset_id, partition_path)

        # Verify upload was not called (no data to merge)
        mock_s3_client.upload_file.assert_not_called()

    def test_merge_all_partitions_uses_parallel_workers(
        self, projection_merger_parallel
    ):
        """Test that merge_all_partitions uses parallel workers when configured."""
        dataset_id = "test_dataset"
        partitions = [
            "SERIES_1/year=2024/month=01",
            "SERIES_2/year=2024/month=02",
            "SERIES_3/year=2024/month=03",
        ]

        # Mock list_staging_partitions
        mock_staging_manager = Mock()
        mock_staging_manager.list_staging_partitions.return_value = partitions

        with patch(
            "src.infrastructure.projections.projection_merger.StagingManager",
            return_value=mock_staging_manager,
        ), patch(
            "src.infrastructure.projections.projection_merger.ThreadPoolExecutor"
        ) as mock_executor_class, patch(
            "src.infrastructure.projections.projection_merger.as_completed"
        ), patch.object(
            projection_merger_parallel, "merge_partition"
        ):
            from concurrent.futures import Future

            # Create futures that will be returned by submit
            futures_list = []
            for _ in partitions:
                future = Future()
                future.set_result(None)
                futures_list.append(future)

            mock_executor = Mock()
            mock_executor.__enter__ = Mock(return_value=mock_executor)
            mock_executor.__exit__ = Mock(return_value=None)
            # Make submit return futures in order
            futures_copy = list(futures_list)
            mock_executor.submit = Mock(
                side_effect=lambda *args: futures_copy.pop(0) if futures_copy else Future()
            )
            mock_executor_class.return_value = mock_executor

            # Mock as_completed to return futures
            with patch(
                "src.infrastructure.projections.projection_merger.as_completed",
                return_value=futures_list,
            ):
                projection_merger_parallel.merge_all_partitions(dataset_id)

            # Verify ThreadPoolExecutor was created with correct workers
            mock_executor_class.assert_called_once_with(max_workers=3)

    def test_merge_all_partitions_sequential_with_one_worker(
        self, projection_merger
    ):
        """Test that merge_all_partitions works with 1 worker (sequential behavior)."""
        dataset_id = "test_dataset"
        partitions = [
            "SERIES_1/year=2024/month=01",
            "SERIES_2/year=2024/month=02",
        ]

        # Mock list_staging_partitions
        mock_staging_manager = Mock()
        mock_staging_manager.list_staging_partitions.return_value = partitions

        with patch(
            "src.infrastructure.projections.projection_merger.StagingManager",
            return_value=mock_staging_manager,
        ), patch.object(
            projection_merger, "merge_partition"
        ) as mock_merge_partition:
            projection_merger.merge_all_partitions(dataset_id)

            # Verify merge_partition was called for each partition
            assert mock_merge_partition.call_count == 2
            mock_merge_partition.assert_any_call(dataset_id, "SERIES_1/year=2024/month=01")
            mock_merge_partition.assert_any_call(dataset_id, "SERIES_2/year=2024/month=02")
