"""Tests for ProjectionManager."""

from unittest.mock import Mock

import pytest

from src.infrastructure.projections.projection_manager import ProjectionManager


class TestProjectionManager:
    """Tests for ProjectionManager class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def projection_manager(self, mock_s3_client):
        """Create ProjectionManager instance."""
        return ProjectionManager(bucket="test-bucket", s3_client=mock_s3_client)

    def test_project_version_executes_full_flow(
        self, projection_manager, mock_s3_client
    ):
        """Test that project_version executes the complete flow."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        manifest = {
            "version_id": version_id,
            "dataset_id": dataset_id,
            "json_files": [
                "SERIES_1/year=2024/month=01/data.json",
                "SERIES_2/year=2024/month=02/data.json",
            ],
        }

        with patch.object(
            projection_manager, "_is_version_already_projected"
        ) as mock_is_projected, patch.object(
            projection_manager, "_load_manifest"
        ) as mock_load_manifest, patch.object(
            projection_manager, "_copy_version_to_staging"
        ) as mock_copy, patch.object(
            projection_manager, "_merge_staging_with_projections"
        ) as mock_merge, patch.object(
            projection_manager, "_atomic_move_to_projections"
        ) as mock_move, patch.object(
            projection_manager, "_record_projected_version"
        ) as mock_record, patch.object(
            projection_manager, "_cleanup_staging"
        ) as mock_cleanup:
            mock_is_projected.return_value = False
            mock_load_manifest.return_value = manifest

            projection_manager.project_version(version_id, dataset_id)

            mock_is_projected.assert_called_once_with(version_id, dataset_id)
            mock_load_manifest.assert_called_once_with(version_id, dataset_id)
            mock_copy.assert_called_once_with(version_id, dataset_id, manifest["json_files"])
            mock_merge.assert_called_once_with(dataset_id)
            mock_move.assert_called_once_with(dataset_id)
            mock_record.assert_called_once_with(version_id, dataset_id)
            mock_cleanup.assert_called_once_with(dataset_id)

    def test_project_version_raises_if_manifest_not_found(
        self, projection_manager, mock_s3_client
    ):
        """Test that project_version raises if manifest is not found."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        with patch.object(
            projection_manager, "_is_version_already_projected"
        ) as mock_is_projected, patch.object(
            projection_manager, "_load_manifest"
        ) as mock_load_manifest:
            mock_is_projected.return_value = False
            mock_load_manifest.return_value = None

            with pytest.raises(ValueError, match="Manifest not found"):
                projection_manager.project_version(version_id, dataset_id)

    def test_copy_version_to_staging_calls_staging_manager(
        self, projection_manager, mock_s3_client
    ):
        """Test that _copy_version_to_staging calls StagingManager correctly."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"
        json_files = ["SERIES_1/year=2024/month=01/data.json"]

        with patch(
            "src.infrastructure.projections.projection_manager.StagingManager"
        ) as mock_staging_manager_class:
            mock_staging_manager = Mock()
            mock_staging_manager.copy_from_version.return_value = [
                "datasets/test_dataset/staging/SERIES_1/year=2024/month=01/data.json"
            ]
            mock_staging_manager_class.return_value = mock_staging_manager

            projection_manager._copy_version_to_staging(version_id, dataset_id, json_files)  # noqa: SLF001

            mock_staging_manager.copy_from_version.assert_called_once_with(
                version_id, dataset_id, json_files
            )

    def test_merge_staging_with_projections_calls_merger(
        self, projection_manager, mock_s3_client
    ):
        """Test that _merge_staging_with_projections calls ProjectionMerger correctly."""
        from unittest.mock import patch

        dataset_id = "test_dataset"

        with patch(
            "src.infrastructure.projections.projection_manager.ProjectionMerger"
        ) as mock_merger_class:
            mock_merger = Mock()
            mock_merger_class.return_value = mock_merger

            projection_manager._merge_staging_with_projections(dataset_id)  # noqa: SLF001

            mock_merger.merge_all_partitions.assert_called_once_with(dataset_id)

    def test_atomic_move_to_projections_calls_mover(
        self, projection_manager, mock_s3_client
    ):
        """Test that _atomic_move_to_projections calls AtomicProjectionMover correctly."""
        from unittest.mock import patch

        dataset_id = "test_dataset"

        with patch(
            "src.infrastructure.projections.projection_manager.AtomicProjectionMover"
        ) as mock_mover_class:
            mock_mover = Mock()
            mock_mover_class.return_value = mock_mover

            projection_manager._atomic_move_to_projections(dataset_id)  # noqa: SLF001

            mock_mover.move_staging_to_projections.assert_called_once_with(dataset_id)

    def test_cleanup_staging_calls_staging_manager(
        self, projection_manager, mock_s3_client
    ):
        """Test that _cleanup_staging calls StagingManager correctly."""
        from unittest.mock import patch

        dataset_id = "test_dataset"

        with patch(
            "src.infrastructure.projections.projection_manager.StagingManager"
        ) as mock_staging_manager_class:
            mock_staging_manager = Mock()
            mock_staging_manager_class.return_value = mock_staging_manager

            projection_manager._cleanup_staging(dataset_id)  # noqa: SLF001

            mock_staging_manager.clear_staging.assert_called_once_with(dataset_id)

    def test_load_manifest_calls_manifest_manager(
        self, projection_manager, mock_s3_client
    ):
        """Test that _load_manifest calls ManifestManager correctly."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"
        expected_manifest = {
            "version_id": version_id,
            "dataset_id": dataset_id,
            "json_files": ["SERIES_1/year=2024/month=01/data.json"],
        }

        with patch(
            "src.infrastructure.projections.projection_manager.ManifestManager"
        ) as mock_manifest_manager_class:
            mock_manifest_manager = Mock()
            mock_manifest_manager.load_manifest.return_value = expected_manifest
            mock_manifest_manager_class.return_value = mock_manifest_manager

            result = projection_manager._load_manifest(version_id, dataset_id)  # noqa: SLF001

            mock_manifest_manager.load_manifest.assert_called_once_with(
                dataset_id, version_id
            )
            assert result == expected_manifest

    def test_project_version_handles_empty_json_files(
        self, projection_manager, mock_s3_client
    ):
        """Test that project_version handles manifest with no JSON files."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        manifest = {
            "version_id": version_id,
            "dataset_id": dataset_id,
            "json_files": [],
        }

        with patch.object(
            projection_manager, "_is_version_already_projected"
        ) as mock_is_projected, patch.object(
            projection_manager, "_load_manifest"
        ) as mock_load_manifest, patch.object(
            projection_manager, "_copy_version_to_staging"
        ) as mock_copy, patch.object(
            projection_manager, "_merge_staging_with_projections"
        ) as mock_merge, patch.object(
            projection_manager, "_atomic_move_to_projections"
        ) as mock_move, patch.object(
            projection_manager, "_record_projected_version"
        ) as mock_record, patch.object(
            projection_manager, "_cleanup_staging"
        ) as mock_cleanup:
            mock_is_projected.return_value = False
            mock_load_manifest.return_value = manifest

            projection_manager.project_version(version_id, dataset_id)

            mock_load_manifest.assert_called_once()
            mock_copy.assert_not_called()
            mock_merge.assert_not_called()
            mock_move.assert_not_called()
            mock_record.assert_not_called()
            mock_cleanup.assert_not_called()

    def test_project_version_skips_if_already_projected(
        self, projection_manager, mock_s3_client
    ):
        """Test that project_version skips if version is already projected."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        with patch.object(
            projection_manager, "_is_version_already_projected"
        ) as mock_is_projected, patch.object(
            projection_manager, "_load_manifest"
        ) as mock_load_manifest, patch.object(
            projection_manager, "_copy_version_to_staging"
        ) as mock_copy, patch.object(
            projection_manager, "_merge_staging_with_projections"
        ) as mock_merge, patch.object(
            projection_manager, "_atomic_move_to_projections"
        ) as mock_move, patch.object(
            projection_manager, "_record_projected_version"
        ) as mock_record, patch.object(
            projection_manager, "_cleanup_staging"
        ) as mock_cleanup:
            mock_is_projected.return_value = True

            projection_manager.project_version(version_id, dataset_id)

            mock_is_projected.assert_called_once_with(version_id, dataset_id)
            mock_load_manifest.assert_not_called()
            mock_copy.assert_not_called()
            mock_merge.assert_not_called()
            mock_move.assert_not_called()
            mock_record.assert_not_called()
            mock_cleanup.assert_not_called()

    def test_is_version_already_projected_calls_manifest_manager(
        self, projection_manager, mock_s3_client
    ):
        """Test that _is_version_already_projected calls ProjectionManifestManager."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        with patch(
            "src.infrastructure.projections.projection_manager.ProjectionManifestManager"
        ) as mock_manifest_manager_class:
            mock_manifest_manager = Mock()
            mock_manifest_manager.is_version_projected.return_value = True
            mock_manifest_manager_class.return_value = mock_manifest_manager

            result = projection_manager._is_version_already_projected(version_id, dataset_id)  # noqa: SLF001

            mock_manifest_manager.is_version_projected.assert_called_once_with(
                dataset_id, version_id
            )
            assert result is True

    def test_record_projected_version_calls_manifest_manager(
        self, projection_manager, mock_s3_client
    ):
        """Test that _record_projected_version calls ProjectionManifestManager."""
        from unittest.mock import patch

        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        with patch(
            "src.infrastructure.projections.projection_manager.ProjectionManifestManager"
        ) as mock_manifest_manager_class:
            mock_manifest_manager = Mock()
            mock_manifest_manager_class.return_value = mock_manifest_manager

            projection_manager._record_projected_version(version_id, dataset_id)  # noqa: SLF001

            mock_manifest_manager.add_projected_version.assert_called_once_with(
                dataset_id, version_id
            )

