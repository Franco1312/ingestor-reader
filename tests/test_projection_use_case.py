"""Tests for ProjectionUseCase."""

from unittest.mock import Mock

import pytest

from src.application.projection_use_case import ProjectionUseCase


class TestProjectionUseCase:
    """Tests for ProjectionUseCase class."""

    @pytest.fixture
    def mock_projection_manager(self):
        """Create a mock ProjectionManager."""
        return Mock()

    @pytest.fixture
    def projection_use_case(self, mock_projection_manager):
        """Create ProjectionUseCase instance."""
        return ProjectionUseCase(projection_manager=mock_projection_manager)

    def test_execute_projection_calls_project_version(
        self, projection_use_case, mock_projection_manager
    ):
        """Test that execute_projection calls project_version with correct parameters."""
        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        projection_use_case.execute_projection(version_id, dataset_id)

        mock_projection_manager.project_version.assert_called_once_with(
            version_id, dataset_id
        )

    def test_execute_projection_handles_errors_gracefully(
        self, projection_use_case, mock_projection_manager
    ):
        """Test that execute_projection handles errors gracefully."""
        version_id = "v20240115_143022"
        dataset_id = "test_dataset"

        mock_projection_manager.project_version.side_effect = ValueError("Manifest not found")

        with pytest.raises(ValueError, match="Manifest not found"):
            projection_use_case.execute_projection(version_id, dataset_id)

