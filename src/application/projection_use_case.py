"""Projection use case for executing projections."""

import logging

from src.infrastructure.projections.projection_manager import ProjectionManager

logger = logging.getLogger(__name__)


class ProjectionUseCase:
    """High-level use case for executing projections."""

    def __init__(self, projection_manager: ProjectionManager):
        """Initialize ProjectionUseCase.

        Args:
            projection_manager: ProjectionManager instance.
        """
        self._projection_manager = projection_manager

    def execute_projection(self, version_id: str, dataset_id: str) -> None:
        """Execute projection for a version.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.

        Raises:
            ValueError: If manifest is not found or other projection errors occur.
        """
        logger.info(
            "Starting projection for version %s, dataset %s", version_id, dataset_id
        )

        try:
            self._projection_manager.project_version(version_id, dataset_id)
            logger.info(
                "Successfully completed projection for version %s, dataset %s",
                version_id,
                dataset_id,
            )
        except Exception as e:
            logger.error(
                "Failed to project version %s for dataset %s: %s",
                version_id,
                dataset_id,
                e,
            )
            raise

