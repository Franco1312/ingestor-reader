"""Projection notification service for HTTP POST."""

import json
import logging
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


class ProjectionNotificationService:
    """Service for publishing projection update notifications via HTTP POST."""

    def __init__(
        self, base_url: str, timeout: float = 30.0, http_client: Any = None
    ):
        """Initialize ProjectionNotificationService.

        Args:
            base_url: Base URL for the notification endpoint.
            timeout: Request timeout in seconds (default: 30.0).
            http_client: Requests client (optional, for testing).
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._http_client = http_client or requests

    def notify_projection_update(
        self,
        dataset_id: str,
        bucket: str,
        version_manifest_path: str,
        projections_path: str,
    ) -> None:
        """Publish a projection update notification via HTTP POST.

        Args:
            dataset_id: Dataset identifier.
            bucket: S3 bucket name.
            version_manifest_path: Path to the version manifest in S3.
            projections_path: Base path to projections in S3.
        """
        event = {
            "event": "projection_update",
            "dataset_id": dataset_id,
            "bucket": bucket,
            "version_manifest_path": version_manifest_path,
            "projections_path": projections_path,
        }

        url = f"{self._base_url}/api/v1/projections/update"

        logger.info(
            "Publishing projection update notification: %s to %s",
            event,
            url,
        )

        try:
            response = self._http_client.post(
                url,
                json=event,
                headers={"Content-Type": "application/json"},
                timeout=self._timeout,
            )
            response.raise_for_status()
            logger.info(
                "Successfully published projection update notification for dataset %s",
                dataset_id,
            )
        except Exception as e:  # noqa: BLE001
            logger.error(
                "Failed to publish projection update notification: %s",
                e,
                exc_info=True,
            )
            # Don't raise - notification failure shouldn't break the projection

