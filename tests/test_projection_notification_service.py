"""Tests for projection notification service."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.infrastructure.notifications.projection_notification_service import (
    ProjectionNotificationService,
)


class TestProjectionNotificationService:
    """Tests for ProjectionNotificationService."""

    def test_notify_projection_update_sends_correct_post_request(self):
        """Test that notify_projection_update sends the correct POST request."""
        # Arrange
        base_url = "http://localhost:3000"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_http_client = MagicMock()
        mock_http_client.post.return_value = mock_response

        service = ProjectionNotificationService(
            base_url=base_url, timeout=30.0, http_client=mock_http_client
        )

        dataset_id = "test_dataset"
        bucket = "test-bucket"
        version_manifest_path = "datasets/test_dataset/versions/v1/manifest.json"
        projections_path = "datasets/test_dataset/projections/"

        # Act
        service.notify_projection_update(
            dataset_id=dataset_id,
            bucket=bucket,
            version_manifest_path=version_manifest_path,
            projections_path=projections_path,
        )

        # Assert
        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args

        assert call_args.kwargs["url"] == f"{base_url}/api/v1/projections/update"
        assert call_args.kwargs["headers"]["Content-Type"] == "application/json"
        assert call_args.kwargs["timeout"] == 30.0

        event = call_args.kwargs["json"]
        assert event["event"] == "projection_update"
        assert event["dataset_id"] == dataset_id
        assert event["bucket"] == bucket
        assert event["version_manifest_path"] == version_manifest_path
        assert event["projections_path"] == projections_path

    def test_notify_projection_update_handles_http_error_gracefully(self):
        """Test that notify_projection_update handles HTTP errors gracefully."""
        # Arrange
        base_url = "http://localhost:3000"
        mock_http_client = MagicMock()
        mock_http_client.post.side_effect = requests.RequestException("HTTP error")
        service = ProjectionNotificationService(
            base_url=base_url, timeout=30.0, http_client=mock_http_client
        )

        # Act & Assert - should not raise
        service.notify_projection_update(
            dataset_id="test_dataset",
            bucket="test-bucket",
            version_manifest_path="datasets/test_dataset/versions/v1/manifest.json",
            projections_path="datasets/test_dataset/projections/",
        )

        # Should have attempted to post
        mock_http_client.post.assert_called_once()

    def test_notify_projection_update_uses_default_timeout(self):
        """Test that notify_projection_update uses default timeout if not provided."""
        # Arrange
        base_url = "http://localhost:3000"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_http_client = MagicMock()
        mock_http_client.post.return_value = mock_response

        service = ProjectionNotificationService(base_url=base_url, http_client=mock_http_client)

        # Act
        service.notify_projection_update(
            dataset_id="test_dataset",
            bucket="test-bucket",
            version_manifest_path="datasets/test_dataset/versions/v1/manifest.json",
            projections_path="datasets/test_dataset/projections/",
        )

        # Assert
        call_args = mock_http_client.post.call_args
        assert call_args.kwargs["timeout"] == 30.0

    def test_notify_projection_update_strips_trailing_slash_from_base_url(self):
        """Test that notify_projection_update strips trailing slash from base_url."""
        # Arrange
        base_url = "http://localhost:3000/"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_http_client = MagicMock()
        mock_http_client.post.return_value = mock_response

        service = ProjectionNotificationService(
            base_url=base_url, http_client=mock_http_client
        )

        # Act
        service.notify_projection_update(
            dataset_id="test_dataset",
            bucket="test-bucket",
            version_manifest_path="datasets/test_dataset/versions/v1/manifest.json",
            projections_path="datasets/test_dataset/projections/",
        )

        # Assert
        call_args = mock_http_client.post.call_args
        assert call_args.kwargs["url"] == "http://localhost:3000/api/v1/projections/update"

