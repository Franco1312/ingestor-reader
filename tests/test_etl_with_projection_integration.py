"""Integration test for ETL pipeline with projections."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from src.application.etl_use_case import ETLUseCase
from src.application.projection_use_case import ProjectionUseCase
from src.infrastructure.projections.projection_manager import ProjectionManager
from tests.builders import DataPointBuilder


class TestETLWithProjectionIntegration:
    """Integration tests for ETL pipeline with projections."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def mock_extractor(self):
        """Create a mock extractor."""
        extractor = Mock()
        extractor.extract.return_value = b"mock_data"
        return extractor

    @pytest.fixture
    def mock_parser(self):
        """Create a mock parser."""
        parser = Mock()
        parser.parse.return_value = [
            DataPointBuilder()
            .with_series_code("TEST_SERIES")
            .with_value(100.0)
            .build()
        ]
        return parser

    @pytest.fixture
    def mock_loader(self, mock_s3_client):
        """Create a mock S3VersionedLoader."""
        loader = Mock()
        loader._s3_client = mock_s3_client
        loader._bucket = "test-bucket"
        loader._dataset_id = "test_dataset"
        return loader

    @pytest.fixture
    def projection_use_case(self, mock_s3_client):
        """Create ProjectionUseCase with mocked ProjectionManager."""
        projection_manager = ProjectionManager(
            bucket="test-bucket", s3_client=mock_s3_client
        )
        return ProjectionUseCase(projection_manager=projection_manager)

    @pytest.fixture
    def etl_use_case(
        self, mock_extractor, mock_parser, mock_loader, projection_use_case
    ):
        """Create ETLUseCase with all dependencies."""
        return ETLUseCase(
            extractor=mock_extractor,
            parser=mock_parser,
            loader=mock_loader,
            projection_use_case=projection_use_case,
        )

    def test_etl_executes_projection_after_load(
        self, etl_use_case, mock_loader, projection_use_case, mock_s3_client
    ):
        """Test that ETL executes projection after successful load."""
        config = {
            "dataset_id": "test_dataset",
            "load": {"bucket": "test-bucket", "aws_region": "us-east-1"},
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

        mock_response = {"Body": MockBody(b"v20240115_143022")}
        mock_s3_client.get_object.return_value = mock_response

        with patch.object(
            projection_use_case, "execute_projection"
        ) as mock_execute_projection:
            mock_loader.load.return_value = None

            etl_use_case.execute(config)

            mock_loader.load.assert_called_once()
            mock_execute_projection.assert_called_once_with(
                "v20240115_143022", "test_dataset"
            )

    def test_etl_skips_projection_if_no_version_found(
        self, etl_use_case, mock_loader, projection_use_case, mock_s3_client
    ):
        """Test that ETL skips projection if no current version is found."""
        from botocore.exceptions import ClientError

        config = {
            "dataset_id": "test_dataset",
            "load": {"bucket": "test-bucket", "aws_region": "us-east-1"},
        }

        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3_client.get_object.side_effect = ClientError(
            error_response, "GetObject"
        )

        with patch.object(
            projection_use_case, "execute_projection"
        ) as mock_execute_projection:
            mock_loader.load.return_value = None

            etl_use_case.execute(config)

            mock_loader.load.assert_called_once()
            mock_execute_projection.assert_not_called()

    def test_etl_skips_projection_if_no_projection_use_case(
        self, mock_extractor, mock_parser, mock_loader
    ):
        """Test that ETL skips projection if no ProjectionUseCase is provided."""
        etl_use_case = ETLUseCase(
            extractor=mock_extractor, parser=mock_parser, loader=mock_loader
        )

        config = {
            "dataset_id": "test_dataset",
            "load": {"bucket": "test-bucket"},
        }

        mock_loader.load.return_value = None

        etl_use_case.execute(config)

        mock_loader.load.assert_called_once()

