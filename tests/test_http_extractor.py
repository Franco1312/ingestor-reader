"""Tests for HTTP extractor."""

from unittest.mock import Mock, patch

import pytest
import requests

from src.infrastructure.plugins.extractors.http_extractor import HttpExtractor


class TestHttpExtractor:
    """Tests for HttpExtractor class."""

    def test_init_with_url(self):
        """Test initialization with URL."""
        config = {"url": "https://example.com/data.xlsx"}
        extractor = HttpExtractor(config)
        assert extractor._url == "https://example.com/data.xlsx"
        assert extractor._format == "xlsx"
        assert extractor._timeout == 30
        assert extractor._verify_ssl is False

    def test_init_without_url_raises_error(self):
        """Test that initialization without URL raises error."""
        with pytest.raises(ValueError, match="source_config must contain 'url' key"):
            HttpExtractor({})

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        config = {
            "url": "https://example.com/data.xlsx",
            "format": "csv",
            "timeout": 60,
            "verify_ssl": True,
        }
        extractor = HttpExtractor(config)
        assert extractor._format == "csv"
        assert extractor._timeout == 60
        assert extractor._verify_ssl is True

    @patch("src.infrastructure.plugins.extractors.http_extractor.requests.get")
    def test_extract_success(self, mock_get):
        """Test successful extraction."""
        mock_response = Mock()
        mock_response.content = b"test data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        extractor = HttpExtractor({"url": "https://example.com/data.xlsx"})
        result = extractor.extract()
        
        assert result == b"test data"
        mock_get.assert_called_once_with(
            "https://example.com/data.xlsx",
            timeout=30,
            verify=False
        )
        mock_response.raise_for_status.assert_called_once()

    @patch("src.infrastructure.plugins.extractors.http_extractor.requests.get")
    def test_extract_with_custom_timeout_and_ssl(self, mock_get):
        """Test extraction with custom timeout and SSL verification."""
        mock_response = Mock()
        mock_response.content = b"test data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        config = {
            "url": "https://example.com/data.xlsx",
            "timeout": 60,
            "verify_ssl": True,
        }
        extractor = HttpExtractor(config)
        extractor.extract()
        
        mock_get.assert_called_once_with(
            "https://example.com/data.xlsx",
            timeout=60,
            verify=True
        )

    @patch("src.infrastructure.plugins.extractors.http_extractor.requests.get")
    def test_extract_http_error(self, mock_get):
        """Test extraction with HTTP error."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        extractor = HttpExtractor({"url": "https://example.com/data.xlsx"})
        
        with pytest.raises(requests.HTTPError):
            extractor.extract()

    @patch("src.infrastructure.plugins.extractors.http_extractor.requests.get")
    def test_extract_connection_error(self, mock_get):
        """Test extraction with connection error."""
        mock_get.side_effect = requests.ConnectionError("Connection failed")
        
        extractor = HttpExtractor({"url": "https://example.com/data.xlsx"})
        
        with pytest.raises(requests.ConnectionError):
            extractor.extract()

