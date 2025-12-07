"""INDEC EMAE-specific HTTP extractor."""

from typing import Any, Dict

import requests

from src.domain.interfaces import Extractor


class IndecEmaeHttpExtractor(Extractor):
    """Extractor for INDEC EMAE dataset that downloads CSV from fixed URL."""

    DEFAULT_TIMEOUT = 30

    def __init__(self, source_config: Dict[str, Any]) -> None:
        """Initialize extractor.

        Args:
            source_config: Configuration containing:
                - url: Direct URL to the CSV file
                - timeout (optional): HTTP timeout
                - verify_ssl (optional): SSL verification flag
        """
        url = source_config.get("url")
        if not url:
            raise ValueError("source_config must contain 'url' key")

        self._url = url
        self._timeout = source_config.get("timeout", self.DEFAULT_TIMEOUT)
        self._verify_ssl = source_config.get("verify_ssl", False)

    def extract(self) -> bytes:
        """Download the CSV file."""
        response = requests.get(
            self._url,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )
        response.raise_for_status()
        return response.content

