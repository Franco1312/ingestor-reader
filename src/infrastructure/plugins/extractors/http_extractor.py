"""HTTP extractor plugin."""

from typing import Any, Dict

import requests

from src.domain.interfaces import Extractor


class HttpExtractor(Extractor):
    """Extract data from HTTP source."""

    DEFAULT_TIMEOUT = 30

    def __init__(self, source_config: Dict[str, Any]) -> None:
        """Initialize HTTP extractor with source configuration.
        
        Args:
            source_config: Dictionary containing 'url' (required) and 'format' (optional).
        """
        if "url" not in source_config:
            raise ValueError("source_config must contain 'url' key")
        
        self._url: str = source_config["url"]
        self._format: str = source_config.get("format", "xlsx")
        self._timeout: int = source_config.get("timeout", self.DEFAULT_TIMEOUT)
        self._verify_ssl: bool = source_config.get("verify_ssl", False)

    def extract(self) -> bytes:
        """Extract data from HTTP source.
        
        Returns:
            bytes: Raw content from the HTTP response.
            
        Raises:
            requests.RequestException: If the HTTP request fails.
        """
        response = requests.get(
            self._url, 
            timeout=self._timeout,
            verify=self._verify_ssl
        )
        response.raise_for_status()
        return response.content
