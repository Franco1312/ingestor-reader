"""S3-based state manager for incremental updates (template)."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.domain.interfaces import StateManager as StateManagerInterface


class S3StateManager(StateManagerInterface):
    """S3-based state manager for incremental updates.
    
    Template for implementing StateManager with S3 storage.
    """

    def __init__(self, bucket: str, key: str, aws_access_key_id: str = None, aws_secret_access_key: str = None):
        """Initialize S3-based state manager.
        
        Args:
            bucket: S3 bucket name.
            key: S3 object key (path to state file).
            aws_access_key_id: AWS access key ID (optional, can use IAM role).
            aws_secret_access_key: AWS secret access key (optional, can use IAM role).
        """
        self._bucket = bucket
        self._key = key
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

    def get_series_last_dates(self, config: Dict[str, Any]) -> Dict[str, datetime]:
        """Get last processed date for each series in config."""
        raise NotImplementedError

    def save_dates_from_data(self, data: List[Dict[str, Any]]) -> None:
        """Save max date for each series from normalized data."""
        raise NotImplementedError

    def get_last_date(self, series_code: str) -> Optional[datetime]:
        """Get last processed date for a series."""
        raise NotImplementedError

