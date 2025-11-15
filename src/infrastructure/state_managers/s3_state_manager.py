"""S3-based state manager for incremental updates."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from src.domain.interfaces import StateManager as StateManagerInterface
from src.infrastructure.utils.date_utils import to_naive

logger = logging.getLogger(__name__)


class S3StateManager(StateManagerInterface):
    """S3-based state manager for incremental updates."""

    def __init__(
        self,
        bucket: str,
        key: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: str = "us-east-1",
    ):
        """Initialize S3-based state manager.
        
        Args:
            bucket: S3 bucket name.
            key: S3 object key (path to state file).
            aws_access_key_id: AWS access key ID (optional, can use IAM role).
            aws_secret_access_key: AWS secret access key (optional, can use IAM role).
            aws_region: AWS region (default: us-east-1).
        """
        self._bucket = bucket
        self._key = key
        self._aws_region = aws_region
        
        # Create S3 client
        if aws_access_key_id and aws_secret_access_key:
            self._s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region,
            )
        else:
            # Use IAM role credentials
            self._s3_client = boto3.client("s3", region_name=aws_region)

    def get_series_last_dates(self, config: Dict[str, Any]) -> Dict[str, datetime]:
        """Get last processed date for each series in config."""
        parse_config = config.get("parse_config", {})
        series_map = parse_config.get("series_map", [])
        
        series_last_dates = {}
        for series_config in series_map:
            series_code = str(series_config.get("internal_series_code", ""))
            if series_code:
                last_date = self.get_last_date(series_code)
                if last_date:
                    series_last_dates[series_code] = last_date
        
        return series_last_dates

    def save_dates_from_data(self, data: List[Dict[str, Any]]) -> None:
        """Save max date for each series from normalized data."""
        if not data:
            return
        
        series_max_dates: Dict[str, datetime] = {}
        for data_point in data:
            series_code = data_point.get("internal_series_code")
            obs_time = data_point.get("obs_time")
            
            if series_code and isinstance(obs_time, datetime):
                series_code = str(series_code)
                obs_time_naive = to_naive(obs_time)
                if obs_time_naive and (series_code not in series_max_dates or obs_time_naive > series_max_dates[series_code]):
                    series_max_dates[series_code] = obs_time_naive
        
        if series_max_dates:
            state = self._load()
            for series_code, max_date in series_max_dates.items():
                state[series_code] = max_date.isoformat()
            self._save(state)

    def get_last_date(self, series_code: str) -> Optional[datetime]:
        """Get last processed date for a series (always naive)."""
        state = self._load()
        date_str = state.get(series_code)
        if not date_str:
            return None
        
        try:
            date = datetime.fromisoformat(date_str)
            return to_naive(date)
        except (ValueError, TypeError):
            logger.warning("Invalid date format in state for series %s: %s", series_code, date_str)
            return None

    def _load(self) -> Dict[str, str]:
        """Load state from S3."""
        try:
            response = self._s3_client.get_object(Bucket=self._bucket, Key=self._key)
            content = response["Body"].read().decode("utf-8").strip()
            if not content:
                return {}
            return json.loads(content)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info("State file not found in S3, starting fresh")
                return {}
            logger.error("Error loading state from S3: %s", e)
            raise
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("Invalid JSON in state file, starting fresh: %s", e)
            return {}

    def _save(self, state: Dict[str, str]) -> None:
        """Save state to S3."""
        try:
            state_json = json.dumps(state, indent=2)
            self._s3_client.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=state_json.encode("utf-8"),
                ContentType="application/json",
            )
            logger.info("State saved to S3: s3://%s/%s", self._bucket, self._key)
        except ClientError as e:
            logger.error("Error saving state to S3: %s", e)
            raise

