"""DynamoDB-based lock manager for distributed locking."""

import time
from typing import Optional

import boto3

from src.domain.interfaces import LockManager as LockManagerInterface


class DynamoDBLockManager(LockManagerInterface):
    """DynamoDB-based lock manager for distributed locking.

    Uses DynamoDB conditional writes for atomic lock acquisition.
    """

    def __init__(
        self,
        table_name: str,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """Initialize DynamoDB lock manager.

        Args:
            table_name: DynamoDB table name for locks.
            region_name: AWS region name (optional, uses default if not provided).
            aws_access_key_id: AWS access key ID (optional, can use IAM role).
            aws_secret_access_key: AWS secret access key (optional, can use IAM role).
        """
        self._table_name = table_name
        self._region_name = region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._client = None

    def _get_client(self):
        """Get or create DynamoDB client."""
        if self._client is None:
            kwargs = {
                k: v
                for k, v in {
                    "region_name": self._region_name,
                    "aws_access_key_id": self._aws_access_key_id,
                    "aws_secret_access_key": self._aws_secret_access_key,
                }.items()
                if v is not None
            }
            self._client = boto3.client("dynamodb", **kwargs)

        return self._client

    def acquire(self, lock_key: str, timeout_seconds: int = 300) -> bool:
        """Acquire a lock for the given key.

        Uses DynamoDB conditional write to atomically acquire lock.
        Lock expires after timeout_seconds.

        Args:
            lock_key: Unique identifier for the lock.
            timeout_seconds: Lock expiration time in seconds (default: 300).

        Returns:
            True if lock was acquired, False if already locked.
        """
        client = self._get_client()
        current_time = int(time.time())
        expiration_time = current_time + timeout_seconds

        try:
            client.put_item(
                TableName=self._table_name,
                Item={
                    "lock_key": {"S": lock_key},
                    "expiration_time": {"N": str(expiration_time)},
                    "acquired_at": {"N": str(current_time)},
                },
                ConditionExpression="attribute_not_exists(lock_key) OR expiration_time < :current_time",
                ExpressionAttributeValues={
                    ":current_time": {"N": str(current_time)},
                },
            )
            return True
        except client.exceptions.ConditionalCheckFailedException:
            return False

    def release(self, lock_key: str) -> None:
        """Release a lock for the given key.

        Args:
            lock_key: Unique identifier for the lock.
        """
        client = self._get_client()

        # Get exception types that may be raised (if available)
        resource_not_found = getattr(client.exceptions, "ResourceNotFoundException", None)
        client_error = getattr(client.exceptions, "ClientError", None)

        try:
            client.delete_item(
                TableName=self._table_name,
                Key={"lock_key": {"S": lock_key}},
            )
        except Exception as e:
            if resource_not_found is not None and isinstance(e, resource_not_found):
                return
            if client_error is not None and isinstance(e, client_error):
                return
            raise
